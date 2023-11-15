import * as readline from 'readline'
import {ActiveStreamsMessage, log_error, log_fatal, log_info, log_warning, MessageType, parse_message, SchemaMessage} from "singer-node"
import {Readable} from "stream"
import ClickhouseConnection from "./ClickhouseConnection"
import {buildMeta, JsonSchemaInspectorContext} from "./jsonSchemaInspector"
import StreamProcessor from "./StreamProcessor"
import {Config} from "./Config"
import {dropStreamTablesQueries} from "./jsonSchemaTranslator"
import {PromisePool} from "@supercharge/promise-pool"

async function processSchemaMessage(msg: SchemaMessage, config: Config, ch: ClickhouseConnection, existingTables: string[]): Promise<StreamProcessor> {

  const meta = buildMeta(new JsonSchemaInspectorContext(
    msg.stream,
    msg.schema,
    msg.keyProperties,
    config.subtable_separator,
    undefined,
    undefined,
    undefined,
    msg.cleaningColumn,
    msg.allKeyProperties,
  ))
  if (config.streamToReplace.includes(meta.prop)) {
    log_info(`[${meta.prop}]: dropping root and children tables`)
    await Promise.all(dropStreamTablesQueries(meta).map((query) => ch.runQuery(query)))
    // refresh list to remove table and children
    existingTables = await ch.listTables()
  }

  return StreamProcessor.createStreamProcessor(ch, meta, config, msg.cleanFirst, existingTables)
}

function tableShouldBeDropped(table: string, activeStreams: string[], subtableSeparator: string, extraActiveTables: string[]): boolean {
  const doesMatchAnActiveStream = activeStreams.concat(extraActiveTables).some((activeTable) => table === activeTable ||
    table.startsWith(activeTable + subtableSeparator))
  const isAlreadyDropped = table.startsWith(ClickhouseConnection.droppedTablePrefix)
  const isArchived = table.startsWith(ClickhouseConnection.archivedTablePrefix)

  return !doesMatchAnActiveStream && !isAlreadyDropped && !isArchived
}

async function processActiveSchemasMessage(msg: ActiveStreamsMessage, config: Config): Promise<void> {
  const ch = new ClickhouseConnection(config)

  const tables = await ch.listTables()

  await Promise.all(
    tables.map(async (table) => {
      if (tableShouldBeDropped(table, msg.streams, config.subtable_separator, config.extra_active_tables)) {
        return ch.renameObsoleteColumn(table)
      }
    }),
  )
}


async function processLine(
  line: string,
  config: Config,
  ch: ClickhouseConnection,
  streamProcessors: Map<string, StreamProcessor>,
  existingTables: string[],
  lineCount: number,
  interrupt: (err: Error) => void): Promise<void> {
  const msg = parse_message(line)

  switch (msg?.type) {
    case MessageType.schema:
      if (streamProcessors.has(msg.stream)) {
        log_warning(`A schema has already been received for stream [${msg.stream}]. Ignoring message`)
        return
      }
      log_info(`[${msg.stream}]: Received schema message.`)
      streamProcessors.set(msg.stream, await processSchemaMessage(msg, config, ch, existingTables))
      break;
    case MessageType.record:
      if (!streamProcessors.has(msg.stream)) {
        throw new Error("Record message received before Schema is defined")
      }
      // undefined has been checked by .has()
      // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
      await streamProcessors.get(msg.stream)!.processRecord(msg.record, lineCount, interrupt)
      break;
    case MessageType.state:
      // On a state message, we insert every batch we are currently building and echo state for tap.
      // If the tap emits state too often, we may need to bufferize state messages
      log_info("Received state message. Commit pending changes...")
      await Promise.all(
        Array.from(streamProcessors.values())
          .map((processor) => processor.commitPendingChanges()),
      )

      // Should be the one and only console log in this package: the tap expects output in stdout to save state
      console.log(JSON.stringify(msg.value))
      break;
    case MessageType.activeStreams:
      // Expected to be read last
      await processActiveSchemasMessage(msg, config)
      break;
    default:
      log_warning(`Message type not handled at line ${lineCount} starting with [${line.substring(0, 50)}]`)
      break;
  }
}

export async function processStream(stream: Readable, config: Config) {
  const ch = new ClickhouseConnection(config)
  let lineCount = 0
  stream.on("error", (err: any) => {
    log_fatal(`${err.message}`)
    throw new Error(`READ ERROR ${err}`)
  })
  let encounteredErr: Error | undefined;

  const rl = readline.createInterface({
    input: stream,
  })
  const abort = (err: Error) => {
    encounteredErr = err
    log_error(err.message)
    log_info("manually closing read stream")
    rl.close()
  }


  const streamProcessors = new Map<string, StreamProcessor>()
  let processLinePromise = Promise.resolve()
  for await (const line of rl) {
    await processLinePromise
    processLinePromise = processLine(line, config, ch, streamProcessors, await ch.listTables(), lineCount++, abort)
  }
  await processLinePromise
  log_info("done reading lines")

  // Ensure exit if an error was encountered. No processing is finalized meaning table state could be
  if (encounteredErr) {
    throw encounteredErr
  }

  await PromisePool
    .withConcurrency(config.batch_size)
    .for(streamProcessors.values())
    .handleError(async (error) => {
      throw error
    })
    .process(async (processor) => await processor.finalizeProcessing())

  rl.close()
}
