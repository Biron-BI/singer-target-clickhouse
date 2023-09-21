import * as readline from 'readline'
import {List, Map} from "immutable"
import {ActiveStreamsMessage, log_fatal, log_info, log_warning, MessageType, parse_message, SchemaMessage} from "singer-node"
import {Readable} from "stream"
import ClickhouseConnection from "./ClickhouseConnection"
import {buildMeta, escapeIdentifier, JsonSchemaInspectorContext} from "./jsonSchemaInspector"
import StreamProcessor from "./StreamProcessor"
import {Config} from "./Config"
import {dropStreamTablesQueries, ensureSchemaIsEquivalent, translateCH} from "./jsonSchemaTranslator"

async function processSchemaMessage(msg: SchemaMessage, config: Config): Promise<StreamProcessor> {
  const ch = new ClickhouseConnection(config)

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
  const queries = translateCH(ch.getDatabase(), meta)

  if (config.streamToReplace.includes(meta.prop)) {
    log_info(`[${meta.prop}]: dropping root and children tables`)
    await Promise.all(dropStreamTablesQueries(meta).map((query) => ch.runQuery(query)))
  }

  const rootAlreadyExists = (await ch.listTables()).map((table) => escapeIdentifier(table)).includes(meta.sqlTableName)
  if (rootAlreadyExists) {
    await ensureSchemaIsEquivalent(meta, ch)
  } else {
    log_info(`[${meta.prop}]: creating tables`)
    await Promise.all(queries.map(ch.runQuery.bind(ch)))
  }
  return await StreamProcessor.createStreamProcessor(meta, config, msg.cleanFirst)
}

function tableShouldBeDropped(table: string, activeStreams: List<string>, subtableSeparator: string): boolean {
  const doesMatchAnActiveStream = activeStreams.some((activeTable) => table === activeTable || table.startsWith(activeTable + subtableSeparator))
  const isAlreadyDropped = table.startsWith(ClickhouseConnection.droppedTablePrefix)
  const isArchived = table.startsWith(ClickhouseConnection.archivedTablePrefix)

  return !doesMatchAnActiveStream && !isAlreadyDropped && !isArchived
}

async function processActiveSchemasMessage(msg: ActiveStreamsMessage, config: Config): Promise<void> {
  const ch = new ClickhouseConnection(config)

  const tables = await ch.listTables()

  await Promise.all(
    tables.map(async (table) => {
      if (tableShouldBeDropped(table, msg.streams, config.subtable_separator)) {
        return ch.renameObsoleteColumn(table)
      }
    }),
  )
}


async function processLine(line: string, config: Config, streamProcessors: Map<string, StreamProcessor>, lineCount: number): Promise<Map<string, StreamProcessor>> {
  const msg = parse_message(line)

  switch (msg?.type) {
    case MessageType.schema:
      if (streamProcessors.has(msg.stream)) {
        log_warning(`A schema has already been received for stream [${msg.stream}]. Ignoring message`)
        return streamProcessors
      }
      return streamProcessors.set(msg.stream, await processSchemaMessage(msg, config))
    case MessageType.record:
      if (!streamProcessors.has(msg.stream)) {
        throw new Error("Record message received before Schema is defined")
      }
      // undefined has been checked by .has()
      // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
      await streamProcessors.get(msg.stream)!.processRecord(msg.record, line.length, lineCount)
      return streamProcessors
    case MessageType.state:
      // On a state message, we insert every batch we are currently building and echo state for tap.
      // If the tap emits state too often, we may need to bufferize state messages
      await Promise.all(streamProcessors.map((processor) => processor.commitPendingChanges()).values())
      // Should be the one and only console log in this package: the tap expects output in stdout to save state
      console.log(JSON.stringify(msg.value))
      return streamProcessors
    case MessageType.activeStreams:
      // Expected to be read last
      await processActiveSchemasMessage(msg, config)
      return streamProcessors
    default:
      log_warning(`Message type not handled at line ${lineCount} starting with [${line.substring(0, 50)}]`)
      return streamProcessors
  }
}

type StreamProcessors = Map<string, StreamProcessor>

export async function processStream(stream: Readable, config: Config) {
  let lineCount = 0
  stream.on("error", (err: any) => {
    log_fatal(`${err.message}`)
    throw new Error(`READ ERROR ${err}`)
  })

  const rl = readline.createInterface({
    input: stream,
  })

  const streamProcessors = await awaitReduce(
    rl[Symbol.asyncIterator](),
    (sp: StreamProcessors, line: string) => {
      // log_debug(`processing line starting with ${line.substring(0, 40)} ...`)
      return processLine(line, config, sp, lineCount++)
    },
    Map<string, StreamProcessor>(),
  )

  for await (const processor of streamProcessors.toList().toArray()) {
    await processor.finalizeProcessing()
  }

  rl.close()
}

async function awaitReduce<S, T>(
  awaitIterable: AsyncIterableIterator<S>,
  reducer: (acc: T, line: S) => Promise<T>,
  initial: T,
): Promise<T> {
  let acc = initial
  for await (const item of awaitIterable) {
    acc = await reducer(acc, item)
  }
  return acc
}
