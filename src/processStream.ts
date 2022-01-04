import * as readline from 'readline'
import {List, Map} from "immutable"
import {log_fatal, log_info, MessageContent, MessageType, SchemaMessageContent} from "singer-node"
import {Readable} from "stream"
import ClickhouseConnection from "./ClickhouseConnection"
import {buildMeta, escapeIdentifier, JsonSchemaInspectorContext} from "./jsonSchemaInspector"
import StreamProcessor from "./StreamProcessor"
import {Config} from "./Config"
import {listTableNames, translateCH} from "./jsonSchemaTranslator"
import {awaitMapValues} from "./utils"

// Remove magic quotes used to escape queries so we can compare content
function unescape(query?: string) {
  return query?.replace(/`/g, "")
}

async function processSchemaMessage(msg: SchemaMessageContent, config: Config): Promise<StreamProcessor> {
  const ch = new ClickhouseConnection(config)

  const meta = buildMeta(new JsonSchemaInspectorContext(
    msg.stream,
    msg.schema,
    List(msg.key_properties),
    undefined,
    undefined,
    undefined,
    msg.cleaningColumn
  ))
  const queries = translateCH(ch.getDatabase(), meta)

  const rootAlreadyExists = (await ch.listTables()).map(escapeIdentifier).includes(meta.sqlTableName)
  if (rootAlreadyExists) {
    await Promise.all(listTableNames(meta).map(async (tableName, idx) => {
      const currentTable = unescape(await ch.describeCreateTable(tableName))
      const newTable = unescape(queries.get(idx))
      if (!newTable || !currentTable || newTable.localeCompare(currentTable)) {
        throw new Error(`Schema modification detected.
Current:  ${currentTable}
New:      ${newTable}
If you wish to update schemas, run with --update-schemas.`)
      }
    }))
  } else {
    log_info(`Creating tables for schema [${msg.stream}]`)
    await Promise.all(queries.map(ch.runQuery.bind(ch)))
  }
  return new StreamProcessor(meta, config).prepareStreamProcessing(msg.cleanFirst)
}

async function processLine(line: string, config: Config, streamProcessors: Map<string, StreamProcessor>): Promise<Map<string, StreamProcessor>> {
  const msg: MessageContent = JSON.parse(line)

  switch (msg.type) {
    case MessageType.schema:
      return streamProcessors.set(msg.stream, await processSchemaMessage(msg, config))
    case MessageType.record:
      if (!streamProcessors.has(msg.stream)) {
        throw new Error("Record message received before Schema is defined")
      }
      return streamProcessors.set(msg.stream,
        await streamProcessors.get(msg.stream)!!.processRecord(msg.record, line.length)
      )
    case MessageType.state:
      // On a state message, we insert every batch we are currently building and echo state for tap.
      // If the tap emits state too often, we may need to bufferize state messages
      const clearedStreamProcessors = awaitMapValues(streamProcessors.map(async (processor, key) => {
        return (await processor.saveNewRecords()).clearIngestion()
      }))
      // Should be the one and only console log in this package: the tap expects output in stdout to save state
      console.log(JSON.stringify(msg.value))
      return clearedStreamProcessors
    default:
      throw new Error("not implemented")
  }
}


export async function processStream(stream: Readable, config: Config) {

  stream.on("error", (err: any) => {
    log_fatal(err.message)
    throw new Error(`READ ERROR ${err}`)
  })

  const rl = readline.createInterface({
    input: stream,
  })

  const streamProcessors = (await reduce(processLine, Map<string, StreamProcessor>(), rl, config))

  // concurrent version does not work correctly for some reason : await Promise.all(streamProcessors.map(async (processor) => processor.doneProcessing()))
  for await (const processor of streamProcessors.toList().toArray()) {
    await processor.doneProcessing()
  }

  rl.close()
}

async function reduce<T>(func: (line: string, config: Config, streamProcessors: T) => Promise<T>, item: T, rl: readline.Interface, config: Config): Promise<T> {
  let o = item

  for await (let line of rl)
    o = await func(line, config, o)

  return o
}
