import * as readline from 'readline'
import {List, Map} from "immutable"
import {log_debug, log_fatal, MessageContent, MessageType, SchemaMessageContent} from "singer-node"
import {buildMeta, escapeIdentifier, JsonSchemaInspectorContext} from "jsonSchemaInspector"
import {Readable} from "stream"
import {listTableNames, translateCH} from "jsonSchemaTranslator"
import ClickhouseConnection from "ClickhouseConnection"
import {Config} from "Config"
import StreamProcessor from "StreamProcessor"

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
    await Promise.all(queries.map(ch.runQuery.bind(ch)))
  }
  return new StreamProcessor(meta, config).prepareStreamProcessing()
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
      console.log("todo")
      return streamProcessors
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

  // The one and only let that I'm not sure how to make immutable
  let streamProcessors = Map<string, StreamProcessor>()

  // We read input stream sequentially to correctly handle message ordering and echo State messages at regular intervals
  for await (const line of rl) {
    log_debug(`Processing new line ${line.substring(0, 40)}...`)
    streamProcessors = await processLine(line, config, streamProcessors)
  }

  // Fixme the concurrent version does not work correctly for some reason
  for await (const processor of streamProcessors.toList().toArray()) {
    await processor.doneProcessing()
  }

  // const synced = await Promise.all(streamProcessors.map(async (processor) => processor.doneProcessing()))

  rl.close()
}
