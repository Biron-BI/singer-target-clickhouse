import * as readline from 'readline'
import {List} from "immutable"
import {log_debug, log_fatal, log_info, MessageContent, MessageType, SchemaMessageContent} from "singer-node"
import {buildMeta, escapeIdentifier, JsonSchemaInspectorContext} from "jsonSchemaInspector"
import {Readable} from "stream"
import {listTableNames, translateCH} from "jsonSchemaTranslator"
import ClickhouseConnection from "ClickhouseConnection"

// a transformer en classe pour set des valeurs par défaut
export interface Config {
  host: string
  port: number
  user: string
  password: string
  database: string
  // max_batch_rows?: number   to implement, could be nice
  // max_batch_size?: number
}

// Remove magic quotes used to escape queries so we can compare content
function unescape(query?: string) {
  return query?.replace(/`/g, "")
}

async function processSchemaMessage(msg: SchemaMessageContent, ch: ClickhouseConnection) {
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
    return Promise.all(queries.map(ch.runQuery.bind(ch)))
  }
}

async function processLine(line: string, config: Config) {
  const msg: MessageContent = JSON.parse(line)

  const ch = new ClickhouseConnection(config)
  switch (msg.type) {
    case MessageType.schema:
      await processSchemaMessage(msg, ch)
      break
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

  // We read input stream sequentially to correctly handle message ordering and echo State messages at regular intervals
  for await (const line of rl) {
    log_debug(`Processing new line ${line.substring(0, 40)}...`)
    await processLine(line, config)
  }
  log_info("closing stream, done processing")
  rl.close()
}
