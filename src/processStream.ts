import * as readline from 'readline'
import {List} from "immutable"
import {log_debug, log_info, MessageContent, MessageType, SchemaMessageContent} from "singer-node"
import {buildMeta, JsonSchemaInspectorContext} from "jsonSchemaInspector"
import {Readable} from "stream"
import {translateCH} from "jsonSchemaTranslator"
import ClickhouseConnection from "ClickhouseConnection"

// a transformer en classe pour set des valeurs par défaut
export interface Config {
  host: string
  port: string | number
  user: string
  password: string
  database: string
  // max_batch_rows?: number   to implement, could be nice
  // max_batch_size?: number
}

function processSchemaMessage(msg: SchemaMessageContent, ch: ClickhouseConnection) {
  const meta = buildMeta(new JsonSchemaInspectorContext(
    msg.stream,
    msg.schema,
    List(msg.key_properties),
    undefined,
  ))

  const queries = translateCH(meta)

  return Promise.all(queries.map((query) => ch.runQuery(query)))
}

async function processLine(line: string, config: Config) {
  const msg: MessageContent = JSON.parse(line)

  const ch = new ClickhouseConnection(config)
  switch (msg.type) {
    case MessageType.schema:
      await processSchemaMessage(msg, ch)
      break;
    default:
      throw new Error("not implemented")
  }
}


export async function processStream(stream: Readable, config: Config) {
  return new Promise<void>((resolve, reject) => {

    stream.on("error", (err: any) => {
      reject(new Error(`READ ERROR ${err}`))
    })

    const rl = readline.createInterface({
      input: stream,
    })
    rl.on('line', async (line) => {
      log_debug("Processing new line")
      await processLine(line, config)
    })

    rl.on('close', () => {
      log_info("Closing stream, done processing")
      resolve()
    })
  })
}
