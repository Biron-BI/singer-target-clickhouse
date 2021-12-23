import * as readline from 'readline'
import {ReadStream} from "tty"
import {List} from "immutable"
import {MessageContent, parse_args, MessageType, SchemaMessageContent} from "singer-node"
import {JsonSchemaInspectorContext} from "jsonSchemaInspector"

interface Config {
  max_batch_rows?: number
  max_batch_size?: number
}

function processSchemaMessage(msg: SchemaMessageContent) {
  const toto = new JsonSchemaInspectorContext(
    msg.stream,
    msg.schema,
    List(msg.key_properties),
    undefined,
  )
}

function processLine(line: string) {
  const msg: MessageContent = JSON.parse(line)

  switch (msg.type) {
    case MessageType.schema:
      processSchemaMessage(msg)
      break;
    default:
      throw new Error("not implemented")
  }
}

export async function processStream(stream: ReadStream, config?: Config) {
  return new Promise<void>((resolve, reject) => {

    stream.on("error", (err) => {
      reject(new Error(`READ ERROR ${err}`))
    })

    const rl = readline.createInterface({
      input: stream,
    })
    rl.on('line', (line) => {
      processLine(line)
    })

    rl.on('close', () => {
      resolve()
    })
  })
}

(function () {
  var oldLog = console.log
  console.log = function (message) {
    console.warn(message + "??")
    // DO MESSAGE HERE.
    oldLog.apply(console, arguments)
  }
})()


console.log("allo ?")


const args = parse_args(List(["database"]))
processStream(process.stdin, args.config)
