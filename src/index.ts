import {log_error, log_info, parse_args, set_level} from "singer-node"
import {processStream} from "processStream"
import {List} from "immutable"

set_level(process.env.LOG_LEVEL || "debug")

const args = parse_args(List(["database", "host", "port", "username", "password"]))
processStream(process.stdin, args.config).then(() => {
  log_info("Stream processing done")
}).catch((err: Error) => {
  log_error(`${err.name}: ${err.message}`)
})
