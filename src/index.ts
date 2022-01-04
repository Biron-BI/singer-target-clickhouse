import {log_error, log_info, parse_args, set_level} from "singer-node"
import {processStream} from "processStream"
import {List} from "immutable"
import {Config} from "Config"


const args = parse_args(List(["database", "host", "port", "username", "password"]))
// @ts-ignore
set_level(args.config.log_level || "debug")
processStream(process.stdin, new Config(args.config)).then(() => {
  log_info("Stream processing done")
}).catch((err: Error) => {
  log_error(`${err.name}: ${err.message}`)
})
