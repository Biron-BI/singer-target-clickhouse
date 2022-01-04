import {log_error, log_info, parse_args, set_level} from "singer-node"
import {List} from "immutable"
import {processStream} from "./processStream"
import {Config} from "./Config"


const args = parse_args(List(["database", "host", "port", "username", "password"]))

const config = new Config(args.config)
set_level(config.log_level)
processStream(process.stdin, new Config(args.config)).then(() => {
  log_info("Stream processing done")
}).catch((err: Error) => {
  log_error(`${err.name}: ${err.message}`)
})
