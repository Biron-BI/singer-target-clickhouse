import {log_error, log_info, parse_args, set_level} from "singer-node"
import {processStream} from "processStream"
import {List} from "immutable"
import {Config} from "Config"

// @ts-ignore
set_level(process.env.LOG_LEVEL || "debug")

const args = parse_args(List(["database", "host", "port", "username", "password"]))
processStream(process.stdin, new Config(args.config)).then(() => {
  log_info("Stream processing done")
}).catch((err: Error) => {
  log_error(`${err.name}: ${err.message}`)
})

function graceFullShutdown() {
  log_info("gracefully shutting down")
  new Config(args.config)
  process.exit(0)
}

process.on('SIGINT', graceFullShutdown);
process.on('SIGTERM', graceFullShutdown);
process.on('SIGQUIT', graceFullShutdown);
