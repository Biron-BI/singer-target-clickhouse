import {log_error, log_info, parse_args, set_level} from "singer-node"
import {List} from "immutable"
import {processStream} from "./processStream"
import {Config} from "./Config"


const args = parse_args(List(["database", "host", "port", "username", "password"]), List([{
  flags: "-u | --update-streams <tables...>",
  description: "Schema whose root and children tables will be dropped / recreated on SCHEMA messages",
}]))

const config = new Config(args.config)
set_level(config.log_level)
processStream(process.stdin, new Config(args.config, List(args.opts.updateStreams ?? []))).then(() => {
  log_info("Stream processing done")
}).catch((err: Error) => {
  log_error(`${err.name}: ${err.message}`)
})
