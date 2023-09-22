import {log_error, log_info, parse_args, set_level, set_prefix} from "singer-node"
import {List} from "immutable"
import {processStream} from "./processStream"
import {Config} from "./Config"
import * as fs from "fs"

const args = parse_args(List(["database", "host", "port", "username", "password"]), List([{
  flags: "-u | --update-streams <tables...>",
  description: "Schema whose root and children tables will be dropped / recreated on SCHEMA messages",
},{
  flags: "--input <file>",
  description: "An alternate file to read from, instead of STDIN",
}]))

const config = new Config(args.config, List(args.opts.updateStreams as string[] ?? []))
set_level(config.log_level)
set_prefix("TARGET")

const stdin = args.opts.input ? fs.createReadStream(args.opts.input as string) : process.stdin

processStream(stdin, config).then(() => {
  log_info("Stream processing done")
}).catch((err) => {
  log_error(`${err}`)
  process.exit(1)
})
