import {log_error, log_info, parse_args, set_log_level, set_output_stream} from "singer-node"
import {processStream} from "./processStream"
import {Config} from "./Config"
import * as fs from "fs"

const args = parse_args(["database", "host", "port", "username", "password"], [{
  flags: "-u | --update-streams <tables...>",
  description: "Schema whose root and children tables will be dropped / recreated on SCHEMA messages",
}, {
  flags: "--input <file>",
  description: "An alternate file to read from, instead of STDIN",
}, {
  flags: "--output <file>",
  description: "An alternate file to write to, instead of STDOUT",
}])

const config = new Config(args.config, args.opts.updateStreams as string[] ?? [])
set_log_level(config.log_level)

const stdin = args.opts.input ? fs.createReadStream(args.opts.input as string) : process.stdin
const stdout = args.opts.output ? fs.createWriteStream(args.opts.output as string) : process.stdout
set_output_stream(stdout)

processStream(stdin, config).then(() => {
  log_info("Stream processing done")
}).catch((err) => {
  log_error(`${err}`)
  process.exit(1)
}).finally(() => {
  if (stdout !== process.stdout) stdout.end()
})
