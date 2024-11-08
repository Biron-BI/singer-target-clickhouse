import {log_error, log_info, LogLevel, parse_args, set_level, set_write_stream} from "singer-node"
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
set_level(config.log_level)

const stdin = args.opts.input ? fs.createReadStream(args.opts.input as string) : process.stdin
const stdout = args.opts.output ? fs.createWriteStream(args.opts.output as string) : process.stdout
if (stdout !== process.stdout) [LogLevel.TRACE, LogLevel.DEBUG, LogLevel.INFO].forEach((logLevel) => set_write_stream(logLevel, process.stdout))

processStream(stdin, stdout, config).then(() => {
  log_info("Stream processing done")
}).catch((err) => {
  log_error(`${err}`)
  process.exit(1)
})
