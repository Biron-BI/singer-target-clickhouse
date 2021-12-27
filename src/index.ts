import {parse_args} from "singer-node"
import {processStream} from "processStream"
import {List} from "immutable"


const args = parse_args(List(["database", "host", "port", "username", "password"]))
processStream(process.stdin, args.config)
