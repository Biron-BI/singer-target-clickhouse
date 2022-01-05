import {LogLevel} from "singer-node"
import {List} from "immutable"

// Represents content of config.json
export interface IConfig {
  host: string
  port: number
  username: string
  password: string
  database: string
  max_batch_rows?: number
  max_batch_size?: number // in bytes
  logging_level?: string
}

export class Config implements IConfig {
  readonly database: string
  readonly host: string
  readonly password: string
  readonly port: number
  readonly username: string
  readonly max_batch_rows: number = 1000
  readonly max_batch_size: number = 1048576 // 1 Mo
  readonly log_level: LogLevel = LogLevel.INFO

  constructor({
                database,
                host,
                max_batch_rows,
                max_batch_size,
                password,
                port,
                username,
                logging_level,
              }: IConfig, public readonly streamToReplace: List<string> = List()) {
    this.database = database
    this.host = host
    this.max_batch_rows = max_batch_rows ?? this.max_batch_rows
    this.max_batch_size = max_batch_size ?? this.max_batch_size
    // @ts-ignore
    this.log_level = logging_level ? LogLevel[logging_level] : this.log_level
    this.password = password
    this.port = port
    this.username = username
  }
}
