import {LogLevel} from "singer-node"

// Represents content of config.json
export interface IConfig {
  host: string
  port: number
  username: string
  password: string
  database: string
  batch_size?: number
  logging_level?: string
  subtable_separator?: string
  translate_values?: boolean
  insert_stream_timeout_sec?: number
  finalize_concurrency?: number
}

export class Config implements IConfig {
  readonly database: string
  readonly host: string
  readonly password: string
  readonly port: number
  readonly username: string
  readonly log_level: LogLevel = LogLevel.INFO
  readonly subtable_separator: string = "__"
  readonly batch_size: number = 100
  readonly translate_values: boolean = false
  readonly insert_stream_timeout_sec?: number = 180
  readonly finalize_concurrency: number = 3

  constructor({
                database,
                host,
                password,
                port,
                username,
                logging_level,
                subtable_separator,
                batch_size,
                translate_values,
                insert_stream_timeout_sec,
                finalize_concurrency,
              }: IConfig, public readonly streamToReplace: string[] = []) {
    this.database = database
    this.host = host

    // @ts-ignore we expect logging level to be a correct value
    this.log_level = logging_level ? LogLevel[logging_level] : this.log_level
    this.password = password
    this.port = port
    this.username = username
    this.subtable_separator = subtable_separator ?? this.subtable_separator
    this.batch_size = batch_size ?? this.batch_size
    this.translate_values = translate_values ?? this.translate_values
    this.insert_stream_timeout_sec = insert_stream_timeout_sec ?? this.insert_stream_timeout_sec
    this.finalize_concurrency = finalize_concurrency ?? this.finalize_concurrency
  }
}
