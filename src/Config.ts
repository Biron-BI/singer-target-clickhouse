import {LogLevel} from "singer-node"

export interface IConfig {
  host: string
  port: number
  user: string
  password: string
  database: string
  max_batch_rows?: number
  max_batch_size?: number // in bytes
  logging_level?: LogLevel
}

export class Config implements IConfig {
  database: string
  host: string
  password: string
  port: number
  user: string
  max_batch_rows: number = 1000
  max_batch_size: number = 1048576 // 1 Mo
  logging_level: LogLevel = LogLevel.INFO

  constructor({
                database,
                host,
                max_batch_rows,
                max_batch_size,
                password,
                port,
                user,
                logging_level,
              }: IConfig) {
    this.database = database
    this.host = host
    this.max_batch_rows = max_batch_rows ?? this.max_batch_rows
    this.max_batch_size = max_batch_size ?? this.max_batch_size
    this.logging_level = logging_level ?? this.logging_level
    this.password = password
    this.port = port
    this.user = user
  }
}
