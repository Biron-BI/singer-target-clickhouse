import {ono} from "ono"
import * as retry from "retry"
import {log_debug, log_error, log_warning} from "singer-node"
import {List} from "immutable"
import {Writable} from "stream"
import {IConfig} from "./Config"
import {escapeValue} from "./utils"

// eslint-disable-next-line @typescript-eslint/no-var-requires
const ClickHouse = require("@apla/clickhouse")

interface ICHQueryResult {
  data: string[][];
  rows: number;
  "rows_before_limit_at_least": number;
  "statistics": {
    "bytes_read": number
    "elapsed": number,
    "rows_read": number,
  };
  "transferred": number;
}

export interface Column {
  name: string,
  type: string,
  is_in_sorting_key: boolean
}

export default class ClickhouseConnection {

  constructor(private connInfo: IConfig) {
  }

  private connection: any

  async checkConnection(): Promise<this> {
    await this.getConnectionPooled()
    return this
  }

  public getDatabase() {
    return this.connInfo.database
  }

  public async listTables(): Promise<List<string>> {
    return List((await this.runQuery("SHOW TABLES")).data).map(([tableName]) => tableName)
  }

  // Produces formatted create table query ready to be compared
  public async describeCreateTable(table: string): Promise<string> {
    return List<string>((await this.runQuery(`SHOW CREATE TABLE ${table}`))
      .data[0][0]
      .split('\n'))
      .map((line) => line.trim())
      .filter((line) => !line.startsWith("SETTINGS")) // Remove settings line as we don't create table with it
      .join(" ")
  }

  public async listColumns(table: string): Promise<List<Column>> {
    const res = await this.runQuery(`SELECT name, type, is_in_sorting_key
                                     FROM system.columns
                                     WHERE database = '${escapeValue(this.connInfo.database)}'
                                       AND table = '${escapeValue(table)}'`)
    return List(res.data.map((row) => ({
      name: row[0],
      type: row[1],
      is_in_sorting_key: Boolean(row[2]),
    })))
  }

  // Expects connection to have been previously initialized, so we can instantly return stream
  public createWriteStream(query: string, resolve: () => void, reject: (err: any) => void): Writable {
    log_debug(`building stream to query sql ${query}`)
    if (!this.connection) {
      throw new Error("Clickhouse connection was not initialized")
    }

    try {
      return this.connection.query(query, {omitFormat: true}, (err: Error) => {
        if (err) {
          log_error(`rejecting query ${query}`)
          reject(err.message)
        } else {
          log_debug(`resolving query ${query}`)
          resolve()
        }
      })
    } catch (err) {
      throw ono("ch stream failed", err)
    }
  }

  // https://github.com/apla/node-clickhouse#promise-interface
  public async runQuery(query: string): Promise<ICHQueryResult> {
    const conn = await this.getConnectionPooled()

    return new Promise((resolve, reject) => {
      const op = retry.operation({
        retries: 2,
        factor: 4,
      })
      op.attempt(async function () {
        try {
          log_debug(`query sql [${query}]`)
          const res = await conn.querying(query)
          resolve(res)
        } catch (err) {
          log_warning(`query sql failed [${query}]: ${err.message}`)
          if (op.retry(err)) {
            return
          }
          reject(ono(err, "even after retries, ch query failed", err))
        }
      })
    })
  }

  private async getConnectionPooled(): Promise<any> {
    if (!this.connection) {
      this.connection = new ClickHouse({
        host: this.connInfo.host,
        user: this.connInfo.username,
        port: this.connInfo.port,
        password: this.connInfo.password,
        queryOptions: {
          mutations_sync: 2, // To run data deletion sync https://clickhouse.tech/docs/en/operations/settings/settings/#mutations_sync
          database: this.connInfo.database,
          date_time_input_format: "best_effort", // To handle all date types, https://clickhouse.tech/docs/en/operations/settings/settings/#settings-date_time_input_format
          insert_null_as_default: 0, // https://clickhouse.com/docs/en/operations/settings/settings/#insert_null_as_default
          input_format_null_as_default: 0,
          input_format_defaults_for_omitted_fields: 0,
        },
      })

      return new Promise((resolve, reject) => {
        this.connection.query("SELECT 1", (err: Error) => {
          if (err) {
            this.connection = undefined
            reject(err)
          } else {
            resolve(this.connection)
          }
        })
      })
    } else {
      return this.connection
    }
  }
}
