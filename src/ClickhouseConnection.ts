import {ono} from "ono"
import * as retry from "retry"
import {log_debug, log_warning} from "singer-node"
import {List} from "immutable"
import {IConfig} from "Config"
import {Writable} from "stream"

const ClickHouse = require("@apla/clickhouse")

interface ICHQueryResult {
  data: any[];
  meta: any[];
  rows: number;
  "rows_before_limit_at_least": number;
  "statistics": {
    "bytes_read": number
    "elapsed": number,
    "rows_read": number,
  };
  "transferred": number;
}

export default class ClickhouseConnection {

  constructor(private connInfo: IConfig) {}

  private connection: any

  async checkConnection(): Promise<void> {
    await this.connectionPool()
  }

  public getDatabase() {
    return this.connInfo.database
  }

  public async listTables(): Promise<List<string>> {
    return List((await this.runQuery("SHOW TABLES")).data).flatMap((elem) => elem)
  }

  // Produces formatted create table query ready to be compared
  // Example: CREATE TABLE db.table ( `idx` Int32, `id` Int32, `value` String, `_root_ver` UInt64 ) ENGINE = MergeTree ORDER BY (idx, id)
  public async describeCreateTable(table: string): Promise<string> {
    return List<string>((await this.runQuery(`SHOW CREATE TABLE ${table}`))
      .data[0][0]
      .split('\n'))
      .map((line) => line.trim())
      .filter((line) => !line.startsWith("SETTINGS")) // Remove settings line as we don't create table with it
      .join(" ")
  }

  private async connectionPool(): Promise<any> {
    if (!this.connection) {
      this.connection = new ClickHouse({
        host: this.connInfo.host,
        user: this.connInfo.user,
        port: this.connInfo.port,
        password: this.connInfo.password,
        queryOptions: {
          mutations_sync: 2, // To run data deletion sync https://clickhouse.tech/docs/en/operations/settings/settings/#mutations_sync
          database: this.connInfo.database,
          date_time_input_format: "best_effort", // To handle all date types, https://clickhouse.tech/docs/en/operations/settings/settings/#settings-date_time_input_format
        },
      })

      return new Promise((resolve, reject) => {
        this.connection.query("SELECT 1", (err: Error, data: any) => {
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

  // https://github.com/apla/node-clickhouse#promise-interface
  public async runQuery(query: string): Promise<ICHQueryResult> {
    const conn = await this.connectionPool()

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

  // https://github.com/apla/node-clickhouse/blob/HEAD/README.md#inserting-with-stream
  public async createWriteStream(query: string, callback: (err: Error, result: any) => any): Promise<Writable> {
    const conn = await this.connectionPool();

    log_debug(`building stream to query sql ${query}`)

    try {
      return conn.query(query, {omitFormat: true}, callback);
    } catch (err) {
      throw ono("ch stream failed", err);
    }
  }
}
