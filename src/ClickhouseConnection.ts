import {ono} from "ono"
import * as retry from "retry"
import {log_debug} from "singer-node"
import {Config} from "processStream"

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

export interface ISqlConnection {
  database: string;
  host: string;
  password: string;
  user: string;
}

export default class ClickhouseConnection {

  constructor(private connInfo: Config) {}

  private connection: any

  async checkConnection(): Promise<void> {
    await this.connectionPool()
  }

  private async connectionPool(): Promise<any> {
    if (!this.connection) {
      this.connection = new ClickHouse({
        host: this.connInfo.host,
        user: this.connInfo.user,
        port: /*this.connInfo.port ||*/ 8123, // todo
        password: this.connInfo.password,
        queryOptions: {
          mutations_sync: 2, // To run data deletion sync https://clickhouse.tech/docs/en/operations/settings/settings/#mutations_sync
          database: this.connInfo.database,
          date_time_input_format: "best_effort", // To handle all date types, https://clickhouse.tech/docs/en/operations/settings/settings/#settings-date_time_input_format
        },
      })

      /* fixme: this would be cleaner but doesn't work
        const queryBuilder = util.promisify(this.connection.query);
                  try {
                      await queryBuilder("SELECT 1");
                      this.connection = connection;
                  } catch (err) {
                      this.connection = undefined;
                      throw ono(err, `fail to init data base connection pool [${this.connInfo.alias}]`);
                  }*/
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

//     // https://github.com/apla/node-clickhouse/blob/HEAD/README.md#inserting-with-stream
//     private async createCHWriteStream(query: string, callback: (err: Error, result: any) => any): Promise<Writable> {
//         const conn = await this.connectionPool();
// //        const asyncQuery = util.promisify(conn.query);
//
//         debugtrace("building stream to query sql %s", query);
//         try {
//             return conn.query(query, {omitFormat: true}, callback);
//         } catch (err) {
//             throw ono("ch stream failed", err);
//         }
//     }
//
//     private pksToTuple(pks: string[], tableDenominator?: string): string {
//         return pks.map((pk => tableDenominator ? `${tableDenominator}.${pk}` : pk)).join(",");
//     }
//
  // https://github.com/apla/node-clickhouse#promise-interface
  // This way of querying is not recommended for large SELECT and INSERTS
  public async runQuery(query: string): Promise<ICHQueryResult> {
    const conn = await this.connectionPool()

    return new Promise((resolve, reject) => {
      const op = retry.operation({
        retries: 3,
        factor: 3,
      })
      op.attempt(async function () {
        try {
          log_debug(`query sql [%s]`)
          const res = await conn.querying(query)
          resolve(res)
        } catch (err) {
          if (op.retry(err)) {
            return
          }
          reject(ono(err, "even after retries, ch query failed", err))
        }
      })
    })
  }
}
