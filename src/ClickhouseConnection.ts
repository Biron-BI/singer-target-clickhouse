import {ono} from "ono"
import * as retry from "retry"
import {log_debug, log_error, log_info, log_warning} from "singer-node"
import {Writable} from "stream"
import {IConfig} from "./Config"
import {escapeValue} from "./utils"
import {Either, makeLeft, makeRight} from "./Either"
import TargetConnection, {IQueryResult} from "./TargetConnection"


// eslint-disable-next-line @typescript-eslint/no-var-requires
const ClickHouse = require("@apla/clickhouse")


export interface Column {
  name: string,
  type: string,
  is_in_sorting_key: boolean
}

export default class ClickhouseConnection implements TargetConnection {

  static droppedTablePrefix = "_dropped_"
  static archivedTablePrefix = "_archived_"
  private connection: any

  constructor(private connInfo: IConfig) {
  }

  public getDatabase() {
    return this.connInfo.database
  }

  public async listTables(): Promise<string[]> {
    return (await this.runQuery("SHOW TABLES")).data.map(([tableName]) => tableName)
  }

  public async addColumn(table: string, newCol: Column): Promise<Either<{
    new: Column,
    err: Error
  }, boolean>> {
    try {
      log_info(`[${table}] Adding column ${table}.${newCol.name} ${newCol.type}`)
      await this.runQuery(`ALTER TABLE ${table}
          ADD COLUMN \`${newCol.name}\` ${newCol.type}`, 2)
      return makeRight(true)
    } catch (e) {
      return makeLeft({
        new: newCol,
        err: e,
      })
    }
  }

  public async renameObsoleteColumn(table: string) {
    log_info(`[${table}] Renaming table ${table}`)
    return this.runQuery(`RENAME TABLE \`${table}\` TO \`${ClickhouseConnection.droppedTablePrefix}${table}\``)
  }

  public async removeColumn(table: string, existing: Column): Promise<Either<{
    existing: Column,
    err: Error
  }, boolean>> {
    log_info(`[${table}] Removing column ${table}.${existing.name} `)
    try {
      await this.runQuery(`ALTER TABLE ${table}
          DROP COLUMN \`${existing.name}\``)
      return makeRight(true)
    } catch (e) {
      return makeLeft({
        existing,
        err: e,
      })
    }
  }

  public async updateColumn(table: string, existing: Column, newCol: Column): Promise<Either<{
    existing: Column,
    new: Column,
    err: Error
  }, boolean>> {
    try {
      log_info(`[${table}] Updating column ${table}.${existing.name} from ${existing.type} to ${newCol.type}`)
      await this.runQuery(`ALTER TABLE ${table}
          MODIFY COLUMN \`${newCol.name}\` ${newCol.type}`, 0)
      return makeRight(true)
    } catch (e) {
      // If it fails midway Clickhouse may keep a corrupted intermediary state where table is changed but mutations cannot be applied; so we revert
      try {
        await this.runQuery(`ALTER TABLE ${table}
            MODIFY COLUMN \`${existing.name}\` ${existing.type}`)
      } catch (revertError) {
        log_error(`could not revert update`)
      }
      return makeLeft({
        existing,
        new: newCol,
        err: e,
      })
    }
  }

  public async listColumns(table: string): Promise<Column[]> {
    const res = await this.runQuery(`SELECT name, type, is_in_sorting_key
                                     FROM system.columns
                                     WHERE database = '${escapeValue(this.connInfo.database)}'
                                       AND table = '${escapeValue(table)}'`)
    return res.data.map((row) => ({
      name: row[0],
      type: row[1],
      is_in_sorting_key: Boolean(row[2]),
    }))
  }

  // Expects connection to have been previously initialized, so we can instantly return stream
  public createWriteStream(query: string, callback: (err: any, res: any) => void): Writable {
    log_debug(`building stream to query sql ${query}`)
    if (!this.connection) {
      throw new Error("Clickhouse connection was not initialized")
    }

    return this.connection.query(query, {omitFormat: true}, callback)
      .on('error', () => {
        log_error(`rejecting query ${query}`)
      })
  }

  // https://github.com/apla/node-clickhouse#promise-interface
  public async runQuery(query: string, retries = 2): Promise<IQueryResult> {
    const conn = await this.getConnectionPooled()

    return new Promise((resolve, reject) => {
      const op = retry.operation({
        retries,
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

          // Max amount of time between two batch insertions. Meaning if the tap stop emitting data for longer than this value, it will throw
          // <!> Doesn't seem to have any effect, clickhouse keeps the configured one.
          http_receive_timeout: this.connInfo.insert_stream_timeout_sec,
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
