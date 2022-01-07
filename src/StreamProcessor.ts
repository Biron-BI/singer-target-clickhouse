// abstract class intended to be inherited by each storage destination: mySQL, clickhouse, snowflake, ... based on the connectionAlias

import {ono} from "ono"
import {pipeline, Transform} from "stream"
import {log_debug, log_fatal, log_info} from "singer-node"
import {List, Set} from "immutable"
import ClickhouseConnection from "./ClickhouseConnection"
import {formatRootPKColumn, ISourceMeta, PkMap} from "./jsonSchemaInspector"
import {Config} from "./Config"
import {escapeValue} from "./utils"
import RecordProcessor from "./RecordProcessor"
import * as util from "util"

// To handle overall ingestion
export default class StreamProcessor {

  // Will contain all values used to clear data based on 'cleaningColumn'
  private readonly clickhouse: ClickhouseConnection

  public constructor(
    private readonly meta: ISourceMeta,
    private readonly config: Config,
    private readonly recordProcessor = new RecordProcessor(meta),
    private readonly currentBatchRows: number = 0,
    private readonly currentBatchSize: number = 0,
    private readonly maxVer: number = -1,
    private readonly cleaningValues: Set<string> = Set(),
  ) {
    this.clickhouse = new ClickhouseConnection(config)
    // this.maxVer = maxVer ?? await this.retrieveMaxRecordVersion() // lacking a 'late init' feature
  }

  /**
   * Returns true if main table has any column
   */
  public hasColumns(): boolean {
    return this.meta && (!this.meta.simpleColumnMappings.isEmpty() || !this.meta.pkMappings.isEmpty())
  }

  public clearIngestion() {
    return new StreamProcessor(this.meta, this.config, undefined, undefined, undefined, this.maxVer + 1)
  }

  public async doneProcessing(): Promise<number> {
    try {
      await this.saveNewRecords()
    } catch (err) {
      throw ono(err, "could not save new records")
    }
    log_info(`[${this.meta.prop}]: finalizing processing`)
    await this.finalizeBatchProcessing()

    return this.currentBatchRows
  }

  /**
   * Prepares tables and local variables for JSON stream processing
   */
  public async prepareStreamProcessing(cleanFirst: boolean) {
    if (cleanFirst) {
      await this.clearTables()
    }

    const maxVersion = cleanFirst ? 0 : await this.retrieveMaxRecordVersion()
    log_info(`[${this.meta.prop}]: initial max version is [${maxVersion}]`)

    return new StreamProcessor(this.meta, this.config, undefined, undefined, undefined, maxVersion)
  }

  private async processBatchIfNeeded() {
    if (this.currentBatchRows >= this.config.max_batch_rows ||
      this.currentBatchSize >= this.config.max_batch_size) {
      log_info(`Inserting current batch (rows: ${this.currentBatchRows} / ${this.config.max_batch_rows} -- size: ${this.currentBatchSize} / ${this.config.max_batch_size})`)
      try {
        return (await this.saveNewRecords())
          .clearIngestion()
      } catch (err) {
        log_fatal("could not save records")
        throw err
      }
    }

    return this
  }

  public async processRecord(record: Record<string, any>, messageSize: number): Promise<StreamProcessor> {
    const cleaningValue = this.meta.cleaningColumn && record[this.meta.cleaningColumn]
    if (cleaningValue && !this.cleaningValues.includes(cleaningValue)) {
      await this.deleteCleaningValue(cleaningValue)
    }
    return (await new StreamProcessor(this.meta,
      this.config,
      this.recordProcessor.pushRecord(record, this.maxVer),
      this.currentBatchRows + 1,
      this.currentBatchSize + messageSize,
      this.maxVer + 1, this.cleaningValues.add(cleaningValue))
      .processBatchIfNeeded())
      .addCleaningValue(cleaningValue)
  }

  private addCleaningValue(value: any) {
    return new StreamProcessor(this.meta,
      this.config,
      this.recordProcessor,
      this.currentBatchRows,
      this.currentBatchSize,
      this.maxVer + 1,
      this.cleaningValues.add(value),
    )
  }


  protected async deleteCleaningValue(value: string): Promise<void> {
    log_info(`[${this.meta.prop}]: cleaning column: deleting based on ${value}`)
    if (this.meta.cleaningColumn) {
      const query = `ALTER
                     TABLE
                     ${this.meta.sqlTableName}
                     DELETE
                     WHERE \`${this.meta.cleaningColumn}\` = '${escapeValue(value)}'`
      await this.clickhouse.runQuery(query)
    } else {
      throw new Error("Trying to delete based on cleaning column but it is undefined")
    }
  }

  async clearTables(): Promise<void> {
    const queries = buildTruncateTableQueries(this.meta)
    await Promise.all(queries.map(async (query) => this.clickhouse.runQuery(query)))
  }

  protected async finalizeBatchProcessing(): Promise<void> {
    if (this.isReplacingMergeTree()) {
      log_info(`[${this.meta.prop}]: removing root duplicates`)
      await this.clickhouse.runQuery(`OPTIMIZE TABLE ${this.meta.sqlTableName} FINAL`)

      log_info(`[${this.meta.prop}]: removing children orphans`)
      await Promise.all(this.meta.children.map(
        (child) => this.deleteChildDuplicates(child)))
    }
    log_info(`[${this.meta.prop}]: ensuring PK integrity is maintained`)
    await this.assertPKIntegrity(this.meta)
  }

  private async deleteChildDuplicates(currentNode: ISourceMeta) {
    // currentNode = child node
    // this.meta = root node

    const query = `ALTER
                   TABLE
                   ${currentNode.sqlTableName}
                   DELETE
                   WHERE (${
                           this.meta.pkMappings
                                   .map((pk) => formatRootPKColumn(pk.prop))
                                   .concat(["_root_ver"])
                                   .join(",")
                   }) NOT IN (SELECT ${
                           this.meta.pkMappings
                                   .map((elem) => elem.sqlIdentifier)
                                   .concat(["_ver"])
                                   .join(",")
                   } FROM ${this.meta.sqlTableName})`
    await this.clickhouse.runQuery(query)

    await Promise.all(currentNode.children.map(this.deleteChildDuplicates.bind(this)))
  }

  /**
   Returns true if conditions were met to create a replacing merge tree
   */
  private isReplacingMergeTree(): boolean {
    return this.meta.pkMappings.size > 0
  }

  private printInsertRecordsStats(): this {
    if (this.currentBatchRows) {
      log_info(`[${this.meta.prop}]: inserted ${this.currentBatchRows} records (${this.currentBatchSize} bytes)`)
    }
    return this
  }


  public async saveNewRecords(): Promise<this> {
    const asyncPipeline = util.promisify(pipeline)

    if (this.recordProcessor) {
      log_debug(`fields for ${this.meta.prop}: ${this.recordProcessor.fields.join(",")}`)
      const queries = await this.recordProcessor.buildInsertQuery()
      await Promise.all(queries.map(async (query) => {

        // fixme
        // eslint-disable-next-line no-async-promise-executor
        return new Promise(async (resolve, reject) => {
          const writeStream = await this.clickhouse.createWriteStream(query.baseQuery, (err, result) => {
            if (err) {
              reject(err)
            } else {
              resolve(result)
            }
          })

          writeStream.on("error", (err) => {
            reject(ono(err, "ch write stream error"))
          })

          const transform: Transform = new Transform({
              objectMode: true,
              transform(chunk, encoding, callback) {
                log_debug(`${query.baseQuery}: ${chunk}`)
                this.push(chunk)
                callback()
              },
            },
          )
          try {
            await asyncPipeline(query.stream, transform, writeStream)
          } catch (err) {
            reject(ono(err, "could not process insert data stream"))
          }
        })
      }))
    }
    return this.printInsertRecordsStats()
  }

  protected async retrieveMaxRecordVersion() {
    if (this.isReplacingMergeTree()) {
      const res = await this.clickhouse.runQuery(`SELECT max(_ver)
                                                  FROM ${this.meta.sqlTableName}`)
      return Number(res.data[0][0])
    }
    return -1
  }

  private async assertPKIntegrity(meta: ISourceMeta) {
    await Promise.all(meta.children.map((child) => this.assertPKIntegrity(child)))

    if (meta.pkMappings.size === 0) {
      return
    }
    const pks: string = meta.pkMappings.map((elem: PkMap) => elem.sqlIdentifier).join(",")

    const query = `SELECT ${pks}
                   FROM (SELECT ${pks} FROM ${meta.sqlTableName} ORDER BY ${pks})
                   WHERE (${pks}) = neighbor(
                           (${pks}), -1, (${meta.pkMappings.map(() => "null").join(",")}))
                   LIMIT 1`
    const result = await this.clickhouse.runQuery(query)
    if (result.rows > 0) {
      throw ono("Duplicate key on table %s, data: %j, aborting process", meta.sqlTableName, result.data)
    }
  }
}

export function buildTruncateTableQueries(meta: ISourceMeta): List<string> {
  return List<string>()
    .push(`TRUNCATE TABLE ${meta.sqlTableName}`)
    .concat(meta.children.flatMap(buildTruncateTableQueries))
}
