// abstract class intended to be inherited by each storage destination: mySQL, clickhouse, snowflake, ... based on the connectionAlias

import {ISourceMeta, PkMap} from "./jsonSchemaInspector"
import RecordProcessor from "RecordProcessor"
import {ono} from "ono"
import {pipeline, Transform} from "stream"
import ClickhouseConnection from "ClickhouseConnection"
import {log_debug} from "singer-node"
import {List} from "immutable"

const conf = require("../config")


const {dereferenceExternalSchema} = require("../utils/dereferenceSchema")
const util = require("util")
const debuglog = util.debuglog("biron-connect:customerDatabase")

// export interface ICompiledSourceMeta {
//   alias: string;
//   cleanFirst: boolean;
//   connAlias: string;
//   directInsert: boolean;
//   meta?: ISourceMeta;
//   schema: JSONSchema7;
//   topic: string;
// }

// To handle overall ingestion
export default abstract class StreamProcessor {

  protected cleanFirst: boolean
  protected maxVer: number = 0

  // Will contain all values used to clear data based on 'cleaningColumn'
  protected cleaningValues: any[] = []

  protected constructor(private meta: ISourceMeta, private clickhouse: ClickhouseConnection) {
  }

  protected sqlProcessor?: RecordProcessor
  private currentBatchNb: number = 0
  private nbRecords: number = 0
  private recordsThreshold: number = conf.get("batch:threshold")


  /**
   * Returns true if main table has any column
   */
  public hasColumns(): boolean {
    return this.meta && (!this.meta.simpleColumnMappings.isEmpty() || !this.meta.pkMappings.isEmpty())
  }

  public async doneProcessing(): Promise<number> {
    try {
      await this.saveNewRecords()
    } catch (err) {
      throw ono(err, "could not save new records")
    }
    await this.finalizeBatchProcessing()

    return this.nbRecords
  }

  public getMetaStorageData(): ISourceMeta {
    return this.meta
  }

  /**
   * Prepares tables and local variables for JSON stream processing
   */
  public async prepareStreamProcessing(cleanFirst: boolean) {
    this.cleanFirst = cleanFirst

    if (cleanFirst) {
      await this.clearTables()
    }
    await this.retrieveMaxRecordVersion()
    this.nbRecords = 0
    this.currentBatchNb = 0
    this.cleaningValues = []
  }

  public async processChunk(data: { [k: string]: any }[], chunkIndex: number) {
    this.nbRecords += data.length
    for (const elem of data) {
      // if (this.meta.cleaningColumn) {
      //   const cleaningValue = elem[this.meta.cleaningColumn]
      //   if (!this.cleaningValues.includes(cleaningValue)) {
      //     this.cleaningValues.push(cleaningValue)
      //     await this.deleteCleaningValue(cleaningValue)
      //   }
      // }
      this.pushRecord(elem, chunkIndex, this.maxVer)
      this.currentBatchNb++
      if (this.currentBatchNb >= this.recordsThreshold) {
        try {
          await this.saveNewRecords()
        } catch (err) {
          throw ono(err, "could not save new after threshold limit passed records")
        }
        this.currentBatchNb = 0
      }
    }
  }

  // Make sure every needed data is set before attempting to process stream
  protected abstract assertEverythingSet(): void;


  private pushRecord(data: { [k: string]: any }, chunkIndex: number, maxVer: number): void {
    if (!this.sqlProcessor) {
      this.sqlProcessor = new RecordProcessor(this.meta)
    }
    this.sqlProcessor.pushRecord(data, chunkIndex, maxVer)
  }


  protected async deleteCleaningValue(value: string): Promise<void> {
    log_debug(`Cleaning column: deleting based on ${value}`)
    if (this.meta.cleaningColumn) {
      const query = `ALTER
                     TABLE
                     ${this.meta.sqlTableName}
                     DELETE
                     WHERE \`${this.meta.cleaningColumn}\` = '${value}'`
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
      await this.clickhouse.runQuery(`OPTIMIZE TABLE ${this.meta.sqlTableName} FINAL`)

      await Promise.all(this.meta.children.map(this.deleteChildDuplicates.bind(this)))
    }
    await this.assertPKIntegrity(this.meta)
  }

  // FIXME
  private async deleteChildDuplicates(currentNode: ISourceMeta) {
    const query = `ALTER
                   TABLE
                   ${currentNode.sqlTableName}
                   DELETE
                   WHERE (${
                           this.meta.pkMappings
                                   // .map((elem) => elem.sqlIdentifierAsRootFk)
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


  protected async saveNewRecords(): Promise<void> {
    const asyncPipeline = util.promisify(pipeline)

    if (this.sqlProcessor) {
      const queries = await this.sqlProcessor.buildInsertQuery()
      await Promise.all(queries.map(async (query) => {
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
                log_debug(`inserting: ${chunk}`)
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
      this.sqlProcessor = undefined
    }
  }

  protected async retrieveMaxRecordVersion() {
    if (this.isReplacingMergeTree()) {
      const res = await this.clickhouse.runQuery(`SELECT max(_ver)
                                                  FROM ${this.meta.sqlTableName}`)
      this.maxVer = Number(res.data[0][0])
    }
  }

  // Improvement: concurrency
  private async assertPKIntegrity(meta: ISourceMeta) {
    await Promise.all(meta.children.map(this.assertPKIntegrity.bind(this)))

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
