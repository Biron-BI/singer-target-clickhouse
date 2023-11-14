import {ono} from "ono"
import {log_info, log_warning} from "singer-node"
import ClickhouseConnection from "./ClickhouseConnection"
import {escapeIdentifier, formatRootPKColumn, ISourceMeta, PkMap} from "./jsonSchemaInspector"
import {Config} from "./Config"
import {escapeValue} from "./utils"
import RecordProcessor from "./RecordProcessor"

// expects root meta as param
const metaRepresentsReplacingMergeTree = (meta: ISourceMeta) => meta.pkMappings.length > 0

// To handle overall ingestion
export default class StreamProcessor {
  private constructor(
    private readonly clickhouse: ClickhouseConnection,
    private readonly meta: ISourceMeta,
    private readonly startedClean: boolean,
    config: Config,
    private maxVer: number,
    private recordProcessor = new RecordProcessor(meta, clickhouse, {
      batchSize: config.batch_size,
      translateValues: config.translate_values,
      autoEndTimeoutMs: (config.insert_stream_timeout_sec - 5) * 1000,
    }),
    private noPendingRows = 0,
    // private currentBatchSize = 0,
    private readonly cleaningValues: string[] = [], // All values used to clear data based on 'cleaningColumn',
  ) {
  }

  static async createStreamProcessor(ch: ClickhouseConnection, meta: ISourceMeta, config: Config, cleanFirst: boolean) {
    const maxVersion = (cleanFirst || !metaRepresentsReplacingMergeTree(meta))
      ? 0
      : Number((await ch.runQuery(`SELECT max(_ver)
                                   FROM ${meta.sqlTableName}`)).data[0][0])

    log_info(`[${meta.prop}]: initial max version is [${maxVersion}]`)

    const streamProcessor = new StreamProcessor(ch, meta, cleanFirst, config, maxVersion)
    if (cleanFirst) {
      await streamProcessor.clearTables()
    }
    return streamProcessor
  }

  async clearTables(): Promise<void> {
    const queries = buildTruncateTableQueries(this.meta)
    await Promise.all(queries.map((query) => this.clickhouse.runQuery(query)))
  }

  public async processRecord(record: Record<string, any>, messageCount: number, abort: (err: Error) => void): Promise<void> {
    if (!this.startedClean) {
      const cleaningValue = this.meta.cleaningColumn && record[this.meta.cleaningColumn]
      if (cleaningValue && !this.cleaningValues.includes(cleaningValue)) {
        await this.deleteCleaningValue(cleaningValue)
        this.cleaningValues.push(cleaningValue)
      }
    }
    this.recordProcessor.pushRecord(record, abort, this.maxVer, undefined, undefined, undefined, messageCount)
    this.maxVer++
    this.noPendingRows++
  }

  public async commitPendingChanges(): Promise<void> {
    if (this.noPendingRows > 0) {
      log_info(`[${this.meta.prop}]: ending batch ingestion for ${this.noPendingRows} rows`)
      await this.recordProcessor.endIngestion()
      this.noPendingRows = 0
      this.maxVer++
    }
  }

  public async finalizeProcessing(): Promise<void> {
    try {
      await this.commitPendingChanges()
    } catch (err) {
      throw ono(err, "could not save new records")
    }
    log_info(`[${this.meta.prop}]: finalizing processing`)

    if (!this.startedClean) {
      if (this.isReplacingMergeTree()) {
        log_info(`[${this.meta.prop}]: removing root duplicates`)
        await this.clickhouse.runQuery(`OPTIMIZE TABLE ${this.meta.sqlTableName} FINAL`)

        if (this.recordProcessor.hasChildren) {
          log_info(`[${this.meta.prop}]: removing children orphans`)
          await Promise.all(this.meta.children.map((child) => this.deleteChildDuplicates(child)))
        }
      }

      log_info(`[${this.meta.prop}]: ensuring PK integrity is maintained`)
      await this.assertPKIntegrity(this.meta)
    }
  }

  protected async deleteCleaningValue(value: string): Promise<void> {
    if (!this.meta.cleaningColumn) {
      log_warning(`[${this.meta.prop}]: unexpected request to clean values: cleaning column undefined`)
      return
    }

    const cleaningColumnMeta = this.meta.simpleColumnMappings.find((column) => column.prop === this.meta.cleaningColumn)
    if (!cleaningColumnMeta) {
      throw new Error(`[${this.meta.prop}] could not resolve cleaning column meta (looking for ${this.meta.cleaningColumn})`)
    }
    if (!cleaningColumnMeta.valueTranslator) {
      throw new Error(`[${this.meta.prop}] could not be used as cleaning column as it do not have a translator`)
    }
    const resolvedValue = cleaningColumnMeta.valueTranslator(value)
    log_info(`[${this.meta.prop}]: cleaning column: deleting based on ${resolvedValue}`)

    const query = `ALTER
                   TABLE
                   ${this.meta.sqlTableName}
                   DELETE
                   WHERE \`${this.meta.cleaningColumn}\` = '${escapeValue(value)}'`
    await this.clickhouse.runQuery(query)
  }

  private async deleteChildDuplicates(currentNode: ISourceMeta) {
    // this.meta always refer to root node

    const query = `ALTER
                   TABLE
                   ${currentNode.sqlTableName}
                   DELETE
                   WHERE (${
                           this.meta.pkMappings
                                   .map((pk) => escapeIdentifier(formatRootPKColumn(pk.prop)))
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

  // returns true if conditions were met to create a replacing merge tree
  private isReplacingMergeTree(): boolean {
    return metaRepresentsReplacingMergeTree(this.meta)
  }

  private async assertPKIntegrity(meta: ISourceMeta) {
    await Promise.all(meta.children.map((child) => this.assertPKIntegrity(child)))

    if (meta.pkMappings.length === 0) {
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

const buildTruncateTableQueries = (meta: ISourceMeta): string[] => [
  `TRUNCATE TABLE ${meta.sqlTableName}`,
  ...meta.children.flatMap(buildTruncateTableQueries),
]
