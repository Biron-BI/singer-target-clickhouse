import {ono} from "ono"
import {log_fatal, log_info, log_warning} from "singer-node"
import {List, Set} from "immutable"
import ClickhouseConnection from "./ClickhouseConnection"
import {escapeIdentifier, formatRootPKColumn, ISourceMeta, PkMap} from "./jsonSchemaInspector"
import {Config} from "./Config"
import {escapeValue} from "./utils"
import RecordProcessor from "./RecordProcessor"

// expects root meta as param
const metaRepresentsReplacingMergeTree = (meta: ISourceMeta) => !meta.pkMappings.isEmpty()

// To handle overall ingestion
export default class StreamProcessor {
  private constructor(
    private readonly clickhouse: ClickhouseConnection,
    private readonly meta: ISourceMeta,
    private readonly config: Config,
    private readonly maxVer: number,
    private readonly recordProcessor = new RecordProcessor(meta, clickhouse),
    private readonly currentBatchRows = 0,
    private readonly currentBatchSize = 0,
    private readonly cleaningValues: Set<string> = Set(), // All values used to clear data based on 'cleaningColumn',
  ) {
  }

  static async createStreamProcessor(meta: ISourceMeta, config: Config, applyDefaultMaxVersion: boolean) {
    const ch = await new ClickhouseConnection(config).checkConnection()
    const maxVersion = (applyDefaultMaxVersion || !metaRepresentsReplacingMergeTree(meta)) ? 0 : Number((await ch.runQuery(`SELECT max(_ver)
                                                                                                                            FROM ${meta.sqlTableName}`)).data[0][0])

    log_info(`[${meta.prop}]: initial max version is [${maxVersion}]`)

    return new StreamProcessor(ch, meta, config, maxVersion)
  }

  public clearIngestion() {
    return new StreamProcessor(this.clickhouse, this.meta, this.config, this.maxVer + 1)
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

  private async processBatchIfNeeded() {
    if (this.currentBatchRows >= this.config.max_batch_rows ||
      this.currentBatchSize >= this.config.max_batch_size) {
      log_info(`Inserting batch`)
      log_info(`rows quota: ${this.currentBatchRows} / ${this.config.max_batch_rows}`)
      log_info(`size quota: ${this.currentBatchSize} / ${this.config.max_batch_size}`)
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

  public async processRecord(record: Record<string, any>, messageSize: number, messageCount: number): Promise<StreamProcessor> {
    const cleaningValue = this.meta.cleaningColumn && record[this.meta.cleaningColumn]
    if (cleaningValue && !this.cleaningValues.includes(cleaningValue)) {
      await this.deleteCleaningValue(cleaningValue)
    }
    return new StreamProcessor(this.clickhouse, this.meta,
      this.config,
      this.maxVer + 1,
      await this.recordProcessor.pushRecord(record, this.maxVer, undefined, undefined, undefined, undefined, messageCount),
      this.currentBatchRows + 1,
      this.currentBatchSize + messageSize,
      cleaningValue ? this.cleaningValues.add(cleaningValue) : this.cleaningValues,
    )
      .processBatchIfNeeded()
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

    const query = `ALTER TABLE ${this.meta.sqlTableName} DELETE WHERE \`${this.meta.cleaningColumn}\` = '${escapeValue(value)}'`
    await this.clickhouse.runQuery(query)
  }

  async clearTables(): Promise<this> {
    const queries = buildTruncateTableQueries(this.meta)
    await Promise.all(queries.map(async (query) => this.clickhouse.runQuery(query)))
    return this
  }

  protected async finalizeBatchProcessing(): Promise<void> {
    if (this.isReplacingMergeTree()) {
      log_info(`[${this.meta.prop}]: removing root duplicates`)
      await this.clickhouse.runQuery(`OPTIMIZE TABLE ${this.meta.sqlTableName} FINAL`)

      log_info(`[${this.meta.prop}]: removing children orphans`)
      await Promise.all(this.meta.children.map((child) => this.deleteChildDuplicates(child)))
    }
    log_info(`[${this.meta.prop}]: ensuring PK integrity is maintained`)
    await this.assertPKIntegrity(this.meta)
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

  private printInsertRecordsStats(): this {
    if (this.currentBatchRows) {
      log_info(`[${this.meta.prop}]: inserted ${this.currentBatchRows} records (${this.currentBatchSize} bytes)`)
    }
    return this
  }


  public async saveNewRecords(): Promise<this> {
    if (this.recordProcessor.isInitialized()) {
      log_info(`[${this.meta.prop}]: ending batch ingestion`)
      await this.recordProcessor.endIngestion()
      return this.printInsertRecordsStats()
    } else {
      return this
    }
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
