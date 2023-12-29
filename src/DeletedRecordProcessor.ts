import {ISourceMeta, PkMap, PKType} from "./jsonSchemaInspector"
import {extractValue} from "./jsonSchemaTranslator"
import TargetConnection from "./TargetConnection"
import {log_info} from "singer-node"

interface DeletedRecordProcessorConfig {
  batchSize: number
  translateValues: boolean
}

export default class DeletedRecordProcessor {
  private bufferedDatasToDelete: any[][] = []
  private readonly currentPkMappings: PkMap[]

  constructor(
    private readonly meta: ISourceMeta,
    private readonly clickhouse: TargetConnection,
    private readonly config: DeletedRecordProcessorConfig,
  ) {
    this.meta = meta
    this.currentPkMappings = this.meta.pkMappings.filter((pkMap) => pkMap.pkType === PKType.CURRENT)
  }

  async pushDeletedRecord(
    data: Record<string, any>,
  ) {
    if (this.currentPkMappings.length === 0) {
      throw new Error(`[${this.meta.prop}] cannot push deleted record to a stream without pk mapping`)
    }

    const pkValues = new Array(this.currentPkMappings.length)
    for (let i = 0; i < this.currentPkMappings.length; i++) {
      pkValues[i] = extractValue(data, this.currentPkMappings[i], this.config.translateValues)
    }

    this.bufferedDatasToDelete.push(this.convertValuesFormat(pkValues))
    if (this.bufferedDatasToDelete.length == this.config.batchSize) {
      await this.deleteBufferedData()
    }
  }

  public async deleteBufferedData() {
    if (this.bufferedDatasToDelete.length > 0) {
      log_info(`[${this.meta.prop}] deleting ${this.bufferedDatasToDelete.length} records`)
      await this.clickhouse.runQuery(`DELETE
                                      FROM ${this.meta.sqlTableName}
                                      WHERE (${this.currentPkMappings.map(elem => elem.sqlIdentifier).join(",")})
                                                IN (${this.bufferedDatasToDelete.map((pks => `(${pks})`))
                                                  .join(',')})`)

      this.bufferedDatasToDelete = []
    }
  }

  private convertValuesFormat = (pkValues: any[]) => pkValues.map((elem, index) => this.convertValueFormat(elem, this.currentPkMappings[index]))

  private convertValueFormat = (pkValue: any, pkMapping: PkMap) => ["String", "FixedString", "DateTime", "Date", "DateTime64", "Date32", "UUID"]
    .includes(pkMapping.chType!) ? `'${pkValue}'` : pkValue
}
