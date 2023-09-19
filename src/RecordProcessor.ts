import {pipeline, Readable} from "stream"
import {Map} from "immutable"
import {ISourceMeta, PKType} from "./jsonSchemaInspector"
import {extractValue} from "./jsonSchemaTranslator"
import {log_debug, log_error, log_info} from "singer-node"
import ClickhouseConnection from "./ClickhouseConnection"

// eslint-disable-next-line @typescript-eslint/no-var-requires
const get = require("lodash.get")

type PKValue = string | number
type PKValues = PKValue[]

type SourceMetaPK = {
  rootValues: PKValues,
  parentValues: PKValues,
  values: PKValues,
  levelValues: PKValues
};

/**
 * Ingests and store data values
 * Tree structure to process stream of data according to precomputed meta data
 * Call pushRecord for each row
 * One node for one table
 */
export default class RecordProcessor {
  private readonly isWithParentPK: boolean
  private readonly isRoot: boolean

  constructor(
    private readonly meta: ISourceMeta,
    private readonly clickhouse: ClickhouseConnection,
    private readonly level = 0,
    private children: Map<string, RecordProcessor> = Map(),
    private ingestionStream?: Readable,
    private ingestionPromise?: Promise<void>,
  ) {
    this.meta = meta
    this.isWithParentPK = this.meta.pkMappings.find((pk) => pk.pkType === PKType.PARENT) !== undefined
    this.isRoot = level === 0
  }

  public isInitialized(): boolean {
    return this.ingestionStream !== undefined
  }

  /*
      Prepare sql query by splitting fields and values
      Creates children
      Structure: One child per table
   */
  pushRecord(
    data: Record<string, any>,
    maxVer: number,
    parentMeta?: SourceMetaPK,
    rootVer?: number,
    indexInParent = -1,
    messageCount = 0,
  ): void {
    // When first record is pushed we start by initializing stream
    if (!this.isInitialized()) {
      this.startIngestion(messageCount)
    }

    // root version number is computed only for a root who has primaryKeys
    // version start at max existing version + 1
    const resolvedRootVer = (this.isRoot && this.meta.pkMappings.length > 0) ? maxVer + 1 : rootVer

    // @ts-ignore
    const pkValues: SourceMetaPK = {
      rootValues: parentMeta?.rootValues.length == 0 ? parentMeta.values : parentMeta?.rootValues ?? [], // On first recursion we retrieve root pk from parentMeta
      parentValues: parentMeta?.values ?? [],
      values: this.meta.pkMappings.filter((pkMap) => pkMap.pkType === PKType.CURRENT).map((pkMapping) => extractValue(data, pkMapping)),
      levelValues: this.isRoot ? ([] as PKValues) : [...(parentMeta?.levelValues || []), indexInParent]
    }

    this.ingestionStream?.push(JSON.stringify(this.buildSQLInsertValues(data,
      pkValues.rootValues
        .concat(this.isWithParentPK ? pkValues.parentValues : [])
        .concat(pkValues.values)
        .concat(pkValues.levelValues),
      resolvedRootVer)))

    // TODO
    // this.children =       Map(this.meta.children.map((child) => {
    //   const processor = this.children.get(child.sqlTableName) ?? new RecordProcessor(child, this.clickhouse, this.level + 1, )
    //
    //   // In this record we expect a list, as that is the reason a children has been created. But some JSON Schema declaration may declare both an array and a list, so we check and create an array with only one item if it is not an array
    //   const childData: Record<string, any> = get(data, child.prop.split("."))
    //
    //   const childDataAsArray: List<Record<string, any>> = Array.isArray(childData) ? List(childData) : List(childData ? [childData] : [])
    //
    //   return childDataAsArray.reduce(([key, acc], elem, idx) => [key, acc.pushRecord(elem, maxVer, pkValues, resolvedRootVer, idx, messageCount)], [child.sqlTableName, processor])
    // }))
  }

  public async endIngestion() {
    log_debug(`closing stream to insert data in ${this.meta.prop}, ${this.meta.sqlTableName}`)
    this.ingestionStream?.push(null)
    await Promise.all([
      this.ingestionPromise,
      Promise.all(this.children.map((child) => child.endIngestion())),
    ])
  }

  public buildSQLInsertField(): string[] {
    const isRoot = this.meta.pkMappings.find((pkMap) => pkMap.pkType === PKType.ROOT) === undefined
    return this.meta.pkMappings
      .map((pkMap) => pkMap.sqlIdentifier)
      .concat(this.meta.simpleColumnMappings.map((cMap) => cMap.sqlIdentifier))
      .concat(isRoot && this.meta.pkMappings.length > 0 ? ["`_ver`"] : ["`_root_ver`"])
  }

  private startIngestion(messageCount: number): void {
    const insertQuery = `INSERT INTO ${this.meta.sqlTableName} (${this.buildSQLInsertField().join(",")}) FORMAT JSONCompactEachRow`
    if (this.isRoot) {
      log_info(`[${this.meta.prop}] handling lines starting at ${messageCount}`)
    }

    const readStream = new Readable({
      // eslint-disable-next-line @typescript-eslint/no-empty-function
      objectMode: true, read() {
      },
    })

    const insertStream = this.clickhouse.createWriteStream(insertQuery)

    const promise = new Promise<void>((resolve, reject) => {
      pipeline(readStream, insertStream, (err) => {
        if (err) {
          log_error(`[${this.meta.prop}]: pipeline error: [${err.message}]`)
          insertStream.destroy(err)
          reject(err)
        } else {
          log_info(`[${this.meta.prop}]: pipeline ended`)
          resolve()
        }
      })
    })

    this.ingestionStream = readStream
    this.ingestionPromise = promise
  }

  // Extract value for all simple columns in a record
  private buildSQLInsertValues = (
    data: Record<string, any>,
    pkValues: any[] = [],
    version?: number,
  ) => {
    const noPk = pkValues.length
    const noSimpleColumn = this.meta.simpleColumnMappings.length
    const result: any[] = new Array(noPk + noSimpleColumn + (version !== undefined ? 1 : 0))
    for (let i = 0; i < noPk; i++) {
      result[i] = pkValues[i]
    }
    for (let i = 0; i < noSimpleColumn; i++) {
      // @ts-ignore
      result[i + noPk] = extractValue(data, this.meta.simpleColumnMappings[i])
    }
    if (version !== undefined)
      result[noPk + noSimpleColumn] = version
    return result
  }
}
