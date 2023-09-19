import {pipeline, Readable} from "stream"
import {List, Map} from "immutable"
import {ISourceMeta, PKType} from "./jsonSchemaInspector"
import {extractValue} from "./jsonSchemaTranslator"
import {log_debug, log_error, log_info} from "singer-node"
import {fillIf} from "./utils"
import ClickhouseConnection from "./ClickhouseConnection"

// eslint-disable-next-line @typescript-eslint/no-var-requires
const get = require("lodash.get")

type PKValue = string | number
type PKValues = List<PKValue>

type SourceMetaPK = {
  rootValues: PKValues,
  parentValues: PKValues,
  values: PKValues,
  levelValues: PKValues
};

// https://clickhouse.tech/docs/en/interfaces/formats/#jsoncompacteachrow
export function jsonToJSONCompactEachRow(v: any) {
  if (v === undefined || v === null) {
    return "null"
  }
  return JSON.stringify(v)
}

/**
 * Ingests and store data values
 * Tree structure to process stream of data according to precomputed meta data
 * Call pushRecord for each row
 * One node for one table
 */
export default class RecordProcessor {
  private readonly isWithParentPK: boolean

  constructor(
    private readonly meta: ISourceMeta,
    private readonly clickhouse: ClickhouseConnection,
    private readonly children: Map<string, RecordProcessor> = Map(),
    private readonly ingestionStream?: Readable,
    private readonly ingestionPromise?: Promise<void>,
  ) {
    this.meta = meta
    this.isWithParentPK = this.meta.pkMappings.find((pk) => pk.pkType === PKType.PARENT) !== undefined
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
    level = 0,
    indexInParent?: number,
    messageCount = 0,
  ): RecordProcessor {
    const isRoot = indexInParent === undefined

    // When first record is pushed we start by initializing stream
    if (!this.isInitialized()) {
      return this.startIngestion(messageCount, isRoot).pushRecord(data, maxVer, parentMeta, rootVer, level, indexInParent, messageCount)
    }


    // root version number is computed only for a root who has primaryKeys
    // version start at max existing version + 1
    const resolvedRootVer = (isRoot && !this.meta.pkMappings.isEmpty()) ? maxVer + 1 : rootVer

    const pkValues: SourceMetaPK = {
      rootValues: parentMeta?.rootValues.isEmpty() ? parentMeta.values : parentMeta?.rootValues ?? List(), // On first recursion we retrieve root pk from parentMeta
      parentValues: parentMeta?.values ?? List(),
      values: this.meta.pkMappings.filter((pkMap) => pkMap.pkType === PKType.CURRENT).map((pkMapping) => extractValue(data, pkMapping)),
      levelValues: isRoot ? List() : parentMeta?.levelValues.push(indexInParent) ?? List(),
    }

    this.ingestionStream?.push(`[${this.buildSQLInsertValues(data,
      pkValues.rootValues
        .concat(this.isWithParentPK ? pkValues.parentValues : List())
        .concat(pkValues.values)
        .concat(pkValues.levelValues),
      resolvedRootVer).join(",")}]`)

    return new RecordProcessor(
      this.meta,
      this.clickhouse,
      Map(this.meta.children.map((child) => {
        const processor = this.children.get(child.sqlTableName) ?? new RecordProcessor(child, this.clickhouse)

        // In this record we expect a list, as that is the reason a children has been created. But some JSON Schema declaration may declare both an array and a list, so we check and create an array with only one item if it is not an array
        const childData: Record<string, any> = get(data, child.prop.split("."))

        const childDataAsArray: List<Record<string, any>> = Array.isArray(childData) ? List(childData) : List(childData ? [childData] : [])

        return childDataAsArray.reduce(([key, acc], elem, idx) => [key, acc.pushRecord(elem, maxVer, pkValues, resolvedRootVer, level + 1, idx, messageCount)], [child.sqlTableName, processor])
      })),
      this.ingestionStream,
      this.ingestionPromise,
    )
  }

  public async endIngestion() {
    log_debug(`closing stream to insert data in ${this.meta.prop}, ${this.meta.sqlTableName}`)
    this.ingestionStream?.push(null)
    await Promise.all([
      this.ingestionPromise,
      Promise.all(this.children.map((child) => child.endIngestion())),
    ])
  }

  public buildSQLInsertField(): List<string> {
    const isRoot = this.meta.pkMappings.find((pkMap) => pkMap.pkType === PKType.ROOT) === undefined
    return this.meta.pkMappings
      .map((pkMap) => pkMap.sqlIdentifier)
      .concat(this.meta.simpleColumnMappings
        .map((cMap) => cMap.sqlIdentifier))
      .concat(fillIf("`_ver`", isRoot && !this.meta.pkMappings.isEmpty()))
      .concat(fillIf("`_root_ver`", !isRoot))
  }

  private startIngestion(messageCount: number, isRoot: boolean): RecordProcessor {
    const insertQuery = `INSERT INTO ${this.meta.sqlTableName} (${this.buildSQLInsertField().join(",")}) FORMAT JSONCompactEachRow`
    if (isRoot) {
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

    return new RecordProcessor(this.meta, this.clickhouse, this.children, readStream, promise)
  }

  // Extract value for all simple columns in a record
  private buildSQLInsertValues = (
    data: Record<string, any>,
    pkValues: List<any> = List(),
    version?: number,
  ) => {
    const noPk = pkValues.size
    const noSimpleColumn = this.meta.simpleColumnMappings.size
    const result: any[] = new Array(noPk + noSimpleColumn + (version !== undefined ? 1 : 0))
    for (let i = 0; i < noPk; i++) {
      result[i] = jsonToJSONCompactEachRow(pkValues.get(i))
    }
    for (let i = 0; i < noSimpleColumn; i++) {
      // @ts-ignore
      result[i + noPk] = jsonToJSONCompactEachRow(extractValue(data, this.meta.simpleColumnMappings.get(i)))
    }
    if (version !== undefined)
      result[noPk + noSimpleColumn] = jsonToJSONCompactEachRow(version)
    return result
  }
}
