import {pipeline, Readable, Transform} from "stream"
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
    private readonly readStream?: Readable,
    private readonly ingestionPromise?: Promise<void>,
  ) {
    this.meta = meta
    this.isWithParentPK = this.meta.pkMappings.find((pk) => pk.pkType === PKType.PARENT) !== undefined
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
  ): RecordProcessor {

    // When first record is pushed we start by initializing stream
    if (!this.readStream) {
      return this.startIngestion().pushRecord(data, maxVer, parentMeta, rootVer, level, indexInParent)
    }

    const isRoot = indexInParent === undefined

    // root version number is computed only for a root who has primaryKeys
    // version start at max existing version + 1
    const resolvedRootVer = (isRoot && !this.meta.pkMappings.isEmpty()) ? maxVer + 1 : rootVer

    const pkValues: SourceMetaPK = {
      rootValues: parentMeta?.rootValues.isEmpty() ? parentMeta.values : parentMeta?.rootValues ?? List(), // On first recursion we retrieve root pk from parentMeta
      parentValues: parentMeta?.values ?? List(),
      values: this.meta.pkMappings.filter((pkMap) => pkMap.pkType === PKType.CURRENT).map((pkMapping) => extractValue(data, pkMapping)),
      levelValues: isRoot ? List() : parentMeta?.levelValues.push(indexInParent) ?? List(),
    }

    this.readStream?.push(`[${this.buildSQLInsertValues(data,
      pkValues.rootValues
        .concat(this.isWithParentPK ? pkValues.parentValues : List())
        .concat(pkValues.values)
        .concat(pkValues.levelValues),
      resolvedRootVer).map(jsonToJSONCompactEachRow).join(",")}]`)

    return new RecordProcessor(
      this.meta,
      this.clickhouse,
      Map(this.meta.children.map((child) => {
        const processor = this.children.get(child.sqlTableName) ?? new RecordProcessor(child, this.clickhouse)

        // In this record we expect a list, as that is the reason a children has been created. But some JSON Schema declaration may declare both an array and a list, so we check and create an array with only one item if it is not an array
        const childData: Record<string, any> = get(data, child.prop.split("."))

        const childDataAsArray: List<Record<string, any>> = Array.isArray(childData) ? List(childData) : List(childData ? [childData] : [])

        return childDataAsArray.reduce(([key, acc], elem, idx) => [key, acc.pushRecord(elem, maxVer, pkValues, resolvedRootVer, level + 1, idx)], [child.sqlTableName, processor])
      })),
      this.readStream,
      this.ingestionPromise,
    )
  }

  public async endIngestion() {
    this.readStream?.push(null)

    await Promise.all(this.children.map((child) => child.endIngestion()))

    await this.ingestionPromise
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

  private startIngestion(): RecordProcessor {
    const insertQuery = `INSERT INTO ${this.meta.sqlTableName} (${this.buildSQLInsertField().join(",")}) FORMAT JSONCompactEachRow`

    const readStream = new Readable({
      // eslint-disable-next-line @typescript-eslint/no-empty-function
      objectMode: true, read() {
      },
    })

    const transform: Transform = new Transform({
        objectMode: true,
        transform(chunk, encoding, callback) {
          log_debug(`${insertQuery}: ${chunk}`)
          this.push(chunk)
          callback()
        },
      },
    )

    const promise = new Promise<void>((resolve, reject) => {
      // We leave resolve and reject responsibility to insertion stream
      const insertStream = this.clickhouse.createWriteStream(insertQuery, resolve, reject)
      pipeline(readStream, transform, insertStream, (err) => {
        if (err) {
          log_error(`[${this.meta.prop}]: pipeline error: [${err.message}]`)
          insertStream.destroy()
        } else {
          log_info(`[${this.meta.prop}]: pipeline ended`)
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
  ) => pkValues
    .concat(this.meta.simpleColumnMappings.map(cm => extractValue(data, cm)))
    .concat(fillIf(version, version !== undefined))
}
