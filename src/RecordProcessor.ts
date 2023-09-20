import {Writable} from "stream"
import {ISourceMeta, PkMap, PKType} from "./jsonSchemaInspector"
import {extractValue} from "./jsonSchemaTranslator"
import {log_debug, log_info} from "singer-node"
import ClickhouseConnection from "./ClickhouseConnection"

// eslint-disable-next-line @typescript-eslint/no-var-requires
const get = require("lodash.get")

type PKValue = string | number
type PKValues = PKValue[]

type SourceMetaPK = {
  rootValues: PKValues | undefined,
  parentValues: PKValues | undefined,
  values: PKValues,
  levelValues: PKValues | undefined,
};

/**
 * Ingests and store data values
 * Tree structure to process stream of data according to precomputed meta data
 * Call pushRecord for each row
 * One node for one table
 */
export default class RecordProcessor {
  readonly hasChildren: boolean
  private readonly isWithParentPK: boolean
  private readonly isRoot: boolean
  private readonly children: { [key: string]: RecordProcessor }
  private ingestionStream?: Writable
  private ingestionPromise?: Promise<void>
  private bufferedDatasToStream: string[] = []
  private readonly currentPkMappings: PkMap[]

  constructor(
    private readonly meta: ISourceMeta,
    private readonly clickhouse: ClickhouseConnection,
    private readonly level = 0,
  ) {
    this.meta = meta
    this.isRoot = level === 0
    this.isWithParentPK = !this.isRoot && this.meta.pkMappings.find((pk) => pk.pkType === PKType.PARENT) !== undefined
    this.hasChildren = meta.children.length > 0
    this.children = meta.children.reduce((acc, child) => {
      const processor = new RecordProcessor(child, this.clickhouse, this.level + 1)
      return {...acc, [child.sqlTableName]: processor}
    }, {})
    this.currentPkMappings = this.meta.pkMappings.filter((pkMap) => pkMap.pkType === PKType.CURRENT)
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

    const currentPkValues = new Array(this.currentPkMappings.length)
    for (let i = 0; i < this.currentPkMappings.length; i++) {
      currentPkValues[i] = extractValue(data, this.currentPkMappings[i])
    }

    const sourceMetaPK: SourceMetaPK = {
      values: currentPkValues,
      rootValues: this.isRoot ? undefined : (parentMeta?.rootValues ?? parentMeta?.values),
      parentValues: this.isRoot ? undefined : parentMeta?.values,
      levelValues: this.isRoot ? undefined : [...(parentMeta?.levelValues || []), indexInParent],
    }

    let pkValues = currentPkValues
    if (!this.isRoot) {
      pkValues = sourceMetaPK.rootValues!
        .concat(this.isWithParentPK ? sourceMetaPK.parentValues! : [])
        .concat(pkValues)
        .concat(sourceMetaPK.levelValues!)
    }

    const dataToStream = JSON.stringify(this.buildSQLInsertValues(data, pkValues, resolvedRootVer))
    this.bufferedDatasToStream.push(dataToStream)
    if (this.bufferedDatasToStream.length == 100) {
      this.sendDatasToStream()
    }

    if (this.hasChildren) {
      for (const child of this.meta.children) {
        const childProcessor: RecordProcessor = this.children[child.sqlTableName]
        // In this record we expect a list, as that is the reason a children has been created. But some JSON Schema declaration may declare both an array and a list, so we check and create an array with only one item if it is not an array
        const childDataRaw: Record<string, any> = get(data, child.prop.split("."))
        const childDataAsArray = Array.isArray(childDataRaw) ? childDataRaw : (childDataRaw ? [childDataRaw] : [])
        for (let idx = 0; idx < childDataAsArray.length; idx++) {
          childProcessor.pushRecord(childDataAsArray[idx], maxVer, sourceMetaPK, resolvedRootVer, idx, messageCount)
        }
      }
    }
  }

  public async endIngestion() {
    log_debug(`closing stream to insert data in ${this.meta.prop}, ${this.meta.sqlTableName}`)
    this.sendDatasToStream()
    this.ingestionStream?.end()
    await Promise.all([
      this.ingestionPromise,
      Promise.all(Object.values(this.children).map((child) => child.endIngestion())),
    ])
  }

  public buildSQLInsertField(): string[] {
    const isRoot = this.meta.pkMappings.find((pkMap) => pkMap.pkType === PKType.ROOT) === undefined
    return this.meta.pkMappings
      .map((pkMap) => pkMap.sqlIdentifier)
      .concat(this.meta.simpleColumnMappings.map((cMap) => cMap.sqlIdentifier))
      .concat(isRoot && this.meta.pkMappings.length > 0 ? ["`_ver`"] : ["`_root_ver`"])
  }

  private sendDatasToStream() {
    if (this.bufferedDatasToStream.length>0) {
      this.bufferedDatasToStream.push("")
      this.ingestionStream!.write(Buffer.from(this.bufferedDatasToStream.join('\n')))
      this.bufferedDatasToStream = []
    }
  }

  private startIngestion(messageCount: number): void {
    const insertQuery = `INSERT INTO ${this.meta.sqlTableName} (${this.buildSQLInsertField().join(",")}) FORMAT JSONCompactEachRow`
    if (this.isRoot) {
      log_info(`[${this.meta.prop}] handling lines starting at ${messageCount}`)
    }

    const insertStream = this.clickhouse.createWriteStream(insertQuery)

    const promise = new Promise<void>((resolve, reject) => {
      insertStream.on('error', reject)
        .on('finish', resolve)
    })

    this.ingestionStream = insertStream
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
      result[i + noPk] = extractValue(data, this.meta.simpleColumnMappings[i])
    }
    if (version !== undefined)
      result[noPk + noSimpleColumn] = version
    return result
  }
}
