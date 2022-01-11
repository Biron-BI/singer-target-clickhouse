import {Readable} from "stream"
import {List, Range} from "immutable"
import {ISourceMeta, PKType} from "./jsonSchemaInspector"
import {extractValue} from "./jsonSchemaTranslator"
import {log_debug} from "singer-node"

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
 * Call pushRecord for each row, and buildInsertQuery when batch is complete
 * One node for one table
 */
export default class RecordProcessor {

  constructor(
    public readonly meta: ISourceMeta,
    public readonly fields: List<string> = List(),
    public readonly values: List<string | number> = List(),
    public readonly children: List<RecordProcessor> = List(),
  ) {
    this.meta = meta
  }

  public buildInsertQuery(): List<{ baseQuery: string, stream: Readable }> {
    const tableToInsertTo: string = this.meta.sqlTableName

    const childResult = () => this.children.flatMap((child) => child.buildInsertQuery())

    log_debug(`[${this.meta.prop}]: fields to insert = [${this.fields.join(",")}]`)
    if (this.fields.size > 0) {
      const query = `INSERT INTO ${tableToInsertTo} FORMAT JSONCompactEachRow`
      const stream: Readable = new Readable({
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        objectMode: true, read(size) {
        },
      })

      Range(0, this.values.size, this.fields.size).toList().map((idx) => stream.push(`[${this.values.slice(idx, this.fields.size + idx).map(jsonToJSONCompactEachRow).join(",")}]`))

      stream.push(null)
      return List([{baseQuery: query, stream}])
        .concat(childResult())
    }
    return childResult()
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

    const isRoot = indexInParent === undefined

    // root version number is computed only for a root who has primaryKeys
    // version start at max existing version + 1
    const resolvedRootVer = (isRoot && !this.meta.pkMappings.isEmpty()) ? maxVer + 1 : rootVer

    // Check if stream ingestion required parentPK
    const isWithParentPK = this.meta.pkMappings.find((pk) => pk.pkType === PKType.PARENT) !== undefined

    const pkValues: SourceMetaPK = {
      rootValues: parentMeta?.rootValues.isEmpty() ? parentMeta.values : parentMeta?.rootValues ?? List(), // On first recursion we retrieve root pk from parentMeta
      parentValues: parentMeta?.values ?? List(),
      values: this.meta.pkMappings.filter((pkMap) => pkMap.pkType === PKType.CURRENT).map((pkMapping) => extractValue(data, pkMapping)),
      levelValues: isRoot ? List () : parentMeta?.levelValues.push(indexInParent) ?? List()
    }

    return new RecordProcessor(
      this.meta,
      this.buildSQLInsertField(this.meta, isRoot),
      this.values.concat(this.buildSQLInsertValues(data,
        pkValues.rootValues
          .concat(isWithParentPK ? pkValues.parentValues : List())
          .concat(pkValues.values)
          .concat(pkValues.levelValues),
        resolvedRootVer)),
      this.meta.children.map((child) => {
        const processor = this.children.find((elem) => elem.meta.sqlTableName === child.sqlTableName) ?? new RecordProcessor(child)
        const childData: List<Record<string, any>> = List(get(data, child.prop.split(".")))
        if (!childData.isEmpty()) {
          return childData.reduce((acc, elem, idx) => acc.pushRecord(elem, maxVer, pkValues, resolvedRootVer, level + 1, idx), processor)
        }
        return processor
      }),
    )
  }

  // Fields that'll be inserted in the database
  private buildSQLInsertField(meta: ISourceMeta, isRoot: boolean): List<string> {
    if (this.fields.size > 0) {
      return this.fields
    }

    return meta.pkMappings
      .map((pkMap) => pkMap.sqlIdentifier)
      .concat(meta.simpleColumnMappings
        .map((cMap) => cMap.sqlIdentifier))
      .concat(fillIf("`_ver`", isRoot && !this.meta.pkMappings.isEmpty()))
      .concat(fillIf("`_root_ver`", !isRoot))
  }

  /**
   *
   * @param data
   * @param pkValues all current pkValues of parent (key_properties + level indexes)
   * @param version to add root_ver
   * @private
   */
  private buildSQLInsertValues = (
    data: Record<string, any>,
    pkValues: List<any> = List(),
    version?: number,
  ) => pkValues
    .concat(this.meta.simpleColumnMappings.map(cm => extractValue(data, cm)))
    .concat(fillIf(version, version !== undefined))
}

function fillIf<T>(value: T, apply: boolean): List<T> {
  return apply ? List([value]) : List()
}
