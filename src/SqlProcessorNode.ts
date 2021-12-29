import {Readable} from "stream"
import {ISourceMeta} from "jsonSchemaInspector"
import {extractValue} from "jsonSchemaTranslator"
import {List, Range} from "immutable"

type SourceMetaPK = ISourceMeta & { values: string[] };

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
// The INGESTOR
export default class SqlProcessorNode {

  constructor(
    private readonly meta: ISourceMeta,
    private readonly fields: List<string> = List(),
    private readonly values: List<string | number> = List(),
    private readonly children: List<SqlProcessorNode> = List()
  ) {
    this.meta = meta
  }


  // Used for direct insert mode
  // private currentRootMeta?: SourceMetaPK


  public buildInsertQuery(): List<{ baseQuery: string, stream: Readable }> {
    const tableToInsertTo: string = this.meta.sqlTableName

    const childResult = () => this.children.flatMap((child) => child.buildInsertQuery())

    if (this.fields.size > 0) {

      const query: string = `INSERT INTO ${tableToInsertTo} FORMAT JSONCompactEachRow`
      const stream: Readable = new Readable({
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
  // pushRecord(data: { [k: string]: any }, chunkIndex: number, maxVer: number, parentMeta?: SourceMetaPK, rootMeta?: SourceMetaPK, rootVer?: number) {
  //   this.currentRootMeta = rootMeta
  //   // We build header fields only once as the meta is the same for all values in this node
  //   if (this.fields.size === 0) {
  //     this.fields = this.buildSQLInsertField(this.meta)
  //     if (rootMeta === undefined) {
  //       if (this.meta.pkMappings.size > 0) {
  //         this.fields.push("_ver")
  //       }
  //     } else {
  //       this.fields.push("_root_ver")
  //     }
  //   }
  //   // Record new version start at max existing version, + position in stream + 1
  //   // Only needed if a column for version exists
  //   if (!rootVer && (this.fields.includes("_ver") || this.fields.includes("_root_ver"))) {
  //     rootVer = maxVer + chunkIndex + 1
  //   }
  //   // We parse values from chunk data received and store them in this node
  //   this.values = this.values.concat(this.buildSQLInsertValues(data, this.meta, parentMeta?.values, rootMeta?.values, rootVer))
  //
  //   // We extract values of primary keys so we can insert them in children later
  //   let pkValues: List<string> = List()
  //   if (this.meta.children.size > 0) {
  //     pkValues = this.meta.pkMappings.map(pkMapping => extractValue(data, pkMapping))
  //   }
  //   const meAsParent: SourceMetaPK = {...this.meta, values: pkValues}
  //
  //   if (!this.currentRootMeta) {
  //     this.currentRootMeta = meAsParent
  //   }
  //
  //   for (const child of this.meta.children) {
  //     // We check if a children with the same meta has already been added
  //     let processor: SqlProcessorNode = this.children.find((elem) => elem.meta.prop === child.prop &&
  //       elem.meta.sqlTableName === child.sqlTableName)
  //     // Otherwise we create a new node
  //     if (!processor) {
  //       processor = new SqlProcessorNode(child)
  //
  //       this.children.push(processor)
  //     }
  //     const childData: { [k: string]: any }[] = _.get(data, child.prop.split("."))
  //
  //     if (childData) {
  //       for (const elem of childData) {
  //         processor.pushRecord(elem, chunkIndex, maxVer, meAsParent, rootMeta || meAsParent, rootVer)
  //       }
  //     }
  //   }
  // }

  //
  // retrieveRootPKValues() {
  //   if (this.currentRootMeta) {
  //     return this.currentRootMeta.values
  //   } else {
  //     throw new Error("No root PK value could be found")
  //   }
  // }

  // Fields that'll be inserted in the database
  private buildSQLInsertField(meta: ISourceMeta): List<string> {
    return meta.pkMappings
      .map((pkMap) => pkMap.sqlIdentifier)
      .concat(meta.simpleColumnMappings
        .map((cMap) => cMap.sqlIdentifier))
  }

  // Values that'll be inserted
  private buildSQLInsertValues(data: { [k: string]: any }, meta: ISourceMeta, pkValues: List<any> = List(), version?: number) {
    const ret = pkValues.concat(meta.simpleColumnMappings.map(cm => extractValue(data, cm)))

    if (version) {
      return ret.push(version)
    }
    return ret
  }
}
