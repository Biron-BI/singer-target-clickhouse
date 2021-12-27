// import SchemaTranslator from "SchemaTranslator"
import {ISourceMeta} from "jsonSchemaInspector"
import {List} from "immutable"

// export const JsonDataToSql = {
//     extractValue(data: { [k: string]: any }, mapping: ColumnMap | PkMap): string {
//         let v = mapping.prop ? _.get(data, mapping.prop.split(".")) : data;
//
//         const value = mapping.prop ? Object.entries(data).find()
//
//         const translator = new SchemaTranslator(mapping);
//         return translator.extractValue(v);
//     }
// };
//
// /**
//  * Returns list of sql instructions to setup sql table
//  */
//

function resolveVersionColumn(isRoot: boolean, hasPkMappings: boolean): string {
  if (isRoot) {
    // We add a versioning column when we want to handle duplicate: for root tables with PK
    if (hasPkMappings) {
      return "`_ver` UInt64"
    }
    return ""
  } else {
    // A child shall always have a parent with a versioning column (It may or may not be written in the bible)
    return "`_root_ver` UInt64"
  }
}

// In case root has no child, it stays a MergeTree
const resolveEngine = (isRoot: boolean, hasPkMappings: boolean): string => isRoot && hasPkMappings ? `ReplacingMergeTree(_ver)` : "MergeTree()"

export function translateCH(meta: ISourceMeta, parentMeta?: ISourceMeta, rootMeta?: ISourceMeta): List<string> {
  if (meta.simpleColumnMappings.size < 1) {
    throw new Error("Attempting to create table without columns")
  }
  const isNodeRoot = rootMeta === undefined
  const createDefs: List<string> = meta.pkMappings
    .map(fkMapping => `${fkMapping.sqlIdentifier} ${fkMapping.chType}`)
    .concat(meta.simpleColumnMappings.map(mapping => {
      const type = mapping.nullable ? `Nullable(${mapping.chType})` : mapping.chType
      return `${mapping.sqlIdentifier} ${type}`
    }))
    .push(resolveVersionColumn(isNodeRoot, meta.pkMappings.size > 0))


  return List<string>()
    .push(`DROP TABLE IF EXISTS ${meta.sqlTableName}`)
    .push(`CREATE TABLE ${meta.sqlTableName}(${createDefs.filter(Boolean).join(",")}) ENGINE = ${resolveEngine(isNodeRoot, meta.pkMappings.size > 0)} ORDER BY (${meta.pkMappings.size > 0 ? meta.pkMappings.map((elem) => elem.sqlIdentifier).join(",") : "tuple()"})`)
    .concat(meta.children.flatMap((child: ISourceMeta) => translateCH(child, meta, rootMeta || meta)))
}
