// import SchemaTranslator from "SchemaTranslator"
import {List} from "immutable"
import {ColumnMap, ISourceMeta, PkMap} from "./jsonSchemaInspector"
import SchemaTranslator from "./SchemaTranslator"

const get = require("lodash.get")

export function extractValue(data: { [k: string]: any }, mapping: ColumnMap | PkMap): string {
  let v = mapping.prop ? get(data, mapping.prop.split(".")) : data
  const translator = new SchemaTranslator(mapping)
  return translator.extractValue(v)
}


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
const resolveEngine = (isRoot: boolean, hasPkMappings: boolean): string => isRoot && hasPkMappings ? `ReplacingMergeTree(_ver)` : "MergeTree"

const resolveOrderBy = (meta: ISourceMeta): string => `${meta.pkMappings.size > 1 ? '(' : ''}${meta.pkMappings.size > 0 ? meta.pkMappings.map((elem) => elem.sqlIdentifier).join(", ") : "tuple()"}${meta.pkMappings.size > 1 ? ')' : ''}`

// From the schema inspection we build the query to create table in Clickhouse.
// Must respect the SHOW CREATE TABLE syntax as we will use it to ensure schema didn't change
export function translateCH(database: string, meta: ISourceMeta, parentMeta?: ISourceMeta, rootMeta?: ISourceMeta): List<string> {
  if (meta.simpleColumnMappings.isEmpty() && meta.pkMappings.isEmpty()) {
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
    .push(`CREATE TABLE ${database}.${meta.sqlTableName} ( ${createDefs.filter(Boolean).join(", ")} ) ENGINE = ${resolveEngine(isNodeRoot, meta.pkMappings.size > 0)} ORDER BY ${resolveOrderBy(meta)}`)
    .concat(meta.children.flatMap((child: ISourceMeta) => translateCH(database, child, meta, rootMeta || meta)))
}

export const listTableNames = (meta: ISourceMeta): List<string> => List<string>([meta.sqlTableName])
  .concat(meta.children.flatMap(listTableNames))
