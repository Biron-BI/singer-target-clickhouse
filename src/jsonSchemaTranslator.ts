import {List} from "immutable"
import {ColumnMap, ISourceMeta, PkMap, PKType} from "./jsonSchemaInspector"
import SchemaTranslator from "./SchemaTranslator"
import ClickhouseConnection, {Column} from "./ClickhouseConnection"
import * as assert from "assert"
import {fillIf} from "./utils"

// eslint-disable-next-line @typescript-eslint/no-var-requires
const get = require("lodash.get")

export function extractValue(data: { [k: string]: any }, mapping: ColumnMap | PkMap): string {
  const v = mapping.prop ? get(data, mapping.prop.split(".")) : data
  const translator = new SchemaTranslator(mapping)
  return translator.extractValue(v)
}

function resolveVersionColumn(isRoot: boolean, hasPkMappings: boolean, withType = true): string {
  const type = withType ? ' UInt64' : ''
  if (isRoot) {
    // We add a versioning column when we want to handle duplicate: for root tables with PK
    if (hasPkMappings) {
      return `\`_ver\`${type}`
    }
    return ""
  } else {
    // A child shall always have a parent with a versioning column
    return `\`_root_ver\`${type}`
  }
}

const resolveEngine = (isRoot: boolean, hasPkMappings: boolean): string => isRoot && hasPkMappings ? `ReplacingMergeTree(_ver)` : "MergeTree"


const buildOrderByContent = (sqlIdentifiers: List<string>): string => `${sqlIdentifiers.size > 1 ? '(' : ''}${sqlIdentifiers.size > 0 ? sqlIdentifiers.map((sqlIdentifier) => sqlIdentifier).join(", ") : "tuple()"}${sqlIdentifiers.size > 1 ? ')' : ''}`

const resolveOrderBy = (meta: ISourceMeta, isRoot: boolean): string => {
  if (isRoot) {
    return buildOrderByContent(meta.pkMappings
      .filter((pkMap) => pkMap.pkType === PKType.CURRENT) // Safeguard, should already be the case
      .map((pkMap) => pkMap.sqlIdentifier),
    )
  } else {
    return buildOrderByContent(meta.pkMappings
      .filter((pkMap) => pkMap.pkType === PKType.ROOT || pkMap.pkType === PKType.LEVEL)
      .map((pkMap) => pkMap.sqlIdentifier),
    )
  }
}

// From the schema inspection we build the query to create table in Clickhouse.
// Must respect the SHOW CREATE TABLE syntax as we also use it to ensure schema didn't change
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
    // @formatter:off
    .push(`CREATE TABLE ${database}.${meta.sqlTableName} ( ${createDefs.filter(Boolean).join(", ")} ) ENGINE = ${resolveEngine(isNodeRoot, meta.pkMappings.size > 0)} ORDER BY ${resolveOrderBy(meta, isNodeRoot)}`)
    .concat(meta.children.flatMap((child: ISourceMeta) => translateCH(database, child, meta, rootMeta || meta)))
}

export const listTableNames = (meta: ISourceMeta): List<string> => List<string>([meta.sqlTableName])
  .concat(meta.children.flatMap(listTableNames))

export const dropStreamTablesQueries = (meta: ISourceMeta): List<string> => List<string>()
  .push(`DROP TABLE if exists ${meta.sqlTableName}`)
  .concat(meta.children.flatMap(dropStreamTablesQueries))


const mapToColumn = (col: ColumnMap): Column => ({
  name: unescape(col.sqlIdentifier) ?? "",
  type: `${col.nullable ? 'Nullable(' : ''}${col.chType}${col.nullable ? ')' : ''}`,
  is_in_sorting_key: false,
})

const pkMapToColumn = (col: ColumnMap): Column => ({
  ...mapToColumn(col),
  is_in_sorting_key: true,
})

// Remove magic quotes around values
const unescape = (v: string) => v.replace(/`/g, "")

export async function ensureSchemaIsEquivalent(meta: ISourceMeta, ch: ClickhouseConnection) {
  await Promise.all(meta.children.map((child) => ensureSchemaIsEquivalent(child, ch)))

  const isRoot = meta.pkMappings.filter((pkMap) => pkMap.pkType === PKType.ROOT).isEmpty()
  const existingColumns = await ch.listColumns(unescape(meta.sqlTableName))
  const expectedColumns = meta.pkMappings
    .filter((pkMap) => {
      if (isRoot) {
        return pkMap.pkType === PKType.CURRENT
      } else {
        return pkMap.pkType === PKType.ROOT || pkMap.pkType === PKType.LEVEL
      }
    })
    .map(pkMapToColumn)
    .concat(meta.simpleColumnMappings.map(mapToColumn))
    .concat(fillIf({
      name: isRoot ? "_ver" : "_root_ver",
      type: "UInt64",
      is_in_sorting_key: false,
    }, !isRoot || (isRoot && meta.pkMappings.find((pk) => pk.pkType === PKType.CURRENT) !== undefined)))

  assert.deepStrictEqual(existingColumns.sort((a, b) => a.name.localeCompare(b.name)).toArray(),
    expectedColumns.sort((a, b) => a.name.localeCompare(b.name)).toArray(), `[${meta.prop}]: schema change detected in table [${meta.sqlTableName}]`)
}
