import {ColumnMap, ISourceMeta, PkMap, PKType} from "./jsonSchemaInspector"
import ClickhouseConnection, {Column} from "./ClickhouseConnection"
import {log_error} from "singer-node"
import {listLeft, mapLeft} from "./Either"

/**
 * @param data
 * @param mapping
 * @param translateValue a configuration option to avoid unnecessary parsing of values, already parsed by the initial JSON.parse()
 */
export function extractValue(data: Record<string, any>, mapping: ColumnMap | PkMap, translateValue: boolean): string {
  let v = mapping.valueExtractor(data)
  if (v === undefined) {
    v = null
  }
  if (!translateValue) {
    return v
  }
  return mapping.valueTranslator ? mapping.valueTranslator(v) : v
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


const buildOrderByContent = (sqlIdentifiers: string[]): string => `${sqlIdentifiers.length > 1 ? '(' : ''}${sqlIdentifiers.length > 0 ? sqlIdentifiers.map((sqlIdentifier) => sqlIdentifier).join(", ") : "tuple()"}${sqlIdentifiers.length > 1 ? ')' : ''}`

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
export function translateCH(database: string, meta: ISourceMeta, parentMeta?: ISourceMeta, rootMeta?: ISourceMeta): string[] {
  if (meta.simpleColumnMappings.length == 0 && meta.pkMappings.length == 0) {
    throw new Error("Attempting to create table without columns")
  }
  const isNodeRoot = rootMeta === undefined
  const createDefs: string[] = meta.pkMappings
    .map(fkMapping => `${fkMapping.sqlIdentifier} ${fkMapping.chType}`)
    .concat(meta.simpleColumnMappings.map(mapping => `${mapping.sqlIdentifier} ${toQualifiedType(mapping)}`))
    .concat(resolveVersionColumn(isNodeRoot, meta.pkMappings.length > 0))

  return [
    // @formatter:off
    `CREATE TABLE ${database}.${meta.sqlTableName} ( ${createDefs.filter(Boolean).join(", ")} ) ENGINE = ${resolveEngine(isNodeRoot, meta.pkMappings.length > 0)} ORDER BY ${resolveOrderBy(meta, isNodeRoot)}`,
    ...meta.children.flatMap((child: ISourceMeta) => translateCH(database, child, meta, rootMeta || meta))
    // @formatter:on
  ]
}

export const listTableNames = (meta: ISourceMeta): string[] => [
  meta.sqlTableName,
  ...meta.children.flatMap(listTableNames),
]

export const dropStreamTablesQueries = (meta: ISourceMeta): string[] => [
  `DROP TABLE if exists ${meta.sqlTableName}`,
  ...meta.children.flatMap(dropStreamTablesQueries),
]

export const toQualifiedType = (mapping: ColumnMap): string => {
  const modifiers: string[] = [
    mapping.nullable ? `Nullable` : null,
    mapping.lowCardinality ? `LowCardinality` : null,
    mapping.nestedArray ? `Array` : null,
  ].filter(Boolean) as string[]
  return modifiers.reduce(
    (acc, modifier) => `${modifier}(${acc})`,
    mapping.chType,
  ) ?? "undefined type"
}

const mapToColumn = (col: ColumnMap): Column => ({
  name: unescape(col.sqlIdentifier) ?? "",
  type: toQualifiedType(col),
  is_in_sorting_key: false,
})

const pkMapToColumn = (col: ColumnMap): Column => ({
  ...mapToColumn(col),
  is_in_sorting_key: true,
})

// Remove magic quotes around values
const unescape = (v: string) => v.replace(/`/g, "")

// Naïve impl. Could speed up process by sorting and iterating only once
export function getColumnsIntersections(existingCols: Column[], requiredCols: Column[]): {
  missing: Column[];
  obsolete: Column[];
  modified: { existing: Column; new: Column }[]
} {
  const missing = requiredCols.filter((required) =>
    existingCols.find((existing) => existing.name === required.name) === undefined,
  )
  const modified = existingCols.reduce((acc: { existing: Column, new: Column }[], existing) => {
    const matching = requiredCols.find((required) => required.name === existing.name && required.type !== existing.type)
    if (matching) {
      return [...acc, {
        existing,
        new: matching,
      }]
    } else {
      return acc
    }
  }, [])
  const obsolete = existingCols.filter((existing) =>
    (requiredCols.find((required) => required.name === existing.name)) === undefined,
  )
  return {
    missing,
    modified,
    obsolete,
  }
}

export async function updateSchema(meta: ISourceMeta, ch: ClickhouseConnection) {
  await Promise.all(meta.children.map((child) => updateSchema(child, ch)))

  const isRoot = meta.pkMappings.filter((pkMap) => pkMap.pkType === PKType.ROOT).length == 0
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
    .concat(meta.pkMappings
      .filter((pkMap) => !isRoot && (pkMap.pkType === PKType.CURRENT || pkMap.pkType === PKType.PARENT)).map(mapToColumn)) // to handle properties added by "all_key_properties
    .concat(meta.simpleColumnMappings.map(mapToColumn))
    .concat(!isRoot || (isRoot && meta.pkMappings.find((pk) => pk.pkType === PKType.CURRENT) !== undefined) ? [{
      name: isRoot ? "_ver" : "_root_ver",
      type: "UInt64",
      is_in_sorting_key: false,
    }] : [])


  const intersections = getColumnsIntersections(existingColumns, expectedColumns)

  const added = (await Promise.all(intersections.missing.map((elem) => ch.addColumn(meta.sqlTableName, elem))))
    .map((res) => mapLeft(res, (ctx) =>
      `Could not create column ${ctx.new.name} ${ctx.new.type}`,
    ))

  const updated = (await Promise.all(intersections.modified.map((elem) => ch.updateColumn(meta.sqlTableName, elem.existing, elem.new))))
    .map((res) => mapLeft(res, (ctx) =>
      `Could not update column ${ctx.new.name} from ${ctx.existing.type} to ${ctx.new.type}`,
    ))

  const removed = (await Promise.all(intersections.obsolete.map((elem) => ch.removeColumn(meta.sqlTableName, elem))))
    .map((res) => mapLeft(res, (ctx) =>
      `Could not drop column ${ctx.existing.name} ${ctx.existing.type}`,
    ))

  const errors = [...listLeft(added), ...listLeft(updated), ...listLeft(removed)]
  errors.forEach((it) => log_error(it))
  if (errors.length > 0) {
    throw new Error("Could not update table")
  }
}
