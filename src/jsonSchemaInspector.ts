import {ExtendedJSONSchema7, log_warning, SchemaKeyProperties} from "singer-node"
import {List, Map, Range, Record as ImmutableRecord} from "immutable"
import {JSONSchema7Definition, JSONSchema7TypeName} from "json-schema"
import {asArray} from "./utils"

// eslint-disable-next-line @typescript-eslint/no-var-requires
const sha1 = require('sha1')

// To use some non-standard property
export interface IExtendedJSONSchema7 extends ExtendedJSONSchema7 {
  decimals?: number;
  precision?: number;
}

export class JsonSchemaInspectorContext {

  constructor(
    public readonly alias: string,
    public readonly schema: IExtendedJSONSchema7,
    public readonly keyProperties: List<string>, // For current level. Only root has if all_key_properties isn't defined
    public readonly subtableSeparator = "__",
    public readonly parentCtx?: JsonSchemaInspectorContext,
    public readonly level: number = 0,
    public readonly tableName = JsonSchemaInspectorContext.defaultTableName(alias, subtableSeparator, parentCtx),
    public readonly cleaningColumn?: string,
    // Optional config to know all key properties at this level and lower. Used to compute _parent_... fields
    public readonly allKeyProperties: SchemaKeyProperties = {props: List(), children: Map()},
  ) {
  }

  static defaultTableName(alias: string, subtableSeparator: string, parentCtx?: JsonSchemaInspectorContext): string {
    return `${parentCtx ? `${parentCtx.tableName}${subtableSeparator}` : ""}${alias}`
  }

  public isTypeObject() {
    return this.schema.type?.includes("object") ?? false
  }

  public isRoot() {
    return this.parentCtx === undefined
  }

  public getRootContext(): JsonSchemaInspectorContext {
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    return this.isRoot() ? this : this.parentCtx!.getRootContext()
  }
}

export interface ISimpleColumnType {
  chType?: string;
  type?: JSONSchema7TypeName;
  typeFormat?: string;
  nullable: boolean
}

export interface IColumnMapping {
  prop?: string;
  sqlIdentifier: string;
}

export type ColumnMap = IColumnMapping & ISimpleColumnType;

// eslint-disable-next-line no-shadow
export enum PKType {
  ROOT = "ROOT",
  PARENT = "PARENT",
  CURRENT = "CURRENT",
  LEVEL = "LEVEL",
}

export interface IPKMapping {
  prop: string;
  sqlIdentifier: string;
  pkType: PKType
}

export type PkMap = IPKMapping & ISimpleColumnType;

export interface ISourceMeta {
  prop: string
  children: List<ISourceMeta>;
  pkMappings: List<PkMap>;
  simpleColumnMappings: List<ColumnMap>;
  sqlTableName: string;
  cleaningColumn?: string;
}

export const formatLevelIndexColumn = (level: number) => `_level_${level}_index`
export const formatRootPKColumn = (prop: string) => `_root_${prop}`
export const formatParentPKColumn = (prop: string) => `_parent_${prop}`

const buildMetaPkProp = (prop: string, ctx: JsonSchemaInspectorContext, pkType: PKType, fieldFormatter?: (v: string) => string): PkMap => ({
  prop,
  sqlIdentifier: escapeIdentifier(fieldFormatter?.(prop) ?? prop, ctx.subtableSeparator),
  ...getSimpleColumnType(ctx, prop),
  nullable: false,
  pkType,
})

const buildMetaPkProps = (ctx: JsonSchemaInspectorContext): List<PkMap> => List<PkMap>()
  // Append '_root_X'
  .concat(ctx.isRoot() ? List<PkMap>() : ctx.getRootContext().keyProperties.map((prop => buildMetaPkProp(prop, ctx.getRootContext(), PKType.ROOT, formatRootPKColumn))))
  // Append '_parent_X' if parent has 'all_key_properties' filled with props
  // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
  .concat((!ctx.parentCtx?.allKeyProperties?.props.isEmpty() && ctx.parentCtx?.keyProperties.map((prop => buildMetaPkProp(prop, ctx.parentCtx!, PKType.PARENT, formatParentPKColumn)))) || List<PkMap>())
  // Append 'X' if defined
  .concat(ctx.keyProperties.map((prop => buildMetaPkProp(prop, ctx, PKType.CURRENT))))
  // Append 'level_N_index' columns
  .concat(Range(0, ctx.level).map((value) => {
    const prop = formatLevelIndexColumn(value)
    return {
      prop,
      sqlIdentifier: escapeIdentifier(prop, ctx.subtableSeparator),
      chType: "Int32",
      nullable: false,
      pkType: PKType.LEVEL,
    } as PkMap
  }).toList())

// transform a schema to a data structure with metadata for Clickhouse (types, primary keys, nullable, ...)
export const buildMeta = (ctx: JsonSchemaInspectorContext): ISourceMeta => ({
  prop: ctx.alias,
  sqlTableName: escapeIdentifier(ctx.tableName, ctx.subtableSeparator),
  pkMappings: buildMetaPkProps(ctx),
  cleaningColumn: ctx.cleaningColumn,
  ...buildMetaProps(ctx),
})

function makeNullable(type?: JSONSchema7TypeName | JSONSchema7TypeName[]): JSONSchema7TypeName[] {
  if (!type) {
    return []
  }
  return asArray(type)
    .toArray()
    .concat(!type.includes("null") ? ["null"] : [])
}

// flatten 1..1 relation properties into the current level
function flattenNestedObject(propDef: IExtendedJSONSchema7, key: string, ctx: JsonSchemaInspectorContext) {
  const NestedSchemaRecord = ImmutableRecord<{
    type: JSONSchema7TypeName,
    properties: Record<string, JSONSchema7Definition>
  }>({type: "object", properties: {}})

  const nullable = getNullable(propDef)
  const nestedSchema = Object.entries(propDef.properties ?? {}).reduce((acc, [nestedKey, nestedPropDef]) => {
    if (typeof nestedPropDef === "boolean") {
      throw new Error("unhandled boolean propdef")
    }
    return acc.set("properties", {
      ...acc.properties,
      [`${key}.${nestedKey}`]: {
        ...nestedPropDef,
        type: nullable ? makeNullable(nestedPropDef.type) : nestedPropDef.type, // if parent is nullable, all children should also be
      },
    })
  }, NestedSchemaRecord({type: "object", properties: {}}))


  return buildMetaProps(new JsonSchemaInspectorContext(
    ctx.alias,
    nestedSchema,
    List(),
    ctx.subtableSeparator,
    ctx,
    ctx.level,
    ctx.tableName,
  ))
}

const createSubTable = (propDef: IExtendedJSONSchema7, key: string, ctx: JsonSchemaInspectorContext): ISourceMeta => buildMeta(new JsonSchemaInspectorContext(
  key,
  (propDef.items || {type: "string"}) as IExtendedJSONSchema7,
  ctx.allKeyProperties.children.get(key)?.props ?? List(),
  ctx.subtableSeparator,
  ctx,
  ctx.level + 1,
  undefined,
  undefined,
  ctx.allKeyProperties.children.get(key),
))

type MetaProps = { children: List<ISourceMeta>, simpleColumnMappings: List<ColumnMap> }

function buildMetaProps(ctx: JsonSchemaInspectorContext): MetaProps {
  if (ctx.isTypeObject()) {
    return Object.entries(ctx.schema.properties ?? {})
      .filter(([key]) => !ctx.keyProperties.includes(key)) // Exclude values already handled in PK
      .reduce((acc: MetaProps, [key, propDef]) => {
        if (typeof propDef === "boolean") {
          throwError(ctx, "propDef as boolean not supported")
          return acc// needed as ts doesn't understand throwError will always end up throwing
        }
        // JSON Spec supports multiple types. We accept this format but only handle one at a time
        const propDefTypes = asArray(propDef.type)

        if (propDefTypes.includes("object")) {
          const {simpleColumnMappings: nestedSimpleColumnMappings, children: nestedChildren} = flattenNestedObject(propDef, key, ctx)
          return {
            ...acc,
            simpleColumnMappings: acc.simpleColumnMappings.concat(nestedSimpleColumnMappings),
            children: acc.children.concat(nestedChildren),
          }
        } else if (propDefTypes.includes("array")) {
          return {
            ...acc,
            children: acc.children.push(createSubTable(propDef, key, ctx)),
          }
        } else {
          const colType = getSimpleColumnType(ctx, key)

          // Column is a scalar value
          if (colType) {
            return {
              ...acc,
              simpleColumnMappings: acc.simpleColumnMappings.push({
                prop: key,
                sqlIdentifier: escapeIdentifier(key, ctx.subtableSeparator),
                ...colType,
              }),
            }
          } else {
            log_warning(`'${ctx.alias}': '${key}': could not be registered (type '${propDef.type}' unrecognized)`)
            return acc
          }
        }
      }, {simpleColumnMappings: List<ColumnMap>(), children: List<ISourceMeta>()})
  } else {
    if (!ctx.schema.type) {
      return {
        simpleColumnMappings: List(),
        children: List(),
      }
    }
    return {
      simpleColumnMappings: List<ColumnMap>([{
        sqlIdentifier: escapeIdentifier("value", ctx.subtableSeparator),
        ...getSimpleColumnType(ctx, undefined),
        nullable: false,
      }]),
      children: List<ISourceMeta>(),
    }
  }
}

// To sanitize "types" keys in JSON Schemas
function excludeNullFromArray(array?: JSONSchema7TypeName | JSONSchema7TypeName[]) {
  return asArray(array).filter((type) => type !== "null")
}

function getNullable(propDef: JSONSchema7Definition) {
  if (typeof propDef === "boolean") {
    throw new Error(`boolean propDef not handled`)
  }
  return asArray(propDef.type).includes("null") ?? false
}

function getSimpleColumnType(ctx: JsonSchemaInspectorContext, key?: string): ISimpleColumnType | undefined {
  const propDef = key ? ctx.schema.properties?.[key] : ctx.schema
  if (!propDef || typeof propDef === "boolean") {
    throwError(ctx, `Key '${key}' does not match any usable prop in schema props '${ctx.schema.properties}'`)
    return
  }
  const type = getSimpleColumnSqlType(ctx, propDef, key)

  return type ? {
    type: excludeNullFromArray(propDef.type).get(0),
    typeFormat: propDef.format,
    chType: type,
    nullable: getNullable(propDef),
  } : undefined
}

// From a JSON Schema type, return Clickhouse type
export function getSimpleColumnSqlType(ctx: JsonSchemaInspectorContext, propDef: IExtendedJSONSchema7, key?: string): string | undefined {
  const type = excludeNullFromArray(propDef.type).get(0)
  const format = propDef.format
  if (type === "string") {
    if (format === "date" || format === "x-excel-date") {
      return "Date"
    } else if (format === "date-time") {
      return "DateTime"
    } else if (format === "date-time64") {
      return "DateTime64"
    } else if (format === "uuid") {
      return "UUID"
    } else {
      return "String"
    }
  } else if (type === "integer") {
    if (!format) {
      return "Int64"
    } else if (format === "int128") {
      return "Int128"
    } else if (format === "int64") {
      return "Int64"
    } else if (format === "int32") {
      return "Int32"
    } else if (format === "int16") {
      return "Int16"
    } else if (format === "int8") {
      return "Int8"
    } else {
      throwError(ctx, `${key}: unsupported integer format [${format}]`)
    }
  } else if (type === "number") {
    if (!format) {
      return `Decimal(${propDef.precision || 16}, ${propDef.decimals || 2})`
    } else if (format === "float64") {
      return "Float64"
    } else if (format === "float32") {
      return "Float32"
    } else {
      throwError(ctx, `${key}: unsupported number format [${format}]`)
    }
  } else if (type === "boolean") {
    if (!format) {
      return "UInt8" // https://clickhouse.tech/docs/en/sql-reference/data-types/boolean/
    } else {
      throwError(ctx, `${key}: unsupported number format [${format}]`)
    }
  } else {
    return undefined
  }
  return undefined
}

// ensure that id is not longer than 64 chars and enclose it within backquotes
export function escapeIdentifier(id: string, subtableSeparator = "__"): string {
  id = id.replace(/\./g, subtableSeparator)
  if (id.length > 64) {
    const uid = sha1(id).substring(0, 10)
    id = id.substring(0, 64 - uid.length - 27) + uid + id.substring(id.length - 27)
  }
  return `\`${id}\``
}

// throws error with additional context
function throwError(ctx: JsonSchemaInspectorContext, msg: string, childAlias?: string): void {
  const alias = `${ctx.alias}${childAlias ? (`.${childAlias}`) : ""}`
  if (ctx.parentCtx) {
    throwError(ctx.parentCtx, msg, alias)
  } else {
    throw new Error(`${alias}: ${msg}`)
  }
}
