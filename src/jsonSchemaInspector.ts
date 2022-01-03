import {ExtendedJSONSchema7, log_warning} from "singer-node"
import {List, Range} from "immutable"
import {asArray} from "utils"
import {JSONSchema7Definition, JSONSchema7TypeName} from "json-schema"

const sha1 = require('sha1')

// To use some non-standard property
export interface IExtendedJSONSchema7 extends ExtendedJSONSchema7 {
  decimals?: number;
  precision?: number;
  // primaryKey?: string[]; // todo pk and cleaning column may be moved elsewhere, in schema message metadata
  // cleaningColumn?: string;
}

export class JsonSchemaInspectorContext {

  constructor(
    public readonly alias: string,
    public readonly schema: IExtendedJSONSchema7,
    public readonly key_properties: List<string>,
    public readonly parentCtx?: JsonSchemaInspectorContext,
    public readonly level: number = 0,
    public readonly tableName = JsonSchemaInspectorContext.defaultTableName(alias, parentCtx),
  ) {
  }

  static defaultTableName(alias: string, parentCtx?: JsonSchemaInspectorContext): string {
    return `${parentCtx ? (`${parentCtx.tableName}__`) : ""}${alias}`
  }

  public isTypeObject() {
    return this.schema.type?.includes("object") ?? false
  }

  public isRoot() {
    return this.parentCtx === undefined
  }

  public getRootContext(): JsonSchemaInspectorContext {
    // @ts-ignore ts failure, undefined check was done by isRoot
    return this.isRoot() ? this : this.parentCtx?.getRootContext()
  }
}

export interface IColumnMapping {
  prop?: string;
  sqlIdentifier: string;
}

export type ColumnMap = IColumnMapping & ISimpleColumnType;

export interface IPKMapping {
  prop: string;
  sqlIdentifier: string;
}

export interface ISimpleColumnType {
  chType?: string;
  type?: JSONSchema7TypeName;
  typeFormat?: string;
  nullable: boolean
}

export type PkMap = IPKMapping & ISimpleColumnType;

export interface ISourceMeta {
  prop: string
  children: List<ISourceMeta>;
  pkMappings: List<PkMap>;
  simpleColumnMappings: List<ColumnMap>;
  // tableName: string
  sqlTableName: string;
  cleaningColumn?: string;
}

export const formatLevelIndexColumn = (level: number) => `_level_${level}_index`
export const formatRootPKColumn = (prop: string) => `_root_${prop}`

// To refactor
const buildMetaPkProps = (ctx: JsonSchemaInspectorContext) => ctx.isRoot() ? ctx.key_properties.map((prop) => ({
  prop,
  sqlIdentifier: escapeIdentifier(prop),
  ...getSimpleColumnType(ctx, prop),
  nullable: false,
})) : ctx.getRootContext().key_properties.map((prop) => ({
  prop,
  sqlIdentifier: escapeIdentifier(formatRootPKColumn(prop)),
  ...getSimpleColumnType(ctx.getRootContext(), prop),
  nullable: false,
})).concat(Range(0, ctx.level).map((value) => {
  const prop = formatLevelIndexColumn(value)
  return {
    prop,
    sqlIdentifier: escapeIdentifier(prop),
    chType: "Int32",
    nullable: false,
  } as PkMap
}).toList())

export const buildMeta = (ctx: JsonSchemaInspectorContext): ISourceMeta => ({
  prop: ctx.alias,
  sqlTableName: escapeIdentifier(ctx.tableName),
  pkMappings: buildMetaPkProps(ctx),
  ...buildMetaProps(ctx),
})

function flattenNestedObject(propDef: IExtendedJSONSchema7, key: string, ctx: JsonSchemaInspectorContext) {
  // flatten 1..1 relation properties into the current level
  const nestedSchema = {
    type: "object" as JSONSchema7TypeName, // ts is dumb
    properties: {} as Record<string, JSONSchema7Definition>,
  }
  Object.entries(propDef.properties ?? {}).forEach(([nestedKey, nestedPropDef]) => {
    nestedSchema.properties[`${key}.${nestedKey}`] = nestedPropDef
  })

  return buildMetaProps(new JsonSchemaInspectorContext(
    ctx.alias,
    nestedSchema,
    List(),
    ctx,
    ctx.level,
    ctx.tableName,
  ))
}

function createSubTable(propDef: IExtendedJSONSchema7, key: string, ctx: JsonSchemaInspectorContext): ISourceMeta {
  return buildMeta(new JsonSchemaInspectorContext(
    key,
    (propDef.items || {type: "string"}) as IExtendedJSONSchema7,
    List(),
    ctx,
    ctx.level + 1,
  ))
}

type MetaProps = { children: List<ISourceMeta>, simpleColumnMappings: List<ColumnMap> }

function buildMetaProps(ctx: JsonSchemaInspectorContext): MetaProps {
  if (ctx.isTypeObject()) {
    return Object.entries(ctx.schema.properties ?? {})
      .filter(([key]) => !ctx.isRoot() || !ctx.key_properties.includes(key))
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
            children: acc.children.push(createSubTable(propDef, key, ctx))
          }
        } else {
          const colType = getSimpleColumnType(ctx, key)

          // Column is a scalar value
          if (colType) {
            return {
              ...acc,
              simpleColumnMappings: acc.simpleColumnMappings.push({
                prop: key,
                sqlIdentifier: escapeIdentifier(key),
                ...colType,
              })
            }
          } else {
            log_warning(`'${ctx.alias}': '${key}': could not be registered (type '${propDef.type}' unrecognized)`)
            return acc
          }
        }
      }, {simpleColumnMappings: List<ColumnMap>(), children: List<ISourceMeta>()})
    // return {simpleColumnMappings: simpleColumnsMappings, children}
  } else {
    if (!ctx.schema.type) {
      return {
        simpleColumnMappings: List(),
        children: List(),
      }
    }
    return {
      simpleColumnMappings: List<ColumnMap>([{
        sqlIdentifier: escapeIdentifier("value"),
        ...getSimpleColumnType(ctx, undefined),
        nullable: false,
      }]),
      children: List<ISourceMeta>(),
    }
  }
}


function excludeNullFromArray(array?: JSONSchema7TypeName | JSONSchema7TypeName[]) {
  return asArray(array).filter((type) => type !== "null")
}

export function getSimpleColumnType(ctx: JsonSchemaInspectorContext, key?: string): ISimpleColumnType | undefined {
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
    nullable: asArray(propDef.type).includes("null") ?? false,
  } : undefined
}

/**
 * according to https://github.com/salviadev/phoenixdoc/wiki/JSON-Schema-Summary
 * @returns string if found, otherwise undefined, can throw error
 */
export function getSimpleColumnSqlType(ctx: JsonSchemaInspectorContext, propDef: IExtendedJSONSchema7, key?: string): string | undefined {
  const type = excludeNullFromArray(propDef.type).get(0)
  const format = propDef.format
  if (type === "string") {
    if (format === "date" || format === "x-excel-date") {
      return "Date"
    } else if (format === "date-time") {
      return "DateTime"
    } else {
      return "String"
    }
  } else if (type === "integer") {
    if (!format) {
      return "Int64"
    } else if (format === "int64") {
      return "Int64"
    } else if (format === "int32") {
      return "Int32"
    } else if (format === "int16") {
      return "Int16"
    } else {
      throwError(ctx, `${key}: unsupported integer format [${format}]`)
    }
  } else if (type === "number") {
    if (!format) {
      // To better comply with json schema, we could changing "precision" and "decimals" to "maximum" and "multipleOf"
      // For now we'll use a custom format
      //          const parsedFormat = parseCustomFormat(propDef.format);
//            return "DECIMAL(" + (parsedFormat.precision || 10) + "," + (parsedFormat.decimals || 2) + ")";
      return `Decimal(${propDef.precision || 16}, ${propDef.decimals || 2})`
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

/**
 * ensure that id is not longer than 64 chars and enclose it within backquotes
 * @param id
 * @return {string}
 */
export function escapeIdentifier(id: string): string {
  id = id.replace(/\./g, "__")
  if (id.length > 64) {
    const uid = sha1(id).substr(0, 10)
    id = id.substr(0, 64 - uid.length - 27) + uid + id.substring(id.length - 27)
  }
  return `\`${id}\``
}

function throwError(ctx: JsonSchemaInspectorContext, msg: string, childAlias?: string): void {
  const alias = `${ctx.alias}${childAlias ? (`.${childAlias}`) : ""}`
  if (ctx.parentCtx) {
    throwError(ctx.parentCtx, msg, alias)
  } else {
    throw new Error(`${alias}: ${msg}`)
  }
}
