import {ExtendedJSONSchema7} from "singer-node"
import {List, Range} from "immutable"
import {asArray} from "asArray"
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

  // static defaultPks(schema: IExtendedJSONSchema7, autoPrimaryKeyNames?: List<string>): List<string> {
  //   if (schema.type !== "object") {
  //     return List()
  //   }
  //
  //   return asArray(schema.primaryKey ?? (autoPrimaryKeyNames?.find(autoPrimaryKeyName => schema.properties?.hasOwnProperty(autoPrimaryKeyName) ?? false) ?? []))
  // }

  static defaultTableName(alias: string, parentCtx?: JsonSchemaInspectorContext): string {
    return `${parentCtx ? (`${parentCtx.tableName}__`) : ""}${alias}`
  }

  public isTypeObject() {
    return this.schema.type?.includes("object") ?? false
  }

  public isRoot() {
    return this.parentCtx === undefined
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
  // sqlIdentifierAsFk: string;
  // sqlIdentifierAsRootFk?: string;
}

export interface ISimpleColumnType {
  chType?: string;
  type?: string;
  typeFormat?: string;
  nullable: boolean
}

export type PkMap = IPKMapping & ISimpleColumnType;

export interface ISourceMeta {
  children: ISourceMeta[];
  pkMappings: List<PkMap>;
  prop: string;
  simpleColumnMappings: ColumnMap[];
  sqlTableComment: string;
  sqlTableName: string;
  cleaningColumn?: string;
}

const formatLevelIndexColumn = (level: number) => `_level_${level}_index`
const formatRootPKColumn = (prop: string) => `_root_${prop}`

export function buildMeta(ctx: JsonSchemaInspectorContext): ISourceMeta {
  return {
    prop: ctx.alias,
    sqlTableName: escapeIdentifier(ctx.tableName),
    sqlTableComment: ctx.tableName,
    pkMappings: ctx.isRoot() ? ctx.key_properties.map((prop) => ({
      prop,
      sqlIdentifier: escapeIdentifier(prop),
      ...getSimpleColumnType(ctx, prop),
      nullable: false,
    })) : Range(0, ctx.level).map((value) => {
      const prop = formatLevelIndexColumn(value)
      return {
        prop,
        sqlIdentifier: escapeIdentifier(prop),
        chType: "Int32",
        nullable: false,
      } as PkMap
    }).toList().concat(ctx.key_properties.map((prop) => ({
      prop,
      sqlIdentifier: escapeIdentifier(formatRootPKColumn(prop)),
      ...getSimpleColumnType(ctx, prop),
      nullable: false,
    }))),
    ...buildMetaProps(ctx),
    // cleaningColumn: ctx.schema.cleaningColumn,
  }
}

export function flattenNestedObject(propDef: IExtendedJSONSchema7, key: string, ctx: JsonSchemaInspectorContext) {
  // flatten 1..1 relation properties into the current level
  const nestedSchema = {
    type: "object" as JSONSchema7TypeName, // ts is dumb
    // required: (isMandatory(key) ? asArray(propDef.required).map((s: string) => `${key}.${s}`) : List<string>()).toArray(),
    properties: {} as Record<string, JSONSchema7Definition>,
  }
  Object.entries(propDef.properties ?? {}).forEach(([nestedKey, nestedPropDef]) => {
    nestedSchema.properties[`${key}.${nestedKey}`] = nestedPropDef
  })

  return buildMetaProps(new JsonSchemaInspectorContext(
    ctx.alias,
    nestedSchema,
    ctx.key_properties,
    ctx,
    ctx.level,
    ctx.tableName,
  ))
}

export function createSubTable(propDef: IExtendedJSONSchema7, key: string, ctx: JsonSchemaInspectorContext): ISourceMeta {
  return buildMeta(new JsonSchemaInspectorContext(
    key,
    (propDef.items || {type: "string"}) as IExtendedJSONSchema7,
    ctx.key_properties,
    ctx,
    ctx.level + 1,
  ))
}

function buildMetaProps(ctx: JsonSchemaInspectorContext): { children: ISourceMeta[], simpleColumnMappings: ColumnMap[] } {
  let simpleColumnsMappings: ColumnMap[] = []
  let children: ISourceMeta[] = []

  if (ctx.isTypeObject()) {
    Object.entries(ctx.schema.properties ?? {})
      .filter(([key,]) => !ctx.isRoot() || !ctx.key_properties.includes(key))
      .forEach(([key, propDef]) => {
        if (typeof propDef === "boolean") {
          throwError(ctx, "propDef as boolean not supported")
          return // needed as ts doesn't understand throwError will always end up throwing
        }

        // JSON Spec supports multiple types. We accept this format but only handle one at a time
        const propDefTypes = asArray(propDef.type)

        if (propDefTypes.includes("object")) {
          const {simpleColumnMappings: nestedSimpleColumnMappings, children: nestedChildren} = flattenNestedObject(propDef, key, ctx)
          simpleColumnsMappings = simpleColumnsMappings.concat(nestedSimpleColumnMappings)
          children = children.concat(nestedChildren)
        } else if (propDefTypes.includes("array")) {
          children.push(createSubTable(propDef, key, ctx))
        } else {
          const colType = getSimpleColumnType(ctx, key)

          // Column is a scalar value
          if (colType) {
            simpleColumnsMappings.push({
              prop: key,
              sqlIdentifier: escapeIdentifier(key),
              ...colType,
            })
          } else {
            throwError(ctx, `${key}: unsupported type [${propDef.type}]`)
          }
        }
      })

  } else {
    simpleColumnsMappings.push({
      sqlIdentifier: escapeIdentifier("value"),
      ...getSimpleColumnType(ctx, undefined),
      nullable: false,
    })
  }
  return {simpleColumnMappings: simpleColumnsMappings, children}
}


function excludeNullFromArray(array?: JSONSchema7TypeName | JSONSchema7TypeName[]) {
  return asArray(array).filter((type) => type !== "null")
}

export function getSimpleColumnType(ctx: JsonSchemaInspectorContext, key?: string): ISimpleColumnType | undefined {
  const propDef: IExtendedJSONSchema7 = key ? ctx.schema.properties?.[key] as ExtendedJSONSchema7 : ctx.schema
  const type = getSimpleColumnSqlType(ctx, propDef, key)

  return type ? {
    type: excludeNullFromArray(propDef.type).get(0),
    typeFormat: propDef.format,
    chType: type,
    nullable: asArray(propDef.type).includes("null") ?? false,
  } : undefined
}

/*
    Expects format: precision:10,scale:2
 */
export function parseCustomFormat(format: string): { [k: string]: string } {
  const regex = /([a-zA-Z]+):([a-zA-Z0-9]+)/g
  const ret: { [k: string]: string } = {}

  let match: any
  while (match = regex.exec(format)) {
    ret[match[1]] = match[2]
  }

  return ret
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
      return "Int32"
    } else if (format === "int64") {
      return "Int64"
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
      return `Decimal(${propDef.precision || 10},${propDef.decimals || 2})`
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
  id = id.replace(/\./g, "_")
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
