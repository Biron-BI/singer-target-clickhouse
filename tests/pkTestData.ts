// Represents an id column, for a PK for instance
import {ColumnMap, ISourceMeta, PkMap, PKType} from "../src/jsonSchemaInspector"
import {Value} from "../src/SchemaTranslator"
import {uselessValueExtractor} from "./helpers"

export const id: PkMap = {
  prop: "id",
  sqlIdentifier: "`id`",
  chType: "UInt32",
  valueExtractor: (data) => parseInt(data.id),
  nullable: false,
  pkType: PKType.CURRENT,
  lowCardinality: false,
}

export const exhaustivePks: PkMap[] = [id, {
  ...id,
  sqlIdentifier: "`name`",
  chType: "String",
  prop: "name",
  valueExtractor: (data) => String(data.name),
}]

// Represents an id column, for a PK for instance
const rootId: PkMap = {
  prop: "_root_id",
  sqlIdentifier: "`_root_id`",
  chType: "UInt32",
  valueExtractor: (data) => parseInt(data._root_id),
  nullable: false,
  pkType: PKType.ROOT,
  lowCardinality: false,
}
// Represents a name column, as a simple column
const name: ColumnMap = {
  nullable: true,
  prop: "name",
  sqlIdentifier: "`name`",
  chType: "String",
  valueExtractor: (data) => data.name,
  lowCardinality: false,
}
// Represents a name column, as a simple column
export const valid: ColumnMap = {
  nullable: false,
  prop: "valid",
  sqlIdentifier: "`valid`",
  chType: "UInt8",
  valueExtractor: (data) => data.valid,
  valueTranslator: (v: Value) => {
    if (v === "true" || v === true || v === 1) {
      return 1
    } else {
      return 0
    }
  },
  lowCardinality: false,
}
export const simpleMeta: ISourceMeta = {
  pkMappings: [],
  simpleColumnMappings: [id, name],
  children: [],
  sqlTableName: "`order`",
  prop: "order",
}
const levelColumn = (lvl: number): PkMap => ({
  prop: `_level_${lvl}_index`,
  sqlIdentifier: `\`_level_${lvl}_index\``,
  chType: "UInt32",
  valueExtractor: () => {
    throw "should never be called"
  },
  nullable: false,
  pkType: PKType.LEVEL,
  lowCardinality: false,
})
export const metaWithPKAndChildren: ISourceMeta = {
  prop: "order",
  pkMappings: [id],
  simpleColumnMappings: [name],
  children: [{
    simpleColumnMappings: [name],
    pkMappings: [
      rootId, levelColumn(0),
    ],
    sqlTableName: "`order__tags`",
    prop: "tags",
    children: [{
      prop: "values",
      sqlTableName: "`order__tags__values`",
      pkMappings: [rootId, levelColumn(0), levelColumn(1)],
      simpleColumnMappings: [name],
      children: [],
    }],
  }],
  sqlTableName: "`order`",
}
export const abort = (err: Error) => {
  throw err
}
export const metaWithNestedValueArray: ISourceMeta = {
  prop: "audits",
  sqlTableName: "`audits`",
  pkMappings: [],
  simpleColumnMappings: [],
  children: [
    {
      prop: "events",
      sqlTableName: "`audits__events`",
      pkMappings: [
        {
          prop: "_level_0_index",
          sqlIdentifier: "`_level_0_index`",
          chType: "Int32",
          valueExtractor: uselessValueExtractor,
          nullable: false,
          pkType: PKType.LEVEL,
          lowCardinality: false,
        },
      ],
      simpleColumnMappings: [],
      children: [
        {
          prop: "previous_value",
          sqlTableName: "`audits__events__previous_value`",
          pkMappings: [
            {
              prop: "_level_0_index",
              sqlIdentifier: "`_level_0_index`",
              chType: "Int32",
              valueExtractor: uselessValueExtractor,
              nullable: false,
              pkType: PKType.LEVEL,
              lowCardinality: false,
            },
            {
              prop: "_level_1_index",
              sqlIdentifier: "`_level_1_index`",
              chType: "Int32",
              valueExtractor: uselessValueExtractor,
              nullable: false,
              pkType: PKType.LEVEL,
              lowCardinality: false,
            },
          ],
          simpleColumnMappings: [
            {
              sqlIdentifier: "`value`",
              valueExtractor: (data) => data,
              chType: "String",
              nullable: false,
              lowCardinality: false,
            },
          ],
          children: [],
        },
      ],
    },
  ],
}