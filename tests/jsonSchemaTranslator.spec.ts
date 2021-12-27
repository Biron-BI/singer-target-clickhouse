import {strict as assert} from 'assert'
import {ISourceMeta} from "../src/jsonSchemaInspector"
import {List} from "immutable"
import {translateCH} from "../src/jsonSchemaTranslator"

const simpleMeta: ISourceMeta = {
  pkMappings: List(),
  simpleColumnMappings: List([{
    nullable: false,
    prop: "id",
    sqlIdentifier: "`id`",
    chType: "Int32",
    type: "integer"
  }, {
    nullable: true,
    prop: "name",
    sqlIdentifier: "`name`",
    chType: "String",
    type: "string"
  }]),
  children: List(),
  prop: "",
  sqlTableName: "order",
  sqlTableComment: "",
}

const emptyMeta: ISourceMeta = {
  pkMappings: List(),
  simpleColumnMappings: List(),
  children: List(),
  prop: "",
  sqlTableName: "order",
  sqlTableComment: "",
}

const metaWithPK: ISourceMeta = {
  pkMappings: List([{
    prop: "id",
    sqlIdentifier: "`id`",
    chType: "UInt32",
    type: "integer",
    nullable: false
  }]),
  simpleColumnMappings: List([{
    nullable: true,
    prop: "name",
    sqlIdentifier: "`name`",
    chType: "String",
    type: "string"
  }]),
  children: List(),
  prop: "",
  sqlTableName: "order",
  sqlTableComment: "",
}

const metaWithPKAndChildren: ISourceMeta = {
  pkMappings: List([{
    prop: "id",
    sqlIdentifier: "`id`",
    chType: "UInt32",
    type: "integer",
    nullable: false
  }]),
  simpleColumnMappings: List([{
    nullable: true,
    prop: "name",
    sqlIdentifier: "`name`",
    chType: "String",
    type: "string"
  }]),
  children: List([{...simpleMeta, sqlTableName: "order_child"}]),
  prop: "",
  sqlTableName: "order",
  sqlTableComment: "",
}

describe("translateCH", () => {

  it("should refuse empty meta", () => {
    assert.throws(() => {
      translateCH(emptyMeta)
    }, Error)
  })

  it("should translate basic meta", () => {
    const res = translateCH(simpleMeta)
    assert.equal(res.size, 2)
    assert.equal(res.get(0), "DROP TABLE IF EXISTS order")
    assert.equal(res.get(1), "CREATE TABLE order(`id` Int32,`name` Nullable(String)) ENGINE = MergeTree() ORDER BY (tuple())")
  })

  it("should translate meta with PK", () => {
    const res = translateCH(metaWithPK)
    assert.equal(res.size, 2)
    assert.equal(res.get(0), "DROP TABLE IF EXISTS order")
    assert.equal(res.get(1), "CREATE TABLE order(`id` UInt32,`name` Nullable(String),`_ver` UInt64) ENGINE = ReplacingMergeTree(_ver) ORDER BY (`id`)")
  })

  it("should translate meta with PK and children", () => {
    const res = translateCH(metaWithPKAndChildren)
    assert.equal(res.size, 4)
    assert.equal(res.get(0), "DROP TABLE IF EXISTS order")
    assert.equal(res.get(1), "CREATE TABLE order(`id` UInt32,`name` Nullable(String),`_ver` UInt64) ENGINE = ReplacingMergeTree(_ver) ORDER BY (`id`)")
    assert.equal(res.get(2), "DROP TABLE IF EXISTS order_child")
    assert.equal(res.get(3), "CREATE TABLE order_child(`id` Int32,`name` Nullable(String),`_root_ver` UInt64) ENGINE = MergeTree() ORDER BY (tuple())")
  })
})
