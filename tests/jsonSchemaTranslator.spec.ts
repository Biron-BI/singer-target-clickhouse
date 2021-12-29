import {strict as assert} from 'assert'
import {ISourceMeta} from "../src/jsonSchemaInspector"
import {List} from "immutable"
import {listTableNames, translateCH} from "../src/jsonSchemaTranslator"

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
  sqlTableName: "`order`",
}

const emptyMeta: ISourceMeta = {
  pkMappings: List(),
  simpleColumnMappings: List(),
  children: List(),
  sqlTableName: "`order`",
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
  sqlTableName: "`order`",
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
  children: List([{...simpleMeta, sqlTableName: "`order_child`", tableName: "order_child"}]),
  sqlTableName: "`order`",
}

describe("translateCH", () => {

  it("should refuse empty meta", () => {
    assert.throws(() => {
      translateCH("db", emptyMeta)
    }, Error)
  })

  it("should translate basic meta", () => {
    const res = translateCH("db", simpleMeta)
    assert.equal(res.size, 1)
    assert.equal(res.get(0), "CREATE TABLE db.`order` ( `id` Int32, `name` Nullable(String) ) ENGINE = MergeTree ORDER BY tuple()")
  })

  it("should translate meta with PK", () => {
    const res = translateCH("db", metaWithPK)
    assert.equal(res.size, 1)
    assert.equal(res.get(0), "CREATE TABLE db.`order` ( `id` UInt32, `name` Nullable(String), `_ver` UInt64 ) ENGINE = ReplacingMergeTree(_ver) ORDER BY `id`")
  })

  it("should translate meta with PK and children", () => {
    const res = translateCH("db", metaWithPKAndChildren)
    assert.equal(res.size, 2)
    assert.equal(res.get(0), "CREATE TABLE db.`order` ( `id` UInt32, `name` Nullable(String), `_ver` UInt64 ) ENGINE = ReplacingMergeTree(_ver) ORDER BY `id`")
    assert.equal(res.get(1), "CREATE TABLE db.`order_child` ( `id` Int32, `name` Nullable(String), `_root_ver` UInt64 ) ENGINE = MergeTree ORDER BY tuple()")
  })
})

describe("listTableNames", () => {
  it('should list all tables names in a single array', function () {
    assert.deepEqual(listTableNames(metaWithPKAndChildren).toArray(), ["`order`", "`order_child`"])
  })
})
