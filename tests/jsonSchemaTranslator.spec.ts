import {strict as assert} from 'assert'
import {ColumnMap, ISourceMeta, PKType} from "../src/jsonSchemaInspector"
import {getColumnsIntersections, listTableNames, translateCH} from "../src/jsonSchemaTranslator"
import {Column} from "../src/ClickhouseConnection"

const simpleMeta: ISourceMeta = {
  pkMappings: [],
  simpleColumnMappings: [{
    nullable: false,
    prop: "id",
    sqlIdentifier: "`id`",
    chType: "Int32",
    valueExtractor: (data) => parseInt(data.id),
    lowCardinality: false,
  }, {
    nullable: true,
    prop: "name",
    sqlIdentifier: "`name`",
    chType: "String",
    valueExtractor: (data) => data.name,
    lowCardinality: false,
  }],
  children: [],
  sqlTableName: "`order`",
  prop: "order",
}

const emptyMeta: ISourceMeta = {
  pkMappings: [],
  simpleColumnMappings: [],
  children: [],
  sqlTableName: "`order`",
  prop: "order",

}

const metaWithPK: ISourceMeta = {
  pkMappings: [{
    prop: "id",
    sqlIdentifier: "`id`",
    chType: "UInt32",
    valueExtractor: (data) => parseInt(data.id),
    nullable: false,
    pkType: PKType.CURRENT,
    lowCardinality: false,
  }],
  simpleColumnMappings: [{
    nullable: true,
    prop: "name",
    sqlIdentifier: "`name`",
    chType: "String",
    valueExtractor: (data) => data.name,
    lowCardinality: false,
  }],
  children: [],
  sqlTableName: "`order`",
  prop: "order",
}

const metaWithPKAndChildren: ISourceMeta = {
  pkMappings: [{
    prop: "id",
    sqlIdentifier: "`id`",
    chType: "UInt32",
    valueExtractor: (data) => parseInt(data.id),
    nullable: false,
    pkType: PKType.CURRENT,
    lowCardinality: false,
  }],
  simpleColumnMappings: [{
    nullable: true,
    prop: "name",
    sqlIdentifier: "`name`",
    chType: "String",
    valueExtractor: (data) => data.name,
    lowCardinality: false,
  }],
  children: [{...simpleMeta, sqlTableName: "`order_child`"}],
  sqlTableName: "`order`",
  prop: "order",
}

describe("translateCH", () => {

  it("should refuse empty meta", () => {
    assert.throws(() => {
      translateCH("db", emptyMeta)
    }, Error)
  })

  it("should translate basic meta", () => {
    const res = translateCH("db", simpleMeta)
    assert.equal(res.length, 1)
    assert.equal(res[0], "CREATE TABLE db.`order` ( `id` Int32, `name` Nullable(String) ) ENGINE = MergeTree ORDER BY tuple()")
  })

  it("should translate meta with PK", () => {
    const res = translateCH("db", metaWithPK)
    assert.equal(res.length, 1)
    assert.equal(res[0], "CREATE TABLE db.`order` ( `id` UInt32, `name` Nullable(String), `_ver` UInt64 ) ENGINE = ReplacingMergeTree(_ver) ORDER BY `id`")
  })

  it("should translate meta with PK and children", () => {
    const res = translateCH("db", metaWithPKAndChildren)
    assert.equal(res.length, 2)
    assert.equal(res[0], "CREATE TABLE db.`order` ( `id` UInt32, `name` Nullable(String), `_ver` UInt64 ) ENGINE = ReplacingMergeTree(_ver) ORDER BY `id`")
    assert.equal(res[1], "CREATE TABLE db.`order_child` ( `id` Int32, `name` Nullable(String), `_root_ver` UInt64 ) ENGINE = MergeTree ORDER BY tuple()")
  })

  it("should translate cardinality", () => {
    const mappings = simpleMeta.simpleColumnMappings
    const res = translateCH("db",
      {
        ...simpleMeta, simpleColumnMappings: [mappings[0], {...mappings[1], lowCardinality: true}] as ColumnMap[],
      },
    )
    assert.equal(res.length, 1)
    assert.equal(res[0], "CREATE TABLE db.`order` ( `id` Int32, `name` LowCardinality(Nullable(String)) ) ENGINE = MergeTree ORDER BY tuple()")
  })
})

describe("listTableNames", () => {
  it('should list all tables names in a single array', function () {
    assert.deepEqual(listTableNames(metaWithPKAndChildren), ["`order`", "`order_child`"])
  })
})

describe("getColumnsIntersections", () => {
  it('should list intersections', function () {
    const notModified = {
      name: "not_modified",
      type: "1",
      is_in_sorting_key: false,
    }
    const toDelete = {
      name: "to_delete",
      type: "1",
      is_in_sorting_key: false,
    }
    const toModifyFromExisting = {
      name: "to_modify",
      type: "1",
      is_in_sorting_key: false,
    }
    const toAdd = {
      name: "to_add",
      type: "1",
      is_in_sorting_key: false,
    }
    const toModifyFromRequired = {
      name: "to_modify",
      type: "2",
      is_in_sorting_key: false,
    }
    const existing: Column[] = [notModified, toDelete, toModifyFromExisting]
    const required: Column[] = [notModified, toAdd, toModifyFromRequired]
    const res = getColumnsIntersections(existing, required)
    assert.deepEqual(res.missing, [toAdd])
    assert.deepEqual(res.modified, [{existing: toModifyFromExisting, new: toModifyFromRequired}])
    assert.deepEqual(res.obsolete, [toDelete])
  })
})
