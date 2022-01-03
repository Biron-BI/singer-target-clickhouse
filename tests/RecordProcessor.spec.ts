import {List} from "immutable"
import {strict as assert} from "assert"
import RecordProcessor from "../src/RecordProcessor"
import {ColumnMap, ISourceMeta, PkMap} from "../src/jsonSchemaInspector"
import {streamToStrList} from "./helpers"

// Represents an id column, for a PK for instance
const id: PkMap = {
  prop: "id",
  sqlIdentifier: "`id`",
  chType: "UInt32",
  type: "integer",
  nullable: false,
}

// Represents an id column, for a PK for instance
const rootId: PkMap = {
  prop: "_root_id",
  sqlIdentifier: "`_root_id`",
  chType: "UInt32",
  type: "integer",
  nullable: false,
}

// Represents a name column, as a simple column
const name: ColumnMap = {
  nullable: true,
  prop: "name",
  sqlIdentifier: "`name`",
  chType: "String",
  type: "string",
}

const simpleMeta: ISourceMeta = {
  pkMappings: List(),
  simpleColumnMappings: List([id, name]),
  children: List(),
  sqlTableName: "`order`",
  prop: "order",
}

const levelColumn = (lvl: number): PkMap => ({
  prop: `_level_${lvl}_index`,
  sqlIdentifier: `\`_level_${lvl}_index\``,
  chType: "UInt32",
  type: "integer",
  nullable: false,
})

const metaWithPKAndChildren: ISourceMeta = {
  prop: "order",
  pkMappings: List([id]),
  simpleColumnMappings: List([name]),
  children: List([{
    simpleColumnMappings: List([name]),
    pkMappings: List([
      rootId, levelColumn(0),
    ]),
    sqlTableName: "`order__tags`",
    tableName: "order__tags",
    prop: "tags",
    children: List([{
      prop: "values",
      sqlTableName: "`order__tags__values`",
      tableName: "order__tags__values",
      pkMappings: List([rootId, levelColumn(0), levelColumn(1)]),
      simpleColumnMappings: List([name]),
      children: List(),
    }]),
  }]),
  sqlTableName: "`order`",
}

describe("RecordProcessor", () => {
  describe("buildInsertQuery", () => {
    it("should handle simple meta", async () => {
      const res = new RecordProcessor(simpleMeta, List(["id", "name"]), List([1, "BVEAS2124", 2, "PUYHDR"])).buildInsertQuery()
      assert.equal(res.size, 1)
      assert.notEqual(res.get(0), undefined)
      assert.equal(res.get(0)?.baseQuery, "INSERT INTO `order` FORMAT JSONCompactEachRow")

      const streamContent = await streamToStrList(res.get(0)!!.stream)
      assert.equal(streamContent.size, 1)
      assert.equal(streamContent.get(0), '[1,"BVEAS2124"][2,"PUYHDR"]')
    })

    it("should handle complex meta", async () => {
      const res = new RecordProcessor(metaWithPKAndChildren, List(["id", "name"]), List([1, "BVEAS2124", 2, "PUYHDR"]),
        List([
          new RecordProcessor(metaWithPKAndChildren.children.get(0)!!, List(["id", "name"]), List([1, "a", 2, "b"])),
        ])).buildInsertQuery()
      assert.equal(res.size, 2)

      assert.notEqual(res.get(0), undefined)
      assert.equal(res.get(0)?.baseQuery, "INSERT INTO `order` FORMAT JSONCompactEachRow")
      assert.equal(res.get(1)?.baseQuery, "INSERT INTO `order__tags` FORMAT JSONCompactEachRow")

      assert.equal((await streamToStrList(res.get(1)!!.stream)).get(0), '[1,"a"][2,"b"]')
    })
  })

  describe("pushRecord", () => {
    it("should handle simple schema and data", async () => {
      const res = new RecordProcessor(simpleMeta)
        .pushRecord(
          {id: 1, name: "a"},  0,
        ).pushRecord(
          {id: 2, name: "b"},  0,
        )

      assert.equal(res.values.get(0), 1)
      assert.equal(res.values.get(1), "a")
      assert.equal(res.values.get(2), 2)
      assert.equal(res.values.get(3), "b")
      assert.equal(res.fields.get(0), "`id`")
      assert.equal(res.fields.get(1), "`name`")
    })

    it("should feed deep nested children", async () => {
      const res = new RecordProcessor(metaWithPKAndChildren)
        .pushRecord(
          {
            id: 1234,
            name: "a", tags: [{
              name: "tag_a", values: [{
                name: "value_a",
              }, {
                name: "value_b",
              }, {
                name: "value_c",
              }],
            }, {
              name: "tag_b", values: [{
                name: "value_d",
              }, {
                name: "value_e",
              }],
            }],
          }, 50,
        )

      assert.equal(res.children.size, 1)

      assert.deepEqual(res.children.get(0)?.children.get(0)?.values.toArray(), [
        1234, 0, 0, "value_a", 51,
        1234, 0, 1, "value_b", 51,
        1234, 0, 2, "value_c", 51,
        1234, 1, 0, "value_d", 51,
        1234, 1, 1, "value_e", 51,
      ])
      assert.deepEqual(res.children.get(0)?.children.get(0)?.fields.toArray(), [
        "`_root_id`",
        "`_level_0_index`",
        "`_level_1_index`",
        "`name`",
        "`_root_ver`",
      ])
    })
  })
})
