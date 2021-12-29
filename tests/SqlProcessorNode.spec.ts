import {List} from "immutable"
import {strict as assert} from "assert"
import SqlProcessorNode from "../src/SqlProcessorNode"
import {ISourceMeta} from "../src/jsonSchemaInspector"
import {streamToStrList} from "./helpers"

const simpleMeta: ISourceMeta = {
  pkMappings: List(),
  simpleColumnMappings: List([{
    nullable: false,
    prop: "id",
    sqlIdentifier: "`id`",
    chType: "Int32",
    type: "integer",
  }, {
    nullable: true,
    prop: "name",
    sqlIdentifier: "`name`",
    chType: "String",
    type: "string",
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
    nullable: false,
  }]),
  simpleColumnMappings: List([{
    nullable: true,
    prop: "name",
    sqlIdentifier: "`name`",
    chType: "String",
    type: "string",
  }]),
  children: List([
    {...simpleMeta, sqlTableName: "`order_child`", tableName: "order_child"},
    {...simpleMeta, sqlTableName: "`order_child_2`", tableName: "order_child_2"},
  ]),
  sqlTableName: "`order`",
}

describe("SqlProcessNode", () => {
  describe("buildInsertQuery", () => {
    it("should handle simple meta", async () => {
      const res = new SqlProcessorNode(simpleMeta, List(["id", "name"]), List([1, "BVEAS2124", 2, "PUYHDR"])).buildInsertQuery()
      assert.equal(res.size, 1)
      assert.notEqual(res.get(0), undefined)
      assert.equal(res.get(0)?.baseQuery, "INSERT INTO `order` FORMAT JSONCompactEachRow")

      const streamContent = await streamToStrList(res.get(0)!!.stream)
      assert.equal(streamContent.size, 1)
      assert.equal(streamContent.get(0), '[1,"BVEAS2124"][2,"PUYHDR"]')
    })

    it("should handle complex meta", async () => {
      const res = new SqlProcessorNode(metaWithPKAndChildren, List(["id", "name"]), List([1, "BVEAS2124", 2, "PUYHDR"]),
        List([
          new SqlProcessorNode(metaWithPKAndChildren.children.get(0)!!, List(["id", "name"]), List([1, "a", 2, "b"])),
          new SqlProcessorNode(metaWithPKAndChildren.children.get(1)!!, List(["id", "name"]), List([1, "c", 2, "d"]))
        ])).buildInsertQuery()
      assert.equal(res.size, 3)

      assert.notEqual(res.get(0), undefined)
      assert.equal(res.get(0)?.baseQuery, "INSERT INTO `order` FORMAT JSONCompactEachRow")
      assert.equal(res.get(1)?.baseQuery, "INSERT INTO `order_child` FORMAT JSONCompactEachRow")
      assert.equal(res.get(2)?.baseQuery, "INSERT INTO `order_child_2` FORMAT JSONCompactEachRow")

      assert.equal((await streamToStrList(res.get(1)!!.stream)).get(0), '[1,"a"][2,"b"]')
      assert.equal((await streamToStrList(res.get(2)!!.stream)).get(0), '[1,"c"][2,"d"]')
    })
  })
})
