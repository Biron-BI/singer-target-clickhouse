import {List} from "immutable"
import {strict as assert} from "assert"
import RecordProcessor from "../src/RecordProcessor"
import {ColumnMap, ISourceMeta, PkMap, PKType} from "../src/jsonSchemaInspector"
import {bootClickhouseContainer, streamToStrList} from "./helpers"
import ClickhouseConnection from "../src/ClickhouseConnection"
import {StartedTestContainer} from "testcontainers"
import {Config} from "../src/Config"
import {LogLevel, set_level} from "singer-node"

// Represents an id column, for a PK for instance
const id: PkMap = {
  prop: "id",
  sqlIdentifier: "`id`",
  chType: "UInt32",
  type: "integer",
  nullable: false,
  pkType: PKType.CURRENT,
}

// Represents an id column, for a PK for instance
const rootId: PkMap = {
  prop: "_root_id",
  sqlIdentifier: "`_root_id`",
  chType: "UInt32",
  type: "integer",
  nullable: false,
  pkType: PKType.ROOT,
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
  pkType: PKType.LEVEL,
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

const initialConnInfo = new Config({
  host: "localhost",
  username: "root",
  password: "azertyuiop",
  port: 8123,
  database: "datbayse",
  // max_batch_rows: 10,
})

describe("RecordProcessor", () => {
  let container: StartedTestContainer
  let connInfo: Config
  before(async function () {
    this.timeout(30000)
    try {
      container = await bootClickhouseContainer(initialConnInfo)
      connInfo = new Config({
        ...initialConnInfo,
        port: container.getMappedPort(initialConnInfo.port),
      })
    } catch (err) {
      console.log("err", err)
    }
    set_level(LogLevel.INFO)
  })

  after(async function () {
    await container.stop()
  })

  describe("pushRecord", () => {
    it("should handle simple schema and data", async () => {
      const res = new RecordProcessor(simpleMeta, new ClickhouseConnection(connInfo))
        .pushRecord(
          {id: 1, name: "a"}, 0,
        ).pushRecord(
          {id: 2, name: "b"}, 0,
        )
      // @ts-ignore
      res.readStream!.push(null)

      // @ts-ignore
      const values = await streamToStrList(res.readStream!)

      assert.equal(values.get(0), '[1,"a"][2,"b"]')
      assert.equal(res.buildSQLInsertField().get(0), "`id`")
      assert.equal(res.buildSQLInsertField().get(1), "`name`")
    }).timeout(30000)

    it("should feed deep nested children", async () => {
      const res = new RecordProcessor(metaWithPKAndChildren, new ClickhouseConnection(connInfo))
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
      // We use ts ignore to access private members without updating class as we don't want to expose readStream

      // @ts-ignore
      const deepStream = res.children.get("`order__tags`")?.children.get("`order__tags__values`")?.readStream!

      deepStream.push(null)

      const values = await streamToStrList(deepStream)

      // @ts-ignore
      assert.equal(res.children.size, 1)

      assert.deepEqual(values.toArray(), ["[1234,0,0,\"value_a\",51][1234,0,1,\"value_b\",51][1234,0,2,\"value_c\",51][1234,1,0,\"value_d\",51][1234,1,1,\"value_e\",51]"])

      // @ts-ignore
      assert.deepEqual(res.children.get("`order__tags`")?.children.get("`order__tags__values`").buildSQLInsertField().toArray(), [
        "`_root_id`",
        "`_level_0_index`",
        "`_level_1_index`",
        "`name`",
        "`_root_ver`",
      ])
    })
      .timeout(30000)
  }).timeout(30000)
})
