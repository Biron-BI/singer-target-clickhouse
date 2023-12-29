import {strict as assert} from "assert"
import RecordProcessor from "../src/RecordProcessor"
import {sleep} from "./helpers"
import {TestConnection} from "./TestConnection"
import {abort, id, metaWithNestedValueArray, metaWithPKAndChildren, simpleMeta, valid} from "./pkTestData"

describe("RecordProcessor", () => {
  describe("pushRecord", async () => {

    it("should handle simple schema and data", async () => {
      const connection = new TestConnection()
      const res = new RecordProcessor(simpleMeta, connection, {
        batchSize: 1,
        translateValues: false,
        autoEndTimeoutMs: 100,
      })
      res.pushRecord({id: 1, name: "a"}, abort, 0)
      res.pushRecord({id: 2, name: "b"}, abort, 0)

      assert.equal(connection.streams[0].data, '[1,"a"]\n[2,"b"]\n')
      assert.equal(res.buildSQLInsertField()[0], "`id`")
      assert.equal(res.buildSQLInsertField()[1], "`name`")
    }).timeout(30000)

    it("should auto timeout to end ingestion", async () => {
      const connection = new TestConnection()
      const res = new RecordProcessor(simpleMeta, connection, {
        batchSize: 5,
        translateValues: false,
        autoEndTimeoutMs: 200,
      })
      res.pushRecord({id: 1, name: "a"}, abort, 0)
      res.pushRecord({id: 2, name: "b"}, abort, 0)
      await sleep(400)

      assert.equal(connection.streams[0].data, '[1,"a"]\n[2,"b"]\n')
      assert.equal(res.buildSQLInsertField()[0], "`id`")
      assert.equal(res.buildSQLInsertField()[1], "`name`")
    }).timeout(30000)

    it("should handle batch size and end ingestion", async () => {
      const connection = new TestConnection()
      const res = new RecordProcessor(simpleMeta, connection, {
        batchSize: 2,
        translateValues: false,
        autoEndTimeoutMs: 100,
      })
      res.pushRecord({id: 1, name: "a"}, abort, 0)
      res.pushRecord({id: 2, name: "b"}, abort, 0)
      res.pushRecord({id: 3, name: "c"}, abort, 0)

      assert.equal(connection.streams[0].data, '[1,"a"]\n[2,"b"]\n')

      await res.endIngestion()

      assert.equal(connection.streams[0].data, '[1,"a"]\n[2,"b"]\n[3,"c"]\n')
    }).timeout(30000)

    it("should handle value translation", async () => {
      const connection = new TestConnection()
      const res = new RecordProcessor({
        ...simpleMeta,
        simpleColumnMappings: [id, valid],
      }, connection, {
        batchSize: 1,
        translateValues: true,
        autoEndTimeoutMs: 100,
      })
      res.pushRecord({id: 1, valid: "true"}, abort, 0)

      assert.equal(connection.streams[0].data, '[1,1]\n')
    }).timeout(30000)

    it("should not translate", async () => {
      const connection = new TestConnection()
      const res = new RecordProcessor({
        ...simpleMeta,
        simpleColumnMappings: [id, valid],
      }, connection, {
        batchSize: 1,
        translateValues: false,
        autoEndTimeoutMs: 100,
      })
      res.pushRecord({id: 1, valid: "true"}, abort, 0)

      assert.equal(connection.streams[0].data, '[1,"true"]\n')
    }).timeout(30000)


    it("should feed deep nested children", async () => {
      const connection = new TestConnection()
      const res = new RecordProcessor(metaWithPKAndChildren, connection, {
        batchSize: 1,
        translateValues: false,
        autoEndTimeoutMs: 100,
      })
      res.pushRecord(
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
        }, abort, 50,
      )

      assert.deepEqual(connection.streams[0].data, "[1234,\"a\",51]\n")
      assert.deepEqual(connection.streams[1].data, "[1234,0,\"tag_a\",51]\n[1234,1,\"tag_b\",51]\n")
      assert.deepEqual(connection.streams[2].data, "[1234,0,0,\"value_a\",51]\n[1234,0,1,\"value_b\",51]\n[1234,0,2,\"value_c\",51]\n[1234,1,0,\"value_d\",51]\n[1234,1,1,\"value_e\",51]\n")

      // @ts-ignore
      assert.deepEqual(res.children["`order__tags`"]?.children["`order__tags__values`"].buildSQLInsertField(), [
        "`_root_id`",
        "`_level_0_index`",
        "`_level_1_index`",
        "`name`",
        "`_root_ver`",
      ])
    })
      .timeout(30000)

    it("should handle nested value array", async () => {
      const connection = new TestConnection()
      const res = new RecordProcessor(metaWithNestedValueArray, connection, {
        batchSize: 1,
        translateValues: false,
        autoEndTimeoutMs: 100,
      })
      res.pushRecord(
        {events: [{previous_value: "Test"}]}, abort, 0,
      )
      await res.endIngestion()

      assert.equal(connection.streams[0].data, '[]\n')
      assert.equal(connection.streams[1].data, '[0]\n')
      assert.equal(connection.streams[2].data, '[0,0,"Test"]\n')

    }).timeout(30000)

  }).timeout(30000)
})
