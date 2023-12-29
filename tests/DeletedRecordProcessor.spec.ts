import {strict as assert} from "assert"
import {TestConnection} from "./TestConnection"
import DeletedRecordProcessor from "../src/DeletedRecordProcessor"
import {exhaustivePks, metaWithPKAndChildren, simpleMeta} from "./pkTestData"

describe("DeletedRecordProcessor", () => {
  describe("pushDeletedRecord", async () => {

    it("should throw when no pk defined", async () => {
      const connection = new TestConnection()
      const res = new DeletedRecordProcessor(simpleMeta, connection, {
        batchSize: 1,
        translateValues: false,
      })
      await assert.rejects(async () => {
        await res.pushDeletedRecord({id: 1, name: "a"})
      }, Error)

    }).timeout(30000)

    it("should run correct query with single pk", async () => {
      const connection = new TestConnection()
      const res = new DeletedRecordProcessor(metaWithPKAndChildren, connection, {
        batchSize: 2,
        translateValues: false,
      })
      await res.pushDeletedRecord({id: 1, name: "a"})
      await res.pushDeletedRecord({id: 2, name: "a"})

      assert.equal(connection.queries.length, 1)
      // @ts-ignore
      assert.equal(connection.queries[0].replaceAll("\n", "").replace(/\s+/g, ' ').trim(), "DELETE FROM `order` WHERE (`id`) IN ((1),(2))")

    }).timeout(30000)

    it("should correct handle batch size", async () => {
      const connection = new TestConnection()
      const res = new DeletedRecordProcessor(metaWithPKAndChildren, connection, {
        batchSize: 1,
        translateValues: false,
      })
      await res.pushDeletedRecord({id: 1, name: "a"})
      await res.pushDeletedRecord({id: 2, name: "a"})

      assert.equal(connection.queries.length, 2)
      // @ts-ignore
      assert.equal(connection.queries[0].replaceAll("\n", "").replace(/\s+/g, ' ').trim(), "DELETE FROM `order` WHERE (`id`) IN ((1))")
      // @ts-ignore
      assert.equal(connection.queries[1].replaceAll("\n", "").replace(/\s+/g, ' ').trim(), "DELETE FROM `order` WHERE (`id`) IN ((2))")

    }).timeout(30000)

    it("should run correct query with multiple pk", async () => {
      const connection = new TestConnection()
      const res = new DeletedRecordProcessor({
        ...metaWithPKAndChildren,
        pkMappings: exhaustivePks
      }, connection, {
        batchSize: 2,
        translateValues: false,
      })
      await res.pushDeletedRecord({id: 1, name: "a"})
      await res.pushDeletedRecord({id: 2, name: "b"})

      assert.equal(connection.queries.length, 1)
      // @ts-ignore
      assert.equal(connection.queries[0].replaceAll("\n", "").replace(/\s+/g, ' ').trim(), "DELETE FROM `order` WHERE (`id`,`name`) IN ((1,'a'),(2,'b'))")

    }).timeout(30000)


  }).timeout(30000)
})
