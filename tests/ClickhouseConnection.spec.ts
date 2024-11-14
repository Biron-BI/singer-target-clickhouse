import {strict as assert} from 'assert'
import {StartedTestContainer} from "testcontainers"
import {LogLevel, set_log_level} from "singer-node"
import {bootClickhouseContainer, runChQueryInContainer} from "./helpers"
import ClickhouseConnection from "../src/ClickhouseConnection"
import {Config, IConfig} from "../src/Config"
import {isLeft, isRight} from "../src/Either"

const connInfo: IConfig = {
  host: "localhost",
  username: "root",
  password: "azertyuiop",
  port: 8123,
  database: "datbayse",
}


describe("ClickhouseConnection", () => {
  let container: StartedTestContainer
  let ch: ClickhouseConnection

  before(async function () {
    this.timeout(30000)
    try {
      container = await bootClickhouseContainer(connInfo);
      connInfo.port = container.getMappedPort(connInfo.port)
      ch = new ClickhouseConnection(new Config(connInfo))
      await runChQueryInContainer(container, connInfo, "CREATE TABLE `tickets__tags` (`_level_0_index` Int32,`_root_id` Int32,`value` String,`_root_ver` UInt64) ENGINE = MergeTree() ORDER BY (`_level_0_index`,`_root_id`)")
      await runChQueryInContainer(container, connInfo, "CREATE TABLE `tickets` (`id` Nullable(Int32)) ENGINE = MergeTree() ORDER BY tuple()")

      await runChQueryInContainer(container, connInfo, "CREATE TABLE `box` (`id` Nullable(Int32), `width` Int32, `name` String, `to_del` String) ENGINE = MergeTree() ORDER BY tuple()")
      await runChQueryInContainer(container, connInfo, "INSERT INTO `box` VALUES (1, 50, 'box1', 'qwer')")

    } catch (err) {
      console.error("err", err);
    }
    set_log_level(LogLevel.TRACE)
  });

  after(async function () {
    await container.stop();
  });

  it("should list tables", async () => {
    assert.deepEqual((await ch.listTables()).sort(), ["box", "tickets", "tickets__tags"])
  })

  it('should describe table', async () => {
    assert.deepEqual((await ch.listColumns("tickets__tags"))
      .sort((a, b) => a.name.localeCompare(b.name)), [
      {
        "is_in_sorting_key": true,
        "name": "_level_0_index",
        "type": "Int32",
      },
      {
        "is_in_sorting_key": true,
        "name": "_root_id",
        "type": "Int32",
      },
      {
        "is_in_sorting_key": false,
        "name": "_root_ver",
        "type": "UInt64",
      },
      {
        "is_in_sorting_key": false,
        "name": "value",
        "type": "String",
      },
    ])
  })
  describe("addColumn", async () => {
    it('should succeed', async () => {
      const res = await ch.addColumn("box", {
        name: "height",
        type: "Int32",
        is_in_sorting_key: false,
      })
      assert.deepEqual(isRight(res), true)
    })

    it('should fail', async () => {
      const newCol = {
        name: "name",
        type: "Int32",
        is_in_sorting_key: false,
      }
      const res = await ch.addColumn("box", newCol)
      assert.deepEqual(isLeft(res), true)
      if (isLeft(res)) { // if for type inference
        const err = res.left
        assert.deepEqual(err.new, newCol)
      }
    }).timeout(10000)
  })

  describe("updateColumn", () => {
    it('should succeed', async () => {
      const res = await ch.updateColumn("box", {
        name: "width",
        type: "Int32",
        is_in_sorting_key: false,
      }, {
        name: "width",
        type: "LowCardinality(Nullable(UInt64))",
        is_in_sorting_key: false,
      })
      assert.deepEqual(isRight(res), true)
    })

    it('should fail', async () => {
      const existing = {
        name: "name",
        type: "String",
        is_in_sorting_key: false,
      }
      const newCol = {
        name: "name",
        type: "Int32",
        is_in_sorting_key: false,
      }
      const res = await ch.updateColumn("box", existing, newCol)
      assert.deepEqual(isLeft(res), true)
      if (isLeft(res)) { // if for type inference
        const err = res.left
        assert.deepEqual(err.existing, existing)
        assert.deepEqual(err.new, newCol)
      }

    })
  })


  describe("deleteColumn", () => {
    it('should succeed', async () => {
      const res = await ch.removeColumn("box", {
        name: "to_del",
        type: "String",
        is_in_sorting_key: false,
      })
      assert.deepEqual(isRight(res), true)
    })

    it('should fail', async () => {
      const existing = {
        name: "missing",
        type: "String",
        is_in_sorting_key: false,
      }
      const res = await ch.removeColumn("box", existing)
      assert.deepEqual(isLeft(res), true)
      if (isLeft(res)) { // if for type inference
        const err = res.left
        assert.deepEqual(err.existing, existing)
      }

    }).timeout(10000)
  })
}).timeout(10000)
