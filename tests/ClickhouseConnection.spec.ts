import {strict as assert} from 'assert'
import {StartedTestContainer} from "testcontainers"
import {LogLevel, set_level} from "singer-node"
import {bootClickhouseContainer, runChQueryInContainer} from "./helpers"
import ClickhouseConnection from "../src/ClickhouseConnection"
import {IConfig} from "../src/Config"

const connInfo: IConfig = {
  host: "localhost",
  username: "root",
  password: "azertyuiop",
  port: 8123,
  database: "datbayse"
}


describe("ClickhouseConnection", () => {
  let container: StartedTestContainer
  let ch: ClickhouseConnection

  before(async function () {
    this.timeout(30000)
    try {
      container = await bootClickhouseContainer(connInfo);
      connInfo.port = container.getMappedPort(connInfo.port)
      ch = new ClickhouseConnection(connInfo)
      await runChQueryInContainer(container, connInfo, "CREATE TABLE `tickets__tags` (`_level_0_index` Int32,`_root_id` Int32,`value` String,`_root_ver` UInt64) ENGINE = MergeTree() ORDER BY (`_level_0_index`,`_root_id`)")
      await runChQueryInContainer(container, connInfo, "CREATE TABLE `tickets` (`id` Nullable(Int32)) ENGINE = MergeTree() ORDER BY tuple()")
    } catch (err) {
      console.log("err", err);
    }
    set_level(LogLevel.TRACE)
  });

  after(async function () {
    await container.stop();
  });

  it ("should list tables", async () => {
    assert.deepEqual((await ch.listTables()).sort().toArray(), ["tickets", "tickets__tags"])
  })

  it('should show create table', async () => {
    assert.equal(await ch.describeCreateTable("tickets__tags"), `CREATE TABLE ${connInfo.database}.tickets__tags ( \`_level_0_index\` Int32, \`_root_id\` Int32, \`value\` String, \`_root_ver\` UInt64 ) ENGINE = MergeTree ORDER BY (_level_0_index, _root_id)`)
  })

  it('should describe table', async () => {
    assert.deepEqual((await ch.listColumns("tickets__tags"))
      .sort((a, b) => a.name.localeCompare(b.name))
      .toArray(), [
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
      }
    ])
  })
}).timeout(10000)
