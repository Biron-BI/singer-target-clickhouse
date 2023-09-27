import {strict as assert} from 'assert'
import * as fs from "fs"
import {processStream} from "../src/processStream"
import {StartedTestContainer} from "testcontainers"
import {LogLevel, set_level} from "singer-node"
import {bootClickhouseContainer, runChQueryInContainer} from "./helpers"
import {Config} from '../src/Config'

const initialConnInfo = new Config({
  host: "localhost",
  username: "root",
  password: "azertyuiop",
  port: 8123,
  database: "datbayse",
  // max_batch_rows: 10,
})

describe("processStream", () => {
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
  beforeEach(async function () {
    this.timeout(30000)
  })

  afterEach(async function () {
    this.timeout(30000)
    // can't drop / recreate nor truncate database so we manually drop tables
    // await runChQueryInContainer(container, connInfo, `TRUNCATE DATABASE ${connInfo.database}`)

    const execResult = await runChQueryInContainer(container, connInfo, `show tables from ${connInfo.database}`)
    const tables = execResult.output.split("\n").filter(Boolean)
    await Promise.all(tables.map(async (table) => {
      await runChQueryInContainer(container, connInfo, `DROP TABLE ${table}`)
    }))

  })

  after(async function () {
    await container.stop()
  });
  describe("Schemas", () => {
    it('should create schemas', async () => {
      await processStream(fs.createReadStream("./tests/data/stream_1.jsonl"), connInfo)
      let execResult = await runChQueryInContainer(container, connInfo, `show tables from ${connInfo.database}`)
      assert.equal(execResult.output.split("\n").length, 22)
      assert.equal(execResult.output.includes("ticket_audits"), true)
      assert.equal(execResult.output.includes("ticket_audits__events__attachments"), true)
      assert.equal(execResult.output.includes("ticket_audits__metadata__notifications_suppressed_for"), true)
      assert.equal(execResult.output.includes("tickets"), true)
      assert.equal(execResult.output.includes("tickets__custom_fields"), true)
    }).timeout(30000)

    it('should create schemas which specifies cardinality', async () => {
      await processStream(fs.createReadStream("./tests/data/stream_cardinality.jsonl"), connInfo)
      let execResult = await runChQueryInContainer(container, connInfo, `show tables from ${connInfo.database}`)
      assert.equal(execResult.output.split("\n").length, 2)
      assert.equal(execResult.output.includes("users"), true)
      execResult = await runChQueryInContainer(container, connInfo, `show create table users`)
      assert.equal(execResult.output.includes("`name` LowCardinality(Nullable(String))"), true)
    }).timeout(30000)

    it('should create schemas which specifiesPK', async () => {
      await processStream(fs.createReadStream("./tests/data/stream_schema_with_all_pk.jsonl"), connInfo)
      let execResult = await runChQueryInContainer(container, connInfo, `describe table ${connInfo.database}.tickets__follower_ids`)
      const rows = execResult.output.split("\n")
      assert.equal(rows[0].includes("_root_id"), true)
      assert.equal(rows[1].includes("_parent_id"), true)
      assert.equal(rows[2].includes("_level_0_index"), true)
    }).timeout(30000)

    it('should do nothing if schemas already exists', async () => {
      await processStream(fs.createReadStream("./tests/data/stream_1.jsonl"), connInfo)
      await processStream(fs.createReadStream("./tests/data/stream_1.jsonl"), connInfo)
      const execResult = await runChQueryInContainer(container, connInfo, `show tables from ${connInfo.database}`)

      assert.equal(execResult.output.split("\n").length, 22)
    }).timeout(30000)

    it('should create / update / delete columns if schema already exists and new has different columns', async () => {
      await processStream(fs.createReadStream("./tests/data/stream_1.jsonl"), connInfo)
      await processStream(fs.createReadStream("./tests/data/stream_1_modified.jsonl"), connInfo)
      let execResult = await runChQueryInContainer(container, connInfo, `show tables from ${connInfo.database}`)

      assert.equal(execResult.output.split("\n").length, 22)

      execResult = await runChQueryInContainer(container, connInfo, `select name, type
                                                                     from system.columns
                                                                     where table = 'tickets'
                                                                       and database = '${initialConnInfo.database}'
                                                                     order by name`)

      // @ts-ignore
      const columns: string[] = execResult.output.split("\n").map(it => it.replaceAll("\t", " "))

      // ensure col is created, updated and deleted
      assert.equal(columns.includes("organization_id Nullable(String)"), true)
      assert.equal(columns.includes("new_requester_id Nullable(Int64)"), true)
      assert.equal(columns.includes("requester_id Nullable(Int64)"), false)

    }).timeout(30000)

    it("should rename tables as dropped when they are no longer active, and exclude dropped and archived", async () => {
      await processStream(fs.createReadStream("./tests/data/stream_1.jsonl"), connInfo)

      await processStream(fs.createReadStream("./tests/data/stream_1_inactive.jsonl"), connInfo)
      let execResult = await runChQueryInContainer(container, connInfo, `show tables from ${connInfo.database}`)
      let tables = execResult.output.split("\n").filter(Boolean)
      assert.equal(tables.length, 21)
      tables.forEach((table) => {
        if (!table.startsWith("ticket_audits")) {
          assert.equal(table.startsWith("_dropped_"), true, `table ${table} should start with dropped`)
        } else {
          assert.equal(table.startsWith("_dropped_"), false, `table ${table} should not start with dropped`)
        }
      })

      await processStream(fs.createReadStream("./tests/data/stream_1_inactive.jsonl"), connInfo)
      execResult = await runChQueryInContainer(container, connInfo, `show tables from ${connInfo.database}`)
      tables = execResult.output.split("\n").filter(Boolean)
      assert.equal(tables.length, 21)
      tables.forEach((table) => {
        if (!table.startsWith("ticket_audits")) {
          assert.equal(table.startsWith("_dropped_"), true, `table ${table} should start with dropped`)
        } else {
          assert.equal(table.startsWith("_dropped_"), false, `table ${table} should not start with dropped`)
        }
        assert.equal(table.startsWith("_dropped__dropped_"), false, `table ${table} should not be renamed twice`)
      })

      await runChQueryInContainer(container, connInfo, `RENAME TABLE ${connInfo.database}._dropped_ticket_metrics TO ${connInfo.database}._archived_ticket_metrics`)
      await processStream(fs.createReadStream("./tests/data/stream_1_inactive.jsonl"), connInfo)
      execResult = await runChQueryInContainer(container, connInfo, `show tables from ${connInfo.database}`)
      tables = execResult.output.split("\n").filter(Boolean)
      assert.equal(tables.length, 21)
      tables.forEach((table) => {
        if (!table.startsWith("ticket_audits")) {
          if (table.includes("ticket_metrics")) {
            assert.equal(table.startsWith("_archived_"), true, `table ${table} should start with archived`)
            assert.equal(table.includes("_dropped_"), false, `table ${table} should not include dropped`)
          } else {
            assert.equal(table.startsWith("_dropped_"), true, `table ${table} should start with dropped`)
          }
        } else {
          assert.equal(table.startsWith("_archived_"), false, `table ${table} should not start with archived`)
          assert.equal(table.startsWith("_dropped_"), false, `table ${table} should not start with dropped`)
        }
      })

    }).timeout(30000)

    it('should throw if schema already exists and new has different columns with incompatible type', async () => {
      await processStream(fs.createReadStream("./tests/data/stream_vanilla.jsonl"), connInfo)
      await assert.rejects(async () => {
        await processStream(fs.createReadStream("./tests/data/stream_vanilla_with_incompatible_update.jsonl"), connInfo)
      }, Error)
    }).timeout(30000)

    it('should ignore second schema definition', async () => {
      await processStream(fs.createReadStream("./tests/data/stream_multiple_schema.jsonl"), connInfo)
    }).timeout(30000)

    it('should recreate if schemas already exists, new is different but specified to be recreated', async () => {
      await processStream(fs.createReadStream("./tests/data/stream_1.jsonl"), connInfo)

      const config = new Config({...connInfo}, ["tickets"])
      await processStream(fs.createReadStream("./tests/data/stream_1_modified.jsonl"), config)
      const execResult = await runChQueryInContainer(container, connInfo, `show tables from ${connInfo.database}`)

      assert.equal(execResult.output.split("\n").length, 22)
    }).timeout(30000)

  }).timeout(30000)

  describe("Records", () => {

    it('should insert simple records', async () => {
      await processStream(fs.createReadStream("./tests/data/stream_short.jsonl"), connInfo)
      const execResult = await runChQueryInContainer(container, connInfo, `select brand_id
                                                                           from tickets
                                                                           where assignee_id = 11`)
      assert.equal(execResult.output, '22\n')
    }).timeout(30000)

    it('should allow reordering of schema', async () => {
      await processStream(fs.createReadStream("./tests/data/stream_short.jsonl"), connInfo)
      await processStream(fs.createReadStream("./tests/data/stream_short_reordered.jsonl"), connInfo)
      const execResult = await runChQueryInContainer(container, connInfo, `select brand_id
                                                                           from tickets
                                                                           where assignee_id = 11`)
      assert.equal(execResult.output, '22\n')
    }).timeout(30000)

    it('should ingest stream from real data', async () => {
      await processStream(fs.createReadStream("./tests/data/covidtracker.jsonl"), connInfo)
      let execResult = await runChQueryInContainer(container, connInfo, `select sum(total_rows)
                                                                         from system.tables
                                                                         where database = '${connInfo.database}'`)
      assert.equal(execResult.output, '5789\n')

      // Ensure no duplicates are created when run second time
      await processStream(fs.createReadStream("./tests/data/covidtracker.jsonl"), connInfo)
      execResult = await runChQueryInContainer(container, connInfo, `select sum(total_rows)
                                                                     from system.tables
                                                                     where database = '${connInfo.database}'`)
      assert.equal(execResult.output, '5789\n')

    }).timeout(60000)

    it('should produce same result from real data whether translate value is effective or not', async () => {
      await processStream(fs.createReadStream("./tests/data/covidtracker.jsonl"), {
        ...connInfo,
        translate_values: false,
      })
      const testQuery = `select sum(total_rows), sum(total_bytes)
                                                                         from system.tables
                                                                         where database = '${connInfo.database}'`
      let execResult = await runChQueryInContainer(container, connInfo, testQuery)
      const initialResult = execResult.output

      const otherDb = "otherDB"
      await runChQueryInContainer(container, connInfo, `CREATE DATABASE ${otherDb}`)
      await processStream(fs.createReadStream("./tests/data/covidtracker.jsonl"), {
        ...connInfo,
        database: otherDb,
        translate_values: true,
      })
      execResult = await runChQueryInContainer(container, {
        ...connInfo,
        database: otherDb,
      }, testQuery)
      assert.equal(execResult.output, initialResult)

    }).timeout(60000)

    it('should handle cleanFirst', async () => {
      await processStream(fs.createReadStream("./tests/data/stream_vanilla.jsonl"), connInfo)
      let execResult = await runChQueryInContainer(container, connInfo, `select count()
                                                                         from \`users\``)
      assert.equal(execResult.output, '4\n')

      await processStream(fs.createReadStream("./tests/data/stream_cleanFirst.jsonl"), connInfo)
      execResult = await runChQueryInContainer(container, connInfo, `select count()
                                                                     from \`users\``)
      assert.equal(execResult.output, '2\n')

    }).timeout(60000)

    it('should handle cleaning column', async () => {
      await processStream(fs.createReadStream("./tests/data/stream_vanilla.jsonl"), connInfo)
      let execResult = await runChQueryInContainer(container, connInfo, `select count()
                                                                         from \`users\``)
      assert.equal(execResult.output, '4\n')

      await processStream(fs.createReadStream("./tests/data/stream_cleaningColumn.jsonl"), connInfo)
      execResult = await runChQueryInContainer(container, connInfo, `select count()
                                                                     from \`users\``)
      assert.equal(execResult.output, '5\n')

      execResult = await runChQueryInContainer(container, connInfo, `select id
                                                                     from \`users\`
                                                                     where name = 'bill'`)
      assert.equal(execResult.output, '7\n')


    }).timeout(60000)

    it('should handle record when schema specifiesPK', async () => {
      await processStream(fs.createReadStream("./tests/data/stream_short_with_all_pk.jsonl"), connInfo)
      let execResult = await runChQueryInContainer(container, connInfo, `describe table ${connInfo.database}.tickets__follower_ids`)
      const rows = execResult.output.split("\n")
      assert.equal(rows[0].includes("_root_id"), true)
      assert.equal(rows[1].includes("_parent_id"), true)
      assert.equal(rows[2].includes("_level_0_index"), true)

      execResult = await runChQueryInContainer(container, connInfo, `select count()
                                                                     from \`tickets\``)
      assert.equal(execResult.output, '1\n')
      execResult = await runChQueryInContainer(container, connInfo, `select count()
                                                                     from \`tickets__follower_ids\``)
      assert.equal(execResult.output, '2\n')
    }).timeout(30000)

    it('should handle record when schema specifies complex PK', async () => {
      await processStream(fs.createReadStream("./tests/data/stream_short_with_all_pk2.jsonl"), connInfo)
      let execResult = await runChQueryInContainer(container, connInfo, `describe table ${connInfo.database}.tickets__follower_ids`)
      const rows = execResult.output.split("\n")
      assert.equal(rows[0].includes("_root_id"), true)
      assert.equal(rows[1].includes("_parent_id"), true)
      assert.equal(rows[2].includes("name"), true)
      assert.equal(rows[3].includes("_level_0_index"), true)

      execResult = await runChQueryInContainer(container, connInfo, `select count()
                                                                     from \`tickets\``)
      assert.equal(execResult.output, '1\n')
      execResult = await runChQueryInContainer(container, connInfo, `select count()
                                                                     from \`tickets__follower_ids\``)
      assert.equal(execResult.output, '2\n')
    }).timeout(30000)

  }).timeout(30000)

}).timeout(30000)