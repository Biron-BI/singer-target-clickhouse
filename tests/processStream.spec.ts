import {strict as assert} from 'assert'
import * as fs from "fs"
import {processStream} from "../src/processStream"
import {StartedTestContainer} from "testcontainers"
import {LogLevel, set_level} from "singer-node"
import {bootClickhouseContainer, runChQueryInContainer} from "./helpers"
import {Config} from '../src/Config'
import {List} from "immutable"

const initialConnInfo = new Config({
  host: "localhost",
  username: "root",
  password: "azertyuiop",
  port: 8123,
  database: "datbayse",
  // max_batch_rows: 1,
})


describe("processStream - Schemas", () => {
  let container: StartedTestContainer
  let connInfo: Config
  beforeEach(async function () {
    this.timeout(30000)
    try {
      container = await bootClickhouseContainer(initialConnInfo)
      connInfo = new Config({
        ...initialConnInfo,
        port: container.getMappedPort(initialConnInfo.port)
      })
    } catch (err) {
      console.log("err", err);
    }
    set_level(LogLevel.INFO)
  });

  afterEach(async function () {
    await container.stop();
  });

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

  it('should throw if schemas already exists and new is different', async () => {
    await processStream(fs.createReadStream("./tests/data/stream_1.jsonl"), connInfo)
    await assert.rejects(async () => {
      await processStream(fs.createReadStream("./tests/data/stream_1_modified.jsonl"), connInfo)
    }, Error)
  }).timeout(30000)

  it('should recreate if schemas already exists, new is different but specified to be recreated', async () => {
    await processStream(fs.createReadStream("./tests/data/stream_1.jsonl"), connInfo)

    const config = new Config({...connInfo}, List(["tickets"]))
    await processStream(fs.createReadStream("./tests/data/stream_1_modified.jsonl"), config)
    const execResult = await runChQueryInContainer(container, connInfo, `show tables from ${connInfo.database}`)

    assert.equal(execResult.output.split("\n").length, 22)
  }).timeout(30000)

}).timeout(30000)

describe("processStream - Records", () => {
  let container: StartedTestContainer
  let connInfo: Config
  beforeEach(async function () {
    this.timeout(30000)
    try {
      container = await bootClickhouseContainer(initialConnInfo);
      connInfo = new Config({
        ...initialConnInfo,
        port: container.getMappedPort(initialConnInfo.port)
      })
    } catch (err) {
      console.log("err", err);
    }
    set_level(LogLevel.INFO)
  });

  afterEach(async function () {
    await container.stop();
  });

  it('should insert simple records', async () => {
    await processStream(fs.createReadStream("./tests/data/stream_short.jsonl"), connInfo)
    const execResult = await runChQueryInContainer(container, connInfo, `select brand_id from tickets where assignee_id = 11`)
    assert.equal(execResult.output, '22\n')

  }).timeout(30000)

  it('should ingest stream from real data', async () => {
    await processStream(fs.createReadStream("./tests/data/covidtracker.jsonl"), connInfo)
    let execResult = await runChQueryInContainer(container, connInfo, `select sum(total_rows) from system.tables where database = '${connInfo.database}'`)
    assert.equal(execResult.output, '5789\n')

    // Ensure no duplicates are created when run second time
    await processStream(fs.createReadStream("./tests/data/covidtracker.jsonl"), connInfo)
    execResult = await runChQueryInContainer(container, connInfo, `select sum(total_rows) from system.tables where database = '${connInfo.database}'`)
    assert.equal(execResult.output, '5789\n')

  }).timeout(60000)

  it('should handle cleanFirst', async () => {
    await processStream(fs.createReadStream("./tests/data/stream_vanilla.jsonl"), connInfo)
    let execResult = await runChQueryInContainer(container, connInfo, `select count() from \`users\``)
    assert.equal(execResult.output, '4\n')

    await processStream(fs.createReadStream("./tests/data/stream_cleanFirst.jsonl"), connInfo)
    execResult = await runChQueryInContainer(container, connInfo, `select count() from \`users\``)
    assert.equal(execResult.output, '2\n')

  }).timeout(60000)

  it('should handle cleaning column', async () => {
    await processStream(fs.createReadStream("./tests/data/stream_vanilla.jsonl"), connInfo)
    let execResult = await runChQueryInContainer(container, connInfo, `select count() from \`users\``)
    assert.equal(execResult.output, '4\n')

    await processStream(fs.createReadStream("./tests/data/stream_cleaningColumn.jsonl"), connInfo)
    execResult = await runChQueryInContainer(container, connInfo, `select count() from \`users\``)
    assert.equal(execResult.output, '5\n')

    execResult = await runChQueryInContainer(container, connInfo, `select id from \`users\` where name = 'bill'`)
    assert.equal(execResult.output, '7\n')


  }).timeout(60000)

  it('should handle record when schema specifiesPK', async () => {
    await processStream(fs.createReadStream("./tests/data/stream_short_with_all_pk.jsonl"), connInfo)
    let execResult = await runChQueryInContainer(container, connInfo, `describe table ${connInfo.database}.tickets__follower_ids`)
    const rows = execResult.output.split("\n")
    assert.equal(rows[0].includes("_root_id"), true)
    assert.equal(rows[1].includes("_parent_id"), true)
    assert.equal(rows[2].includes("_level_0_index"), true)

    execResult = await runChQueryInContainer(container, connInfo, `select count() from \`tickets\``)
    assert.equal(execResult.output, '1\n')
    execResult = await runChQueryInContainer(container, connInfo, `select count() from \`tickets__follower_ids\``)
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

    execResult = await runChQueryInContainer(container, connInfo, `select count() from \`tickets\``)
    assert.equal(execResult.output, '1\n')
    execResult = await runChQueryInContainer(container, connInfo, `select count() from \`tickets__follower_ids\``)
    assert.equal(execResult.output, '2\n')
  }).timeout(30000)

}).timeout(30000)

