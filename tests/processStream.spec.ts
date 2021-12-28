import {strict as assert} from 'assert'
import * as fs from "fs"
import {Config, processStream} from "../src/processStream"
import {StartedTestContainer} from "testcontainers"
import {set_level} from "singer-node"
import {bootClickhouseContainer, runChQueryInContainer} from "./helpers"

const connInfo: Config = {
  host: "localhost",
  user: "root",
  password: "azertyuiop",
  port: 8123,
  database: "datbayse"
}


describe("processStream - Schemas", () => {
  let container: StartedTestContainer
  beforeEach(async function () {
    this.timeout(30000)
    try {
      connInfo.port = 8123
      container = await bootClickhouseContainer(connInfo);
      connInfo.port = container.getMappedPort(connInfo.port)
    } catch (err) {
      console.log("err", err);
    }
    set_level("trace")
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

}).timeout(30000)
