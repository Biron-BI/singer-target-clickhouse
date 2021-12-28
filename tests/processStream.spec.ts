import {strict as assert} from 'assert'
import * as fs from "fs"
import {Config, processStream} from "../src/processStream"
import {StartedTestContainer} from "testcontainers"
import {set_level} from "singer-node"
import {bootClickhouseContainer} from "./helpers"

const connInfo: Config = {
  host: "localhost",
  user: "root",
  password: "azertyuiop",
  port: 8123,
  database: "datbayse"
}


describe("processStream", () => {
  let container: StartedTestContainer
  before(async function () {
    this.timeout(30000)
    try {
      container = await bootClickhouseContainer(connInfo);
    } catch (err) {
      console.log("err", err);
    }
    set_level("trace")
  });

  after(async function () {
    await container.stop();
  });

  it('should create schemas', async () => {
    await processStream(fs.createReadStream("./tests/data/stream_1.jsonl"), connInfo)
    let execResult = await container.exec(["clickhouse-client", "-u", connInfo.user, "--password="+ connInfo.password, `--query=show tables from ${connInfo.database}`]);
    assert.equal(execResult.exitCode, 0)
    assert.equal(execResult.output.includes("ticket_audits"), true)
    assert.equal(execResult.output.includes("ticket_audits__events__attachments"), true)
    assert.equal(execResult.output.includes("ticket_audits__metadata_notifications_suppressed_for"), true)
    assert.equal(execResult.output.includes("tickets"), true)
    assert.equal(execResult.output.includes("tickets__custom_fields"), true)
    //
    // execResult = await container.exec(["clickhouse-client", "-u", connInfo.user, "--password="+ connInfo.password, `--query=show create table ${connInfo.database}.ticket_audits__events__attachments`]);
    //
    // assert.equal(execResult.output, 12) // fixme
  }).timeout(30000)


  it('should do nothing if schemas already exists schemas', async () => {
    await processStream(fs.createReadStream("./tests/data/stream_1.jsonl"), connInfo)
    let execResult = await container.exec(["clickhouse-client", "-u", connInfo.user, "--password="+ connInfo.password, `--query=show tables from ${connInfo.database}`]);
    assert.equal(execResult.exitCode, 0)
    assert.equal(execResult.output.split("\n").length, 17)
    await processStream(fs.createReadStream("./tests/data/stream_1.jsonl"), connInfo)
    execResult = await container.exec(["clickhouse-client", "-u", connInfo.user, "--password="+ connInfo.password, `--query=show tables from ${connInfo.database}`]);
    assert.equal(execResult.output.split("\n").length, 17)
  }).timeout(30000)

  it('should throw if schemas already exists and new is different', async () => {
    await processStream(fs.createReadStream("./tests/data/stream_1.jsonl"), connInfo)
    let execResult = await container.exec(["clickhouse-client", "-u", connInfo.user, "--password="+ connInfo.password, `--query=show tables from ${connInfo.database}`]);
    assert.equal(execResult.exitCode, 0)
    assert.equal(execResult.output.split("\n").length, 17)
    await processStream(fs.createReadStream("./tests/data/stream_1_modified.jsonl"), connInfo)
    execResult = await container.exec(["clickhouse-client", "-u", connInfo.user, "--password="+ connInfo.password, `--query=show tables from ${connInfo.database}`]);
    assert.equal(execResult.output.split("\n").length, 17)
  }).timeout(30000)

}).timeout(30000)
