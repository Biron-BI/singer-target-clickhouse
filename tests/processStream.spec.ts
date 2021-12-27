import {strict as assert} from 'assert'
import * as fs from "fs"
import {Config, processStream} from "../src/processStream"
import {GenericContainer, StartedTestContainer} from "testcontainers"

const connInfo: Config = {
  host: "localhost",
  user: "root",
  password: "azertyuiop",
  port: 8123,
  database: "datbayse"
}

export async function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}


export async function bootClickhouseContainer(connInfo: Config): Promise<StartedTestContainer> {
  const ret = await new GenericContainer("yandex/clickhouse-server:21.9.2.17")
    .withNetworkMode("host")
    .withEnv("CLICKHOUSE_DB", connInfo.database)
    .withEnv("CLICKHOUSE_USER", connInfo.user)
    .withEnv("CLICKHOUSE_PASSWORD", connInfo.password)
    .withExposedPorts(8123, 9000)
    .start();
  await sleep(3000); // clickhouse needs some more time
  return ret;
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
  });

  after(async function () {
    await container.stop();
  });

  it('should create schemas', async () => {
    await processStream(fs.createReadStream("./tests/data/stream_1.jsonl"), connInfo)
    let execResult = await container.exec(["clickhouse-client", "-u", connInfo.user, "--password="+ connInfo.password, `--query=show tables from ${connInfo.database}`]);
    await sleep(5000) // fixme
    assert.equal(execResult.exitCode, 0)
    console.log('outuput', execResult.output)
    assert.equal(execResult.output.includes("ticket_audits"), true)
    assert.equal(execResult.output.includes("ticket_audits__events__attachments"), true)
    assert.equal(execResult.output.includes("ticket_audits__metadata_notifications_suppressed_for"), true)
    assert.equal(execResult.output.includes("tickets"), true)
    assert.equal(execResult.output.includes("tickets__custom_fields"), true)

    execResult = await container.exec(["clickhouse-client", "-u", connInfo.user, "--password="+ connInfo.password, `--query=show create table ${connInfo.database}.ticket_audits__events__attachments`]);

    assert.equal(execResult.output, 12) // fixme
  }).timeout(30000)
}).timeout(30000)
