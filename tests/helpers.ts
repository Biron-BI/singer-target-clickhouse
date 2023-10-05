import {GenericContainer, StartedTestContainer} from "testcontainers"
import {strict as assert} from 'assert'
import {IConfig} from "../src/Config"

export async function bootClickhouseContainer(connInfo: IConfig): Promise<StartedTestContainer> {
  const container = await new GenericContainer("clickhouse/clickhouse-server:23.3.13.6")
    .withEnv("CLICKHOUSE_DB", connInfo.database)
    .withEnv("CLICKHOUSE_USER", connInfo.username)
    .withEnv("CLICKHOUSE_PASSWORD", connInfo.password)
    .withExposedPorts(Number(connInfo.port))
    .start()

  // waiting for ch to boot
  for (let i = 0; i < 30; i++) {
    const res = await runChQueryInContainer(container, connInfo, "select 1", false)
    if (res.exitCode === 0) {
      break
    }
    await sleep(100)
  }
  return container
}

export async function runChQueryInContainer(container: StartedTestContainer, connInfo: IConfig, query: string, checkOk = true) {
  const ret = await container.exec(["clickhouse-client", "-u", connInfo.username, "--password=" + connInfo.password, "-d", connInfo.database, `--query=${query}`])

  if (checkOk) {
    assert.equal(ret.exitCode, 0, ret.output)
  }
  return ret
}

export async function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

export const uselessValueExtractor = () => {
  throw "should never be called"
}
