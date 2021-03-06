import {GenericContainer, StartedTestContainer} from "testcontainers"
import {strict as assert} from 'assert'
import {IConfig} from "../src/Config"
import * as readline from "readline"
import {Readable} from "stream"
import {List} from "immutable"

export async function bootClickhouseContainer(connInfo: IConfig): Promise<StartedTestContainer> {
  const container = await new GenericContainer("yandex/clickhouse-server:21.9.2.17")
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
    assert.equal(ret.exitCode, 0)
  }
  return ret
}

export async function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

export async function streamToStrList(stream: Readable) {
  const ret: string[] = []
  const rl = readline.createInterface({
    input: stream,
  })

  for await (const line of rl) {
    ret.push(line)
  }
  return List(ret)
}
