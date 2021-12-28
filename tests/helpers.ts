import {Config} from "../src/processStream"
import {GenericContainer, StartedTestContainer} from "testcontainers"

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

export async function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}
