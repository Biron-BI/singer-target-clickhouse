# singer-target-clickhouse-kotlin

## Project goal

This is a **Kotlin rewrite of an existing TypeScript project** located at
`/home/sestienney/idea-project/biron/singer-target-clickhouse/` (a Singer target
that writes tap output into ClickHouse).

## Current status

The integration test suite has been **fully ported from TS and validated**:
`src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/ProcessStreamTest.kt`
(a kotest `DescribeSpec`) mirrors `tests/processStream.spec.ts` from the TS
repo, and all tests pass against the published TS Docker image.

There is also a smaller `MainTest.kt` sanity spec that exercises raw ClickHouse
DDL behavior through `JdbcTemplate`.

**Next phase: port the application code itself from TS to Kotlin**, replacing
the reliance on the published `ghcr.io/biron-bi/target-clickhouse:2.11.0` image
with an equivalent Kotlin implementation. The ported tests will be the
acceptance criteria for that port.

## Test strategy (keep in mind during the app port)

Each integration test in `ProcessStreamTest.kt`:

1. Boots a real ClickHouse container via Testcontainers
   (`clickhouse/clickhouse-server:25.8.4.13`).
2. Runs the **published** Docker image of the original TS target
   (`ghcr.io/biron-bi/target-clickhouse:2.11.0`) as a one-shot container, with
   a test config mounted at `/config.json` and a fixture JSONL at
   `/input.jsonl`.
3. Uses a Spring `JdbcTemplate` (via `DriverManagerDataSource`) to query the
   container and assert the resulting state matches the TS behavior.

Because the published Docker image is known to be functionally correct, **the
TS test suite is the source of truth**. During the app port, the Kotlin
implementation must produce the same ClickHouse state for the same fixture
inputs. When the Kotlin app is ready, the `runTarget(...)` helper inside the
spec should be swapped from "start the published TS image" to "invoke the
Kotlin app" — the rest of the assertions should continue to pass unchanged.

Fixture JSONL files live next to the spec under
`src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/` and
mirror the TS `tests/data/` directory.

## Running tests

```sh
./gradlew test --tests "com.biron.singerTargetClickhouse.utilsTest.kotest.ProcessStreamTest"
```

Docker must be available locally (tests will pull the two images above on first
run).

## Important quirks to know

- The TS target reads `--update-streams` only from the CLI, never from the JSON
  config file. The `runTarget(inputFile, configFile, updateStreams = ...)`
  helper in the spec passes it on the command line. A Kotlin port must accept
  the same CLI flag.
- The ClickHouse JDBC v2 driver returns arrays as `com.clickhouse.jdbc.types.Array`
  (which implements `java.sql.Array`), **not** `com.clickhouse.jdbc.ClickHouseArray`.
  Type-check against `java.sql.Array`.
- `com.clickhouse.jdbc.ClickHouseDataSource` / `ClickHouseDriver` are deprecated
  in the 0.9.x driver with no direct replacement. Use Spring's
  `DriverManagerDataSource` — the driver is picked up via `ServiceLoader` on
  the `jdbc:clickhouse:…` URL, no deprecated class is referenced directly.
- The test that exercises `insert_stream_timeout_sec` cannot use `--input
  <file>`: EOF on the file flushes the batch immediately and defeats the
  timeout. Instead, the container is started with `sh -c` overriding the
  entrypoint, piping the schema+record into node via a subshell that sleeps,
  so stdin stays open past the timeout. The Kotlin port must honour the same
  timeout semantics on stdin-driven runs.
- The ClickHouse container user created by testcontainers' `ClickHouseContainer`
  has access to all databases via the default profile, but **does not have
  access-management privileges** (`CREATE USER`, `GRANT`). Tests should not
  call those statements; just `CREATE DATABASE` and use the existing user.
