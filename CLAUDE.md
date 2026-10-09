# singer-target-clickhouse

A [Singer](https://singer.io/) target written in Kotlin (JVM 21) that reads tap
output (JSONL on stdin, or `--input <file>`) and writes it into ClickHouse. The
CLI is `RootCommand` in `Main.kt` (clikt); config fields and Singer extensions
are documented in `README.md`. The Docker image
`ghcr.io/biron-bi/target-clickhouse` is built and pushed by
`.github/workflows/build.yaml` on `v*` tags.

## Tests

```sh
./gradlew test
```

Docker must be available: `ClickhouseConnectionIntegrationTest`,
`ClickhouseJdbcSmokeTest` and `StreamPipelineIntegrationTest` boot a real
ClickHouse via Testcontainers (image pinned in `TestImages.kt`). The other specs
are plain kotest unit specs.

`StreamPipelineIntegrationTest` runs the pipeline in-process
(`StreamPipeline.forConfig(cfg).run(...)`) on the JSONL fixtures in
`StreamPipelineIntegrationTestResults/`, then asserts the resulting ClickHouse
state through a Spring `JdbcTemplate`.

## Benchmarks

`scripts/benchmark.sh --baseline <git-ref> [--candidate <git-ref>] <input.jsonl.gz>`
benchmarks two versions of the target (the candidate defaults to the working
tree) and checks with `scripts/compare-databases.sh` that both produced the
same content. See the script header for the options and caveats.

## Important quirks to know

- `--update-streams` is only read from the CLI, never from the JSON config file.
- The ClickHouse JDBC v2 driver returns arrays as `com.clickhouse.jdbc.types.Array`
  (which implements `java.sql.Array`), **not** `com.clickhouse.jdbc.ClickHouseArray`.
  Type-check against `java.sql.Array`.
- `com.clickhouse.jdbc.ClickHouseDataSource` / `ClickHouseDriver` are deprecated
  in the 0.9.x driver with no direct replacement. Use Spring's
  `DriverManagerDataSource` — the driver is picked up via `ServiceLoader` on
  the `jdbc:clickhouse:…` URL, no deprecated class is referenced directly.
- EOF on the input flushes the pending batch immediately, so a test that
  exercises `insert_stream_timeout_sec` must keep its input open past the
  timeout (`StreamPipelineIntegrationTest` uses a `PipedInputStream` fed from a
  coroutine).
- The ClickHouse container user created by testcontainers' `ClickHouseContainer`
  has access to all databases via the default profile, but **does not have
  access-management privileges** (`CREATE USER`, `GRANT`). Tests should not
  call those statements; just `CREATE DATABASE` and use the existing user.
