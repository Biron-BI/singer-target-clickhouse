import {strict as assert} from 'assert'
import * as fs from "fs"
import {processStream} from "../src/processStream"
import {StartedTestContainer} from "testcontainers"
import {LogLevel, set_log_level, set_output_stream} from "singer-node"
import {bootClickhouseContainer, runChQueryInContainer, sleep} from "./helpers"
import {Config} from '../src/Config'
import {Readable, Writable} from "stream"

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
      console.error("err", err)
    }
    set_log_level(LogLevel.INFO)
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
  describe("outputStream", () => {
    it('should write state to passed outputStream', async () => {
      const data: any[] = []
      set_output_stream(new Writable({
        write(chunk: any, encoding: BufferEncoding, callback: (error?: Error | null) => void): void {
          data.push(chunk);
          callback();
        },
      }))
      await processStream(fs.createReadStream("./tests/data/stream_with_state.jsonl"), connInfo)
      assert.equal(Buffer.concat(data).toString(), `{"bookmarks":{"toto":"tata"},",currently_syncing":"tickets"}\n{"bookmarks":{},"currently_syncing":null}\n`)
    }).timeout(30000)
  })
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

    it("should create schema with nullable scalar array", async () => {
      await processStream(fs.createReadStream("./tests/data/stream_schema_array_nullable.jsonl"), connInfo)
      const execResult = await runChQueryInContainer(container, connInfo, `select name, type
                                                                           from system.columns
                                                                           where table LIKE 'return_requests_%'
                                                                             and database = '${connInfo.database}'
                                                                             and name = 'value'`)

      assert.equal(execResult.output, "value\tNullable(String)\n")
    })

    it("should create schema with nullable scalar array as clickhouse array", async () => {
      await processStream(fs.createReadStream("./tests/data/stream_schema_with_array.jsonl"), connInfo)
      await processStream(fs.createReadStream("./tests/data/stream_schema_with_array.jsonl"), connInfo)
      let execResult = await runChQueryInContainer(container, connInfo, `select columns.name, columns.type
                                                                         from system.columns
                                                                         where database = '${connInfo.database}'
                                                                           and table = 'query_log'`)
      let rows = execResult.output.split("\n")
      assert.equal(rows[0], "databases\tArray(String)")
      assert.equal(rows[1], "event_time\tDateTime")

      execResult = await runChQueryInContainer(container, connInfo, `select databases
                                                                     from ${connInfo.database}.query_log`)
      rows = execResult.output.split("\n")
      assert.equal(rows[0], "['kento','nanami']")
    })

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

    describe("columns update", () => {
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

      it('should start by truncating before applying schema update', async () => {
        await processStream(fs.createReadStream("./tests/data/stream_nullable.jsonl"), connInfo)
        await processStream(fs.createReadStream("./tests/data/stream_non_nullable.jsonl"), connInfo)

        let execResult = await runChQueryInContainer(container, connInfo, `select name, type
                                                                           from system.columns
                                                                           where table = 'users'
                                                                             and database = '${initialConnInfo.database}'
                                                                           order by name`)

        // @ts-ignore
        const columns: string[] = execResult.output.split("\n").map(it => it.replaceAll("\t", " ")).filter(Boolean)

        assert.equal(columns.includes("id Int64"), true)
        assert.equal(columns.length, 1)
      }).timeout(30000)
    })

    it('should handle state at the end of the stream + a closing state, launched several times', async () => {
      for (let i = 0; i < 10; i++) {
        await processStream(fs.createReadStream("./tests/data/stream_with_state.jsonl"), connInfo)
        await processStream(fs.createReadStream("./tests/data/stream_tiny.jsonl"), connInfo)
      }

      let execResult = await runChQueryInContainer(container, connInfo, `select *
                                                                         from ${initialConnInfo.database}.tickets`)

      // @ts-ignore
      const columns: string[] = execResult.output.split("\n").map(it => it.replaceAll("\t", ",")).filter(Boolean)

      assert.equal(columns.length, 3)
      assert.equal(columns[1], "2,59")
    }).timeout(30000)

    it("should rename tables as dropped when they are no longer active, and exclude dropped and archived", async () => {
      await processStream(fs.createReadStream("./tests/data/stream_1.jsonl"), connInfo)

      await processStream(fs.createReadStream("./tests/data/stream_1_inactive.jsonl"), connInfo)
      let execResult = await runChQueryInContainer(container, connInfo, `show tables from ${connInfo.database}`)
      let tables = execResult.output.split("\n").filter(Boolean)
      assert.equal(tables.length, 21)
      tables.forEach((table) => {
        if (!table.includes("ticket_audits")) {
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
        if (!table.includes("ticket_audits")) {
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
        if (!table.includes("ticket_audits")) {
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

    it("should not rename tables as dropped when they are no longer active if they are registered as extra_active", async () => {
      const config = {
        ...connInfo,
        extra_active_tables: ["tickets"],
      }
      await processStream(fs.createReadStream("./tests/data/stream_1.jsonl"), config)
      await processStream(fs.createReadStream("./tests/data/stream_1_inactive.jsonl"), config)
      let execResult = await runChQueryInContainer(container, connInfo, `show tables from ${connInfo.database}`)
      let tables = execResult.output.split("\n").filter(Boolean)
      assert.equal(tables.length, 21)
      tables.forEach((table) => {
        if (!table.includes("ticket_audits") && !table.includes("tickets")) {
          assert.equal(table.startsWith("_dropped_"), true, `table ${table} should start with dropped`)
        } else {
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

    it('should handle additional nested array', async () => {
      await processStream(fs.createReadStream("./tests/data/stream_nested_array.jsonl"), connInfo)
      await processStream(fs.createReadStream("./tests/data/stream_nested_array_additional.jsonl"), connInfo)

      const execResult = await runChQueryInContainer(container, connInfo, `show tables from ${connInfo.database}`)
      const tables = execResult.output.split("\n").filter(Boolean)

      assert.deepStrictEqual(tables, ["users", "users__roles", "users__roles__scopes"])
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


    it('should insert record after some time even if stream isnt ended nor state message were received', async () => {
      const schema = {
        "type": "SCHEMA", "stream": "tickets", "schema": {
          "properties": {
            "id": {"type": ["integer"]},
          }, "type": ["null", "object"],
        }, "key_properties": ["id"],
      }
      const record = {
        "type": "RECORD", "stream": "tickets", "record": {
          "id": 155,
        },
      }

      const s = new Readable();
      s._read = (a) => {
      }; // redundant? see update below
      s.push(JSON.stringify(schema) + "\n");
      s.push(JSON.stringify(record) + "\n");

      processStream(s, {
        ...connInfo,
        batch_size: 10,
        insert_stream_timeout_sec: 8,
      })

      await sleep(1000)

      let execResult = await runChQueryInContainer(container, connInfo, "select id from tickets")
      assert.equal(execResult.output, "")
      await sleep(4000)

      execResult = await runChQueryInContainer(container, connInfo, "select id from tickets")

      assert.equal(execResult.output, "155\n")

    }).timeout(30000)

    it('should allow reordering of schema', async () => {
      await processStream(fs.createReadStream("./tests/data/stream_short.jsonl"), connInfo)
      await processStream(fs.createReadStream("./tests/data/stream_short_reordered.jsonl"), connInfo)
      const execResult = await runChQueryInContainer(container, connInfo, `select brand_id
                                                                           from tickets
                                                                           where assignee_id = 11`)
      assert.equal(execResult.output, '22\n')
    }).timeout(30000)

    it('should flatten nested object', async () => {
      await processStream(fs.createReadStream("./tests/data/stream_nested_object.jsonl"), connInfo)
      const execResult = await runChQueryInContainer(container, connInfo, `select follower_ids__name
                                                                           from tickets`)
      assert.equal(execResult.output, 'jack\n')
    }).timeout(30000)

    it('should ingest stream from real data: covidtracker', async () => {
      await processStream(fs.createReadStream("./tests/data/covidtracker.jsonl"), connInfo)
      let execResult = await runChQueryInContainer(container, connInfo, `select sum(total_rows), sum(tables.total_bytes)
                                                                         from system.tables
                                                                         where database = '${connInfo.database}'`)
      assert.equal(execResult.output, '5789\t1345158\n')

      // Ensure no duplicates are created when run second time
      await processStream(fs.createReadStream("./tests/data/covidtracker.jsonl"), connInfo)
      execResult = await runChQueryInContainer(container, connInfo, `select sum(total_rows)
                                                                     from system.tables
                                                                     where database = '${connInfo.database}'`)
      assert.equal(execResult.output, '5789\n')

    }).timeout(60000)

    it('should ingest stream from real data: clickhouse query log', async () => {
      await processStream(fs.createReadStream("./tests/data/clickhouse_query_log.jsonl"), connInfo)
      let execResult = await runChQueryInContainer(container, connInfo, `select sum(total_rows)
                                                                         from system.tables
                                                                         where database = '${connInfo.database}'`)
      assert.equal(execResult.output, '1\n')

      // Ensure no duplicates are created when run second time
      await processStream(fs.createReadStream("./tests/data/clickhouse_query_log.jsonl"), connInfo)
      execResult = await runChQueryInContainer(container, connInfo, `select sum(total_rows)
                                                                     from system.tables
                                                                     where database = '${connInfo.database}'`)
      assert.equal(execResult.output, '1\n')

      // execResult = await runChQueryInContainer(container, connInfo, `desc ${connInfo.database}.query_log`)
      // assert.equal(execResult.output, "['system']\t['max_block_size','max_query_size','join_use_nulls','http_receive_timeout','max_expanded_ast_elements','max_memory_usage','max_parser_depth','lock_acquire_timeout']\n")


      execResult = await runChQueryInContainer(container, connInfo, `select databases, \`Settings.Names\`
                                                                     from ${connInfo.database}.query_log`)
      assert.equal(execResult.output, "['system']\t['max_block_size','max_query_size','join_use_nulls','http_receive_timeout','max_expanded_ast_elements','max_memory_usage','max_parser_depth','lock_acquire_timeout']\n")

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

    it('should update schema by creating sub table', async () => {
      await processStream(fs.createReadStream("./tests/data/stream_vanilla.jsonl"), connInfo)
      let execResult = await runChQueryInContainer(container, connInfo, `select count()
                                                                         from \`users\``)
      assert.equal(execResult.output, '4\n')


      await processStream(fs.createReadStream("./tests/data/stream_with_array.jsonl"), connInfo)
      execResult = await runChQueryInContainer(container, connInfo, `select count()
                                                                     from \`users__roles\``)
      assert.equal(execResult.output, '5\n')
    })

    it('should throw when new pks are added', async () => {
      await processStream(fs.createReadStream("./tests/data/stream_vanilla_with_pks.jsonl"), connInfo)
      let execResult = await runChQueryInContainer(container, connInfo, `select count()
                                                                         from \`users\``)
      assert.equal(execResult.output, '4\n')

      await assert.rejects(processStream(fs.createReadStream("./tests/data/stream_vanilla_with_new_pks.jsonl"), connInfo))
    })

    it('should throw when pks are deleted', async () => {
      await processStream(fs.createReadStream("./tests/data/stream_vanilla_with_pks.jsonl"), connInfo)
      let execResult = await runChQueryInContainer(container, connInfo, `select count()
                                                                         from \`users\``)
      assert.equal(execResult.output, '4\n')

      await assert.rejects(processStream(fs.createReadStream("./tests/data/stream_vanilla_with_removed_pks.jsonl"), connInfo))
    })

    it('should allow pk to be added if stream is in cleanFirst', async () => {
      await processStream(fs.createReadStream("./tests/data/stream_vanilla_with_pks.jsonl"), connInfo)
      let execResult = await runChQueryInContainer(container, connInfo, `select count()
                                                                         from \`users\``)
      assert.equal(execResult.output, '4\n')

      await processStream(fs.createReadStream("./tests/data/stream_vanilla_with_new_pks_and_clean_first.jsonl"), connInfo)
      execResult = await runChQueryInContainer(container, connInfo, `select count()
                                                                     from \`users\``)
      assert.equal(execResult.output, '4\n')
    })


    it('should handle cleaning column in standard columns', async () => {
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


    it('should handle cleaning column in pk', async () => {
      await processStream(fs.createReadStream("./tests/data/stream_cleaningColumn_pk.jsonl"), connInfo)
      let execResult = await runChQueryInContainer(container, connInfo, `select id, name
                                                                         from \`users\``)
      // @ts-ignore
      let rows = execResult.output.replaceAll("\t", " ").split("\n")
      assert.equal(rows[0], "5 bob")
      assert.equal(rows[1], "7 bill")
      assert.equal(rows[2], "8 bill")
      assert.equal(rows[3], "9 helen")

      await processStream(fs.createReadStream("./tests/data/stream_cleaningColumn_pk_2.jsonl"), connInfo)
      execResult = await runChQueryInContainer(container, connInfo, `select id, name
                                                                     from \`users\``)
      // @ts-ignore
      rows = execResult.output.replaceAll("\t", " ").split("\n")
      assert.equal(rows[0], "5 bob")
      assert.equal(rows[1], "9 helen")
      assert.equal(rows[2], "10 bill")

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

    it('should handle stream which deletes existing data with one simple pk', async () => {
      await processStream(fs.createReadStream("./tests/data/stream_tiny.jsonl"), connInfo)
      let execResult = await runChQueryInContainer(container, connInfo, `select id
                                                                         from ${connInfo.database}.tickets`)
      assert.equal(execResult.output, "1\n2\n3\n")
      await processStream(fs.createReadStream("./tests/data/stream_tiny_with_delete.jsonl"), connInfo)
      execResult = await runChQueryInContainer(container, connInfo, `select id
                                                                     from ${connInfo.database}.tickets`)
      assert.equal(execResult.output, "1\n3\n")
    }).timeout(30000)

    it('should handle stream which deletes existing data with multiple pk', async () => {
      await processStream(fs.createReadStream("./tests/data/stream_vanilla_with_pks.jsonl"), connInfo)
      let execResult = await runChQueryInContainer(container, connInfo, `select id, name
                                                                         from ${connInfo.database}.users`)
      // @ts-ignore
      assert.equal(execResult.output.replaceAll("\t", " ").replaceAll("\n", " "), "1 bill 2 bill 3 jack 4 joe ")
      await processStream(fs.createReadStream("./tests/data/stream_vanilla_with_pks_and_deletion.jsonl"), connInfo)
      execResult = await runChQueryInContainer(container, connInfo, `select id, name
                                                                     from ${connInfo.database}.users`)
      // @ts-ignore
      assert.equal(execResult.output.replaceAll("\t", " ").replaceAll("\n", " "), "1 bill 2 bill 4 joe ")
    }).timeout(30000)

    it('should deduplicate tables when receiving only schema', async () => {
      await processStream(fs.createReadStream("./tests/data/stream_vanilla_with_pks.jsonl"), connInfo)
      let execResult = await runChQueryInContainer(container, connInfo, `select count()
                                                                         from users`)
      assert.equal(execResult.output, '4\n')
      await runChQueryInContainer(container, connInfo, `INSERT INTO users
                                                        VALUES (4, 'joe', 90);`)
      execResult = await runChQueryInContainer(container, connInfo, `select count()
                                                                     from users`)
      assert.equal(execResult.output, '5\n')
      await processStream(fs.createReadStream("./tests/data/stream_vanilla_with_pks_no_records.jsonl"), connInfo)
      execResult = await runChQueryInContainer(container, connInfo, `select count()
                                                                     from users`)
      assert.equal(execResult.output, '4\n')
    }).timeout(30000)


  }).timeout(30000)

}).timeout(30000)
