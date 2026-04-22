@file:Suppress("SqlNoDataSourceInspection")

package com.biron.singerTargetClickhouse.utilsTest.kotest

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.github.oshai.kotlinlogging.KotlinLogging
import io.kotest.assertions.nondeterministic.eventually
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.DescribeSpec
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainAll
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.collections.shouldNotContain
import io.kotest.matchers.file.shouldExist
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.string.shouldInclude
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.queryForObject
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.testcontainers.clickhouse.ClickHouseContainer
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.startupcheck.IndefiniteWaitOneShotStartupCheckStrategy
import org.testcontainers.utility.MountableFile
import java.io.File
import kotlin.time.Duration.Companion.seconds


private const val TARGET_IMAGE = "ghcr.io/biron-bi/target-clickhouse:2.11.0"
private const val CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:24.12.3.47"

private val DATA_DIR: String =
	"./src/test/kotlin/${ProcessStreamTest::class.java.packageName.replace('.', '/')}/data"

data class Config(
	val host: String,
	val username: String,
	val password: String,
	val port: Int,
	val database: String,
	val extra_active_tables: List<String> = emptyList(),
	val batch_size: Int? = 100,
	val insert_stream_timeout_sec: Int? = 180,
	val translate_values: Boolean = false,
)

class ProcessStreamTest : DescribeSpec({

	val logger = KotlinLogging.logger {}

	val initialConnInfo = Config(
		host = "clickhouse-server",
		port = 8123,
		database = "datayse",
		username = "user",
		password = "averysecurepassword",
	)

	lateinit var container: ClickHouseContainer
	lateinit var jdbcTemplate: JdbcTemplate
	lateinit var network: Network

	beforeSpec {
		network = Network.newNetwork()
		container = ClickHouseContainer(CLICKHOUSE_IMAGE)
			.withNetwork(network)
			.withNetworkAliases("clickhouse-server")
			.apply {
				withUsername(initialConnInfo.username)
				withPassword(initialConnInfo.password)
				withDatabaseName(initialConnInfo.database)
				start()
			}

		jdbcTemplate = DriverManagerDataSource(
			"jdbc:clickhouse://${container.host}:${container.getMappedPort(initialConnInfo.port)}",
			container.username,
			container.password,
		).let(::JdbcTemplate)
	}

	afterSpec {
		container.stop()
	}

	beforeEach {
		jdbcTemplate.execute("DROP DATABASE IF EXISTS ${initialConnInfo.database};")
		jdbcTemplate.execute("CREATE DATABASE ${initialConnInfo.database};")
	}

	val jsonMapper = jacksonObjectMapper()

	fun writeConfig(config: Config): File = File.createTempFile("test-config", ".json").apply {
		writeText(jsonMapper.writeValueAsString(config))
	}

	fun targetContainer(configFile: File): GenericContainer<Nothing> =
		GenericContainer<Nothing>(TARGET_IMAGE).apply {
			withNetwork(network)
			withCopyFileToContainer(MountableFile.forHostPath(configFile.absolutePath), "/config.json")
			withStartupCheckStrategy(IndefiniteWaitOneShotStartupCheckStrategy())
			withLogConsumer { frame -> logger.info { "[target-clickhouse] ${frame.utf8String}" } }
		}

	fun runTarget(
		inputFile: String,
		configFile: File = writeConfig(initialConnInfo),
		updateStreams: List<String> = emptyList(),
	): GenericContainer<Nothing> {
		val args = arrayOf("--config", "/config.json", "--input", "/input.jsonl", "--output", "/state.jsonl") +
			if (updateStreams.isEmpty()) emptyArray() else arrayOf("--update-streams", *updateStreams.toTypedArray())

		return targetContainer(configFile).apply {
			withNetworkAliases("clickhouse-client")
			withCopyFileToContainer(MountableFile.forHostPath("$DATA_DIR/$inputFile"), "/input.jsonl")
			withCommand(*args)
			start()
			currentContainerInfo!!.state!!.takeIf { it.exitCodeLong != 0L }
				?.let { state -> logger.info { "Target container exited non-zero: ${state.error}" } }
		}
	}

	val db = initialConnInfo.database

	fun showTables(): List<String> = jdbcTemplate.queryForList("SHOW TABLES FROM $db", String::class.java)

	fun queryRows(sql: String, separator: String = "\t"): List<String> =
		jdbcTemplate.queryForList(sql).map { it.values.joinToString(separator) }

	fun queryCount(table: String): Int =
		jdbcTemplate.queryForObject<Int>("select count(*) from $db.$table")

	fun Any?.asArrayString(): String = when (this) {
		is java.sql.Array -> (array as Array<*>).joinToString(prefix = "[", postfix = "]") { "'$it'" }
		else -> toString()
	}

	describe("outputStream") {
		it("should write state to passed outputStream") {
			val stateFile = File.createTempFile("state", ".jsonl").also { it.deleteOnExit() }
			runTarget("stream_with_state.jsonl").copyFileFromContainer("/state.jsonl", stateFile.absolutePath)

			stateFile.shouldExist()
			stateFile.readLines(Charsets.UTF_8).map { it.trimStart('\uFEFF') } shouldBe listOf(
				"""{"bookmarks":{"toto":"tata"},",currently_syncing":"tickets"}""",
				"""{"bookmarks":{},"currently_syncing":null}""",
			)
		}
	}

	describe("Schemas") {
		it("should create schemas") {
			runTarget("stream_1.jsonl")
			showTables().also {
				it shouldHaveSize 21
				it shouldContainAll listOf(
					"ticket_audits",
					"ticket_audits__events__attachments",
					"ticket_audits__metadata__notifications_suppressed_for",
					"tickets",
					"tickets__custom_fields",
				)
			}
		}

		it("should create schema with nullable scalar array") {
			runTarget("stream_schema_array_nullable.jsonl")

			queryRows(
				"""
                SELECT name, type
                FROM system.columns
                WHERE table LIKE 'return_requests_%'
                  AND database = '$db'
                  AND name = 'value'
				""".trimIndent()
			).joinToString("\n") shouldBe "value\tNullable(String)"
		}

		it("should create schema with nullable scalar array as ClickHouse array") {
			runTarget("stream_schema_with_array.jsonl")
			runTarget("stream_schema_with_array.jsonl")

			queryRows(
				"""
                SELECT name, type
                FROM system.columns
                WHERE database = '$db'
                  AND table = 'query_log'
				""".trimIndent()
			).take(2) shouldContainExactly listOf(
				"databases\tArray(String)",
				"event_time\tDateTime",
			)

			jdbcTemplate.queryForList("SELECT databases FROM $db.query_log")
				.map { it["databases"].asArrayString() }
				.first() shouldBe "['kento', 'nanami']"
		}

		it("should create schemas which specifies cardinality") {
			runTarget("stream_cardinality.jsonl")

			showTables() shouldContainExactly listOf("users")

			queryRows("SHOW CREATE TABLE $db.users")
				.joinToString("\n") shouldContain "`name` LowCardinality(Nullable(String))"
		}

		it("should create schemas which specifiesPK") {
			runTarget("stream_schema_with_all_pk.jsonl")

			queryRows("describe table $db.tickets__follower_ids").also {
				it[0] shouldInclude "_root_id"
				it[1] shouldInclude "_parent_id"
				it[2] shouldInclude "_level_0_index"
			}
		}

		it("should do nothing if schemas already exists") {
			runTarget("stream_1.jsonl")
			runTarget("stream_1.jsonl")
			showTables() shouldHaveSize 21
		}
	}

	describe("columns update") {

		it("should create / update / delete columns if schema already exists and new has different columns") {
			runTarget("stream_1.jsonl")
			runTarget("stream_1_modified.jsonl")

			showTables() shouldHaveSize 21

			queryRows(
				"""
                select name, type
                from system.columns
                where table = 'tickets'
                and database = '$db'
                order by name
				""".trimIndent(),
				separator = " ",
			).also {
				it shouldContainAll listOf("organization_id Nullable(String)", "new_requester_id Nullable(Int64)")
				it shouldNotContain "requester_id Nullable(Int64)"
			}
		}

		it("should start by truncating before applying schema update") {
			runTarget("stream_nullable.jsonl")
			runTarget("stream_non_nullable.jsonl")

			queryRows(
				"""
                select name, type
                from system.columns
                where table = 'users'
                and database = '$db'
                order by name
				""".trimIndent(),
				separator = " ",
			) shouldContainExactly listOf("id Int64")
		}

		it("should handle state at the end of the stream + a closing state, launched several times") {
			repeat(10) {
				runTarget("stream_with_state.jsonl")
				runTarget("stream_tiny.jsonl")
			}

			queryRows("select * from $db.tickets", separator = ",").also {
				it shouldHaveSize 3
				it[1] shouldBe "2,59"
			}
		}

		it("should rename tables as dropped when they are no longer active, and exclude dropped and archived") {
			runTarget("stream_1.jsonl")
			runTarget("stream_1_inactive.jsonl")

			showTables().also {
				it shouldHaveSize 21
				it.forEach { table ->
					table.startsWith("_dropped_") shouldBe !table.contains("ticket_audits")
				}
			}

			runTarget("stream_1_inactive.jsonl")
			showTables().also {
				it shouldHaveSize 21
				it.forEach { table ->
					table.startsWith("_dropped_") shouldBe !table.contains("ticket_audits")
					table.startsWith("_dropped__dropped_") shouldBe false
				}
			}

			jdbcTemplate.execute("RENAME TABLE $db._dropped_ticket_metrics TO $db._archived_ticket_metrics")
			runTarget("stream_1_inactive.jsonl")

			showTables().also {
				it shouldHaveSize 21
				it.forEach { table ->
					when {
						table.contains("ticket_audits") -> {
							table.startsWith("_archived_") shouldBe false
							table.startsWith("_dropped_") shouldBe false
						}
						table.contains("ticket_metrics") -> {
							table.startsWith("_archived_") shouldBe true
							table.contains("_dropped_") shouldBe false
						}
						else -> table.startsWith("_dropped_") shouldBe true
					}
					table.startsWith("_dropped__dropped_") shouldBe false
				}
			}
		}

		it("should not rename tables as dropped when they are no longer active if they are registered as extra_active") {
			val config = writeConfig(initialConnInfo.copy(extra_active_tables = listOf("tickets")))
			runTarget("stream_1.jsonl", configFile = config)
			runTarget("stream_1_inactive.jsonl", configFile = config)

			showTables().also {
				it shouldHaveSize 21
				it.forEach { table ->
					val protected = table.contains("ticket_audits") || table.contains("tickets")
					table.startsWith("_dropped_") shouldBe !protected
				}
			}
		}

		it("should throw if schema already exists and new has different columns with incompatible type") {
			runTarget("stream_vanilla.jsonl")
			shouldThrow<Exception> { runTarget("stream_vanilla_with_incompatible_update.jsonl") }
		}

		it("should throw if schema has no primary key but has array children") {
			shouldThrow<Exception> { runTarget("stream_with_nested_array_without_root_pk.jsonl") }
		}

		it("should handle second schema definition by commiting pending changes") {
			runTarget("stream_multiple_schema.jsonl")
			queryCount("tickets") shouldBe 1
		}

		it("should recreate if schemas already exists, new is different but specified to be recreated") {
			runTarget("stream_1.jsonl")
			runTarget("stream_1_modified.jsonl", updateStreams = listOf("tickets"))
			showTables() shouldHaveSize 21
		}

		it("should handle additional nested array") {
			runTarget("stream_nested_array.jsonl")
			runTarget("stream_nested_array_additional.jsonl")

			jdbcTemplate.queryForList("show tables from $db") shouldContainExactly listOf(
				mapOf("name" to "users"),
				mapOf("name" to "users__roles"),
				mapOf("name" to "users__roles__scopes"),
			)
		}
	}

	describe("Records") {

		it("should insert simple records") {
			runTarget("stream_short.jsonl")
			jdbcTemplate.queryForList(
				"select brand_id from $db.tickets where assignee_id = 11"
			) shouldContainExactly listOf(mapOf("brand_id" to 22L))
		}

		// Verifies that a record is inserted after insert_stream_timeout_sec even when no
		// end-of-stream or state message is received. The regular --input path can't be used:
		// EOF on the input file flushes the batch immediately and defeats the timeout.
		// Instead, override the entrypoint to pipe schema+record into node via a subshell
		// that stays open with `sleep` so stdin does not close.
		it("should insert record after some time even if stream isn't ended nor state message were received") {
			val insertTimeoutSec = 8
			val keepOpenSec = 30
			val config = writeConfig(
				initialConnInfo.copy(batch_size = 10, insert_stream_timeout_sec = insertTimeoutSec)
			)

			val schemaJson = jsonMapper.writeValueAsString(
				mapOf(
					"type" to "SCHEMA",
					"stream" to "tickets",
					"schema" to mapOf(
						"properties" to mapOf("id" to mapOf("type" to listOf("integer"))),
						"type" to listOf("null", "object"),
					),
					"key_properties" to listOf("id"),
				)
			)
			val recordJson = jsonMapper.writeValueAsString(
				mapOf("type" to "RECORD", "stream" to "tickets", "record" to mapOf("id" to 155))
			)

			fun shellQuote(s: String) = "'" + s.replace("'", "'\\''") + "'"
			val shellCmd =
				"(printf '%s\\n' ${shellQuote(schemaJson)}; printf '%s\\n' ${shellQuote(recordJson)}; " +
					"sleep $keepOpenSec) | " +
					"node /usr/local/lib/node_modules/target-clickhouse/dist/index.js " +
					"--config /config.json --output /state.jsonl"

			val container = targetContainer(config).apply {
				withCreateContainerCmdModifier { cmd ->
					cmd.withEntrypoint("sh", "-c")
					cmd.withCmd(shellCmd)
				}
			}

			runBlocking {
				val job = launch(Dispatchers.IO) {
					try {
						container.start()
					} catch (e: Exception) {
						logger.info(e) { "target-clickhouse container terminated" }
					}
				}

				try {
					eventually(20.seconds) {
						jdbcTemplate.queryForMap("EXISTS $db.tickets").values.first() shouldBe 1
					}

					// Table exists but no record should be inserted yet (timeout hasn't fired)
					delay(1000)
					jdbcTemplate.queryForList("select id from $db.tickets").shouldBeEmpty()

					// Eventually the batch is flushed by insert_stream_timeout_sec
					eventually((insertTimeoutSec + 10).seconds) {
						jdbcTemplate.queryForList("select id from $db.tickets") shouldContainExactly
							listOf(mapOf("id" to 155L))
					}
				} finally {
					runCatching { container.stop() }
					job.cancel()
				}
			}
		}

		it("should allow reordering of schema") {
			runTarget("stream_short.jsonl")
			runTarget("stream_short_reordered.jsonl")

			jdbcTemplate.queryForList(
				"select brand_id from $db.tickets where assignee_id = 11"
			) shouldContainExactly listOf(mapOf("brand_id" to 22L))
		}

		it("should flatten nested object") {
			runTarget("stream_nested_object.jsonl")
			jdbcTemplate.queryForList(
				"select follower_ids__name from $db.tickets"
			) shouldContainExactly listOf(mapOf("follower_ids__name" to "jack"))
		}

		it("should ingest stream from real data: covidtracker") {
			runTarget("covidtracker.jsonl")
			jdbcTemplate.queryForObject(
				"select sum(total_rows), sum(tables.total_bytes) from system.tables where database = '$db'"
			) { rs, _ -> "${rs.getInt(1)}\t${rs.getInt(2)}" } shouldBe "5789\t1334466"

			runTarget("covidtracker.jsonl")
			jdbcTemplate.queryForObject(
				"select sum(total_rows) from system.tables where database = '$db'",
				Int::class.java,
			) shouldBe 5789
		}

		it("should ingest stream from real data: clickhouse query log") {
			val totalRowsQuery = "select sum(total_rows) from system.tables where database = '$db'"

			runTarget("clickhouse_query_log.jsonl")
			jdbcTemplate.queryForObject(totalRowsQuery, Int::class.java) shouldBe 1

			runTarget("clickhouse_query_log.jsonl")
			jdbcTemplate.queryForObject(totalRowsQuery, Int::class.java) shouldBe 1

			jdbcTemplate.queryForList("select databases, `Settings.Names` from $db.query_log")
				.map { "${it["databases"].asArrayString()}\t${it["Settings.Names"].asArrayString()}" }
				.first() shouldBe "['system']\t['max_block_size', 'max_query_size', 'join_use_nulls', " +
				"'http_receive_timeout', 'max_expanded_ast_elements', 'max_memory_usage', " +
				"'max_parser_depth', 'lock_acquire_timeout']"
		}

		it("should produce same result from real data whether translate value is effective or not") {
			runTarget("covidtracker.jsonl", configFile = writeConfig(initialConnInfo.copy(translate_values = false)))
			val sumQuery = "select sum(total_rows), sum(total_bytes) from system.tables where database = '$db'"
			val baseline = jdbcTemplate.queryForList(sumQuery)

			val otherDb = "otherDB"
			jdbcTemplate.execute("CREATE DATABASE IF NOT EXISTS $otherDb;")

			runTarget(
				"covidtracker.jsonl",
				configFile = writeConfig(initialConnInfo.copy(translate_values = true, database = otherDb)),
			)
			jdbcTemplate.queryForList(sumQuery.replace(db, otherDb)) shouldBe baseline
		}

		it("should handle cleanFirst") {
			runTarget("stream_vanilla.jsonl")
			queryCount("users") shouldBe 4

			runTarget("stream_cleanFirst.jsonl")
			queryCount("users") shouldBe 2
		}

		it("should update schema by creating sub table") {
			runTarget("stream_vanilla.jsonl")
			queryCount("users") shouldBe 4

			runTarget("stream_with_array.jsonl")
			queryCount("users__roles") shouldBe 5
		}

		it("should throw when new pks are added") {
			runTarget("stream_vanilla_with_pks.jsonl")
			queryCount("users") shouldBe 4

			shouldThrow<Exception> { runTarget("stream_vanilla_with_new_pks.jsonl") }
		}

		it("should throw when pks are deleted") {
			runTarget("stream_vanilla_with_pks.jsonl")
			queryCount("users") shouldBe 4

			shouldThrow<Exception> { runTarget("stream_vanilla_with_removed_pks.jsonl") }
		}

		it("should allow pk to be added if stream is in cleanFirst") {
			runTarget("stream_vanilla_with_pks.jsonl")
			queryCount("users") shouldBe 4

			runTarget("stream_vanilla_with_new_pks_and_clean_first.jsonl")
			queryCount("users") shouldBe 4
		}

		it("should handle cleaning column in standard columns") {
			runTarget("stream_vanilla.jsonl")
			queryCount("users") shouldBe 4

			runTarget("stream_cleaningColumn.jsonl")
			queryCount("users") shouldBe 5

			jdbcTemplate.queryForObject<Int>("select id from $db.users where name = 'bill'") shouldBe 7
		}

		it("should handle cleaning column in pk") {
			runTarget("stream_cleaningColumn_pk.jsonl")
			queryRows("select id, name from $db.users") shouldContainExactly
				listOf("5\tbob", "7\tbill", "8\tbill", "9\thelen")

			runTarget("stream_cleaningColumn_pk_2.jsonl")
			queryRows("select id, name from $db.users") shouldContainExactly
				listOf("5\tbob", "9\thelen", "10\tbill")
		}

		it("should handle record when schema specifiesPK") {
			runTarget("stream_short_with_all_pk.jsonl")

			queryRows("describe table $db.tickets__follower_ids").also {
				it[0] shouldInclude "_root_id"
				it[1] shouldInclude "_parent_id"
				it[2] shouldInclude "_level_0_index"
			}

			queryCount("tickets") shouldBe 1
			queryCount("tickets__follower_ids") shouldBe 2
		}

		it("should handle record when schema specifies complex PK") {
			runTarget("stream_short_with_all_pk2.jsonl")

			queryRows("describe table $db.tickets__follower_ids").also {
				it[0] shouldInclude "_root_id"
				it[1] shouldInclude "_parent_id"
				it[2] shouldInclude "name"
				it[3] shouldInclude "_level_0_index"
			}

			queryCount("tickets") shouldBe 1
			queryCount("tickets__follower_ids") shouldBe 2
		}

		it("should handle stream which deletes existing data with one simple pk") {
			runTarget("stream_tiny.jsonl")
			queryRows("select id from $db.tickets") shouldContainExactly listOf("1", "2", "3")

			runTarget("stream_tiny_with_delete.jsonl")
			queryRows("select id from $db.tickets") shouldContainExactly listOf("1", "3")
		}

		it("should handle stream which deletes existing data with multiple pk") {
			runTarget("stream_vanilla_with_pks.jsonl")
			queryRows("select id, name from $db.users", separator = " ") shouldContainExactly
				listOf("1 bill", "2 bill", "3 jack", "4 joe")

			runTarget("stream_vanilla_with_pks_and_deletion.jsonl")
			queryRows("select id, name from $db.users", separator = " ") shouldContainExactly
				listOf("1 bill", "2 bill", "4 joe")
		}

		it("should deduplicate tables when receiving only schema") {
			runTarget("stream_vanilla_with_pks.jsonl")
			queryCount("users") shouldBe 4

			jdbcTemplate.execute("INSERT INTO $db.users VALUES (4, 'joe', 90);")
			queryCount("users") shouldBe 5

			runTarget("stream_vanilla_with_pks_no_records.jsonl")
			queryCount("users") shouldBe 4
		}
	}
})
