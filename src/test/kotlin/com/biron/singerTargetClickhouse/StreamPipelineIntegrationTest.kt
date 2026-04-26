@file:Suppress("SqlNoDataSourceInspection")

package com.biron.singerTargetClickhouse

import com.biron.singerTargetClickhouse.utilsTest.createFileWithContent
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.google.common.jimfs.Configuration
import com.google.common.jimfs.Jimfs
import io.github.oshai.kotlinlogging.KotlinLogging
import io.kotest.assertions.nondeterministic.eventually
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.collections.*
import io.kotest.matchers.paths.shouldExist
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.string.shouldInclude
import io.kotest.property.Arb
import io.kotest.property.arbitrary.Codepoint
import io.kotest.property.arbitrary.alphanumeric
import io.kotest.property.arbitrary.next
import io.kotest.property.arbitrary.string
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.queryForList
import org.springframework.jdbc.core.queryForObject
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.testcontainers.clickhouse.ClickHouseContainer
import java.io.BufferedWriter
import java.io.OutputStreamWriter
import java.io.PipedInputStream
import java.io.PipedOutputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import kotlin.io.path.Path
import kotlin.io.path.inputStream
import kotlin.io.path.readLines
import kotlin.time.Duration.Companion.seconds


class StreamPipelineIntegrationTest : ShouldSpec({
	val dataDir = "./src/test/kotlin/${this::class.qualifiedName!!.replace('.', '/')}Results"
	val logger = KotlinLogging.logger {}

	val baseConfig = TestTargetConfig(
		host = "placeholder",
		port = 8123,
		database = "datayse",
		username = "user",
		password = Arb.string(20, Codepoint.alphanumeric()).next(),
	)

	lateinit var container: ClickHouseContainer
	lateinit var jdbcTemplate: JdbcTemplate

	val fs = Jimfs.newFileSystem(Configuration.unix())
	val tmpDir = fs.getPath("/tmp").also { Files.createDirectories(it) }
	val configCounter = AtomicInteger()
	val stateCounter = AtomicInteger()

	beforeSpec {
		container = ClickHouseContainer(CLICKHOUSE_IMAGE).apply {
			withUsername(baseConfig.username)
			withPassword(baseConfig.password)
			withDatabaseName(baseConfig.database)
			start()
		}

		jdbcTemplate = JdbcTemplate(
			DriverManagerDataSource(
				"jdbc:clickhouse://${container.host}:${container.getMappedPort(baseConfig.port)}",
				container.username,
				container.password,
			),
		)
	}

	afterSpec {
		container.stop()
		fs.close()
	}

	beforeEach {
		jdbcTemplate.execute("DROP DATABASE IF EXISTS ${baseConfig.database};")
		jdbcTemplate.execute("CREATE DATABASE ${baseConfig.database};")
	}

	val jsonMapper = jacksonObjectMapper()
		.setSerializationInclusion(JsonInclude.Include.NON_NULL)
		.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)

	fun writeConfig(config: TestTargetConfig): Path =
		fs.createFileWithContent("/tmp/test-config-${configCounter.incrementAndGet()}.json", jsonMapper.writeValueAsString(config))

	fun newStateFile(): Path = tmpDir.resolve("state-${stateCounter.incrementAndGet()}.jsonl")
		.also { Files.createFile(it) }

	/** Parse the JSON config written by tests then override host/port with the live testcontainer. */
	fun toTargetConfig(jsonPath: Path): TargetConfig =
		Files.newBufferedReader(jsonPath, StandardCharsets.UTF_8).use { TargetConfig.fromJson(it) }
			.copy(host = container.host, port = container.getMappedPort(baseConfig.port))

	data class RunResult(val stateFile: Path)

	fun runTarget(
		inputFile: String,
		configFile: Path = writeConfig(baseConfig),
		updateStreams: List<String> = emptyList(),
	): RunResult {
		val cfg = toTargetConfig(configFile)
		val stateFile = newStateFile()
		Path(dataDir, inputFile).inputStream().use { input ->
			BufferedWriter(OutputStreamWriter(Files.newOutputStream(stateFile), StandardCharsets.UTF_8)).use { writer ->
				StreamPipeline.forConfig(cfg).run(input, writer, updateStreams)
			}
		}
		return RunResult(stateFile)
	}

	val db = baseConfig.database

	fun showTables(): List<String> = jdbcTemplate.queryForList<String>("SHOW TABLES FROM $db")

	fun queryRows(sql: String, separator: String = "\t"): List<String> =
		jdbcTemplate.queryForList(sql).map { it.values.joinToString(separator) }

	fun queryCount(table: String): Int =
		jdbcTemplate.queryForObject<Int>("select count(*) from $db.$table")

	fun Any?.asArrayString(): String = when (this) {
		is java.sql.Array -> (array as Array<*>).joinToString(prefix = "[", postfix = "]") { "'$it'" }
		else -> toString()
	}

	context("outputStream") {
		should("should write state to passed outputStream") {
			val result = runTarget("stream_with_state.jsonl")

			result.stateFile.shouldExist()
			result.stateFile.readLines(Charsets.UTF_8).map { it.trimStart('\uFEFF') } shouldBe listOf(
				"""{"bookmarks":{"toto":"tata"},",currently_syncing":"tickets"}""",
				"""{"bookmarks":{},"currently_syncing":null}""",
			)
		}

		should("should commit pending records when a STATE message is received mid-stream") {
			// Verifies STATE forces a mid-stream commit. With batch_size=100 and
			// insert_stream_timeout_sec=180 (auto-end ~175s), a single buffered RECORD would not
			// be flushed by either threshold within the test window — only STATE can land it.
			val cfg = toTargetConfig(
				writeConfig(baseConfig.copy(batchSize = 100, insertStreamTimeoutSec = 180)),
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
				),
			)

			fun recordJson(id: Int): String = jsonMapper.writeValueAsString(
				mapOf("type" to "RECORD", "stream" to "tickets", "record" to mapOf("id" to id)),
			)

			val stateJson = jsonMapper.writeValueAsString(
				mapOf("type" to "STATE", "value" to mapOf("bookmark" to "checkpoint-1")),
			)

			val pipedIn = PipedInputStream(64 * 1024)
			val pipedOut = PipedOutputStream(pipedIn)
			val stateFile = newStateFile()
			val output = BufferedWriter(OutputStreamWriter(Files.newOutputStream(stateFile), StandardCharsets.UTF_8))

			runBlocking {
				val job = launch(Dispatchers.IO) {
					try {
						StreamPipeline.forConfig(cfg).run(pipedIn, output)
					} catch (e: Exception) {
						logger.info(e) { "pipeline terminated" }
					}
				}

				try {
					pipedOut.write((schemaJson + "\n").toByteArray(StandardCharsets.UTF_8))
					pipedOut.write((recordJson(101) + "\n").toByteArray(StandardCharsets.UTF_8))
					pipedOut.write((stateJson + "\n").toByteArray(StandardCharsets.UTF_8))
					pipedOut.flush()

					// Row 101 must land before EOF — only the STATE-driven commit can produce this.
					eventually(15.seconds) {
						jdbcTemplate.queryForList("select id from $db.tickets") shouldContainExactly
								listOf(mapOf("id" to 101L))
					}

					pipedOut.write((recordJson(202) + "\n").toByteArray(StandardCharsets.UTF_8))
					pipedOut.flush()
				} finally {
					runCatching { pipedOut.close() }
					job.join()
					runCatching { output.close() }
				}
			}

			jdbcTemplate.queryForList("select id from $db.tickets order by id") shouldContainExactly
					listOf(mapOf("id" to 101L), mapOf("id" to 202L))

			stateFile.readLines(Charsets.UTF_8) shouldBe listOf(
				"""{"bookmark":"checkpoint-1"}""",
			)
		}
	}

	context("Schemas") {
		should("should create schemas") {
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

		should("should create schema with nullable scalar array") {
			runTarget("stream_schema_array_nullable.jsonl")

			queryRows(
				"""
				SELECT name, type
				FROM system.columns
				WHERE table LIKE 'return_requests_%'
				  AND database = '$db'
				  AND name = 'value'
				""".trimIndent(),
			).joinToString("\n") shouldBe "value\tNullable(String)"
		}

		should("should create schema with nullable scalar array as ClickHouse array") {
			runTarget("stream_schema_with_array.jsonl")
			runTarget("stream_schema_with_array.jsonl")

			queryRows(
				"""
				SELECT name, type
				FROM system.columns
				WHERE database = '$db'
				  AND table = 'query_log'
				""".trimIndent(),
			).take(2) shouldContainExactly listOf(
				"databases\tArray(String)",
				"event_time\tDateTime",
			)

			jdbcTemplate.queryForList("SELECT databases FROM $db.query_log")
				.map { it["databases"].asArrayString() }
				.first() shouldBe "['kento', 'nanami']"
		}

		should("should create schemas which specifies cardinality") {
			runTarget("stream_cardinality.jsonl")

			showTables() shouldContainExactly listOf("users")

			queryRows("SHOW CREATE TABLE $db.users")
				.joinToString("\n") shouldContain "`name` LowCardinality(Nullable(String))"
		}

		should("should create schemas which specifiesPK") {
			runTarget("stream_schema_with_all_pk.jsonl")

			queryRows("describe table $db.tickets__follower_ids").also {
				it[0] shouldInclude "_root_id"
				it[1] shouldInclude "_parent_id"
				it[2] shouldInclude "_level_0_index"
			}
		}

		should("should do nothing if schemas already exists") {
			runTarget("stream_1.jsonl")
			runTarget("stream_1.jsonl")
			showTables() shouldHaveSize 21
		}

		should("should create columns for UUID, Int128, Float32 and custom Decimal(p,d) formats") {
			runTarget("stream_type_formats.jsonl")

			queryRows(
				"""
				SELECT name, type
				FROM system.columns
				WHERE database = '$db'
				  AND table = 'type_zoo'
				  AND name NOT LIKE '\_%'
				ORDER BY name
				""".trimIndent(),
			) shouldContainExactly listOf(
				"big_float\tNullable(Float64)",
				"big_int\tNullable(Int128)",
				"id\tInt64",
				"precise_decimal\tNullable(Decimal(10, 4))",
				"small_float\tNullable(Float32)",
				"small_int\tNullable(Int8)",
				"uuid_col\tNullable(UUID)",
			)

			queryCount("type_zoo") shouldBe 1
		}

		should("should honor a custom subtable_separator config") {
			val cfg = writeConfig(baseConfig.copy(subtableSeparator = "_X_"))
			runTarget("stream_nested_array_additional.jsonl", configFile = cfg)

			showTables() shouldContainExactly listOf("users", "users_X_roles", "users_X_roles_X_scopes")
		}
	}

	context("columns update") {
		should("should create / update / delete columns if schema already exists and new has different columns") {
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

		should("should start by truncating before applying schema update") {
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

		should("should handle state at the end of the stream + a closing state, launched several times") {
			repeat(10) {
				runTarget("stream_with_state.jsonl")
				runTarget("stream_tiny.jsonl")
			}

			queryRows("select * from $db.tickets", separator = ",").also {
				it shouldHaveSize 3
				it[1] shouldBe "2,59"
			}
		}

		should("should rename tables as dropped when they are no longer active, and exclude dropped and archived") {
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

		should("should not rename tables as dropped when they are no longer active if they are registered as extra_active") {
			val config = writeConfig(baseConfig.copy(extraActiveTables = listOf("tickets")))
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

		should("should throw if schema already exists and new has different columns with incompatible type") {
			runTarget("stream_vanilla.jsonl")
			shouldThrow<Exception> { runTarget("stream_vanilla_with_incompatible_update.jsonl") }
		}

		should("should throw if schema has no primary key but has array children") {
			shouldThrow<IllegalStateException> {
				runTarget("stream_with_nested_array_without_root_pk.jsonl")
			}.message shouldContain "array child with no root key properties"
		}

		should("should handle second schema definition by commiting pending changes") {
			runTarget("stream_multiple_schema.jsonl")
			queryCount("tickets") shouldBe 1
		}

		should("should recreate if schemas already exists, new is different but specified to be recreated") {
			runTarget("stream_1.jsonl")
			runTarget("stream_1_modified.jsonl", updateStreams = listOf("tickets"))
			showTables() shouldHaveSize 21
		}

		should("should handle additional nested array") {
			runTarget("stream_nested_array.jsonl")
			runTarget("stream_nested_array_additional.jsonl")

			jdbcTemplate.queryForList("show tables from $db") shouldContainExactly listOf(
				mapOf("name" to "users"),
				mapOf("name" to "users__roles"),
				mapOf("name" to "users__roles__scopes"),
			)
		}
	}

	context("Records") {
		should("should insert simple records") {
			runTarget("stream_short.jsonl")
			jdbcTemplate.queryForList(
				"select brand_id from $db.tickets where assignee_id = 11",
			) shouldContainExactly listOf(mapOf("brand_id" to 22L))
		}

		// Verifies that a batch is committed after insert_stream_timeout_sec when no
		// end-of-stream or state message is received. We replace the shell-based approach
		// of the TS test with a PipedInputStream kept open from a coroutine.
		should("should insert record after some time even if stream isn't ended nor state message were received") {
			val insertTimeoutSec = 8
			val cfg = toTargetConfig(
				writeConfig(baseConfig.copy(batchSize = 10, insertStreamTimeoutSec = insertTimeoutSec)),
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
				),
			)
			val recordJson = jsonMapper.writeValueAsString(
				mapOf("type" to "RECORD", "stream" to "tickets", "record" to mapOf("id" to 155)),
			)

			val pipedIn = PipedInputStream(64 * 1024)
			val pipedOut = PipedOutputStream(pipedIn)
			val stateFile = newStateFile()
			val output = BufferedWriter(OutputStreamWriter(Files.newOutputStream(stateFile), StandardCharsets.UTF_8))

			runBlocking {
				val job = launch(Dispatchers.IO) {
					try {
						StreamPipeline.forConfig(cfg).run(pipedIn, output)
					} catch (e: Exception) {
						logger.info(e) { "pipeline terminated" }
					}
				}

				try {
					pipedOut.write((schemaJson + "\n").toByteArray(StandardCharsets.UTF_8))
					pipedOut.write((recordJson + "\n").toByteArray(StandardCharsets.UTF_8))
					pipedOut.flush()

					eventually(20.seconds) {
						jdbcTemplate.queryForMap("EXISTS $db.tickets").values.first() shouldBe 1
					}

					delay(1000)
					jdbcTemplate.queryForList("select id from $db.tickets").shouldBeEmpty()

					eventually((insertTimeoutSec + 10).seconds) {
						jdbcTemplate.queryForList("select id from $db.tickets") shouldContainExactly
								listOf(mapOf("id" to 155L))
					}
				} finally {
					runCatching { pipedOut.close() }
					runCatching { output.close() }
					job.cancel()
				}
			}
		}

		should("should allow reordering of schema") {
			runTarget("stream_short.jsonl")
			runTarget("stream_short_reordered.jsonl")

			jdbcTemplate.queryForList(
				"select brand_id from $db.tickets where assignee_id = 11",
			) shouldContainExactly listOf(mapOf("brand_id" to 22L))
		}

		should("should flatten nested object") {
			runTarget("stream_nested_object.jsonl")
			jdbcTemplate.queryForList(
				"select follower_ids__name from $db.tickets",
			) shouldContainExactly listOf(mapOf("follower_ids__name" to "jack"))
		}

		should("should ingest stream from real data: covidtracker") {
			runTarget("covidtracker.jsonl")
			jdbcTemplate.queryForObject(
				"select sum(total_rows), sum(tables.total_bytes) from system.tables where database = '$db'",
			) { rs, _ -> "${rs.getInt(1)}\t${rs.getInt(2)}" } shouldBe "5789\t1334978"

			runTarget("covidtracker.jsonl")
			jdbcTemplate.queryForObject<Int>("select sum(total_rows) from system.tables where database = '$db'") shouldBe 5789
		}

		should("should ingest stream from real data: clickhouse query log") {
			val totalRowsQuery = "select sum(total_rows) from system.tables where database = '$db'"

			runTarget("clickhouse_query_log.jsonl")
			jdbcTemplate.queryForObject<Int>(totalRowsQuery) shouldBe 1

			runTarget("clickhouse_query_log.jsonl")
			jdbcTemplate.queryForObject<Int>(totalRowsQuery) shouldBe 1

			jdbcTemplate.queryForList("select databases, `Settings.Names` from $db.query_log")
				.map { "${it["databases"].asArrayString()}\t${it["Settings.Names"].asArrayString()}" }
				.first() shouldBe "['system']\t['max_block_size', 'max_query_size', 'join_use_nulls', " +
					"'http_receive_timeout', 'max_expanded_ast_elements', 'max_memory_usage', " +
					"'max_parser_depth', 'lock_acquire_timeout']"
		}

		should("should produce same result from real data whether translate value is effective or not") {
			runTarget("covidtracker.jsonl", configFile = writeConfig(baseConfig.copy(translateValues = false)))
			val sumQuery = "select sum(total_rows), sum(total_bytes) from system.tables where database = '$db'"
			val baseline = jdbcTemplate.queryForList(sumQuery)

			val otherDb = "otherDB"
			jdbcTemplate.execute("CREATE DATABASE IF NOT EXISTS $otherDb;")

			runTarget(
				"covidtracker.jsonl",
				configFile = writeConfig(baseConfig.copy(translateValues = true, database = otherDb)),
			)
			jdbcTemplate.queryForList(sumQuery.replace(db, otherDb)) shouldBe baseline
		}

		should("should handle cleanFirst") {
			runTarget("stream_vanilla.jsonl")
			queryCount("users") shouldBe 4

			runTarget("stream_cleanFirst.jsonl")
			queryCount("users") shouldBe 2
		}

		should("should update schema by creating sub table") {
			runTarget("stream_vanilla.jsonl")
			queryCount("users") shouldBe 4

			runTarget("stream_with_array.jsonl")
			queryCount("users__roles") shouldBe 5
		}

		should("should throw when new pks are added") {
			runTarget("stream_vanilla_with_pks.jsonl")
			queryCount("users") shouldBe 4

			shouldThrow<IllegalStateException> {
				runTarget("stream_vanilla_with_new_pks.jsonl")
			}.message shouldContain "Could not update table because of key properties"
		}

		should("should throw when pks are deleted") {
			runTarget("stream_vanilla_with_pks.jsonl")
			queryCount("users") shouldBe 4

			shouldThrow<IllegalStateException> {
				runTarget("stream_vanilla_with_removed_pks.jsonl")
			}.message shouldContain "Could not update table because of key properties"
		}

		should("should allow pk to be added if stream is in cleanFirst") {
			runTarget("stream_vanilla_with_pks.jsonl")
			queryCount("users") shouldBe 4

			runTarget("stream_vanilla_with_new_pks_and_clean_first.jsonl")
			queryCount("users") shouldBe 4
		}

		should("should handle cleaning column in standard columns") {
			runTarget("stream_vanilla.jsonl")
			queryCount("users") shouldBe 4

			runTarget("stream_cleaningColumn.jsonl")
			queryCount("users") shouldBe 5

			jdbcTemplate.queryForObject<Int>("select id from $db.users where name = 'bill'") shouldBe 7
		}

		should("should handle cleaning column in pk") {
			runTarget("stream_cleaningColumn_pk.jsonl")
			queryRows("select id, name from $db.users") shouldContainExactly
					listOf("5\tbob", "7\tbill", "8\tbill", "9\thelen")

			runTarget("stream_cleaningColumn_pk_2.jsonl")
			queryRows("select id, name from $db.users") shouldContainExactly
					listOf("5\tbob", "9\thelen", "10\tbill")
		}

		should("should handle record when schema specifiesPK") {
			runTarget("stream_short_with_all_pk.jsonl")

			queryRows("describe table $db.tickets__follower_ids").also {
				it[0] shouldInclude "_root_id"
				it[1] shouldInclude "_parent_id"
				it[2] shouldInclude "_level_0_index"
			}

			queryCount("tickets") shouldBe 1
			queryCount("tickets__follower_ids") shouldBe 2
		}

		should("should handle record when schema specifies complex PK") {
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

		should("should handle stream which deletes existing data with one simple pk") {
			runTarget("stream_tiny.jsonl")
			queryRows("select id from $db.tickets") shouldContainExactly listOf("1", "2", "3")

			runTarget("stream_tiny_with_delete.jsonl")
			queryRows("select id from $db.tickets") shouldContainExactly listOf("1", "3")
		}

		should("should handle stream which deletes existing data with multiple pk") {
			runTarget("stream_vanilla_with_pks.jsonl")
			queryRows("select id, name from $db.users", separator = " ") shouldContainExactly
					listOf("1 bill", "2 bill", "3 jack", "4 joe")

			runTarget("stream_vanilla_with_pks_and_deletion.jsonl")
			queryRows("select id, name from $db.users", separator = " ") shouldContainExactly
					listOf("1 bill", "2 bill", "4 joe")
		}

		should("should deduplicate tables when receiving only schema") {
			runTarget("stream_vanilla_with_pks.jsonl")
			queryCount("users") shouldBe 4

			jdbcTemplate.execute("INSERT INTO $db.users VALUES (4, 'joe', 90);")
			queryCount("users") shouldBe 5

			runTarget("stream_vanilla_with_pks_no_records.jsonl")
			queryCount("users") shouldBe 4
		}

		should("should handle DELETED_RECORD whose body contains only PK fields") {
			runTarget("stream_deleted_record_pk_only.jsonl")

			queryRows("select id, name, age from $db.users order by id") shouldContainExactly
					listOf("1\talice\t30", "3\tcarol\t40")
		}

		should("should throw when a RECORD arrives before its SCHEMA") {
			shouldThrow<IllegalStateException> {
				runTarget("stream_record_before_schema.jsonl")
			}.message shouldContain "before Schema is defined"
		}

		should("should throw when DELETED_RECORD is sent on a stream without primary keys") {
			shouldThrow<IllegalStateException> {
				runTarget("stream_deleted_record_no_pk.jsonl")
			}.message shouldContain "cannot push deleted record to a stream without pk mapping"
		}
	}
})

private data class TestTargetConfig(
	val host: String,
	val username: String,
	val password: String,
	val port: Int,
	val database: String,
	val extraActiveTables: List<String> = emptyList(),
	val batchSize: Int? = 100,
	val insertStreamTimeoutSec: Int? = 180,
	val translateValues: Boolean = false,
	val subtableSeparator: String? = null,
)
