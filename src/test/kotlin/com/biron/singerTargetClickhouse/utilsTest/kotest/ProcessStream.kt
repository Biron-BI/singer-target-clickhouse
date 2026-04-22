@file:Suppress("SqlNoDataSourceInspection")

package com.biron.singerTargetClickhouse.utilsTest.kotest

import com.clickhouse.jdbc.ClickHouseArray
import com.clickhouse.jdbc.ClickHouseDataSource
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.github.oshai.kotlinlogging.KotlinLogging
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.DescribeSpec
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldNotContain
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.string.shouldInclude
import kotlinx.coroutines.*
import org.springframework.jdbc.core.JdbcTemplate
import org.testcontainers.clickhouse.ClickHouseContainer
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.startupcheck.IndefiniteWaitOneShotStartupCheckStrategy
import org.testcontainers.utility.MountableFile
import java.io.File
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Paths
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds


data class Config(
	val host: String,
	val username: String,
	val password: String,
	val port: Int,
	val database: String,
	val extra_active_tables: List<String> = emptyList(),
	val tablesToRecreate: List<String> = emptyList(),
	val batch_size: Int? = 100,
	val insert_stream_timeout_sec: Int? = 180,
	val translate_values: Boolean = false
)

class ProcessStreamTest : DescribeSpec({

	val logger = KotlinLogging.logger {}

	val initialConnInfo = Config(
		host = "clickhouse-server",
		port = 8123,
		database = "datayse",
		username = "user",
		password = "averysecurepassword"
	)

	lateinit var container: ClickHouseContainer
	lateinit var jdbcTemplate: JdbcTemplate
	lateinit var network: Network
	beforeSpec {
		try {
			network = Network.newNetwork()

			container = ClickHouseContainer("clickhouse/clickhouse-server:24.12.3.47")
				.withNetwork(network)
				.withNetworkAliases("clickhouse-server")
				.apply {
					withUsername(initialConnInfo.username)
					withPassword(initialConnInfo.password)
					withDatabaseName(initialConnInfo.database)
					start()
				}

			jdbcTemplate = ClickHouseDataSource(
				"jdbc:clickhouse://${container.host}:${container.getMappedPort(initialConnInfo.port)}",
				mapOf("user" to container.username, "password" to container.password).toProperties(),
			)
				.let(::JdbcTemplate)
		} catch (e: Exception) {
			logger.error(e) { "Error during startup of ClickHouse container" }
			throw e
		}
	}
	afterSpec {
		try {
			container.stop()
		} catch (e: Exception) {
			logger.error(e) { "Error during stopping of ClickHouse container" }
			throw e
		}
	}
	beforeEach {
		jdbcTemplate.execute("DROP DATABASE IF EXISTS ${initialConnInfo.database};")
		jdbcTemplate.execute("CREATE DATABASE ${initialConnInfo.database};")
	}

	fun runDockerCommand(configFilePath: String, filePath: String): GenericContainer<Nothing> {
		try {
			val targetContainer = GenericContainer<Nothing>("ghcr.io/biron-bi/target-clickhouse:2.11.0").apply {
				withNetwork(network)
				withNetworkAliases("clickhouse-client")
				withCopyFileToContainer(MountableFile.forHostPath(configFilePath), "/config.json")
				withCopyFileToContainer(MountableFile.forHostPath(filePath), "/input.jsonl")
				withCommand("--config", "/config.json", "--input", "/input.jsonl", "--output", "/state.jsonl")
				withStartupCheckStrategy(IndefiniteWaitOneShotStartupCheckStrategy())
				withLogConsumer { logger.info("[target-clickhouse] ${it.utf8String}") }
			}
				.apply { start() }

			val state = targetContainer.currentContainerInfo!!.state!!
			if (state.exitCodeLong != 0L) {
				logger.info("Error during execution of the command with Testcontainers: ${state.error}")
			}
			return targetContainer
		} catch (e: Exception) {
			logger.info(e, { "Error during execution of the command with Testcontainers" })
			throw e
		}
	}

	fun configFile(initialConnInfo: Config): File {
		val config = File.createTempFile("test-config", ".json").apply {
			val baseConfig = """
        {
            "host": "${initialConnInfo.host}",
            "port": ${initialConnInfo.port},
            "database": "${initialConnInfo.database}",
            "username": "${initialConnInfo.username}",
            "password": "${initialConnInfo.password}",
            "extra_active_tables": ${initialConnInfo.extra_active_tables.joinToString(prefix = "[", postfix = "]") { "\"$it\"" }},
            "tablesToRecreate": ${initialConnInfo.tablesToRecreate.joinToString(prefix = "[", postfix = "]") { "\"$it\"" }},
            "batch_size": ${initialConnInfo.batch_size},
            "insert_stream_timeout_sec": ${initialConnInfo.insert_stream_timeout_sec},
            "translate_values": ${initialConnInfo.translate_values}
        }
        """.trimIndent()

			writeText(baseConfig)
		}
		return config
	}

	describe("outputStream") {

		it("should write state to passed outputStream") {
			val configFile = configFile(initialConnInfo)
			val targetContainer = runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_with_state.jsonl"
			)
			targetContainer.copyFileFromContainer("/state.jsonl", "/tmp/state.jsonl")

			val stateFilePath = Paths.get("/tmp/state.jsonl")
			if (Files.exists(stateFilePath)) {
				val stateContent = Files.readAllLines(stateFilePath, Charset.forName("UTF-8"))
					.map { it.trimStart('\uFEFF') }

				stateContent.size shouldBe 2

				val expectedContent = listOf(
					"""{"bookmarks":{"toto":"tata"},",currently_syncing":"tickets"}""",
					"""{"bookmarks":{},"currently_syncing":null}"""
				)
				stateContent shouldBe expectedContent
			} else {
				logger.info("Le fichier state.jsonl n'existe pas")
			}
		}
	}
	describe("Schemas") {
		it("should create schemas") {
			val configFile = configFile(initialConnInfo)
			runDockerCommand(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_1.jsonl")

			val tables = jdbcTemplate.queryForList("SHOW TABLES FROM ${initialConnInfo.database}", String::class.java)
			tables.size shouldBe 21
			tables shouldContain "ticket_audits"
			tables shouldContain "ticket_audits__events__attachments"
			tables shouldContain "ticket_audits__metadata__notifications_suppressed_for"
			tables shouldContain "tickets"
			tables shouldContain "tickets__custom_fields"
		}

		it("should create schema with nullable scalar array") {
			val configFile = configFile(initialConnInfo)
			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_schema_array_nullable.jsonl"
			)

			val query = """
                    SELECT name, type
                    FROM system.columns
                    WHERE table LIKE 'return_requests_%'
                      AND database = '${initialConnInfo.database}'
                      AND name = 'value'
                """.trimIndent()

			val result = jdbcTemplate.queryForList(query).map { row ->
				"${row["name"]}\t${row["type"]}"
			}.joinToString("\n")

			result shouldBe "value\tNullable(String)"
		}

		it("should create schema with nullable scalar array as ClickHouse array") {
			val configFile = configFile(initialConnInfo)
			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_schema_with_array.jsonl"
			)

			val columnsQuery = """
                        SELECT name, type
                        FROM system.columns
                        WHERE database = '${initialConnInfo.database}'
                          AND table = 'query_log'
                    """.trimIndent()

			val columnsResult = jdbcTemplate.queryForList(columnsQuery).map { row ->
				"${row["name"]}\t${row["type"]}"
			}

			columnsResult[0] shouldBe "databases\tArray(String)"
			columnsResult[1] shouldBe "event_time\tDateTime"

			val dataQuery = """
                        SELECT databases
                        FROM ${initialConnInfo.database}.query_log
                    """.trimIndent()

			val dataResult = jdbcTemplate.queryForList(dataQuery).map { row ->
				val databases = row["databases"]
				when (databases) {
					is ClickHouseArray -> {
						(databases.array as Array<*>).joinToString(prefix = "[", postfix = "]") { "'$it'" }
					}

					else -> databases.toString()
				}
			}
			dataResult[0] shouldBe "['kento', 'nanami']"
		}

		it("should create schemas which specifies cardinality") {
			val configFile = configFile(initialConnInfo)
			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_cardinality.jsonl"
			)

			val query = """
                    show tables from ${initialConnInfo.database}
                """.trimIndent()

			val tables = jdbcTemplate.queryForList(query).map { row ->
				row.values.first().toString()
			}
			tables.size shouldBe 1
			tables shouldContain "users"

			val createTableQuery = """
                    SHOW CREATE TABLE ${initialConnInfo.database}.users
                """.trimIndent()

			val createTableOutput = jdbcTemplate.queryForList(createTableQuery).joinToString("\n") { row ->
				row.values.joinToString("\t")
			}
			createTableOutput shouldContain "`name` LowCardinality(Nullable(String))"

		}

		it("should create schemas which specifiesPK") {
			val configFile = configFile(initialConnInfo)
			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_schema_with_all_pk.jsonl"
			)

			val query = """
                    describe table ${initialConnInfo.database}.tickets__follower_ids
                """.trimIndent()
			val columns = jdbcTemplate.queryForList(query).map { row ->
				row.values.joinToString("\t")
			}

			columns[0] shouldInclude "_root_id"
			columns[1] shouldInclude "_parent_id"
			columns[2] shouldInclude "_level_0_index"
		}

		it("should do nothing if schemas already exists") {
			val configFile = configFile(initialConnInfo)
			runDockerCommand(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_1.jsonl")
			runDockerCommand(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_1.jsonl")

			val tablesAfter = jdbcTemplate.queryForList("SHOW TABLES FROM ${initialConnInfo.database}", String::class.java)
			tablesAfter.size shouldBe 21
		}
	}
	describe("columns update") {

		it("should create / update / delete columns if schema already exists and new has different columns") {
			val configFile = configFile(initialConnInfo)
			runDockerCommand(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_1.jsonl")
			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_1_modified.jsonl"
			)

			val columns = jdbcTemplate.queryForList("show tables from ${initialConnInfo.database}")
			columns.size shouldBe 21

			val execResult = jdbcTemplate.queryForList(
				"select name, type\n" +
						"from system.columns\n" +
						"where table = 'tickets'\n" +
						"and database = '${initialConnInfo.database}'\n" +
						"order by name"
			).map { row ->
				row.values.joinToString("\t")
			}.map { it.replace("\t", " ") }

			execResult shouldContain "organization_id Nullable(String)"
			execResult shouldContain "new_requester_id Nullable(Int64)"
			execResult shouldNotContain "requester_id Nullable(Int64)"

		}

		it("should start by truncating before applying schema update") {
			val configFile = configFile(initialConnInfo)
			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_nullable.jsonl"
			)
			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_non_nullable.jsonl"
			)

			val execResult = jdbcTemplate.queryForList(
				"select name, type\n" +
						"from system.columns\n" +
						"where table = 'users'\n" +
						"and database = '${initialConnInfo.database}'\n" +
						"order by name"
			).map { row ->
				row.values.joinToString("\t")
			}.map { it.replace("\t", " ") }

			execResult shouldContain "id Int64"
			execResult.size shouldBe 1
		}

		it("should handle state at the end of the stream + a closing state, launched several times") {
			val configFile = configFile(initialConnInfo)
			for (i in 0 until 10) {
				runDockerCommand(
					configFile.absolutePath,
					"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_with_state.jsonl"
				)
				runDockerCommand(
					configFile.absolutePath,
					"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_tiny.jsonl"
				)
			}

			val execResult = jdbcTemplate.queryForList("select * from ${initialConnInfo.database}.tickets").map { row ->
				row.values.joinToString("\t")
			}.map { it.replace("\t", ",") }

			execResult.size shouldBe 3
			execResult[1] shouldBe "2,59"
		}

		it("should rename tables as dropped when they are no longer active, and exclude dropped and archived") {
			val configFile = configFile(initialConnInfo)
			runDockerCommand(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_1.jsonl")
			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_1_inactive.jsonl"
			)

			val tables = jdbcTemplate.queryForList("show tables from ${initialConnInfo.database}", String::class.java)
			tables.size shouldBe 21
			tables.forEach { table ->
				if (!table.contains("ticket_audits")) {
					table.startsWith("_dropped_") shouldBe true
					println("Table $table should start with '_dropped_'")
				} else {
					table.startsWith("_dropped_") shouldBe false
					println("Table $table should not start with '_dropped_'")
				}
			}
			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_1_inactive.jsonl"
			)
			val execResult =
				jdbcTemplate.queryForList("show tables from ${initialConnInfo.database}", String::class.java)
			execResult.size shouldBe 21
			execResult.forEach { table ->
				if (!table.contains("ticket_audits")) {
					table.startsWith("_dropped_") shouldBe true
					println("Table $table should start with '_dropped_'")
				} else {
					table.startsWith("_dropped_") shouldBe false
					println("Table $table should not start with '_dropped_'")
				}
				table.startsWith("_dropped__dropped_") shouldBe false
				println("table $table should not be renamed twice")
			}

			jdbcTemplate.execute("RENAME TABLE ${initialConnInfo.database}._dropped_ticket_metrics TO ${initialConnInfo.database}._archived_ticket_metrics")
			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_1_inactive.jsonl"
			)
			val showTables =
				jdbcTemplate.queryForList("show tables from ${initialConnInfo.database}", String::class.java)

			showTables.size shouldBe 21
			showTables.forEach { table ->
				if (!table.contains("ticket_audits")) {
					if (table.contains("ticket_metrics")) {
						table.startsWith("_archived_") shouldBe true
						println("Table $table should start with '_archived_'")

						table.contains("_dropped_") shouldBe false
						println("Table $table should not include '_dropped_'")
					} else {
						table.startsWith("_dropped_") shouldBe true
						println("Table $table should start with '_dropped_'")
					}
				} else {
					table.startsWith("_archived_") shouldBe false
					println("Table $table should not start with '_archived_'")

					table.startsWith("_dropped_") shouldBe false
					println("Table $table should not start with '_dropped_'")
				}
				table.startsWith("_dropped__dropped_") shouldBe false
				println("Table $table should not be renamed twice")
			}
		}

		it("should not rename tables as dropped when they are no longer active if they are registered as extra_active") {
			val configFile = configFile(initialConnInfo.copy(extra_active_tables = listOf("tickets")))
			println(configFile.readText(Charsets.UTF_8))

			runDockerCommand(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_1.jsonl")
			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_1_inactive.jsonl"
			)
			val execResult =
				jdbcTemplate.queryForList("show tables from ${initialConnInfo.database}", String::class.java)

			execResult.size shouldBe 21
			execResult.forEach { table ->
				if (!table.contains("ticket_audits") && !table.contains("tickets")) {
					table.startsWith("_dropped_") shouldBe true
					println("Table $table should start with '_dropped_'")
				} else {
					table.startsWith("_dropped_") shouldBe false
					println("Table $table should not start with '_dropped_'")
				}
			}
		}

		it("should throw if schema already exists and new has different columns with incompatible type") {
			val configFile = configFile(initialConnInfo)
			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_vanilla.jsonl"
			)
			shouldThrow<Exception> {
				runDockerCommand(
					configFile.absolutePath,
					"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_vanilla_with_incompatible_update.jsonl"
				)
			}
		}

		it("should throw if schema has no primary key but has array children") {
			val configFile = configFile(initialConnInfo)
			shouldThrow<Exception> {
				runDockerCommand(
					configFile.absolutePath,
					"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_with_nested_array_without_root_pk.jsonl"
				)
			}
		}

		it("should ignore second schema definition") {
			val configFile = configFile(initialConnInfo)
			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_multiple_schema.jsonl"
			)
		}

		it("should recreate if schemas already exists, new is different but specified to be recreated") {
			val configFile = configFile(initialConnInfo)
			runDockerCommand(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_1.jsonl")

			val newConfigFile = configFile(initialConnInfo.copy(tablesToRecreate = listOf("tickets")))
			println("newConfigFile : ${newConfigFile.readText(Charsets.UTF_8)}")
			runDockerCommand(
				newConfigFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_1_modified.jsonl"
			)

			val tables = jdbcTemplate.queryForList("show tables from ${initialConnInfo.database}")
			tables.size shouldBe 21
		}

		it("should handle additional nested array") {
			val configFile = configFile(initialConnInfo)
			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_nested_array.jsonl"
			)
			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_nested_array_additional.jsonl"
			)
			val tables = jdbcTemplate.queryForList("show tables from ${initialConnInfo.database}")
			tables shouldContainExactly listOf(
				mapOf("name" to "users"),
				mapOf("name" to "users__roles"),
				mapOf("name" to "users__roles__scopes")
			)
		}
	}
	describe("Records") {

		it("should insert simple records") {
			val configFile = configFile(initialConnInfo)
			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_short.jsonl"
			)

			val execResult = jdbcTemplate.queryForList("select brand_id from ${initialConnInfo.database}.tickets where assignee_id = 11")
			execResult shouldContainExactly listOf(mapOf("brand_id" to 22L))
		}

		//is inserted immediately instead of waiting for the "insert_stream_timeout_sec" delay
		it("should insert record after some time even if stream isn't ended nor state message were received") {
			runBlocking {
				val schema = mapOf(
					"type" to "SCHEMA",
					"stream" to "tickets",
					"schema" to mapOf(
						"properties" to mapOf(
							"id" to mapOf("type" to listOf("integer"))
						),
						"type" to listOf("null", "object")
					),
					"key_properties" to listOf("id")
				)
				val record = mapOf(
					"type" to "RECORD",
					"stream" to "tickets",
					"record" to mapOf("id" to 155)
				)
				val mapper = jacksonObjectMapper()
				val schemaJson = mapper.writeValueAsString(schema)
				val recordJson = mapper.writeValueAsString(record)
				val tempFile = File.createTempFile("schema", ".json").apply {
					writeText("$schemaJson\n$recordJson")
				}
				logger.info("tempFile : ${tempFile.readText(Charsets.UTF_8)}")
				val config = configFile(initialConnInfo.copy(batch_size = 10, insert_stream_timeout_sec = 15))
				logger.info("config : ${config.readText(Charsets.UTF_8)}")
				val job = launch(Dispatchers.IO) { runDockerCommand(config.absolutePath, tempFile.absolutePath) }

				withTimeout(10.seconds) {
					while (jdbcTemplate.queryForMap("EXISTS ${initialConnInfo.database}.tickets").values.first() == 0) {
						delay(50.milliseconds)
					}
				}

				delay(1000)
				jdbcTemplate.queryForList("Select id from ${initialConnInfo.database}.tickets").shouldBeEmpty()

				val maxAttempts = 10
				var attempt = 0
				var recordInserted = false
				while (attempt < maxAttempts && !recordInserted) {
					delay(1000)
					val execResult = jdbcTemplate.queryForList("select id from ${initialConnInfo.database}.tickets")
					logger.info("execResult after ${attempt + 1} attempts: $execResult")
					recordInserted = execResult.any { it["id"] == 155L }
					attempt++
				}
				job.join()

				val execResult = jdbcTemplate.queryForList("select id from ${initialConnInfo.database}.tickets")
				logger.info("execResult after final attempt: $execResult")
				execResult shouldContainExactly listOf(mapOf("id" to 155L))
			}
		}


		it("should allow reordering of schema") {
			val configFile = configFile(initialConnInfo)
			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_short.jsonl"
			)
			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_short_reordered.jsonl"
			)
			val execResult = jdbcTemplate.queryForList("select brand_id from ${initialConnInfo.database}.tickets where assignee_id = 11")
			execResult shouldContainExactly listOf(mapOf("brand_id" to 22L))
		}

		it("should flatten nested object") {
			val configFile = configFile(initialConnInfo)
			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_nested_object.jsonl"
			)
			val execResult = jdbcTemplate.queryForList("select follower_ids__name from ${initialConnInfo.database}.tickets")
			execResult shouldContainExactly listOf(mapOf("follower_ids__name" to "jack"))
		}

		it("should ingest stream from real data: covidtracker") {
			val configFile = configFile(initialConnInfo)
			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/covidtracker.jsonl"
			)
			val execResult =
				jdbcTemplate.queryForObject("select sum(total_rows), sum(tables.total_bytes) from system.tables where database = '${initialConnInfo.database}'") { rs, _ ->
					rs.getInt(1).toString() + "\t" + rs.getInt(2).toString()
				}
			execResult shouldBe "5789\t1334466"

			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/covidtracker.jsonl"
			)
			val execResults = jdbcTemplate.queryForObject(
				"select sum(total_rows) from system.tables where database = '${initialConnInfo.database}'",
				Int::class.java
			)
			execResults shouldBe 5789

		}

		it("should ingest stream from real data: clickhouse query log") {
			val configFile = configFile(initialConnInfo)
			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/clickhouse_query_log.jsonl"
			)
			val execResult = jdbcTemplate.queryForObject(
				"select sum(total_rows) from system.tables where database = '${initialConnInfo.database}'",
				Int::class.java
			)
			execResult shouldBe 1
			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/clickhouse_query_log.jsonl"
			)
			val execResult2 = jdbcTemplate.queryForObject(
				"select sum(total_rows) from system.tables where database = '${initialConnInfo.database}'",
				Int::class.java
			)
			execResult2 shouldBe 1

			val execResult3 =
				jdbcTemplate.queryForList("select databases, `Settings.Names` from ${initialConnInfo.database}.query_log").map { row ->
					val databases = when (val db = row["databases"]) {
						is ClickHouseArray -> (db.array as Array<*>).joinToString(prefix = "[", postfix = "]") { "'$it'" }
						else -> db.toString()
					}
					val settings = when (val st = row["Settings.Names"]) {
						is ClickHouseArray -> (st.array as Array<*>).joinToString(prefix = "[", postfix = "]") { "'$it'" }
						else -> st.toString()
					}
					"$databases\t$settings"
				}
			execResult3.first() shouldBe "['system']\t['max_block_size', 'max_query_size', 'join_use_nulls', 'http_receive_timeout', 'max_expanded_ast_elements', 'max_memory_usage', 'max_parser_depth', 'lock_acquire_timeout']"
		}

		it("should produce same result from real data whether translate value is effective or not") {
			val configFile = configFile(initialConnInfo.copy(translate_values = false))

			println("configFile : ${configFile.readText(Charsets.UTF_8)}")
			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/covidtracker.jsonl"
			)
			val testQuery = "select sum(total_rows), sum(total_bytes) from system.tables where database = '${initialConnInfo.database}'"
			val execResult = jdbcTemplate.queryForList(testQuery)

			val otherDb = "otherDB"
			jdbcTemplate.execute("CREATE DATABASE IF NOT EXISTS $otherDb;")
			jdbcTemplate.execute("CREATE USER IF NOT EXISTS ${initialConnInfo.username} IDENTIFIED WITH plaintext_password BY '${initialConnInfo.password}';")
			jdbcTemplate.execute("GRANT ALL ON $otherDb.* TO ${initialConnInfo.username};")

			val configFile2 = configFile(initialConnInfo.copy(translate_values = true, database = otherDb))
			runDockerCommand(
				configFile2.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/covidtracker.jsonl"
			)
			val execResult2 = jdbcTemplate.queryForList(testQuery.replace(initialConnInfo.database, otherDb))

			execResult shouldBe execResult2
		}

		it("should handle cleanFirst") {
			val configFile = configFile(initialConnInfo)
			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_vanilla.jsonl"
			)
			val execResult = jdbcTemplate.queryForObject("select count(*) from ${initialConnInfo.database}.users", Int::class.java)
			execResult shouldBe 4

			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_with_array.jsonl"
			)
			val execResult2 = jdbcTemplate.queryForObject("select count(*) from ${initialConnInfo.database}.users__roles", Int::class.java)
			execResult2 shouldBe 5
		}

		it("should throw when new pks are added") {
			val configFile = configFile(initialConnInfo)
			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_vanilla_with_pks.jsonl"
			)
			val execResult = jdbcTemplate.queryForObject("select count(*) from ${initialConnInfo.database}.users", Int::class.java)
			execResult shouldBe 4

			shouldThrow<Exception> {
				runDockerCommand(
					configFile.absolutePath,
					"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_vanilla_with_new_pks.jsonl"
				)
			}
		}

		it("should throw when pks are deleted") {
			val configFile = configFile(initialConnInfo)
			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_vanilla_with_pks.jsonl"
			)
			val execResult = jdbcTemplate.queryForObject("select count(*) from ${initialConnInfo.database}.users", Int::class.java)
			execResult shouldBe 4

			shouldThrow<Exception> {
				runDockerCommand(
					configFile.absolutePath,
					"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_vanilla_with_removed_pks.jsonl"
				)
			}
		}

		it("should allow pk to be added if stream is in cleanFirst") {
			val configFile = configFile(initialConnInfo)
			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_vanilla_with_pks.jsonl"
			)
			val execResult = jdbcTemplate.queryForObject("select count(*) from ${initialConnInfo.database}.users", Int::class.java)
			execResult shouldBe 4

			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_vanilla_with_new_pks_and_clean_first.jsonl"
			)
			val execResult2 = jdbcTemplate.queryForObject("select count(*) from ${initialConnInfo.database}.users", Int::class.java)
			execResult2 shouldBe 4
		}

		it("should handle cleaning column in standard columns") {
			val configFile = configFile(initialConnInfo)

			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_vanilla.jsonl"
			)
			val execResult = jdbcTemplate.queryForObject("select count(*) from ${initialConnInfo.database}.users", Int::class.java)
			execResult shouldBe 4

			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_cleaningColumn.jsonl"
			)
			val execResult2 = jdbcTemplate.queryForObject("select count(*) from ${initialConnInfo.database}.users", Int::class.java)
			execResult2 shouldBe 5

			val execResult3 = jdbcTemplate.queryForObject("select id from ${initialConnInfo.database}.users where name = 'bill'", Int::class.java)
			execResult3 shouldBe 7
		}

		it("should handle cleaning column in pk") {
			val configFile = configFile(initialConnInfo)
			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_cleaningColumn_pk.jsonl"
			)
			val execResult = jdbcTemplate.queryForList("select id, name from ${initialConnInfo.database}.users").map { row ->
				row.values.joinToString("\t")
			}
			execResult shouldContainExactly listOf("5\tbob", "7\tbill", "8\tbill", "9\thelen")

			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_cleaningColumn_pk_2.jsonl"
			)
			val execResult2 = jdbcTemplate.queryForList("select id, name from ${initialConnInfo.database}.users").map { row ->
				row.values.joinToString("\t")
			}
			execResult2 shouldContainExactly listOf("5\tbob", "9\thelen", "10\tbill")

		}

		it("should handle record when schema specifiesPK") {
			val configFile = configFile(initialConnInfo)
			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_short_with_all_pk.jsonl"
			)
			val execResult = jdbcTemplate.queryForList("describe table ${initialConnInfo.database}.tickets__follower_ids").map { row ->
				row.values.joinToString("\t")
			}
			execResult.get(0) shouldInclude "_root_id"
			execResult.get(1) shouldInclude "_parent_id"
			execResult.get(2) shouldInclude "_level_0_index"

			var execResultForOject = jdbcTemplate.queryForObject("select count(*) from ${initialConnInfo.database}.tickets", Int::class.java)
			execResultForOject shouldBe 1

			execResultForOject =
				jdbcTemplate.queryForObject("select count(*) from ${initialConnInfo.database}.tickets__follower_ids", Int::class.java)
			execResultForOject shouldBe 2

		}

		it("should handle record when schema specifies complex PK") {
			val configFile = configFile(initialConnInfo)
			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_short_with_all_pk2.jsonl"
			)
			val execResult = jdbcTemplate.queryForList("describe table ${initialConnInfo.database}.tickets__follower_ids").map { row ->
				row.values.joinToString("\t")
			}
			execResult.get(0) shouldInclude "_root_id"
			execResult.get(1) shouldInclude "_parent_id"
			execResult.get(2) shouldInclude "name"
			execResult.get(3) shouldInclude "_level_0_index"

			var execResultForOject = jdbcTemplate.queryForObject("select count(*) from ${initialConnInfo.database}.tickets", Int::class.java)
			execResultForOject shouldBe 1

			execResultForOject =
				jdbcTemplate.queryForObject("select count(*) from ${initialConnInfo.database}.tickets__follower_ids", Int::class.java)
			execResultForOject shouldBe 2
		}

		it("should handle stream which deletes existing data with one simple pk") {
			val configFile = configFile(initialConnInfo)
			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_tiny.jsonl"
			)
			var execResult = jdbcTemplate.queryForList("select id from ${initialConnInfo.database}.tickets").map { row ->
				row.values.joinToString("\t")
			}
			execResult shouldContainExactly listOf("1", "2", "3")

			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_tiny_with_delete.jsonl"
			)
			execResult = jdbcTemplate.queryForList("select id from ${initialConnInfo.database}.tickets").map { row ->
				row.values.joinToString("\t")
			}
			execResult shouldContainExactly listOf("1", "3")
		}

		it("should handle stream which deletes existing data with multiple pk") {
			val configFile = configFile(initialConnInfo)
			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_vanilla_with_pks.jsonl"
			)
			var execResult = jdbcTemplate.queryForList("select id, name from ${initialConnInfo.database}.users").map { row ->
				row.values.joinToString(" ")
			}
			execResult shouldContainExactly listOf("1 bill", "2 bill", "3 jack", "4 joe")
			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_vanilla_with_pks_and_deletion.jsonl"
			)
			execResult = jdbcTemplate.queryForList("select id, name from ${initialConnInfo.database}.users").map { row ->
				row.values.joinToString(" ")
			}
			execResult shouldContainExactly listOf("1 bill", "2 bill", "4 joe")
		}

		it("should deduplicate tables when receiving only schema") {
			val configFile = configFile(initialConnInfo)
			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_vanilla_with_pks.jsonl"
			)
			var execResult = jdbcTemplate.queryForObject("select count(*) from ${initialConnInfo.database}.users", Int::class.java)
			execResult shouldBe 4

			jdbcTemplate.execute("INSERT INTO ${initialConnInfo.database}.users VALUES (4, 'joe', 90);")
			execResult = jdbcTemplate.queryForObject("select count(*) from ${initialConnInfo.database}.users", Int::class.java)
			execResult shouldBe 5

			runDockerCommand(
				configFile.absolutePath,
				"./src/test/kotlin/com/biron/singerTargetClickhouse/utilsTest/kotest/data/stream_vanilla_with_pks_no_records.jsonl"
			)
			execResult = jdbcTemplate.queryForObject("select count(*) from ${initialConnInfo.database}.users", Int::class.java)
			execResult shouldBe 4
		}
	}
})
