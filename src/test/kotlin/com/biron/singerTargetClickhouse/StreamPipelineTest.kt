package com.biron.singerTargetClickhouse

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.mockk.every
import io.mockk.mockk
import java.io.StringWriter

class StreamPipelineTest : ShouldSpec({

	afterTest { checkAndClearAllMocks() }

	fun schema(stream: String, cleanFirst: Boolean = false): String =
		"""{
			"type":"SCHEMA",
			"stream":"$stream",
			"schema":{"type":["null","object"],"properties":{"id":{"type":"integer"}}},
			"key_properties":["id"],
			"clean_first":${if (cleanFirst) "true" else "false"}
		}""".trimIndent()

	fun record(stream: String, id: Int): String =
		"""{"type":"RECORD","stream":"$stream","record":{"id":$id}}"""

	fun state(bookmark: String): String =
		"""{"type":"STATE","value":{"bookmark":"$bookmark"}}"""

	fun connectionFor(expectedConfig: TargetConfig, conn: TargetConnection): (TargetConfig) -> TargetConnection =
		{ actualConfig ->
			actualConfig shouldBe expectedConfig
			conn
		}

	context("StreamPipeline (real StreamProcessor)") {
		val baseConfig = TargetConfig(
			host = "h", port = 1, username = "u", password = "p", database = "db",
			finalizeConcurrency = 1,
		)

		should("writes STATE messages verbatim and triggers a commit") {
			val conn: TargetConnection = mockk {
				every { listTables() } returns emptyList()
				every { getDatabase() } returns "db"
			}
			conn.captureRunQueries()
			val rowWriters = conn.captureRowWriters()

			val out = StringWriter()
			val input = buildString {
				append(schema("users")); append("\n")
				append(record("users", 1)); append("\n")
				append(state("x")); append("\n")
			}.byteInputStream()

			StreamPipeline.forConfig(baseConfig, connectionFor(baseConfig, conn)).run(input, out)

			out.toString().trim() shouldBe """{"bookmark":"x"}"""
			rowWriters.streams.size shouldBe 1
			rowWriters.streams.first().closed shouldBe true
		}

		should("renames non-active tables on ACTIVE_STREAMS") {
			val conn: TargetConnection = mockk {
				every { listTables() } returns listOf("tickets", "tickets__tags", "obsolete")
				every { renameObsoleteTable("obsolete") } returns QueryResult(emptyList(), 0)
			}

			val out = StringWriter()
			val input = """{"type":"ACTIVE_STREAMS","streams":["tickets"]}""".byteInputStream()

			StreamPipeline.forConfig(baseConfig, connectionFor(baseConfig, conn)).run(input, out)
			// Strict mockk: any rename of "tickets" or "tickets__tags" would fail (no stub).
		}

		should("keeps subtables, extra-active tables, and tables already prefixed _dropped_/_archived_") {
			val tables = listOf(
				"tickets",          // active stream — keep
				"tickets__tags",    // subtable of active stream — keep
				"audits",           // extra_active_tables — keep
				"audits__events",   // subtable of extra-active — keep
				"_dropped_legacy",  // already dropped — keep
				"_archived_legacy", // archived — keep
				"legacy_metrics",   // unrelated — rename
			)
			val conn: TargetConnection = mockk {
				every { listTables() } returns tables
				every { renameObsoleteTable("legacy_metrics") } returns QueryResult(emptyList(), 0)
			}

			val out = StringWriter()
			val input = """{"type":"ACTIVE_STREAMS","streams":["tickets"]}""".byteInputStream()

			val cfg = baseConfig.copy(extraActiveTables = listOf("audits"))
			StreamPipeline.forConfig(cfg, connectionFor(cfg, conn)).run(input, out)
			// Strict mockk: any other rename would fail (no stub).
		}

		should("respects a custom subtable separator when matching active streams") {
			val conn: TargetConnection = mockk {
				every { listTables() } returns listOf("orders", "orders::items", "orders__items")
				every { renameObsoleteTable("orders__items") } returns QueryResult(emptyList(), 0)
			}

			val out = StringWriter()
			val input = """{"type":"ACTIVE_STREAMS","streams":["orders"]}""".byteInputStream()

			val cfg = baseConfig.copy(subtableSeparator = "::")
			StreamPipeline.forConfig(cfg, connectionFor(cfg, conn)).run(input, out)
		}
	}

	context("StreamPipeline dispatch (with injected fake factory)") {
		val baseConfig = TargetConfig(
			host = "h", port = 1, username = "u", password = "p", database = "db",
			finalizeConcurrency = 1,
		)

		val schemaUsers = schema("users")
		val schemaOrders = schema("orders")

		fun runWith(
			factory: RecordingStreamProcessorFactory,
			vararg lines: String,
			streamsToReplace: List<String> = emptyList(),
			setup: TargetConnection.() -> Unit = {},
		): Pair<TargetConnection, StringWriter> {
			val conn: TargetConnection = mockk {
				every { listTables() } returns emptyList()
			}
			conn.setup()
			val out = StringWriter()
			val input = (lines.joinToString("\n") + "\n").byteInputStream()
			StreamPipeline.forConfig(baseConfig, connectionFor(baseConfig, conn), streamProcessorFactory = factory.asFactory())
				.run(input, out, streamsToReplace)
			return conn to out
		}

		should("invokes the factory once per SCHEMA, with the stream name and cleanFirst flag") {
			val factory = RecordingStreamProcessorFactory()
			runWith(factory, schemaUsers, schemaOrders)

			factory.invocations.map { it.stream } shouldBe listOf("users", "orders")
			factory.invocations.all { !it.cleanFirst } shouldBe true
		}

		should("dispatches RECORD messages to the per-stream processor") {
			val factory = RecordingStreamProcessorFactory()
			runWith(
				factory,
				schemaUsers, record("users", 1), record("users", 2),
				schemaOrders, record("orders", 99),
			)

			val users = factory.produced.first { it.stream == "users" }
			val orders = factory.produced.first { it.stream == "orders" }
			users.recordedRecords shouldHaveSize 2
			orders.recordedRecords shouldHaveSize 1
		}

		should("commits the previous processor when a second SCHEMA arrives for the same stream") {
			val factory = RecordingStreamProcessorFactory()
			runWith(
				factory,
				schemaUsers, record("users", 1),
				schemaUsers, // re-emitting SCHEMA must commit the in-flight processor
				record("users", 2),
			)

			factory.produced shouldHaveSize 2
			factory.produced[0].commitCount shouldBe 1
		}

		should("commits every registered processor when STATE is received") {
			val factory = RecordingStreamProcessorFactory()
			val (_, out) = runWith(
				factory,
				schemaUsers, record("users", 1),
				schemaOrders, record("orders", 99),
				state("checkpoint-1"),
			)

			factory.produced.forEach { it.commitCount shouldBe 1 }
			out.toString().trim() shouldBe """{"bookmark":"checkpoint-1"}"""
		}

		should("finalizes every registered processor at end-of-stream") {
			val factory = RecordingStreamProcessorFactory()
			runWith(factory, schemaUsers, schemaOrders)

			factory.produced.forEach { it.finalizeCount shouldBe 1 }
		}

		should("does not finalize when a record-handler exception aborts the run") {
			val factory = RecordingStreamProcessorFactory().apply {
				onProduced = { it.processRecordError = IllegalStateException("simulated failure") }
			}
			val conn: TargetConnection = mockk {
				every { listTables() } returns emptyList()
			}
			val out = StringWriter()
			val input = (schemaUsers + "\n" + record("users", 1) + "\n").byteInputStream()

			shouldThrow<IllegalStateException> {
				StreamPipeline.forConfig(baseConfig, connectionFor(baseConfig, conn), streamProcessorFactory = factory.asFactory()).run(input, out)
			}.message shouldContain "simulated failure"

			factory.produced.first().finalizeCount shouldBe 0
		}

		should("propagates cleanFirst from the SCHEMA message to the factory") {
			val factory = RecordingStreamProcessorFactory()
			runWith(factory, schema("users", cleanFirst = true))

			factory.invocations.single().cleanFirst shouldBe true
		}

		should("drops tables for streams listed in streamsToReplace before creating the new processor") {
			val factory = RecordingStreamProcessorFactory()
			lateinit var queries: RunQueryCapture
			runWith(
				factory,
				schemaUsers,
				streamsToReplace = listOf("users"),
				setup = { queries = captureRunQueries() },
			)

			queries.queries.any { it.startsWith("DROP TABLE IF EXISTS `users`") } shouldBe true
			factory.invocations.single().stream shouldBe "users"
		}

		should("throws when a RECORD arrives before its SCHEMA") {
			val factory = RecordingStreamProcessorFactory()
			val conn: TargetConnection = mockk {
				every { listTables() } returns emptyList()
			}
			val out = StringWriter()
			val input = (record("users", 1) + "\n").byteInputStream()

			shouldThrow<IllegalStateException> {
				StreamPipeline.forConfig(baseConfig, connectionFor(baseConfig, conn), streamProcessorFactory = factory.asFactory()).run(input, out)
			}.message shouldContain "before Schema is defined"
		}
	}
})
