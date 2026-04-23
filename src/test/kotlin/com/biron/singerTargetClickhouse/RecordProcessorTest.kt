package com.biron.singerTargetClickhouse

import io.kotest.core.spec.style.DescribeSpec
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe

class RecordProcessorTest : DescribeSpec({

	fun processor(
		meta: SourceMeta = simpleMeta,
		batchSize: Int = 1,
		translateValues: Boolean = false,
		autoEndTimeoutMs: Long = 10_000,
	): Pair<FakeTargetConnection, RecordProcessor> {
		val conn = FakeTargetConnection()
		val proc = RecordProcessor(meta, conn, RecordProcessorConfig(batchSize, translateValues, autoEndTimeoutMs))
		return conn to proc
	}

	describe("pushRecord") {
		it("handles simple schema and data") {
			val (conn, proc) = processor(batchSize = 1)
			proc.pushRecord(mapOf("id" to 1, "name" to "a"), abort, 0)
			proc.pushRecord(mapOf("id" to 2, "name" to "b"), abort, 0)

			conn.streams[0].data shouldBe "[1,\"a\"]\n[2,\"b\"]\n"
			proc.buildSQLInsertField() shouldContainExactly listOf("`id`", "`name`")
		}

		it("auto-ends ingestion after inactivity") {
			val (conn, proc) = processor(batchSize = 5, autoEndTimeoutMs = 200)
			proc.pushRecord(mapOf("id" to 1, "name" to "a"), abort, 0)
			proc.pushRecord(mapOf("id" to 2, "name" to "b"), abort, 0)
			Thread.sleep(500)

			conn.streams[0].data shouldBe "[1,\"a\"]\n[2,\"b\"]\n"
			conn.streams[0].closed shouldBe true
		}

		it("flushes at batch size and finishes on endIngestion") {
			val (conn, proc) = processor(batchSize = 2, autoEndTimeoutMs = 2_000)
			proc.pushRecord(mapOf("id" to 1, "name" to "a"), abort, 0)
			proc.pushRecord(mapOf("id" to 2, "name" to "b"), abort, 0)
			proc.pushRecord(mapOf("id" to 3, "name" to "c"), abort, 0)

			conn.streams[0].data shouldBe "[1,\"a\"]\n[2,\"b\"]\n"

			proc.endIngestion()
			conn.streams[0].data shouldBe "[1,\"a\"]\n[2,\"b\"]\n[3,\"c\"]\n"
		}

		it("applies value translation when configured") {
			val (conn, proc) = processor(
				meta = simpleMeta.copy(simpleColumnMappings = listOf(idAsColumn, validColumn)),
				batchSize = 1,
				translateValues = true,
			)
			proc.pushRecord(mapOf("id" to 1, "valid" to "true"), abort, 0)

			conn.streams[0].data shouldBe "[1,1]\n"
		}

		it("does not translate when translateValues=false") {
			val (conn, proc) = processor(
				meta = simpleMeta.copy(simpleColumnMappings = listOf(idAsColumn, validColumn)),
				batchSize = 1,
				translateValues = false,
			)
			proc.pushRecord(mapOf("id" to 1, "valid" to "true"), abort, 0)

			conn.streams[0].data shouldBe "[1,\"true\"]\n"
		}

		it("feeds deep nested children with propagated root version") {
			val (conn, proc) = processor(meta = metaWithPKAndChildren, batchSize = 1, autoEndTimeoutMs = 2_000)
			proc.pushRecord(
				data = mapOf(
					"id" to 1234,
					"name" to "a",
					"tags" to listOf(
						mapOf(
							"name" to "tag_a",
							"values" to listOf(
								mapOf("name" to "value_a"),
								mapOf("name" to "value_b"),
								mapOf("name" to "value_c"),
							),
						),
						mapOf(
							"name" to "tag_b",
							"values" to listOf(
								mapOf("name" to "value_d"),
								mapOf("name" to "value_e"),
							),
						),
					),
				),
				abort = abort,
				maxVer = 50,
			)

			// Streams are opened in the order rows are first pushed.
			val byTable = conn.streams.associateBy { it.query.substringAfter("INSERT INTO ").substringBefore(" ") }
			byTable["`order`"]!!.data shouldBe "[1234,\"a\",51]\n"
			byTable["`order__tags`"]!!.data shouldBe "[1234,0,\"tag_a\",51]\n[1234,1,\"tag_b\",51]\n"
			byTable["`order__tags__values`"]!!.data shouldBe
				"[1234,0,0,\"value_a\",51]\n[1234,0,1,\"value_b\",51]\n[1234,0,2,\"value_c\",51]\n" +
				"[1234,1,0,\"value_d\",51]\n[1234,1,1,\"value_e\",51]\n"
		}

		it("handles nested value array (scalar item)") {
			val (conn, proc) = processor(meta = metaWithNestedValueArray, batchSize = 1)
			proc.pushRecord(
				data = mapOf("events" to listOf(mapOf("previous_value" to "Test"))),
				abort = abort,
				maxVer = 0,
			)
			proc.endIngestion()

			val byTable = conn.streams.associateBy { it.query.substringAfter("INSERT INTO ").substringBefore(" ") }
			byTable["`audits`"]!!.data shouldBe "[]\n"
			byTable["`audits__events`"]!!.data shouldBe "[0]\n"
			byTable["`audits__events__previous_value`"]!!.data shouldBe "[0,0,\"Test\"]\n"
		}
	}

	describe("buildSQLInsertField") {
		it("appends _ver for root with current PKs") {
			val (_, proc) = processor(
				meta = simpleMeta.copy(
					pkMappings = listOf(id),
					simpleColumnMappings = listOf(
						ColumnMap(
							prop = "name", sqlIdentifier = "`name`", chType = "String",
							valueExtractor = { (it as? Map<*, *>)?.get("name") },
							valueTranslator = null, typeFormat = null,
							nullable = true, lowCardinality = false, nestedArray = false,
						),
					),
				),
			)
			proc.buildSQLInsertField() shouldContainExactly listOf("`id`", "`name`", "`_ver`")
		}

		it("omits _ver for root without PKs") {
			val (_, proc) = processor()
			proc.buildSQLInsertField() shouldContainExactly listOf("`id`", "`name`")
		}
	}
})
