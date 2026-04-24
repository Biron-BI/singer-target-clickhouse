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
	): Triple<FakeTargetConnection, RecordProcessor, SourceMeta> {
		val conn = FakeTargetConnection()
		val proc = RecordProcessor(meta, conn, RecordProcessorConfig(batchSize, translateValues, autoEndTimeoutMs))
		return Triple(conn, proc, meta)
	}

	fun RecordProcessor.push(
		meta: SourceMeta,
		data: Any?,
		translateValues: Boolean = false,
		maxVer: Long = 0,
	) = pushRecord(mapToRow(meta, data, translateValues), abort, maxVer)

	describe("pushRecord") {
		it("handles simple schema and data") {
			val (conn, proc, meta) = processor(batchSize = 1)
			proc.push(meta, mapOf("id" to 1, "name" to "a"))
			proc.push(meta, mapOf("id" to 2, "name" to "b"))

			conn.streams[0].data shouldBe "[1,\"a\"]\n[2,\"b\"]\n"
			proc.buildSQLInsertField() shouldContainExactly listOf("`id`", "`name`")
		}

		it("auto-ends ingestion after inactivity") {
			val (conn, proc, meta) = processor(batchSize = 5, autoEndTimeoutMs = 200)
			proc.push(meta, mapOf("id" to 1, "name" to "a"))
			proc.push(meta, mapOf("id" to 2, "name" to "b"))
			Thread.sleep(500)

			conn.streams[0].data shouldBe "[1,\"a\"]\n[2,\"b\"]\n"
			conn.streams[0].closed shouldBe true
		}

		it("flushes at batch size and finishes on endIngestion") {
			val (conn, proc, meta) = processor(batchSize = 2, autoEndTimeoutMs = 2_000)
			proc.push(meta, mapOf("id" to 1, "name" to "a"))
			proc.push(meta, mapOf("id" to 2, "name" to "b"))
			proc.push(meta, mapOf("id" to 3, "name" to "c"))

			conn.streams[0].data shouldBe "[1,\"a\"]\n[2,\"b\"]\n"

			proc.endIngestion()
			conn.streams[0].data shouldBe "[1,\"a\"]\n[2,\"b\"]\n[3,\"c\"]\n"
		}

		it("applies value translation when configured") {
			val (conn, proc, meta) = processor(
				meta = simpleMeta.copy(simpleColumnMappings = listOf(idAsColumn, validColumn)),
				batchSize = 1,
				translateValues = true,
			)
			proc.push(meta, mapOf("id" to 1, "valid" to "true"), translateValues = true)

			conn.streams[0].data shouldBe "[1,1]\n"
		}

		it("does not translate when translateValues=false") {
			val (conn, proc, meta) = processor(
				meta = simpleMeta.copy(simpleColumnMappings = listOf(idAsColumn, validColumn)),
				batchSize = 1,
				translateValues = false,
			)
			proc.push(meta, mapOf("id" to 1, "valid" to "true"))

			conn.streams[0].data shouldBe "[1,\"true\"]\n"
		}

		it("feeds deep nested children with propagated root version") {
			val (conn, proc, meta) = processor(meta = metaWithPKAndChildren, batchSize = 1, autoEndTimeoutMs = 2_000)
			proc.push(
				meta = meta,
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
			val (conn, proc, meta) = processor(meta = metaWithNestedValueArray, batchSize = 1)
			proc.push(
				meta = meta,
				data = mapOf("events" to listOf(mapOf("previous_value" to "Test"))),
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
			val (_, proc, _) = processor(
				meta = simpleMeta.copy(
					pkMappings = listOf(id),
					simpleColumnMappings = listOf(
						ColumnMap(
							prop = "name", sqlIdentifier = "`name`", chType = "String",
							valueExtractor = { (it as? Map<*, *>)?.get("name") },
							schemaType = null, typeFormat = null,
							nullable = true, lowCardinality = false, nestedArray = false,
						),
					),
				),
			)
			proc.buildSQLInsertField() shouldContainExactly listOf("`id`", "`name`", "`_ver`")
		}

		it("omits _ver for root without PKs") {
			val (_, proc, _) = processor()
			proc.buildSQLInsertField() shouldContainExactly listOf("`id`", "`name`")
		}
	}
})
