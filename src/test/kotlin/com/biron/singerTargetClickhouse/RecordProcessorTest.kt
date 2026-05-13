package com.biron.singerTargetClickhouse

import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.mockk.mockk

class RecordProcessorTest : ShouldSpec({

	afterTest { checkAndClearAllMocks() }

	fun processor(
		meta: SourceMeta = simpleMeta,
		batchSize: Int = 1,
		translateValues: Boolean = false,
		autoEndTimeoutMs: Long = 10_000,
	): Triple<RowWriterCapture, RecordProcessor, SourceMeta> {
		val conn: TargetConnection = mockk()
		val captures = conn.captureRowWriters()
		val underTest = RecordProcessor(meta, conn, RecordProcessorConfig(batchSize, translateValues, autoEndTimeoutMs))
		return Triple(captures, underTest, meta)
	}

	fun RecordProcessor.push(
		meta: SourceMeta,
		data: Any?,
		translateValues: Boolean = false,
		maxVer: Long = 0,
	) = pushRecord(mapToRow(meta, data, translateValues), abort, maxVer)

	context("pushRecord") {
		should("handles simple schema and data") {
			val (captures, underTest, meta) = processor(batchSize = 1)
			underTest.push(meta, mapOf("id" to 1, "name" to "a"))
			underTest.push(meta, mapOf("id" to 2, "name" to "b"))

			captures.streams[0].data shouldBe """
				[1,"a"]
				[2,"b"]
				
				""".trimIndent()
			underTest.buildSQLInsertField() shouldContainExactly listOf("`id`", "`name`")
		}

		should("auto-ends ingestion after inactivity") {
			val (captures, underTest, meta) = processor(batchSize = 5, autoEndTimeoutMs = 200)
			underTest.push(meta, mapOf("id" to 1, "name" to "a"))
			underTest.push(meta, mapOf("id" to 2, "name" to "b"))
			Thread.sleep(500)

			captures.streams[0].data shouldBe """
				[1,"a"]
				[2,"b"]
				
				""".trimIndent()
			captures.streams[0].closed shouldBe true
		}

		should("flushes at batch size and finishes on endIngestion") {
			val (captures, underTest, meta) = processor(batchSize = 2, autoEndTimeoutMs = 2_000)
			underTest.push(meta, mapOf("id" to 1, "name" to "a"))
			underTest.push(meta, mapOf("id" to 2, "name" to "b"))
			underTest.push(meta, mapOf("id" to 3, "name" to "c"))

			captures.streams[0].data shouldBe """
				[1,"a"]
				[2,"b"]
				
				""".trimIndent()

			underTest.endIngestion()
			captures.streams[0].data shouldBe """
				[1,"a"]
				[2,"b"]
				[3,"c"]
				
				""".trimIndent()
		}

		should("applies value translation when configured") {
			val (captures, underTest, meta) = processor(
				meta = simpleMeta.copy(simpleColumnMappings = listOf(idAsColumn, validColumn)),
				batchSize = 1,
				translateValues = true,
			)
			underTest.push(meta, mapOf("id" to 1, "valid" to "true"), translateValues = true)

			captures.streams[0].data shouldBe """
				[1,1]
				
				""".trimIndent()
		}

		should("does not translate when translateValues=false") {
			val (captures, underTest, meta) = processor(
				meta = simpleMeta.copy(simpleColumnMappings = listOf(idAsColumn, validColumn)),
				batchSize = 1,
				translateValues = false,
			)
			underTest.push(meta, mapOf("id" to 1, "valid" to "true"))

			captures.streams[0].data shouldBe """
				[1,"true"]
				
				""".trimIndent()
		}

		should("feeds deep nested children with propagated root version") {
			val (captures, underTest, meta) = processor(meta = metaWithPKAndChildren, batchSize = 1, autoEndTimeoutMs = 2_000)
			underTest.push(
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
				maxVer = 51,
			)

			// Streams are opened in the order rows are first pushed.
			val byTable = captures.streams.associate {
				it.query.substringAfter("INSERT INTO ").substringBefore(" ") to it.data
			}
			byTable shouldBe mapOf(
				"`order`" to "[1234,\"a\",51]\n",
				"`order__tags`" to "[1234,0,\"tag_a\",51]\n[1234,1,\"tag_b\",51]\n",
				"`order__tags__values`" to
						"[1234,0,0,\"value_a\",51]\n[1234,0,1,\"value_b\",51]\n[1234,0,2,\"value_c\",51]\n" +
						"[1234,1,0,\"value_d\",51]\n[1234,1,1,\"value_e\",51]\n",
			)
		}

		should("handles nested value array (scalar item)") {
			val (captures, underTest, meta) = processor(meta = metaWithNestedValueArray, batchSize = 1)
			underTest.push(
				meta = meta,
				data = mapOf("events" to listOf(mapOf("previous_value" to "Test"))),
			)
			underTest.endIngestion()

			val byTable = captures.streams.associate {
				it.query.substringAfter("INSERT INTO ").substringBefore(" ") to it.data
			}
			byTable shouldBe mapOf(
				"`audits`" to "[]\n",
				"`audits__events`" to "[0]\n",
				"`audits__events__previous_value`" to "[0,0,\"Test\"]\n",
			)
		}
	}

	context("endIngestion / dispatchToChildren edges") {
		should("endIngestion is a no-op when no rows have been pushed") {
			// No openRowWriter stub: a stray writer-open would be a strict-mock failure.
			val underTest = RecordProcessor(simpleMeta, mockk(), RecordProcessorConfig(1, false, 10_000))
			underTest.endIngestion()
		}

		should("does not flush when subtable slot is null on the row") {
			val (captures, underTest, meta) = processor(meta = metaWithPKAndChildren, batchSize = 1, autoEndTimeoutMs = 2_000)
			underTest.push(meta = meta, data = mapOf("id" to 1, "name" to "a", "tags" to null), maxVer = 1)

			val byTable = captures.streams.associate {
				it.query.substringAfter("INSERT INTO ").substringBefore(" ") to it.data
			}
			byTable.keys shouldBe setOf("`order`")
			byTable.getValue("`order`") shouldBe "[1,\"a\",1]\n"
		}

		should("dispatches subtable rows even when a sibling subtable is missing") {
			val (captures, underTest, meta) = processor(meta = metaWithPKAndChildren, batchSize = 1, autoEndTimeoutMs = 2_000)
			underTest.push(
				meta = meta,
				data = mapOf("id" to 1, "name" to "a", "tags" to listOf(mapOf("name" to "tag_a", "values" to null))),
				maxVer = 5,
			)

			val byTable = captures.streams.associate {
				it.query.substringAfter("INSERT INTO ").substringBefore(" ") to it.data
			}
			byTable.keys shouldBe setOf("`order`", "`order__tags`")
			byTable.getValue("`order__tags`") shouldBe "[1,0,\"tag_a\",5]\n"
		}
	}

	context("buildSQLInsertField") {
		// These tests never push records, so they do not need an `openRowWriter` stub.
		fun bareProcessor(meta: SourceMeta) = RecordProcessor(meta, mockk(), RecordProcessorConfig(1, false, 10_000))

		should("appends _ver for root with current PKs") {
			val underTest = bareProcessor(
				simpleMeta.copy(
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
			underTest.buildSQLInsertField() shouldContainExactly listOf("`id`", "`name`", "`_ver`")
		}

		should("omits _ver for root without PKs") {
			val underTest = bareProcessor(simpleMeta)
			underTest.buildSQLInsertField() shouldContainExactly listOf("`id`", "`name`")
		}
	}
})
