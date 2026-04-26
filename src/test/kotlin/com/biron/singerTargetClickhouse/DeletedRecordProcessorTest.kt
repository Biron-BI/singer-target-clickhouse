package com.biron.singerTargetClickhouse

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.string.shouldContain
import io.mockk.mockk

class DeletedRecordProcessorTest : ShouldSpec({

	afterTest { checkAndClearAllMocks() }

	context("pushDeletedRecord") {
		should("throws when meta has no CURRENT pk") {
			val underTest = DeletedRecordProcessor(simpleMeta, mockk(), DeletedRecordProcessorConfig(10, false))
			shouldThrow<IllegalStateException> {
				underTest.pushDeletedRecord(mapToRow(simpleMeta, mapOf("id" to 1)))
			}.message shouldContain "cannot push deleted record to a stream without pk mapping"
		}

		should("flushes at batch size") {
			val conn: TargetConnection = mockk()
			val queries = conn.captureRunQueries()
			val meta = simpleMeta.copy(pkMappings = listOf(id), simpleColumnMappings = emptyList())

			val underTest = DeletedRecordProcessor(meta, conn, DeletedRecordProcessorConfig(batchSize = 2, translateValues = false))
			underTest.pushDeletedRecord(mapToRow(meta, mapOf("id" to 1)))
			queries.queries.shouldBeEmpty()
			underTest.pushDeletedRecord(mapToRow(meta, mapOf("id" to 2)))

			queries.queries shouldHaveSize 1
			queries.queries[0] shouldContain "DELETE FROM `order`"
			queries.queries[0] shouldContain "WHERE (`id`) IN ((1),(2))"
		}

		should("deleteBufferedData flushes the leftover buffer") {
			val conn: TargetConnection = mockk()
			val queries = conn.captureRunQueries()
			val meta = simpleMeta.copy(pkMappings = listOf(id), simpleColumnMappings = emptyList())

			val underTest = DeletedRecordProcessor(meta, conn, DeletedRecordProcessorConfig(100, false))
			underTest.pushDeletedRecord(mapToRow(meta, mapOf("id" to 42)))
			underTest.deleteBufferedData()

			queries.queries[0] shouldContain "WHERE (`id`) IN ((42))"
		}

		should("quotes string PK values") {
			val conn: TargetConnection = mockk()
			val queries = conn.captureRunQueries()
			val stringPk = id.copy(
				chType = "String",
				schemaType = "string",
				valueExtractor = { (it as? Map<*, *>)?.get("id")?.toString() },
			)
			val meta = simpleMeta.copy(pkMappings = listOf(stringPk), simpleColumnMappings = emptyList())

			val underTest = DeletedRecordProcessor(meta, conn, DeletedRecordProcessorConfig(batchSize = 100, translateValues = true))
			underTest.pushDeletedRecord(mapToRow(meta, mapOf("id" to "abc"), translateValues = true))
			underTest.deleteBufferedData()

			queries.queries[0] shouldContain "WHERE (`id`) IN (('abc'))"
		}

		should("does not quote numeric PK values") {
			val conn: TargetConnection = mockk()
			val queries = conn.captureRunQueries()
			val meta = simpleMeta.copy(pkMappings = listOf(id), simpleColumnMappings = emptyList())

			val underTest = DeletedRecordProcessor(meta, conn, DeletedRecordProcessorConfig(100, false))
			underTest.pushDeletedRecord(mapToRow(meta, mapOf("id" to 123)))
			underTest.deleteBufferedData()

			queries.queries[0] shouldContain "WHERE (`id`) IN ((123))"
		}
	}
})
