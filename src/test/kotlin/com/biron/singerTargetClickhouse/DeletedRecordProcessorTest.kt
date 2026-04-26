package com.biron.singerTargetClickhouse

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.DescribeSpec
import io.kotest.matchers.shouldBe

class DeletedRecordProcessorTest : DescribeSpec({

	describe("pushDeletedRecord") {
		it("throws when meta has no CURRENT pk") {
			val proc = DeletedRecordProcessor(simpleMeta, FakeTargetConnection(), DeletedRecordProcessorConfig(10, false))
			shouldThrow<IllegalStateException> {
				proc.pushDeletedRecord(mapToRow(simpleMeta, mapOf("id" to 1)))
			}
		}

		it("flushes at batch size") {
			val conn = FakeTargetConnection()
			val meta = simpleMeta.copy(pkMappings = listOf(id), simpleColumnMappings = emptyList())
			val proc = DeletedRecordProcessor(meta, conn, DeletedRecordProcessorConfig(batchSize = 2, translateValues = false))

			proc.pushDeletedRecord(mapToRow(meta, mapOf("id" to 1)))
			conn.runQueryLog.size shouldBe 0
			proc.pushDeletedRecord(mapToRow(meta, mapOf("id" to 2)))

			conn.runQueryLog.size shouldBe 1
			conn.runQueryLog[0].contains("DELETE FROM `order`") shouldBe true
			conn.runQueryLog[0].contains("WHERE (`id`) IN ((1),(2))") shouldBe true
		}

		it("deleteBufferedData flushes the leftover buffer") {
			val conn = FakeTargetConnection()
			val meta = simpleMeta.copy(pkMappings = listOf(id), simpleColumnMappings = emptyList())
			val proc = DeletedRecordProcessor(meta, conn, DeletedRecordProcessorConfig(100, false))

			proc.pushDeletedRecord(mapToRow(meta, mapOf("id" to 42)))
			proc.deleteBufferedData()

			conn.runQueryLog[0].contains("WHERE (`id`) IN ((42))") shouldBe true
		}

		it("quotes string PK values") {
			val conn = FakeTargetConnection()
			val stringPk = id.copy(
				chType = "String",
				schemaType = "string",
				valueExtractor = { (it as? Map<*, *>)?.get("id")?.toString() },
			)
			val meta = simpleMeta.copy(pkMappings = listOf(stringPk), simpleColumnMappings = emptyList())
			val proc = DeletedRecordProcessor(meta, conn, DeletedRecordProcessorConfig(batchSize = 100, translateValues = true))

			proc.pushDeletedRecord(mapToRow(meta, mapOf("id" to "abc"), translateValues = true))
			proc.deleteBufferedData()

			conn.runQueryLog[0].contains("WHERE (`id`) IN (('abc'))") shouldBe true
		}

		it("does not quote numeric PK values") {
			val conn = FakeTargetConnection()
			val meta = simpleMeta.copy(pkMappings = listOf(id), simpleColumnMappings = emptyList())
			val proc = DeletedRecordProcessor(meta, conn, DeletedRecordProcessorConfig(100, false))

			proc.pushDeletedRecord(mapToRow(meta, mapOf("id" to 123)))
			proc.deleteBufferedData()

			conn.runQueryLog[0].contains("WHERE (`id`) IN ((123))") shouldBe true
		}
	}
})
