package com.biron.singerTargetClickhouse

import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.mockk.mockk

class DeletedRowsReclaimerTest : ShouldSpec({

	afterTest { checkAndClearAllMocks() }

	context("tablesWithDeletedRows") {
		should("looks up the given tables in a single system.parts query") {
			val conn: TargetConnection = mockk()
			val queries = conn.captureRunQueries {
				QueryResult(listOf(listOf<Any?>("users", "1000"), listOf<Any?>("users__tags", 20L)), rows = 2)
			}

			DeletedRowsReclaimer(conn, 0.05).tablesWithDeletedRows(listOf("users", "users__tags", "o'k")) shouldContainExactly
					listOf(MaskedTable("users", 1000), MaskedTable("users__tags", 20))

			queries.queries.single().also {
				it shouldContain "SELECT table, sum(rows) FROM system.parts"
				it shouldContain "WHERE database = currentDatabase() AND active"
				it shouldContain """AND table IN ('users', 'users__tags', 'o\'k')"""
				it shouldContain "HAVING countIf(has_lightweight_delete) > 0"
			}
		}

		should("does not query ClickHouse without tables") {
			// Strict mockk: runQuery is unstubbed.
			DeletedRowsReclaimer(mockk(), 0.05).tablesWithDeletedRows(emptyList()).shouldBeEmpty()
		}
	}

	context("reclaim") {
		fun deletedRows(deleted: Long): (String) -> QueryResult = { q ->
			if (q.contains("countIf(NOT _row_exists)")) QueryResult(listOf(listOf<Any?>(deleted)), rows = 1)
			else QueryResult(emptyList(), rows = 0)
		}

		should("applies the deleted mask once the deleted rows reach the ratio of the table's rows on disk") {
			val conn: TargetConnection = mockk()
			val queries = conn.captureRunQueries(deletedRows(5))

			DeletedRowsReclaimer(conn, 0.05).reclaim(MaskedTable("users", rowsOnDisk = 100))

			queries.queries shouldHaveSize 2
			queries.queries[1] shouldBe "ALTER TABLE `users` APPLY DELETED MASK SETTINGS mutations_sync = 2"
			queries.queries[0].also {
				it shouldContain "FROM `users`"
				it shouldContain "WHERE database = currentDatabase() AND table = 'users' AND active AND has_lightweight_delete"
				it shouldContain "SETTINGS apply_deleted_mask = 0"
			}
		}

		should("leaves the deleted rows on disk below the ratio") {
			val conn: TargetConnection = mockk()
			val queries = conn.captureRunQueries(deletedRows(4))

			DeletedRowsReclaimer(conn, 0.05).reclaim(MaskedTable("users", rowsOnDisk = 100))

			queries.queries.none { it.contains("APPLY DELETED MASK") } shouldBe true
		}

		should("does nothing when the masked parts were merged in the meantime") {
			val conn: TargetConnection = mockk()
			val queries = conn.captureRunQueries(deletedRows(0))

			DeletedRowsReclaimer(conn, 0.0).reclaim(MaskedTable("users", rowsOnDisk = 100))

			queries.queries.none { it.contains("APPLY DELETED MASK") } shouldBe true
		}
	}
})
