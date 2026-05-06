package com.biron.singerTargetClickhouse

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.mockk.every
import io.mockk.mockk

class StreamProcessorTest : ShouldSpec({

	afterTest { checkAndClearAllMocks() }

	val baseConfig = TargetConfig(
		host = "h", port = 1, username = "u", password = "p", database = "db",
	)

	val typedNameColumn = ColumnMap(
		prop = "name", sqlIdentifier = "`name`", chType = "String",
		valueExtractor = { (it as? Map<*, *>)?.get("name") },
		schemaType = "string", typeFormat = null,
		nullable = true, lowCardinality = false, nestedArray = false,
	)

	fun metaNoPk(): SourceMeta = SourceMeta(
		prop = "order",
		sqlTableName = "`order`",
		pkMappings = emptyList(),
		simpleColumnMappings = listOf(idAsColumn),
		children = emptyList(),
	)

	fun metaWithPk(): SourceMeta = SourceMeta(
		prop = "order",
		sqlTableName = "`order`",
		pkMappings = listOf(id),
		simpleColumnMappings = listOf(typedNameColumn),
		children = emptyList(),
	)

	fun metaWithPkOnly(): SourceMeta = SourceMeta(
		prop = "order",
		sqlTableName = "`order`",
		pkMappings = listOf(id),
		simpleColumnMappings = emptyList(),
		children = emptyList(),
	)

	fun metaWithCleaningColumn(): SourceMeta = SourceMeta(
		prop = "order",
		sqlTableName = "`order`",
		pkMappings = listOf(id),
		simpleColumnMappings = listOf(typedNameColumn),
		children = emptyList(),
		cleaningColumn = "name",
	)

	context("create") {
		should("creates the root table when none exist") {
			val conn: TargetConnection = mockk {
				every { getDatabase() } returns "db"
			}
			val queries = conn.captureRunQueries()

			StreamProcessor.create(conn, metaNoPk(), baseConfig, cleanFirst = false, existingTables = emptyList(), cleaningColumnSlot = null)

			queries.queries.any { it.startsWith("CREATE TABLE db.`order`") } shouldBe true
		}

		should("does not re-create the root when the table already exists") {
			val conn: TargetConnection = mockk {
				every { listColumns("order") } returns listOf(Column("id", "Int32", isInSortingKey = false))
			}

			StreamProcessor.create(conn, metaNoPk(), baseConfig, cleanFirst = false, existingTables = listOf("order"), cleaningColumnSlot = null)
		}

		should("drops the root before recreating it when cleanFirst=true") {
			val conn: TargetConnection = mockk {
				every { getDatabase() } returns "db"
			}
			val queries = conn.captureRunQueries()

			StreamProcessor.create(conn, metaWithPk(), baseConfig, cleanFirst = true, existingTables = listOf("order"), cleaningColumnSlot = null)

			val dropIdx = queries.queries.indexOfFirst { it.startsWith("DROP TABLE IF EXISTS `order`") }
			val createIdx = queries.queries.indexOfFirst { it.startsWith("CREATE TABLE db.`order`") }
			(dropIdx >= 0) shouldBe true
			(createIdx > dropIdx) shouldBe true
		}

		should("queries max(_ver) for ReplacingMergeTree when not cleanFirst") {
			val conn: TargetConnection = mockk {
				every { getDatabase() } returns "db"
			}
			val queries = conn.captureRunQueries { q ->
				if (q.contains("max(_ver)")) QueryResult(listOf(listOf<Any?>(42L)), rows = 1)
				else QueryResult(emptyList(), rows = 0)
			}

			StreamProcessor.create(conn, metaWithPk(), baseConfig, cleanFirst = false, existingTables = emptyList(), cleaningColumnSlot = null)

			queries.queries.count { it.contains("max(_ver)") } shouldBe 1
		}

		should("does not query max(_ver) when cleanFirst=true") {
			val conn: TargetConnection = mockk {
				every { getDatabase() } returns "db"
			}
			val queries = conn.captureRunQueries()

			StreamProcessor.create(conn, metaWithPk(), baseConfig, cleanFirst = true, existingTables = emptyList(), cleaningColumnSlot = null)

			queries.queries.none { it.contains("max(_ver)") } shouldBe true
		}

		should("does not query max(_ver) for non-ReplacingMergeTree meta") {
			val conn: TargetConnection = mockk {
				every { getDatabase() } returns "db"
			}
			val queries = conn.captureRunQueries()

			StreamProcessor.create(conn, metaNoPk(), baseConfig, cleanFirst = false, existingTables = emptyList(), cleaningColumnSlot = null)

			queries.queries.none { it.contains("max(_ver)") } shouldBe true
		}
	}

	context("processRecord with cleaning column") {
		should("issues an ALTER TABLE DELETE on first non-empty cleaning value") {
			val conn: TargetConnection = mockk {
				every { getDatabase() } returns "db"
			}
			val queries = conn.captureRunQueries()
			conn.captureRowWriters()

			val meta = metaWithCleaningColumn()
			val underTest = StreamProcessor.create(conn, meta, baseConfig, false, emptyList(), 1)

			underTest.processRecord(mapToRow(meta, mapOf("id" to 1, "name" to "alice")), 0, abort)

			val deletes = queries.queries.filter { it.contains("ALTER TABLE `order` DELETE") }
			deletes shouldHaveSize 1
			deletes[0] shouldContain "`name` = 'alice'"
		}

		should("issues a single DELETE per distinct cleaning value") {
			val conn: TargetConnection = mockk {
				every { getDatabase() } returns "db"
			}
			val queries = conn.captureRunQueries()
			conn.captureRowWriters()

			val meta = metaWithCleaningColumn()
			val underTest = StreamProcessor.create(conn, meta, baseConfig, false, emptyList(), 1)

			underTest.processRecord(mapToRow(meta, mapOf("id" to 1, "name" to "alice")), 0, abort)
			underTest.processRecord(mapToRow(meta, mapOf("id" to 2, "name" to "alice")), 1, abort)
			underTest.processRecord(mapToRow(meta, mapOf("id" to 3, "name" to "bob")), 2, abort)

			val deletes = queries.queries.filter { it.contains("ALTER TABLE `order` DELETE") }
			deletes shouldHaveSize 2
			deletes.any { it.contains("'alice'") } shouldBe true
			deletes.any { it.contains("'bob'") } shouldBe true
		}

		should("escapes single quotes in the cleaning value") {
			val conn: TargetConnection = mockk {
				every { getDatabase() } returns "db"
			}
			val queries = conn.captureRunQueries()
			conn.captureRowWriters()

			val meta = metaWithCleaningColumn()
			val underTest = StreamProcessor.create(conn, meta, baseConfig, false, emptyList(), 1)

			underTest.processRecord(mapToRow(meta, mapOf("id" to 1, "name" to "o'brien")), 0, abort)

			queries.queries.any { it.contains("`name` = 'o\\'\\brien'") } shouldBe true
		}

		should("skips the cleaning DELETE entirely when cleanFirst=true") {
			val conn: TargetConnection = mockk {
				every { getDatabase() } returns "db"
			}
			val queries = conn.captureRunQueries()
			conn.captureRowWriters()

			val meta = metaWithCleaningColumn()
			val underTest = StreamProcessor.create(conn, meta, baseConfig, true, emptyList(), 1)

			underTest.processRecord(mapToRow(meta, mapOf("id" to 1, "name" to "alice")), 0, abort)

			queries.queries.none { it.contains("ALTER TABLE `order` DELETE") } shouldBe true
		}

		should("throws when the cleaning column is not present in the meta") {
			val conn: TargetConnection = mockk {
				every { getDatabase() } returns "db"
			}
			conn.captureRunQueries()
			// No openRowWriter stub: the cleaning-column validation throws before any insert is opened.

			// `cleaningColumn = "mystery"` does not match any pkMapping or simpleColumnMapping.
			val meta = metaWithCleaningColumn().copy(cleaningColumn = "mystery")
			val underTest = StreamProcessor.create(conn, meta, baseConfig, false, emptyList(), 1)

			shouldThrow<IllegalStateException> {
				underTest.processRecord(mapToRow(metaWithCleaningColumn(), mapOf("id" to 1, "name" to "alice")), 0, abort)
			}.message shouldContain "could not resolve cleaning column meta"
		}

		should("throws when the cleaning column has no typed schema") {
			val conn: TargetConnection = mockk {
				every { getDatabase() } returns "db"
			}
			conn.captureRunQueries()

			// Same column as before, but `schemaType = null` makes it ineligible.
			val untypedNameColumn = typedNameColumn.copy(schemaType = null)
			val meta = metaWithCleaningColumn().copy(simpleColumnMappings = listOf(untypedNameColumn))
			val underTest = StreamProcessor.create(conn, meta, baseConfig, false, emptyList(), 1)

			shouldThrow<IllegalStateException> {
				underTest.processRecord(mapToRow(meta, mapOf("id" to 1, "name" to "alice")), 0, abort)
			}.message shouldContain "no typed schema"
		}

		should("ignores empty cleaning values") {
			val conn: TargetConnection = mockk {
				every { getDatabase() } returns "db"
			}
			val queries = conn.captureRunQueries()
			conn.captureRowWriters()

			val meta = metaWithCleaningColumn()
			val underTest = StreamProcessor.create(conn, meta, baseConfig, false, emptyList(), 1)

			underTest.processRecord(mapToRow(meta, mapOf("id" to 1, "name" to "")), 0, abort)
			underTest.processRecord(mapToRow(meta, mapOf("id" to 2)), 1, abort)

			queries.queries.none { it.contains("ALTER TABLE `order` DELETE") } shouldBe true
		}
	}

	context("commitPendingChanges") {
		should("is a no-op when nothing has been pushed") {
			val conn: TargetConnection = mockk {
				every { getDatabase() } returns "db"
			}
			conn.captureRunQueries()

			val underTest = StreamProcessor.create(conn, metaNoPk(), baseConfig, false, emptyList(), null)

			// No openRowWriter stub: strict mockk fails the test if commit accidentally opens a stream.
			underTest.commitPendingChanges()
		}

		should("closes the open insert stream when rows are pending") {
			val conn: TargetConnection = mockk {
				every { getDatabase() } returns "db"
			}
			conn.captureRunQueries()
			val rowWriters = conn.captureRowWriters()

			val meta = metaNoPk()
			val underTest = StreamProcessor.create(conn, meta, baseConfig, false, emptyList(), null)

			underTest.processRecord(mapToRow(meta, mapOf("id" to 1)), 0, abort)
			underTest.commitPendingChanges()

			rowWriters.streams shouldHaveSize 1
			rowWriters.streams.first().closed shouldBe true
		}

		should("does not double-commit when called twice with no rows in between") {
			val conn: TargetConnection = mockk {
				every { getDatabase() } returns "db"
			}
			conn.captureRunQueries()
			val rowWriters = conn.captureRowWriters()

			val meta = metaNoPk()
			val underTest = StreamProcessor.create(conn, meta, baseConfig, false, emptyList(), null)

			underTest.processRecord(mapToRow(meta, mapOf("id" to 1)), 0, abort)
			underTest.commitPendingChanges()
			underTest.commitPendingChanges()

			rowWriters.streams shouldHaveSize 1
		}
	}

	context("processDeletedRecord") {
		should("buffers the delete and issues DELETE FROM on commit") {
			val conn: TargetConnection = mockk {
				every { getDatabase() } returns "db"
			}
			val queries = conn.captureRunQueries()

			val meta = metaWithPkOnly()
			val underTest = StreamProcessor.create(conn, meta, baseConfig, false, emptyList(), null)
			val beforeDeleteSize = queries.queries.size

			underTest.processDeletedRecord(mapToRow(meta, mapOf("id" to 7)))
			queries.queries.drop(beforeDeleteSize).none { it.contains("DELETE FROM `order`") } shouldBe true

			underTest.commitPendingChanges()

			val deletes = queries.queries.filter { it.contains("DELETE FROM `order`") }
			deletes shouldHaveSize 1
			deletes[0] shouldContain "(7)"
		}
	}

	context("finalizeProcessing") {
		should("runs OPTIMIZE TABLE FINAL when ReplacingMergeTree and not startedClean") {
			val conn: TargetConnection = mockk {
				every { getDatabase() } returns "db"
			}
			val queries = conn.captureRunQueries()

			val underTest = StreamProcessor.create(conn, metaWithPk(), baseConfig, false, emptyList(), null)

			underTest.finalizeProcessing()

			queries.queries.any { it.contains("OPTIMIZE TABLE `order` FINAL") } shouldBe true
		}

		should("removes children orphans when meta has children") {
			val conn: TargetConnection = mockk {
				every { getDatabase() } returns "db"
			}
			val queries = conn.captureRunQueries()

			val underTest = StreamProcessor.create(conn, metaWithPKAndChildren, baseConfig, false, emptyList(), null)

			underTest.finalizeProcessing()

			queries.queries.any { it.contains("ALTER TABLE `order__tags` DELETE") } shouldBe true
			queries.queries.any { it.contains("ALTER TABLE `order__tags__values` DELETE") } shouldBe true
		}

		should("skips OPTIMIZE and PK integrity check when startedClean=true") {
			val conn: TargetConnection = mockk {
				every { getDatabase() } returns "db"
			}
			val queries = conn.captureRunQueries()

			val underTest = StreamProcessor.create(conn, metaWithPk(), baseConfig, true, emptyList(), null)

			underTest.finalizeProcessing()

			queries.queries.none { it.contains("OPTIMIZE TABLE") } shouldBe true
			queries.queries.none { it.contains("ROW_NUMBER") } shouldBe true
		}

		should("runs PK integrity check and throws when duplicates are detected") {
			val conn: TargetConnection = mockk {
				every { getDatabase() } returns "db"
			}
			conn.captureRunQueries { q ->
				if (q.contains("ROW_NUMBER")) QueryResult(listOf(listOf<Any?>(1L)), rows = 1)
				else QueryResult(emptyList(), rows = 0)
			}

			val underTest = StreamProcessor.create(conn, metaWithPk(), baseConfig, false, emptyList(), null)

			shouldThrow<IllegalStateException> {
				underTest.finalizeProcessing()
			}.message shouldContain "Duplicate key on table"
		}

		should("does not run OPTIMIZE for non-ReplacingMergeTree meta") {
			val conn: TargetConnection = mockk {
				every { getDatabase() } returns "db"
			}
			val queries = conn.captureRunQueries()

			val underTest = StreamProcessor.create(conn, metaNoPk(), baseConfig, false, emptyList(), null)

			underTest.finalizeProcessing()

			queries.queries.none { it.contains("OPTIMIZE TABLE") } shouldBe true
			queries.queries.none { it.contains("ROW_NUMBER") } shouldBe true
		}

		should("commits pending rows before finalizing") {
			val conn: TargetConnection = mockk {
				every { getDatabase() } returns "db"
			}
			conn.captureRunQueries()
			val rowWriters = conn.captureRowWriters()

			val meta = metaWithPk()
			val underTest = StreamProcessor.create(conn, meta, baseConfig, false, emptyList(), null)

			underTest.processRecord(mapToRow(meta, mapOf("id" to 1, "name" to "a")), 0, abort)
			underTest.finalizeProcessing()

			rowWriters.streams shouldHaveSize 1
			rowWriters.streams.first().closed shouldBe true
		}

		should("wraps an underlying commit failure in IllegalStateException") {
			val conn: TargetConnection = mockk {
				every { getDatabase() } returns "db"
			}
			conn.captureRunQueries { q ->
				if (q.contains("DELETE FROM `order`")) error("boom")
				else QueryResult(emptyList(), rows = 0)
			}

			val meta = metaWithPkOnly()
			val underTest = StreamProcessor.create(conn, meta, baseConfig, false, emptyList(), null)

			underTest.processDeletedRecord(mapToRow(meta, mapOf("id" to 1)))

			shouldThrow<IllegalStateException> {
				underTest.finalizeProcessing()
			}.message shouldContain "could not save new records"
		}
	}
})
