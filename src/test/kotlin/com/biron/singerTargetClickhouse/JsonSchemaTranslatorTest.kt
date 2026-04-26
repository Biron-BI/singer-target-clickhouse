package com.biron.singerTargetClickhouse

import arrow.core.left
import arrow.core.right
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.mockk.every
import io.mockk.mockk

class JsonSchemaTranslatorTest : ShouldSpec({

	afterTest { checkAndClearAllMocks() }

	val idColumn = ColumnMap(
		prop = "id",
		sqlIdentifier = "`id`",
		chType = "Int32",
		valueExtractor = { (it as Map<*, *>)["id"] },
		schemaType = null,
		typeFormat = null,
		nullable = false,
		lowCardinality = false,
		nestedArray = false,
	)
	val nameColumn = ColumnMap(
		prop = "name",
		sqlIdentifier = "`name`",
		chType = "String",
		valueExtractor = { (it as Map<*, *>)["name"] },
		schemaType = null,
		typeFormat = null,
		nullable = true,
		lowCardinality = false,
		nestedArray = false,
	)
	val idPk = PkMap(
		prop = "id",
		sqlIdentifier = "`id`",
		chType = "UInt32",
		valueExtractor = { (it as Map<*, *>)["id"] },
		schemaType = null,
		typeFormat = null,
		nullable = false,
		lowCardinality = false,
		nestedArray = false,
		pkType = PKType.CURRENT,
	)

	val simpleMeta = SourceMeta(
		prop = "order",
		sqlTableName = "`order`",
		pkMappings = emptyList(),
		simpleColumnMappings = listOf(idColumn, nameColumn),
		children = emptyList(),
	)
	val emptyMeta = simpleMeta.copy(simpleColumnMappings = emptyList())
	val metaWithPK = SourceMeta(
		prop = "order",
		sqlTableName = "`order`",
		pkMappings = listOf(idPk),
		simpleColumnMappings = listOf(nameColumn),
		children = emptyList(),
	)
	val metaWithPKAndChildren = metaWithPK.copy(
		children = listOf(simpleMeta.copy(sqlTableName = "`order_child`")),
	)

	context("translateCH") {
		should("refuses empty meta") {
			shouldThrow<IllegalStateException> {
				translateCH("db", emptyMeta, recursive = true)
			}.message shouldContain "Attempting to create table without columns"
		}

		should("translates basic meta") {
			translateCH("db", simpleMeta, recursive = true) shouldContainExactly listOf(
				"CREATE TABLE db.`order` ( `id` Int32, `name` Nullable(String) ) ENGINE = MergeTree ORDER BY tuple()",
			)
		}

		should("translates meta with PK") {
			translateCH("db", metaWithPK, recursive = true) shouldContainExactly listOf(
				"CREATE TABLE db.`order` ( `id` UInt32, `name` Nullable(String), `_ver` UInt64 ) ENGINE = ReplacingMergeTree(_ver) ORDER BY `id`",
			)
		}

		should("translates meta with PK and children recursively") {
			translateCH("db", metaWithPKAndChildren, recursive = true) shouldContainExactly listOf(
				"CREATE TABLE db.`order` ( `id` UInt32, `name` Nullable(String), `_ver` UInt64 ) ENGINE = ReplacingMergeTree(_ver) ORDER BY `id`",
				"CREATE TABLE db.`order_child` ( `id` Int32, `name` Nullable(String), `_root_ver` UInt64 ) ENGINE = MergeTree ORDER BY tuple()",
			)
		}

		should("translates with LowCardinality modifier") {
			val lowCard = simpleMeta.copy(
				simpleColumnMappings = listOf(idColumn, nameColumn.copy(lowCardinality = true)),
			)
			translateCH("db", lowCard, recursive = true) shouldContainExactly listOf(
				"CREATE TABLE db.`order` ( `id` Int32, `name` LowCardinality(Nullable(String)) ) ENGINE = MergeTree ORDER BY tuple()",
			)
		}
	}

	context("toQualifiedType") {
		val baseCol = idColumn.copy(nullable = false, lowCardinality = false, nestedArray = false)

		should("no modifiers") { toQualifiedType(baseCol) shouldBe "Int32" }
		should("nestedArray") { toQualifiedType(baseCol.copy(chType = "String", nestedArray = true)) shouldBe "Array(String)" }
		should("nullable") { toQualifiedType(baseCol.copy(chType = "UInt64", nullable = true)) shouldBe "Nullable(UInt64)" }
		should("lowCardinality") { toQualifiedType(baseCol.copy(chType = "DateTime", lowCardinality = true)) shouldBe "LowCardinality(DateTime)" }
		should("multi") {
			toQualifiedType(baseCol.copy(chType = "UInt8", nullable = true, lowCardinality = true, nestedArray = true)) shouldBe
				"Array(LowCardinality(Nullable(UInt8)))"
		}
		should("placeholders 'undefined type' when chType is null") {
			toQualifiedType(baseCol.copy(chType = null, nullable = true)) shouldBe "Nullable(undefined type)"
		}
	}

	context("dropStreamTablesQueries") {
		should("emits a DROP for the root and each child, depth-first") {
			dropStreamTablesQueries(metaWithPKAndChildren) shouldContainExactly listOf(
				"DROP TABLE IF EXISTS `order`",
				"DROP TABLE IF EXISTS `order_child`",
			)
		}
	}

	context("updateSchema") {
		should("creates the table when missing, then aligns columns by issuing add/update/drop") {
			val conn: TargetConnection = mockk {
				every { listColumns("order") } returns listOf(
					Column("id", "UInt32", isInSortingKey = true),
					Column("name", "String", isInSortingKey = false),
					Column("legacy", "String", isInSortingKey = false),
					Column("_ver", "UInt64", isInSortingKey = false),
				)
				every { addColumn("`order`", match { it.name == "extra" }) } returns Unit.right()
				every { updateColumn("`order`", match { it.name == "name" }, match { it.type == "Nullable(String)" }) } returns Unit.right()
				every { removeColumn("`order`", match { it.name == "legacy" }) } returns Unit.right()
			}

			val meta = metaWithPK.copy(
				simpleColumnMappings = listOf(
					nameColumn.copy(chType = "String", nullable = true),
					idColumn.copy(prop = "extra", sqlIdentifier = "`extra`", chType = "Int32"),
				),
			)
			updateSchema(meta, conn, existingTables = listOf("order"))
		}

		should("creates the table from scratch when not in existingTables") {
			val conn: TargetConnection = mockk {
				every { getDatabase() } returns "db"
				every { listColumns("order") } returns listOf(
					Column("id", "UInt32", isInSortingKey = true),
					Column("name", "Nullable(String)", isInSortingKey = false),
					Column("_ver", "UInt64", isInSortingKey = false),
				)
			}
			val queries = conn.captureRunQueries()
			updateSchema(metaWithPK, conn, existingTables = emptyList())

			queries.queries.first() shouldContain "CREATE TABLE db.`order`"
		}

		should("throws when a primary key was added vs the live table") {
			val conn: TargetConnection = mockk {
				every { listColumns("order") } returns listOf(
					Column("name", "Nullable(String)", isInSortingKey = false),
					Column("_ver", "UInt64", isInSortingKey = false),
				)
			}
			shouldThrow<IllegalStateException> {
				updateSchema(metaWithPK, conn, existingTables = listOf("order"))
			}.message shouldContain "Could not update table because of key properties"
		}

		should("throws when a primary key was removed vs the live table") {
			val conn: TargetConnection = mockk {
				every { listColumns("order") } returns listOf(
					Column("id", "UInt32", isInSortingKey = true),
					Column("legacy_pk", "UInt32", isInSortingKey = true),
					Column("name", "Nullable(String)", isInSortingKey = false),
					Column("_ver", "UInt64", isInSortingKey = false),
				)
			}
			shouldThrow<IllegalStateException> {
				updateSchema(metaWithPK, conn, existingTables = listOf("order"))
			}.message shouldContain "Could not update table because of key properties"
		}

		should("aggregates per-column failures into one terminal error") {
			val conn: TargetConnection = mockk {
				every { listColumns("order") } returns listOf(
					Column("id", "UInt32", isInSortingKey = true),
					Column("name", "String", isInSortingKey = false),
					Column("_ver", "UInt64", isInSortingKey = false),
				)
				every {
					updateColumn("`order`", match { it.name == "name" }, match { it.type == "Nullable(String)" })
				} returns UpdateColumnError(
					existing = Column("name", "String", isInSortingKey = false),
					newCol = Column("name", "Nullable(String)", isInSortingKey = false),
					error = RuntimeException("boom"),
				).left()
			}
			val meta = metaWithPK.copy(
				simpleColumnMappings = listOf(nameColumn.copy(chType = "String", nullable = true)),
			)
			shouldThrow<IllegalStateException> {
				updateSchema(meta, conn, existingTables = listOf("order"))
			}.message shouldContain "Could not update table"
		}

		should("propagates addColumn failure as a terminal error") {
			val conn: TargetConnection = mockk {
				every { listColumns("order") } returns listOf(
					Column("id", "UInt32", isInSortingKey = true),
					Column("name", "Nullable(String)", isInSortingKey = false),
					Column("_ver", "UInt64", isInSortingKey = false),
				)
				every { addColumn("`order`", match { it.name == "extra" }) } returns AddColumnError(
					newCol = Column("extra", "Int32", isInSortingKey = false),
					error = RuntimeException("denied"),
				).left()
			}
			val meta = metaWithPK.copy(
				simpleColumnMappings = listOf(
					nameColumn,
					idColumn.copy(prop = "extra", sqlIdentifier = "`extra`", chType = "Int32"),
				),
			)
			shouldThrow<IllegalStateException> {
				updateSchema(meta, conn, existingTables = listOf("order"))
			}.message shouldContain "Could not update table"
		}

		should("propagates removeColumn failure as a terminal error") {
			val conn: TargetConnection = mockk {
				every { listColumns("order") } returns listOf(
					Column("id", "UInt32", isInSortingKey = true),
					Column("name", "Nullable(String)", isInSortingKey = false),
					Column("legacy", "String", isInSortingKey = false),
					Column("_ver", "UInt64", isInSortingKey = false),
				)
				every { removeColumn("`order`", match { it.name == "legacy" }) } returns RemoveColumnError(
					existing = Column("legacy", "String", isInSortingKey = false),
					error = RuntimeException("denied"),
				).left()
			}
			shouldThrow<IllegalStateException> {
				updateSchema(metaWithPK, conn, existingTables = listOf("order"))
			}.message shouldContain "Could not update table"
		}

		should("recurses into children before processing the root") {
			// updateSchema treats the simpleMeta-shaped child as a standalone root (it has no
			// PKType.ROOT mappings), so the expected schema has no `_root_ver` column.
			val conn: TargetConnection = mockk {
				every { listColumns("order") } returns listOf(
					Column("id", "UInt32", isInSortingKey = true),
					Column("name", "Nullable(String)", isInSortingKey = false),
					Column("_ver", "UInt64", isInSortingKey = false),
				)
				every { listColumns("order_child") } returns listOf(
					Column("id", "Int32", isInSortingKey = false),
				)
				every { addColumn("`order_child`", match { it.name == "name" }) } returns Unit.right()
			}
			updateSchema(metaWithPKAndChildren, conn, existingTables = listOf("order", "order_child"))
		}
	}

	context("dropStreamTablesQueries deep") {
		should("walks all descendant subtables depth-first") {
			val deepMeta = simpleMeta.copy(
				sqlTableName = "`root`",
				children = listOf(
					simpleMeta.copy(
						sqlTableName = "`root__a`",
						children = listOf(simpleMeta.copy(sqlTableName = "`root__a__x`")),
					),
					simpleMeta.copy(sqlTableName = "`root__b`"),
				),
			)
			dropStreamTablesQueries(deepMeta) shouldContainExactly listOf(
				"DROP TABLE IF EXISTS `root`",
				"DROP TABLE IF EXISTS `root__a`",
				"DROP TABLE IF EXISTS `root__a__x`",
				"DROP TABLE IF EXISTS `root__b`",
			)
		}
	}
})
