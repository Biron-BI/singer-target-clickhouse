package com.biron.singerTargetClickhouse

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.DescribeSpec
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe

class JsonSchemaTranslatorTest : DescribeSpec({

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

	describe("translateCH") {
		it("refuses empty meta") {
			shouldThrow<IllegalStateException> { translateCH("db", emptyMeta, recursive = true) }
		}

		it("translates basic meta") {
			translateCH("db", simpleMeta, recursive = true) shouldContainExactly listOf(
				"CREATE TABLE db.`order` ( `id` Int32, `name` Nullable(String) ) ENGINE = MergeTree ORDER BY tuple()",
			)
		}

		it("translates meta with PK") {
			translateCH("db", metaWithPK, recursive = true) shouldContainExactly listOf(
				"CREATE TABLE db.`order` ( `id` UInt32, `name` Nullable(String), `_ver` UInt64 ) ENGINE = ReplacingMergeTree(_ver) ORDER BY `id`",
			)
		}

		it("translates meta with PK and children recursively") {
			translateCH("db", metaWithPKAndChildren, recursive = true) shouldContainExactly listOf(
				"CREATE TABLE db.`order` ( `id` UInt32, `name` Nullable(String), `_ver` UInt64 ) ENGINE = ReplacingMergeTree(_ver) ORDER BY `id`",
				"CREATE TABLE db.`order_child` ( `id` Int32, `name` Nullable(String), `_root_ver` UInt64 ) ENGINE = MergeTree ORDER BY tuple()",
			)
		}

		it("translates with LowCardinality modifier") {
			val lowCard = simpleMeta.copy(
				simpleColumnMappings = listOf(idColumn, nameColumn.copy(lowCardinality = true)),
			)
			translateCH("db", lowCard, recursive = true) shouldContainExactly listOf(
				"CREATE TABLE db.`order` ( `id` Int32, `name` LowCardinality(Nullable(String)) ) ENGINE = MergeTree ORDER BY tuple()",
			)
		}
	}

	describe("listTableNames") {
		it("lists all table names depth-first") {
			listTableNames(metaWithPKAndChildren) shouldContainExactly listOf("`order`", "`order_child`")
		}
	}

	describe("toQualifiedType") {
		val baseCol = idColumn.copy(nullable = false, lowCardinality = false, nestedArray = false)

		it("no modifiers") { toQualifiedType(baseCol) shouldBe "Int32" }
		it("nestedArray") { toQualifiedType(baseCol.copy(chType = "String", nestedArray = true)) shouldBe "Array(String)" }
		it("nullable") { toQualifiedType(baseCol.copy(chType = "UInt64", nullable = true)) shouldBe "Nullable(UInt64)" }
		it("lowCardinality") { toQualifiedType(baseCol.copy(chType = "DateTime", lowCardinality = true)) shouldBe "LowCardinality(DateTime)" }
		it("multi") {
			toQualifiedType(baseCol.copy(chType = "UInt8", nullable = true, lowCardinality = true, nestedArray = true)) shouldBe
				"Array(LowCardinality(Nullable(UInt8)))"
		}
	}

	describe("getColumnsIntersections") {
		it("partitions columns into missing/modified/obsolete") {
			val notModified = Column("not_modified", "1", isInSortingKey = false)
			val toDelete = Column("to_delete", "1", isInSortingKey = false)
			val toModifyFromExisting = Column("to_modify", "1", isInSortingKey = false)
			val toAdd = Column("to_add", "1", isInSortingKey = false)
			val toModifyFromRequired = Column("to_modify", "2", isInSortingKey = false)

			val res = getColumnsIntersections(
				listOf(notModified, toDelete, toModifyFromExisting),
				listOf(notModified, toAdd, toModifyFromRequired),
			)

			res.missing shouldContainExactly listOf(toAdd)
			res.modified shouldContainExactly listOf(ModifiedColumn(toModifyFromExisting, toModifyFromRequired))
			res.obsolete shouldContainExactly listOf(toDelete)
		}
	}
})
