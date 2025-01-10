package com.biron.singerTargetClickhouse

import com.clickhouse.jdbc.ClickHouseDataSource
import com.clickhouse.jdbc.ClickHouseDriver
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.collections.shouldContainAll
import io.kotest.matchers.collections.shouldNotContain
import io.kotest.matchers.shouldBe
import org.springframework.jdbc.core.JdbcTemplate
import org.testcontainers.clickhouse.ClickHouseContainer
import java.util.logging.Logger

class MainTest : ShouldSpec({
	lateinit var container: ClickHouseContainer
	val logger = Logger.getLogger(MainTest::class.java.name)

	fun getJdbcTemplate(container: ClickHouseContainer) =
		ClickHouseDataSource("jdbc:clickhouse://${container.host}:${container.getMappedPort(8123)}?compress=0")
			.let(::JdbcTemplate)

	beforeSpec {
		container = object : ClickHouseContainer("clickhouse/clickhouse-server:23.10.4.25") {
			override fun getDriverClassName() = ClickHouseDriver::class.qualifiedName
		}
			.apply { start() }

		try {
			getJdbcTemplate(container).execute("CREATE TABLE box (id Nullable(Int32), width Int32, name String, to_del String) ENGINE = MergeTree() ORDER BY tuple()")
			getJdbcTemplate(container).execute("CREATE TABLE tickets (id Nullable(Int32)) ENGINE = MergeTree() ORDER BY tuple()")
			getJdbcTemplate(container).execute("CREATE TABLE tickets__tags (_level_0_index Int32, _root_id Int32, value String, _root_ver UInt64) ENGINE = MergeTree() ORDER BY (_level_0_index, _root_id)")
			getJdbcTemplate(container).execute("INSERT INTO `box` VALUES (1, 50, 'box1', 'qwer')")
		}
		catch (e: Exception) {
			logger.severe("Error initializing tables: ${e.message}")
			throw e
		}
		}
	afterSpec{
		container.stop()
	}

	should("connection be usable") {
		getJdbcTemplate(container).queryForList("show databases").map { it["name"] as String } shouldContainAll listOf("default", "system")
	}

	should("should list tables") {
		getJdbcTemplate(container).queryForList("show tables").map {
			it["name"] as String
		} shouldContainAll listOf("box", "tickets", "tickets__tags")
	}


	should("should discribe table"){
		val expectedColumns = listOf(
			mapOf("name" to "_level_0_index", "type" to "Int32"),
			mapOf("name" to "_root_id", "type" to "Int32"),
			mapOf("name" to "_root_ver", "type" to "UInt64"),
			mapOf("name" to "value", "type" to "String")
		).sortedBy { it["name"] as String }

		val actualColumns = getJdbcTemplate(container).queryForList("desc tickets__tags").map {
			mapOf(
				"name" to it["name"] as String,
				"type" to it["type"] as String,
			)
		}.sortedBy { it["name"].toString() }

		actualColumns shouldContainAll expectedColumns
	}

	context("addColumn") {
		should("success case"){
			val jdbcTemplate = getJdbcTemplate(container)
			jdbcTemplate.execute("ALTER TABLE box ADD COLUMN height Int32")
			val columns = jdbcTemplate.queryForList("desc box").map {
				mapOf(
					"name" to it["name"] as String,
					"type" to it["type"] as String,
				)
			}.sortedBy { it["name"].toString() }

			val excepted = listOf(
				mapOf("name" to "height", "type" to "Int32")
			).sortedBy { it["name"] as String }

			columns shouldContainAll excepted

		}
		should("failure case"){
			val jdbcTemplate = getJdbcTemplate(container)
			val exception = shouldThrow<Exception> {
				jdbcTemplate.execute("ALTER TABLE box ADD COLUMN name Int32")
			}

			val rootCause = exception.cause?.cause
			rootCause?.message shouldBe "Code: 15. DB::Exception: Cannot add column `name`: column with this name already exists. (DUPLICATE_COLUMN) (version 23.10.4.25 (official build))\n"

		}
	}
	context("updateColumn") {
		should("success case") {
			val jdbcTemplate = getJdbcTemplate(container)
			jdbcTemplate.execute("ALTER TABLE box MODIFY COLUMN width LowCardinality(Nullable(UInt64))")

			val columns = jdbcTemplate.queryForList("desc box").map {
				mapOf(
					"name" to it["name"] as String,
					"type" to it["type"] as String
				)
			}.sortedBy { it["name"].toString() }

			val expected = listOf(
				mapOf("name" to "width", "type" to "LowCardinality(Nullable(UInt64))"),
			).sortedBy { it["name"] as String }

			columns shouldContainAll expected
		}
		should("failure case") {
			val jdbcTemplate = getJdbcTemplate(container)
			val exception = shouldThrow<Exception> {
				jdbcTemplate.execute("ALTER TABLE box MODIFY COLUMN name Int32")
			}

			exception.message shouldBe "StatementCallback; bad SQL grammar [ALTER TABLE box MODIFY COLUMN name Int32]"
		}
	}
	context("deleteColumn") {
		should("success case") {
			val jdbcTemplate = getJdbcTemplate(container)
			jdbcTemplate.execute("TRUNCATE TABLE box")
			jdbcTemplate.execute("ALTER TABLE box DROP COLUMN to_del")

			val columns = jdbcTemplate.queryForList("desc box").map {
				mapOf(
					"name" to it["name"] as String,
					"type" to it["type"] as String
				)
			}.sortedBy { it["name"].toString() }

			columns shouldNotContain mapOf("name" to "to_del", "type" to "String")
		}
		should("failure case") {
			val jdbcTemplate = getJdbcTemplate(container)
			val exception = shouldThrow<Exception> {
				jdbcTemplate.execute("ALTER TABLE box DROP COLUMN missing")
			}

			val rootCause = exception.cause?.cause
			rootCause?.message shouldBe "Code: 10. DB::Exception: Wrong column name. Cannot find column `missing` to drop. (NOT_FOUND_COLUMN_IN_BLOCK) (version 23.10.4.25 (official build))\n"

		}

	}

})

