package com.biron.singerTargetClickhouse

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.collections.shouldContainAll
import io.kotest.matchers.collections.shouldNotContain
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.string.shouldStartWith
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.testcontainers.clickhouse.ClickHouseContainer

class ClickhouseJdbcSmokeTest : ShouldSpec({
	val user = "default"
	val password = "default"

	lateinit var container: ClickHouseContainer
	lateinit var jdbcTemplate: JdbcTemplate

	beforeSpec {
		container = ClickHouseContainer(CLICKHOUSE_IMAGE).apply {
			withUsername(user)
			withPassword(password)
			start()
		}

		jdbcTemplate = DriverManagerDataSource(
			"jdbc:clickhouse://${container.host}:${container.getMappedPort(8123)}?compress=0",
			user,
			password,
		).let(::JdbcTemplate)

		jdbcTemplate.execute(
			"CREATE TABLE box (id Nullable(Int32), width Int32, name String, to_del String) ENGINE = MergeTree() ORDER BY tuple()"
		)
		jdbcTemplate.execute("CREATE TABLE tickets (id Nullable(Int32)) ENGINE = MergeTree() ORDER BY tuple()")
		jdbcTemplate.execute(
			"CREATE TABLE tickets__tags (_level_0_index Int32, _root_id Int32, value String, _root_ver UInt64) ENGINE = MergeTree() ORDER BY (_level_0_index, _root_id)"
		)
		jdbcTemplate.execute("INSERT INTO `box` VALUES (1, 50, 'box1', 'qwer')")
	}

	afterSpec {
		container.stop()
	}

	fun describe(table: String): List<Map<String, String>> =
		jdbcTemplate.queryForList("desc $table").map {
			mapOf("name" to it["name"] as String, "type" to it["type"] as String)
		}

	should("connection be usable") {
		jdbcTemplate.queryForList("show databases").map { it["name"] as String } shouldContainAll
				listOf("default", "system")
	}

	should("should list tables") {
		jdbcTemplate.queryForList("show tables").map { it["name"] as String } shouldContainAll
				listOf("box", "tickets", "tickets__tags")
	}

	should("should describe table") {
		describe("tickets__tags") shouldContainAll listOf(
			mapOf("name" to "_level_0_index", "type" to "Int32"),
			mapOf("name" to "_root_id", "type" to "Int32"),
			mapOf("name" to "_root_ver", "type" to "UInt64"),
			mapOf("name" to "value", "type" to "String"),
		)
	}

	context("addColumn") {
		should("success case") {
			jdbcTemplate.execute("ALTER TABLE box ADD COLUMN height Int32")
			describe("box") shouldContainAll listOf(mapOf("name" to "height", "type" to "Int32"))
		}

		should("failure case") {
			val exception = shouldThrow<Exception> {
				jdbcTemplate.execute("ALTER TABLE box ADD COLUMN name Int32")
			}
			exception.cause?.cause?.message shouldStartWith "Code: 15. DB::Exception: Cannot add column `name`: column with this name already exists. (DUPLICATE_COLUMN) (version"
		}
	}

	context("updateColumn") {
		should("success case") {
			jdbcTemplate.execute("ALTER TABLE box MODIFY COLUMN width LowCardinality(Nullable(String))")
			describe("box") shouldContainAll listOf(
				mapOf("name" to "width", "type" to "LowCardinality(Nullable(String))"),
			)
		}

		should("failure case") {
			val exception = shouldThrow<Exception> {
				jdbcTemplate.execute("ALTER TABLE box MODIFY COLUMN name Int32")
			}
			exception.message shouldContain "DB::Exception: Cannot parse string 'box1' as Int32: syntax error at begin of string"
		}
	}

	context("deleteColumn") {
		should("success case") {
			jdbcTemplate.execute("TRUNCATE TABLE box")
			jdbcTemplate.execute("ALTER TABLE box DROP COLUMN to_del")
			describe("box") shouldNotContain mapOf("name" to "to_del", "type" to "String")
		}

		should("failure case") {
			val exception = shouldThrow<Exception> {
				jdbcTemplate.execute("ALTER TABLE box DROP COLUMN missing")
			}
			exception.cause?.cause?.message shouldStartWith "Code: 10. DB::Exception: Wrong column name. Cannot find column `missing` to drop. (NOT_FOUND_COLUMN_IN_BLOCK) (version "
		}
	}
})
