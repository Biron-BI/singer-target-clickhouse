@file:Suppress("SqlNoDataSourceInspection")

package com.biron.singerTargetClickhouse

import io.kotest.assertions.arrow.core.shouldBeLeft
import io.kotest.assertions.arrow.core.shouldBeRight
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.collections.shouldNotContain
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.testcontainers.clickhouse.ClickHouseContainer

class ClickhouseConnectionIntegrationTest : ShouldSpec({

	val databaseName = "test"
	lateinit var container: ClickHouseContainer
	lateinit var config: TargetConfig
	lateinit var jdbc: JdbcTemplate

	beforeSpec {
		container = ClickHouseContainer(CLICKHOUSE_IMAGE)
			.withPassword("averysecurepassword")
			.withDatabaseName(databaseName)
		container.start()

		config = TargetConfig(
			host = container.host,
			port = container.getMappedPort(8123),
			username = container.username,
			password = container.password,
			database = container.databaseName,
		)

		jdbc = JdbcTemplate(
			DriverManagerDataSource(
				"jdbc:clickhouse://${container.host}:${container.getMappedPort(8123)}",
				container.username,
				container.password,
			),
		)
	}

	afterSpec { container.stop() }

	beforeEach {
		jdbc.execute("DROP DATABASE IF EXISTS `$databaseName`")
		jdbc.execute("CREATE DATABASE `$databaseName`")
	}

	context("runQuery") {
		should("returns data for SELECT") {
			ClickhouseConnection(config).runQuery("SELECT 1, 'foo'") shouldBe
					QueryResult(listOf(listOf(1.toShort(), "foo")), 1)
		}

		should("returns empty data for DDL") {
			val underTest = ClickhouseConnection(config)
			underTest.runQuery("CREATE TABLE box (id Int32) ENGINE = MergeTree ORDER BY tuple()")
			underTest.runQuery("DROP TABLE box")
		}
	}

	context("listTables") {
		should("lists existing tables") {
			jdbc.execute("CREATE TABLE `$databaseName`.a (id Int32) ENGINE = MergeTree ORDER BY tuple()")
			jdbc.execute("CREATE TABLE `$databaseName`.b (id Int32) ENGINE = MergeTree ORDER BY tuple()")
			ClickhouseConnection(config).listTables() shouldBe listOf("a", "b")
		}
	}

	context("listColumns") {
		should("returns name, type and sorting-key flag") {
			jdbc.execute(
				"""
				CREATE TABLE `$databaseName`.box
				(id Int32, name Nullable(String), created_at DateTime)
				ENGINE = MergeTree ORDER BY id
				""".trimIndent(),
			)
			ClickhouseConnection(config).listColumns("box").toSet() shouldBe setOf(
				Column("id", "Int32", isInSortingKey = true),
				Column("name", "Nullable(String)", isInSortingKey = false),
				Column("created_at", "DateTime", isInSortingKey = false),
			)
		}
	}

	context("addColumn") {
		should("adds a column on success") {
			jdbc.execute("CREATE TABLE `$databaseName`.box (id Int32) ENGINE = MergeTree ORDER BY tuple()")

			val underTest = ClickhouseConnection(config)
			underTest.addColumn("box", Column("name", "Nullable(String)", isInSortingKey = false)).shouldBeRight()
			underTest.listColumns("box").toSet() shouldBe setOf(
				Column("id", "Int32", isInSortingKey = false),
				Column("name", "Nullable(String)", isInSortingKey = false),
			)
		}

		should("returns Left on duplicate column") {
			jdbc.execute("CREATE TABLE `$databaseName`.box (id Int32, name String) ENGINE = MergeTree ORDER BY tuple()")

			val newCol = Column("name", "Int32", isInSortingKey = false)
			ClickhouseConnection(config).addColumn("box", newCol).shouldBeLeft().also {
				it.newCol shouldBe newCol
			}
		}
	}

	context("removeColumn") {
		should("drops a column") {
			jdbc.execute("CREATE TABLE `$databaseName`.box (id Int32, gone String) ENGINE = MergeTree ORDER BY tuple()")

			val underTest = ClickhouseConnection(config)
			underTest.removeColumn("box", Column("gone", "String", isInSortingKey = false)).shouldBeRight()
			underTest.listColumns("box").toSet() shouldBe setOf(Column("id", "Int32", isInSortingKey = false))
		}
	}

	context("renameObsoleteTable") {
		should("renames a table with dropped prefix") {
			jdbc.execute("CREATE TABLE `$databaseName`.box (id Int32) ENGINE = MergeTree ORDER BY tuple()")

			val underTest = ClickhouseConnection(config)
			underTest.renameObsoleteTable("box")
			underTest.listTables().shouldContain("_dropped_box").shouldNotContain("box")
		}
	}

	context("JDBC URL settings reach the server") {
		should("date_time_input_format=best_effort: parses RFC 1123-style datetimes that the server's default 'basic' format rejects") {
			jdbc.execute("CREATE TABLE `$databaseName`.events (id Int32, ts DateTime) ENGINE = MergeTree ORDER BY id")

			// Sanity check: a connection that does NOT carry the setting (the test's bare jdbc) is rejected by the server's default `basic` format.
			shouldThrow<Exception> {
				jdbc.execute("INSERT INTO `$databaseName`.events VALUES (0, '15 Jan 2024 10:00:00')")
			}

			// ClickhouseConnection's JDBC URL prefixes settings with `clickhouse_setting_`, so best_effort is forwarded to the server and the same
			// VALUES literal is accepted.
			ClickhouseConnection(config).runQuery("INSERT INTO `$databaseName`.events VALUES (1, '15 Jan 2024 10:00:00')", 0)

			jdbc.queryForList("SELECT id, toString(ts) AS ts FROM `$databaseName`.events ORDER BY id") shouldBe
					listOf(mapOf("id" to 1, "ts" to "2024-01-15 10:00:00"))
		}

		should("insert_null_as_default=0: NULL into a non-Nullable column is rejected instead of being silently replaced by the type default") {
			jdbc.execute("CREATE TABLE `$databaseName`.t (id Int32, name String) ENGINE = MergeTree ORDER BY id")

			// Sanity check: with the server's default insert_null_as_default=1, NULL silently becomes ''.
			jdbc.execute("INSERT INTO `$databaseName`.t VALUES (1, NULL)")
			jdbc.queryForList("SELECT id, name FROM `$databaseName`.t WHERE id = 1") shouldBe
					listOf(mapOf("id" to 1, "name" to ""))

			// ClickhouseConnection sets insert_null_as_default=0, so the same INSERT errors out rather than corrupting the data with a default value.
			shouldThrow<Exception> {
				ClickhouseConnection(config).runQuery("INSERT INTO `$databaseName`.t VALUES (2, NULL)", 0)
			}
		}
	}

	context("openRowWriter streaming insert") {
		should("persists rows written in JSONCompactEachRow format") {
			jdbc.execute("CREATE TABLE `$databaseName`.items (id Int32, name String) ENGINE = MergeTree ORDER BY tuple()")

			ClickhouseConnection(config).openRowWriter("INSERT INTO items (`id`,`name`) FORMAT JSONCompactEachRow").use { writer ->
				writer.write("[1,\"alice\"]\n".toByteArray())
				writer.write("[2,\"bob\"]\n".toByteArray())
			}

			jdbc.queryForList("SELECT id, name FROM `$databaseName`.items ORDER BY id") shouldBe listOf(
				mapOf("id" to 1, "name" to "alice"),
				mapOf("id" to 2, "name" to "bob"),
			)
		}

		should("throws on close when server rejects the batch") {
			jdbc.execute("CREATE TABLE `$databaseName`.items (id Int32) ENGINE = MergeTree ORDER BY tuple()")

			shouldThrow<IllegalStateException> {
				ClickhouseConnection(config).openRowWriter("INSERT INTO items (`id`) FORMAT JSONCompactEachRow").use { writer ->
					writer.write("[\"not-an-int\"]\n".toByteArray())
				}
			}.apply {
				message shouldContain "ClickHouse insert failed"
				message shouldContain "Int32"
			}
		}
	}
})
