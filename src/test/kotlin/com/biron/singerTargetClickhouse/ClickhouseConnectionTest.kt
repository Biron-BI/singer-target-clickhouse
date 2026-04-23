@file:Suppress("SqlNoDataSourceInspection")

package com.biron.singerTargetClickhouse

import io.kotest.assertions.arrow.core.shouldBeLeft
import io.kotest.assertions.arrow.core.shouldBeRight
import io.kotest.core.spec.style.DescribeSpec
import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.testcontainers.clickhouse.ClickHouseContainer

private const val CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:24.12.3.47"

class ClickhouseConnectionTest : DescribeSpec({

	lateinit var container: ClickHouseContainer
	lateinit var config: TargetConfig
	lateinit var jdbc: JdbcTemplate

	beforeSpec {
		container = ClickHouseContainer(CLICKHOUSE_IMAGE)
			.withUsername("user")
			.withPassword("averysecurepassword")
			.withDatabaseName("testdb")
		container.start()

		config = TargetConfig(
			host = container.host,
			port = container.getMappedPort(8123),
			username = container.username,
			password = container.password,
			database = "testdb",
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
		jdbc.execute("DROP DATABASE IF EXISTS testdb")
		jdbc.execute("CREATE DATABASE testdb")
	}

	fun newConnection() = ClickhouseConnection(config)

	describe("runQuery") {
		it("returns data for SELECT") {
			val ch = newConnection()
			val res = ch.runQuery("SELECT 1, 'foo'")
			res.rows shouldBe 1
			res.data.size shouldBe 1
			res.data[0][0].toString() shouldBe "1"
			res.data[0][1] shouldBe "foo"
		}

		it("returns empty data for DDL") {
			val ch = newConnection()
			ch.runQuery("CREATE TABLE box (id Int32) ENGINE = MergeTree ORDER BY tuple()")
			ch.runQuery("DROP TABLE box")
			// no assertion on data, just verify no exception
		}
	}

	describe("listTables") {
		it("lists existing tables") {
			jdbc.execute("CREATE TABLE testdb.a (id Int32) ENGINE = MergeTree ORDER BY tuple()")
			jdbc.execute("CREATE TABLE testdb.b (id Int32) ENGINE = MergeTree ORDER BY tuple()")
			newConnection().listTables() shouldContain "a"
			newConnection().listTables() shouldContain "b"
		}
	}

	describe("listColumns") {
		it("returns name, type and sorting-key flag") {
			jdbc.execute(
				"""
				CREATE TABLE testdb.box
				(id Int32, name Nullable(String), created_at DateTime)
				ENGINE = MergeTree ORDER BY id
				""".trimIndent(),
			)
			val cols = newConnection().listColumns("box").associateBy { it.name }
			cols["id"]!!.type shouldBe "Int32"
			cols["id"]!!.isInSortingKey shouldBe true
			cols["name"]!!.type shouldBe "Nullable(String)"
			cols["name"]!!.isInSortingKey shouldBe false
		}
	}

	describe("addColumn") {
		it("adds a column on success") {
			jdbc.execute("CREATE TABLE testdb.box (id Int32) ENGINE = MergeTree ORDER BY tuple()")
			val ch = newConnection()

			ch.addColumn("box", Column("name", "Nullable(String)", isInSortingKey = false)).shouldBeRight()
			ch.listColumns("box").map { it.name } shouldContain "name"
		}

		it("returns Left on duplicate column") {
			jdbc.execute("CREATE TABLE testdb.box (id Int32, name String) ENGINE = MergeTree ORDER BY tuple()")
			val ch = newConnection()

			val err = ch.addColumn("box", Column("name", "Int32", isInSortingKey = false)).shouldBeLeft()
			err.newCol.name shouldBe "name"
		}
	}

	describe("removeColumn") {
		it("drops a column") {
			jdbc.execute("CREATE TABLE testdb.box (id Int32, gone String) ENGINE = MergeTree ORDER BY tuple()")
			val ch = newConnection()

			ch.removeColumn("box", Column("gone", "String", isInSortingKey = false)).shouldBeRight()
			ch.listColumns("box").map { it.name } shouldContainExactly listOf("id")
		}
	}

	describe("renameObsoleteColumn") {
		it("renames a table with dropped prefix") {
			jdbc.execute("CREATE TABLE testdb.box (id Int32) ENGINE = MergeTree ORDER BY tuple()")
			newConnection().renameObsoleteColumn("box")

			newConnection().listTables() shouldContain "_dropped_box"
		}
	}

	describe("openRowWriter streaming insert") {
		it("persists rows written in JSONCompactEachRow format") {
			jdbc.execute("CREATE TABLE testdb.items (id Int32, name String) ENGINE = MergeTree ORDER BY tuple()")
			val ch = newConnection()

			ch.openRowWriter("INSERT INTO items (`id`,`name`) FORMAT JSONCompactEachRow").use { writer ->
				writer.write("[1,\"alice\"]\n".toByteArray())
				writer.write("[2,\"bob\"]\n".toByteArray())
			}

			jdbc.queryForList("SELECT id, name FROM testdb.items ORDER BY id").let { rows ->
				rows.size shouldBe 2
				rows[0]["id"].toString() shouldBe "1"
				rows[0]["name"] shouldBe "alice"
				rows[1]["id"].toString() shouldBe "2"
				rows[1]["name"] shouldBe "bob"
			}
		}

		it("throws on close when server rejects the batch") {
			jdbc.execute("CREATE TABLE testdb.items (id Int32) ENGINE = MergeTree ORDER BY tuple()")
			val ch = newConnection()

			try {
				ch.openRowWriter("INSERT INTO items (`id`) FORMAT JSONCompactEachRow").use { writer ->
					writer.write("[\"not-an-int\"]\n".toByteArray())
				}
				throw AssertionError("expected failure on close")
			} catch (e: IllegalStateException) {
				// expected
			}
		}
	}
})
