package com.biron.singerTargetClickhouse

import com.clickhouse.jdbc.ClickHouseDataSource
import com.clickhouse.jdbc.ClickHouseDriver
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.collections.shouldContainAll
import org.springframework.jdbc.core.JdbcTemplate
import org.testcontainers.clickhouse.ClickHouseContainer

class MainTest : ShouldSpec({
	lateinit var container: ClickHouseContainer
	beforeSpec {
		container = object : ClickHouseContainer("clickhouse/clickhouse-server:23.10.4.25") {
			override fun getDriverClassName() = ClickHouseDriver::class.qualifiedName
		}
			.apply { start() }
	}

	fun getJdbcTemplate(container: ClickHouseContainer) =
		ClickHouseDataSource("jdbc:clickhouse://${container.host}:${container.getMappedPort(8123)}?compress=0")
			.let(::JdbcTemplate)

	should("connection be usable") {
		getJdbcTemplate(container).queryForList("show databases").map { it["name"] as String } shouldContainAll listOf("default", "system")
	}
})
