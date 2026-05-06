package com.biron.singerTargetClickhouse

import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import org.slf4j.event.Level

class TargetConfigTest : ShouldSpec({
	should("loads minimal config with defaults") {
		val json = """
			{
				"host": "ch",
				"port": 8123,
				"username": "u",
				"password": "p",
				"database": "d"
			}
		""".trimIndent()

		TargetConfig.fromJson(json.reader()) shouldBe TargetConfig(
			host = "ch",
			port = 8123,
			username = "u",
			password = "p",
			database = "d",
		)
	}

	should("overrides snake_case fields when provided") {
		val json = """
			{
				"host": "ch", "port": 8123, "username": "u", "password": "p", "database": "d",
				"batch_size": 2000,
				"deletion_batch_size": 500,
				"translate_values": true,
				"insert_stream_timeout_sec": 45,
				"finalize_concurrency": 8,
				"subtable_separator": "::",
				"extra_active_tables": ["a", "b"],
				"logging_level": "DEBUG"
			}
		""".trimIndent()

		TargetConfig.fromJson(json.reader()) shouldBe TargetConfig(
			host = "ch",
			port = 8123,
			username = "u",
			password = "p",
			database = "d",
			logLevel = Level.DEBUG,
			subtableSeparator = "::",
			batchSize = 2000,
			deletionBatchSize = 500,
			translateValues = true,
			insertStreamTimeoutSec = 45,
			finalizeConcurrency = 8,
			extraActiveTables = listOf("a", "b"),
		)
	}

	should("falls back to defaults when optional fields are explicitly null") {
		val json = """
			{
				"database": "d",
				"logging_level": null,
				"subtable_separator": "__",
				"batch_size": null,
				"translate_values": false,
				"insert_stream_timeout_sec": 120,
				"host": "kirbytes1",
				"port": 8123,
				"password": "redacted",
				"username": "dbcopy",
				"extra_active_tables": ["_singer_state"]
			}
		""".trimIndent()

		TargetConfig.fromJson(json.reader()) shouldBe TargetConfig(
			host = "kirbytes1",
			port = 8123,
			username = "dbcopy",
			password = "redacted",
			database = "d",
			translateValues = false,
			insertStreamTimeoutSec = 120,
			extraActiveTables = listOf("_singer_state"),
		)
	}

	should("ignores unknown fields") {
		val json = """
			{
				"host": "ch", "port": 8123, "username": "u", "password": "p", "database": "d",
				"mystery_field": 42
			}
		""".trimIndent()

		TargetConfig.fromJson(json.reader()).database shouldBe "d"
	}
})
