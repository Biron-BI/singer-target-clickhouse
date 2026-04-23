package com.biron.singerTargetClickhouse

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import org.slf4j.event.Level

class TargetConfigTest : StringSpec({
	"loads minimal config with defaults" {
		val json = """
			{
				"host": "ch",
				"port": 8123,
				"username": "u",
				"password": "p",
				"database": "d"
			}
		""".trimIndent()

		val config = TargetConfig.fromJson(json.reader())

		config.host shouldBe "ch"
		config.port shouldBe 8123
		config.username shouldBe "u"
		config.database shouldBe "d"
		config.batchSize shouldBe 100
		config.deletionBatchSize shouldBe 100
		config.subtableSeparator shouldBe "__"
		config.translateValues shouldBe false
		config.insertStreamTimeoutSec shouldBe 180
		config.finalizeConcurrency shouldBe 3
		config.extraActiveTables shouldBe emptyList()
		config.logLevel shouldBe Level.INFO
	}

	"overrides snake_case fields when provided" {
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

		val config = TargetConfig.fromJson(json.reader())

		config.batchSize shouldBe 2000
		config.deletionBatchSize shouldBe 500
		config.translateValues shouldBe true
		config.insertStreamTimeoutSec shouldBe 45
		config.finalizeConcurrency shouldBe 8
		config.subtableSeparator shouldBe "::"
		config.extraActiveTables shouldBe listOf("a", "b")
		config.logLevel shouldBe Level.DEBUG
	}

	"ignores unknown fields" {
		val json = """
			{
				"host": "ch", "port": 8123, "username": "u", "password": "p", "database": "d",
				"mystery_field": 42
			}
		""".trimIndent()

		TargetConfig.fromJson(json.reader()).database shouldBe "d"
	}
})
