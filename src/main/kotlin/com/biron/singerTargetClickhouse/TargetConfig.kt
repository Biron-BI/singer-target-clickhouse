package com.biron.singerTargetClickhouse

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSetter
import com.fasterxml.jackson.annotation.Nulls
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.module.kotlin.jsonMapper
import com.fasterxml.jackson.module.kotlin.kotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import org.slf4j.event.Level
import java.io.Reader

data class TargetConfig(
	val host: String,
	val port: Int,
	val username: String,
	val password: String,
	val database: String,
	@param:JsonProperty("logging_level")
	val logLevel: Level = Level.INFO,
	val subtableSeparator: String = "__",
	val batchSize: Int = 100,
	val deletionBatchSize: Int = 100,
	val translateValues: Boolean = false,
	val insertStreamTimeoutSec: Int = 180,
	val finalizeConcurrency: Int = 3,
	val extraActiveTables: List<String> = emptyList(),
) {
	companion object {
		private val objectMapper = jsonMapper {
			addModule(kotlinModule())
			propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
			disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
			defaultSetterInfo(JsonSetter.Value.forValueNulls(Nulls.SKIP))
		}

		fun fromJson(reader: Reader): TargetConfig = objectMapper.readValue(reader)
	}
}
