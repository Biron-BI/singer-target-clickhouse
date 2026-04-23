package com.biron.singerTargetClickhouse

import com.biron.singer.core.domain.JsonSchema
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.type.MapType
import com.fasterxml.jackson.module.kotlin.jsonMapper
import com.fasterxml.jackson.module.kotlin.kotlinModule

sealed interface TargetMessage {
	val type: String

	data class Schema(
		val stream: String,
		val schema: JsonSchema,
		val keyProperties: List<String>,
		val cleanFirst: Boolean = false,
		val cleaningColumn: String? = null,
		val allKeyProperties: SchemaKeyProperties = SchemaKeyProperties.empty,
	) : TargetMessage {
		override val type = "SCHEMA"
	}

	data class Record(
		val stream: String,
		val record: Map<String, Any?>,
	) : TargetMessage {
		override val type = "RECORD"
	}

	data class DeletedRecord(
		val stream: String,
		val record: Map<String, Any?>,
	) : TargetMessage {
		override val type = "DELETED_RECORD"
	}

	data class State(
		/** Generic Jackson tree (Map / List / primitive / null). Re-serialized verbatim on output. */
		val value: Any?,
	) : TargetMessage {
		override val type = "STATE"
	}

	data class ActiveStreams(
		val streams: List<String>,
	) : TargetMessage {
		override val type = "ACTIVE_STREAMS"
	}

	data class Unknown(val raw: String) : TargetMessage {
		override val type = "UNKNOWN"
	}
}

/**
 * Mirrors singer-node's SchemaKeyProperties: key properties for the current level plus,
 * recursively, children. Used to compute `_parent_X` columns in child tables when the
 * parent level has primary keys.
 */
data class SchemaKeyProperties(
	val props: List<String>,
	val children: Map<String, SchemaKeyProperties>,
) {
	companion object {
		val empty = SchemaKeyProperties(emptyList(), emptyMap())
	}
}

object TargetMessageParser {
	private val objectMapper: ObjectMapper = jsonMapper {
		addModule(kotlinModule())
		disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
	}

	// Deserializing straight into a Map on the hot path avoids the JsonNode→TokenBuffer→Map
	// round-trip that dominated the profile (readTree + convertValue together were ~50% of CPU).
	private val mapType: MapType = objectMapper.typeFactory.constructMapType(
		LinkedHashMap::class.java, String::class.java, Any::class.java,
	)

	fun parse(line: String): TargetMessage? {
		val trimmed = line.trim()
		if (trimmed.isEmpty()) return null
		val map = runCatching { readMap(trimmed) }.getOrNull() ?: return TargetMessage.Unknown(line)

		return when (map["type"] as? String) {
			"SCHEMA" -> parseSchema(map)
			"RECORD" -> TargetMessage.Record(
				stream = (map["stream"] as? String).orEmpty(),
				record = asMap(map["record"]),
			)

			"DELETED_RECORD" -> TargetMessage.DeletedRecord(
				stream = (map["stream"] as? String).orEmpty(),
				record = asMap(map["record"]),
			)

			"STATE" -> TargetMessage.State(value = map["value"])
			"ACTIVE_STREAMS" -> TargetMessage.ActiveStreams(
				streams = (map["streams"] as? List<*>)?.map { it.toString() }.orEmpty(),
			)

			else -> TargetMessage.Unknown(line)
		}
	}

	@Suppress("UNCHECKED_CAST")
	private fun readMap(s: String): Map<String, Any?>? =
		objectMapper.readValue(s, mapType) as Map<String, Any?>?

	private fun parseSchema(map: Map<String, Any?>): TargetMessage.Schema = TargetMessage.Schema(
		stream = (map["stream"] as? String).orEmpty(),
		schema = objectMapper.convertValue(map["schema"], JsonSchema::class.java),
		keyProperties = (map["key_properties"] as? List<*>)?.map { it.toString() }.orEmpty(),
		cleanFirst = (map["clean_first"] as? Boolean) ?: false,
		cleaningColumn = map["cleaning_column"] as? String,
		allKeyProperties = (map["all_key_properties"] as? Map<*, *>)
			?.let(::parseKeyProperties) ?: SchemaKeyProperties.empty,
	)

	private fun parseKeyProperties(node: Map<*, *>): SchemaKeyProperties = SchemaKeyProperties(
		props = (node["props"] as? List<*>)?.map { it.toString() }.orEmpty(),
		children = (node["children"] as? Map<*, *>)?.entries?.associate { (key, value) ->
			key.toString() to parseKeyProperties(value as Map<*, *>)
		}.orEmpty(),
	)

	@Suppress("UNCHECKED_CAST")
	private fun asMap(value: Any?): Map<String, Any?> =
		if (value == null) emptyMap() else value as Map<String, Any?>
}
