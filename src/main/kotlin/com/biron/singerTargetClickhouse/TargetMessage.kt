package com.biron.singerTargetClickhouse

import com.biron.singer.core.domain.JsonSchema
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
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
		val value: JsonNode,
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

	fun parse(line: String): TargetMessage? {
		val trimmed = line.trim()
		if (trimmed.isEmpty()) return null
		val node = runCatching { objectMapper.readTree(trimmed) }.getOrNull() ?: return TargetMessage.Unknown(line)
		if (!node.isObject) return TargetMessage.Unknown(line)
		return when (node["type"]?.asText()) {
			"SCHEMA" -> parseSchema(node)
			"RECORD" -> TargetMessage.Record(
				stream = node["stream"].asText(),
				record = asMap(node["record"]),
			)

			"DELETED_RECORD" -> TargetMessage.DeletedRecord(
				stream = node["stream"].asText(),
				record = asMap(node["record"]),
			)

			"STATE" -> TargetMessage.State(value = node["value"] ?: objectMapper.nullNode())
			"ACTIVE_STREAMS" -> TargetMessage.ActiveStreams(
				streams = node["streams"]?.map { it.asText() }.orEmpty(),
			)

			else -> TargetMessage.Unknown(line)
		}
	}

	private fun parseSchema(node: JsonNode): TargetMessage.Schema = TargetMessage.Schema(
		stream = node["stream"].asText(),
		schema = objectMapper.treeToValue(node["schema"], JsonSchema::class.java),
		keyProperties = node["key_properties"]?.map { it.asText() }.orEmpty(),
		cleanFirst = node["clean_first"]?.asBoolean(false) ?: false,
		cleaningColumn = node["cleaning_column"]?.takeIf { !it.isNull }?.asText(),
		allKeyProperties = node["all_key_properties"]?.let(::parseKeyProperties) ?: SchemaKeyProperties.empty,
	)

	private fun parseKeyProperties(node: JsonNode): SchemaKeyProperties = SchemaKeyProperties(
		props = node["props"]?.map { it.asText() }.orEmpty(),
		children = node["children"]?.fields()?.asSequence()
			?.associate { (key, value) -> key to parseKeyProperties(value) }
			.orEmpty(),
	)

	@Suppress("UNCHECKED_CAST")
	private fun asMap(node: JsonNode?): Map<String, Any?> =
		if (node == null || node.isNull) emptyMap()
		else objectMapper.convertValue(node, Map::class.java) as Map<String, Any?>
}
