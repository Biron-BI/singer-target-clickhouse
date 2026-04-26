package com.biron.singerTargetClickhouse

import com.biron.singer.core.domain.JsonSchema
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.type.MapType
import com.fasterxml.jackson.module.kotlin.jsonMapper
import com.fasterxml.jackson.module.kotlin.kotlinModule
import java.io.InputStream

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

	/** Decoded record body, slot layout matching the stream's [StreamReader]. */
	data class Record(
		val stream: String,
		val row: RecordRow,
	) : TargetMessage {
		override val type = "RECORD"
	}

	/**
	 * Deleted record. Singer's DELETED_RECORD envelope carries **only the current-level PK
	 * fields** in its body — not simple columns, not subtables. The decoded [row] therefore
	 * has its PK slots `[0, pkCount)` populated and every other slot left null; consumers
	 * (see [DeletedRecordProcessor]) must only read the PK slots.
	 */
	data class DeletedRecord(
		val stream: String,
		val row: RecordRow,
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

/**
 * Streaming Singer-message parser. Owns a per-stream [StreamReader] registry populated
 * as `SCHEMA` messages flow through, and decodes `RECORD` / `DELETED_RECORD` bodies
 * straight from the JSON token stream into [RecordRow]s — no intermediate
 * `LinkedHashMap`, no per-column extractor lookup on the hot path.
 *
 * Thread-confined to its caller (the producer thread in [processStream]). Construction
 * is cheap; allocate one per top-level invocation.
 */
class TargetMessageParser(
	private val subtableSeparator: String = "__",
	private val translateValues: Boolean = false,
) {
	private val streamReaders: MutableMap<String, StreamReader> = HashMap()

	private val objectMapper: ObjectMapper = jsonMapper {
		addModule(kotlinModule())
		disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
	}

	private val mapType: MapType = objectMapper.typeFactory.constructMapType(
		LinkedHashMap::class.java, String::class.java, Any::class.java,
	)

	fun createParser(input: InputStream): JsonParser = objectMapper.factory.createParser(input).apply { codec = objectMapper }

	/**
	 * Read the next message from [parser], advancing it past the value. Returns null on EOF.
	 * Does not recover from malformed JSON mid-stream — exceptions propagate. For line-based
	 * recovery use [parse].
	 */
	fun readNext(parser: JsonParser): TargetMessage? {
		val token = parser.nextToken() ?: return null
		if (token != JsonToken.START_OBJECT) {
			if (token == JsonToken.START_ARRAY) parser.skipChildren()
			return TargetMessage.Unknown("<non-object top-level token: $token>")
		}
		return readEnvelope(parser)
	}

	/**
	 * Parse a single JSONL line. On malformed JSON returns [TargetMessage.Unknown] rather
	 * than throwing — mirrors the prior line-based parser's recovery semantics.
	 */
	fun parse(line: String): TargetMessage? {
		val trimmed = line.trim()
		if (trimmed.isEmpty()) return null
		return runCatching {
			createParser(trimmed.byteInputStream()).use { p -> readNext(p) }
		}.getOrElse { TargetMessage.Unknown(line) }
	}

	/**
	 * Streaming envelope reader. Walks the outer message in source order: when we reach
	 * the `record` field AND `type`+`stream` are already known, dispatch directly to the
	 * registered [StreamReader]. For the rare out-of-order envelope, fall back to reading
	 * the body untyped and routing it through the reader at the end.
	 */
	private fun readEnvelope(parser: JsonParser): TargetMessage {
		var type: String? = null
		var stream: String? = null
		var row: RecordRow? = null
		var stateValue: Any? = null
		var stateSeen = false
		var streamsList: List<String>? = null

		// SCHEMA-specific fields (rare path)
		var schemaValue: Any? = null
		var keyProperties: List<String>? = null
		var cleanFirst = false
		var cleaningColumn: String? = null
		var allKeyPropertiesRaw: Any? = null

		// Fallback: if `record` arrives before `type`, buffer it as untyped.
		var bufferedRecord: Map<String, Any?>? = null

		while (parser.nextToken() != JsonToken.END_OBJECT) {
			val field = parser.currentName()
			parser.nextToken()

			when (field) {
				"type" -> type = parser.text
				"stream" -> stream = parser.text
				"record" -> {
					val st = stream
					if ((type == "RECORD" || type == "DELETED_RECORD") && st != null) {
						val reader = streamReaders[st]
							?: error("$type received before Schema is defined for stream=$st")
						row = reader.read(parser)
					} else {
						bufferedRecord = readUntypedMap(parser)
					}
				}

				"value" -> {
					stateSeen = true
					stateValue = parser.readValueAs(Any::class.java)
				}

				"streams" -> streamsList = readStringList(parser)
				"schema" -> schemaValue = parser.readValueAs(Any::class.java)
				"key_properties" -> keyProperties = readStringList(parser)
				"clean_first" -> cleanFirst = parser.currentToken == JsonToken.VALUE_TRUE
				"cleaning_column" -> cleaningColumn = if (parser.currentToken == JsonToken.VALUE_NULL) null else parser.text
				"all_key_properties" -> allKeyPropertiesRaw = parser.readValueAs(Any::class.java)
				else -> parser.skipChildren()
			}
		}

		return when (type) {
			"RECORD" -> TargetMessage.Record(stream.orEmpty(), resolveRow(type, stream, row, bufferedRecord))
			"DELETED_RECORD" -> TargetMessage.DeletedRecord(stream.orEmpty(), resolveRow(type, stream, row, bufferedRecord))
			"SCHEMA" -> buildSchema(stream, schemaValue, keyProperties, cleanFirst, cleaningColumn, allKeyPropertiesRaw)
			"STATE" -> TargetMessage.State(value = if (stateSeen) stateValue else null)
			"ACTIVE_STREAMS" -> TargetMessage.ActiveStreams(streams = streamsList ?: emptyList())
			else -> TargetMessage.Unknown("type=${type ?: "null"}")
		}
	}

	private fun resolveRow(type: String, stream: String?, row: RecordRow?, bufferedRecord: Map<String, Any?>?): RecordRow {
		if (row != null) return row
		val st = stream.orEmpty()
		val map = bufferedRecord ?: error("$type for stream=$st with empty body")
		val reader = streamReaders[st]
			?: error("$type received before Schema is defined for stream=$st")
		// Out-of-order envelope: we buffered the body as a map, round-trip it through the reader.
		val json = objectMapper.writeValueAsString(map)
		val synthesized = objectMapper.factory.createParser(json).apply { codec = objectMapper }
		synthesized.nextToken()
		return reader.read(synthesized)
	}

	private fun buildSchema(
		stream: String?,
		schemaValue: Any?,
		keyProperties: List<String>?,
		cleanFirst: Boolean,
		cleaningColumn: String?,
		allKeyPropertiesRaw: Any?,
	): TargetMessage.Schema {
		val schema = if (schemaValue != null) objectMapper.convertValue(schemaValue, JsonSchema::class.java) else JsonSchema()
		val allKeyProperties = (allKeyPropertiesRaw as? Map<*, *>)?.let(::parseKeyProperties) ?: SchemaKeyProperties.empty
		val msg = TargetMessage.Schema(
			stream = stream.orEmpty(),
			schema = schema,
			keyProperties = keyProperties ?: emptyList(),
			cleanFirst = cleanFirst,
			cleaningColumn = cleaningColumn,
			allKeyProperties = allKeyProperties,
		)
		registerReader(msg)
		return msg
	}

	/** Build + register the [StreamReader] now so subsequent RECORD/DELETED_RECORDs stream-parse. */
	private fun registerReader(msg: TargetMessage.Schema) {
		val meta = buildMeta(
			JsonSchemaInspectorContext(
				alias = msg.stream,
				schema = msg.schema,
				keyProperties = msg.keyProperties,
				subtableSeparator = subtableSeparator,
				cleaningColumn = msg.cleaningColumn,
				allKeyProperties = msg.allKeyProperties,
			),
		)
		streamReaders[msg.stream] = buildStreamReader(meta, translateValues)
	}

	private fun parseKeyProperties(node: Map<*, *>): SchemaKeyProperties = SchemaKeyProperties(
		props = (node["props"] as? List<*>)?.map { it.toString() }.orEmpty(),
		children = (node["children"] as? Map<*, *>)?.entries?.associate { (key, value) ->
			key.toString() to parseKeyProperties(value as Map<*, *>)
		}.orEmpty(),
	)

	@Suppress("UNCHECKED_CAST")
	private fun readUntypedMap(parser: JsonParser): Map<String, Any?> =
		if (parser.currentToken == JsonToken.VALUE_NULL) emptyMap()
		else objectMapper.readValue<Map<String, Any?>>(parser, mapType)

	private fun readStringList(parser: JsonParser): List<String> {
		if (parser.currentToken != JsonToken.START_ARRAY) {
			parser.skipChildren()
			return emptyList()
		}
		val out = ArrayList<String>()
		while (parser.nextToken() != JsonToken.END_ARRAY) {
			out += parser.text
		}
		return out
	}
}
