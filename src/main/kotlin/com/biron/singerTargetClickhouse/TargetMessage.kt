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

	/** Legacy map-based record, emitted by the object-level [TargetMessageParser.parse] used in tests. */
	data class Record(
		val stream: String,
		val record: Map<String, Any?>,
	) : TargetMessage {
		override val type = "RECORD"
	}

	/**
	 * Hot-path record, emitted by [StreamingMessageParser] after a `SCHEMA` message has
	 * registered a [StreamReader] for the stream. The row is already in the slot layout
	 * expected by `RecordProcessor.pushRecord`.
	 */
	data class TypedRecord(
		val stream: String,
		val row: RecordRow,
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

/**
 * Legacy line-based parser. Always emits [TargetMessage.Record] with a Map payload.
 * Kept for the unit-test contract and as a recovery-on-malformed-JSON fallback.
 * The production hot path goes through [StreamingMessageParser] instead.
 */
object TargetMessageParser {
	private val objectMapper: ObjectMapper = jsonMapper {
		addModule(kotlinModule())
		disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
	}

	private val mapType: MapType = objectMapper.typeFactory.constructMapType(
		LinkedHashMap::class.java, String::class.java, Any::class.java,
	)

	/**
	 * Parse a single JSONL line. On malformed JSON returns [TargetMessage.Unknown] rather than throwing.
	 */
	fun parse(line: String): TargetMessage? {
		val trimmed = line.trim()
		if (trimmed.isEmpty()) return null
		val map = runCatching { readMap(trimmed) }.getOrNull() ?: return TargetMessage.Unknown(line)
		return dispatch(map, rawFallback = line)
	}

	private fun dispatch(map: Map<String, Any?>, rawFallback: String?): TargetMessage =
		when (map["type"] as? String) {
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

			else -> TargetMessage.Unknown(rawFallback ?: map.toString())
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

/**
 * Hot-path streaming parser. Holds a per-stream [StreamReader] registry populated as
 * `SCHEMA` messages flow through, and decodes `RECORD` bodies straight from the JSON token
 * stream into [RecordRow]s — no intermediate `LinkedHashMap`, no per-column extractor
 * lookup.
 *
 * Thread-confined to the producer (parser) thread. Construction is cheap; allocate one
 * per top-level invocation.
 */
class StreamingMessageParser(
	private val subtableSeparator: String,
	private val translateValues: Boolean,
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
	 * Unlike [TargetMessageParser.parse] this does not recover from malformed JSON mid-stream.
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
	 * Streaming envelope reader. Walks the outer message in source order: when we reach
	 * the `record` field AND `type`+`stream` are already known, we dispatch directly to the
	 * registered [StreamReader] — the whole point of this path. For non-RECORD messages
	 * (rare), fall back to reading the body untyped and dispatching at the end.
	 */
	private fun readEnvelope(parser: JsonParser): TargetMessage {
		var type: String? = null
		var stream: String? = null
		var row: RecordRow? = null
		var deletedRecordMap: Map<String, Any?>? = null
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
					when {
						type == "RECORD" && st != null -> {
							val reader = streamReaders[st]
								?: throw IllegalStateException("Record message received before Schema is defined for stream=$st")
							row = reader.read(parser)
						}

						type == "DELETED_RECORD" -> deletedRecordMap = readUntypedMap(parser)
						else -> bufferedRecord = readUntypedMap(parser)
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
			"RECORD" -> buildRecord(stream, row, bufferedRecord)
			"DELETED_RECORD" -> TargetMessage.DeletedRecord(
				stream = stream.orEmpty(),
				record = deletedRecordMap ?: bufferedRecord ?: emptyMap(),
			)

			"SCHEMA" -> buildSchema(stream, schemaValue, keyProperties, cleanFirst, cleaningColumn, allKeyPropertiesRaw)
			"STATE" -> TargetMessage.State(value = if (stateSeen) stateValue else null)
			"ACTIVE_STREAMS" -> TargetMessage.ActiveStreams(streams = streamsList ?: emptyList())
			else -> TargetMessage.Unknown("type=${type ?: "null"}")
		}
	}

	private fun buildRecord(stream: String?, row: RecordRow?, bufferedRecord: Map<String, Any?>?): TargetMessage {
		val st = stream.orEmpty()
		if (row != null) return TargetMessage.TypedRecord(st, row)

		// Out-of-order envelope: `record` arrived before `type`, so we buffered it as a map.
		// Round-trip through the stream's reader now that we know the type.
		val map = bufferedRecord ?: return TargetMessage.Unknown("RECORD for stream=$st with empty body")
		val reader = streamReaders[st]
			?: throw IllegalStateException("Record message received before Schema is defined for stream=$st")
		val json = objectMapper.writeValueAsString(map)
		val synthesized = objectMapper.factory.createParser(json).apply { codec = objectMapper }
		synthesized.nextToken()
		return TargetMessage.TypedRecord(st, reader.read(synthesized))
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

	/** Build + register the [StreamReader] now so subsequent RECORDs on this stream can stream-parse. */
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
