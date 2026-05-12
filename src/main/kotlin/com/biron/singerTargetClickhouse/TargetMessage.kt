package com.biron.singerTargetClickhouse

import com.biron.singer.core.domain.JsonSchema
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.core.StreamReadConstraints
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jsonMapper
import com.fasterxml.jackson.module.kotlin.kotlinModule
import java.io.InputStream

sealed interface TargetMessage {
	val type: String

	/**
	 * [meta] and [reader] are derived from the schema fields and built once by
	 * [TargetMessageParser]. Downstream consumers (DDL, [StreamProcessor]) reuse them
	 * instead of recomputing — see the parser-thread comment on [TargetMessageParser].
	 */
	data class Schema(
		val stream: String,
		val schema: JsonSchema,
		val keyProperties: List<String>,
		val cleanFirst: Boolean = false,
		val cleaningColumn: String? = null,
		val allKeyProperties: SchemaKeyProperties = SchemaKeyProperties.empty,
		val meta: SourceMeta,
		val reader: StreamReader,
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
		val streams: Set<String>,
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
 * Thread-confined to its caller (the producer thread in [StreamPipeline]). Construction
 * is cheap; allocate one per top-level invocation.
 */
class TargetMessageParser(
	private val subtableSeparator: String,
	private val translateValues: Boolean,
) {
	private val streamReaders: MutableMap<String, StreamReader> = HashMap()

	private val objectMapper: ObjectMapper = jsonMapper {
		addModule(kotlinModule())
		disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
	}.also {
		it.factory.setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(Int.MAX_VALUE).build())
	}

	fun createParser(input: InputStream): JsonParser = objectMapper.factory.createParser(input).apply { codec = objectMapper }

	/**
	 * Read the next message from [parser], advancing it past the value. Returns null on EOF.
	 * Does not recover from malformed JSON mid-stream — exceptions propagate.
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
	 * Streaming envelope reader. Walks the outer message in source order. Singer producers
	 * always emit `type` (and `stream` for record-bearing messages) before `record`; we
	 * rely on that ordering to dispatch each `record` body straight into a [StreamReader]
	 * with no intermediate object model. Messages that violate the ordering are rejected.
	 */
	private fun readEnvelope(parser: JsonParser): TargetMessage {
		val acc = EnvelopeAccumulator()
		while (parser.nextToken() != JsonToken.END_OBJECT) {
			val field = parser.currentName()
			parser.nextToken()
			readEnvelopeField(parser, field, acc)
		}
		return acc.toMessage()
	}

	private fun readEnvelopeField(parser: JsonParser, field: String, acc: EnvelopeAccumulator) {
		when (field) {
			"type" -> acc.type = parser.text
			"stream" -> acc.stream = parser.text
			"record" -> readRecordField(parser, acc)
			"value" -> {
				acc.stateSeen = true
				acc.stateValue = parser.readValueAs(Any::class.java)
			}

			"streams" -> acc.streamsList = readStringList(parser)
			"schema" -> acc.schemaValue = parser.readValueAs(Any::class.java)
			"key_properties" -> acc.keyProperties = readStringList(parser)
			"clean_first" -> acc.cleanFirst = parser.currentToken == JsonToken.VALUE_TRUE
			"cleaning_column" -> acc.cleaningColumn =
				if (parser.currentToken == JsonToken.VALUE_NULL) null else parser.text

			"all_key_properties" -> acc.allKeyPropertiesRaw = parser.readValueAs(Any::class.java)
			else -> parser.skipChildren()
		}
	}

	private fun readRecordField(parser: JsonParser, acc: EnvelopeAccumulator) {
		val type = acc.type
		if (type != "RECORD" && type != "DELETED_RECORD") {
			// Either ordering violation (record before type) or a non-record message that
			// happens to carry a `record` field — skip it; toMessage() will surface the
			// right error if the type later turns out to be RECORD/DELETED_RECORD.
			parser.skipChildren()
			return
		}
		val stream = acc.stream
			?: error("Singer $type message must emit 'stream' before 'record'")
		val reader = streamReaders[stream]
			?: error("$type received before Schema is defined for stream=$stream")
		acc.row = reader.read(parser)
	}

	/** Mutable accumulator for one Singer envelope; SRP-isolates field collection from message construction. */
	private inner class EnvelopeAccumulator {
		var type: String? = null
		var stream: String? = null
		var row: RecordRow? = null
		var stateValue: Any? = null
		var stateSeen: Boolean = false
		var streamsList: List<String>? = null
		var schemaValue: Any? = null
		var keyProperties: List<String>? = null
		var cleanFirst: Boolean = false
		var cleaningColumn: String? = null
		var allKeyPropertiesRaw: Any? = null

		val requiredStream: String
			get() = stream ?: error("Singer message of type=${type ?: "?"} requires a [stream] field")

		fun toMessage(): TargetMessage = when (type) {
			"RECORD" -> TargetMessage.Record(requiredStream, requireRow("RECORD"))
			"DELETED_RECORD" -> TargetMessage.DeletedRecord(requiredStream, requireRow("DELETED_RECORD"))
			"SCHEMA" -> buildSchema()
			"STATE" -> TargetMessage.State(value = if (stateSeen) stateValue else null)
			"ACTIVE_STREAMS" -> TargetMessage.ActiveStreams(streams = streamsList.orEmpty().toSet())
			else -> TargetMessage.Unknown("type=${type ?: "null"}")
		}

		private fun requireRow(messageType: String): RecordRow =
			row ?: error("Singer $messageType message must emit 'type' and 'stream' before 'record' (stream=$requiredStream)")

		private fun buildSchema(): TargetMessage.Schema {
			val schema = schemaValue?.let { objectMapper.convertValue(it, JsonSchema::class.java) } ?: JsonSchema()
			val keyProperties = keyProperties.orEmpty()
			val allKeyProperties = (allKeyPropertiesRaw as? Map<*, *>)?.let(::parseKeyProperties) ?: SchemaKeyProperties.empty
			val meta = buildMeta(
				JsonSchemaInspectorContext(
					requiredStream,
					schema,
					keyProperties,
					subtableSeparator,
					cleaningColumn = cleaningColumn,
					allKeyProperties = allKeyProperties
				),
			)
			val reader = StreamReader.from(meta, translateValues)
				.also { streamReaders[requiredStream] = it }
			return TargetMessage.Schema(requiredStream, schema, keyProperties, cleanFirst, cleaningColumn, allKeyProperties, meta, reader)
		}
	}

	private fun parseKeyProperties(node: Map<*, *>): SchemaKeyProperties = SchemaKeyProperties(
		props = (node["props"] as? List<*>)?.map { it.toString() }.orEmpty(),
		children = (node["children"] as? Map<*, *>)?.entries?.associate { (key, value) ->
			key.toString() to parseKeyProperties(value as Map<*, *>)
		}.orEmpty(),
	)

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
