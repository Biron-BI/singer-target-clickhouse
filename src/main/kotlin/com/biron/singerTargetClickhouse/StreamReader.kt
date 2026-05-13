package com.biron.singerTargetClickhouse

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken

/**
 * A flat slot-indexed representation of one decoded record. Layout is determined by the
 * owning [StreamReader]: first the local (CURRENT) PK values, then simple columns, then
 * one slot per array-subtable whose contents are a `List<RecordRow>`.
 *
 * Using a raw array (not a `data class`) keeps allocation to one object plus one backing
 * array per record — the hot-path cost we are trying to minimize.
 */
typealias RecordRow = Array<Any?>

/**
 * Recursively deserializes a JSON record straight into a [RecordRow] using a reader tree
 * derived from [SourceMeta]. Replaces the prior `parser → LinkedHashMap → per-column
 * extractor.get` pipeline: no intermediate map build, no hash lookups per column, slot
 * positions are captured once at SCHEMA time.
 *
 * One instance per stream; thread-confined to the parser thread.
 */
class StreamReader private constructor(
	val width: Int,
	/** Slot of the cleaning column (either a CURRENT pk or a simple column), or null when the stream has none. */
	val cleaningColumnSlot: Int?,
	private val root: FieldReader,
) {
	fun read(parser: JsonParser): RecordRow =
		arrayOfNulls<Any?>(width)
			.apply { root.read(parser, this) }

	sealed interface FieldReader {
		/** Called with [parser] positioned on the value to consume. Writes into [row] as needed. */
		fun read(parser: JsonParser, row: RecordRow)
	}

	/**
	 * Consumes a JSON object, dispatching each field to its child reader (or skipping unknown
	 * fields). `null` is treated as "leave all descendant slots untouched" to match the
	 * prior Map-based semantics.
	 */
	private class ObjectReader(private val children: Map<String, FieldReader>) : FieldReader {
		override fun read(parser: JsonParser, row: RecordRow) {
			when (parser.currentToken) {
				JsonToken.VALUE_NULL -> return
				JsonToken.START_OBJECT -> {
					while (parser.nextToken() != JsonToken.END_OBJECT) {
						val name = parser.currentName()
						parser.nextToken()
						val child = children[name]
						if (child == null) parser.skipChildren() else child.read(parser, row)
					}
				}

				else -> parser.skipChildren()
			}
		}
	}

	/**
	 * Reads an array-subtable: each element is decoded with [element] (the child stream's
	 * reader) into its own [RecordRow], and the resulting list is stored in [slot] for the
	 * parent-side [RecordProcessor] to dispatch. Matches the prior
	 * "null → emptyList; non-list → singleton-list" semantics from the Map path.
	 */
	private class SubtableReader(
		private val slot: Int,
		private val element: StreamReader,
	) : FieldReader {
		override fun read(parser: JsonParser, row: RecordRow) {
			when (parser.currentToken) {
				JsonToken.VALUE_NULL -> return
				JsonToken.START_ARRAY -> {
					val rows = ArrayList<RecordRow>()
					while (parser.nextToken() != JsonToken.END_ARRAY) {
						rows.add(element.read(parser))
					}
					row[slot] = rows
				}

				else -> row[slot] = listOf(element.read(parser))
			}
		}
	}

	/** Reads a scalar value without coercion — JSON type drives the Kotlin type. */
	private class UntypedScalarReader(private val slot: Int) : FieldReader {
		override fun read(parser: JsonParser, row: RecordRow) {
			row[slot] = readUntyped(parser)
		}
	}

	/** schemaType == "string": coerces any non-null value via `toString()`-equivalent. */
	private class StringReader(private val slot: Int) : FieldReader {
		override fun read(parser: JsonParser, row: RecordRow) {
			row[slot] = when (parser.currentToken) {
				JsonToken.VALUE_NULL -> null
				JsonToken.START_OBJECT, JsonToken.START_ARRAY -> parser.readValueAs(Any::class.java)?.toString()
				else -> parser.text
			}
		}
	}

	/** schemaType == "integer": Number→toLong, String→parseLong|toDouble.toLong, else null. */
	private class IntegerReader(private val slot: Int) : FieldReader {
		override fun read(parser: JsonParser, row: RecordRow) {
			row[slot] = when (parser.currentToken) {
				JsonToken.VALUE_NUMBER_INT -> parser.longValue
				JsonToken.VALUE_NUMBER_FLOAT -> parser.doubleValue.toLong()
				JsonToken.VALUE_STRING -> parser.text.trim().let { it.toLongOrNull() ?: it.toDoubleOrNull()?.toLong() }
				JsonToken.VALUE_NULL -> null
				JsonToken.START_OBJECT, JsonToken.START_ARRAY -> {
					parser.skipChildren(); null
				}

				else -> null
			}
		}
	}

	/** schemaType == "number": Number→toDouble, String→parseDouble, else null. */
	private class NumberReader(private val slot: Int) : FieldReader {
		override fun read(parser: JsonParser, row: RecordRow) {
			row[slot] = when (parser.currentToken) {
				JsonToken.VALUE_NUMBER_INT, JsonToken.VALUE_NUMBER_FLOAT -> parser.doubleValue
				JsonToken.VALUE_STRING -> parser.text.trim().toDoubleOrNull()
				JsonToken.VALUE_NULL -> null
				JsonToken.START_OBJECT, JsonToken.START_ARRAY -> {
					parser.skipChildren(); null
				}

				else -> null
			}
		}
	}

	/** schemaType == "boolean": emits 1 / 0 / null. */
	private class BooleanReader(private val slot: Int) : FieldReader {
		override fun read(parser: JsonParser, row: RecordRow) {
			row[slot] = when (parser.currentToken) {
				JsonToken.VALUE_NULL -> null
				JsonToken.VALUE_TRUE -> 1
				JsonToken.VALUE_FALSE -> 0
				JsonToken.VALUE_STRING -> if (parser.text == "true") 1 else 0
				JsonToken.VALUE_NUMBER_INT -> if (parser.longValue == 1L) 1 else 0
				JsonToken.VALUE_NUMBER_FLOAT -> if (parser.doubleValue == 1.0) 1 else 0
				JsonToken.START_OBJECT, JsonToken.START_ARRAY -> {
					parser.skipChildren(); 0
				}

				else -> 0
			}
		}
	}

	/**
	 * Builds the [ObjectReader] tree. Columns whose `prop` uses [NESTED_SUB_OBJECT_SEPARATOR]
	 * contribute to nested object nodes — sibling columns under the same outer key share one
	 * [ObjectReader] at that depth.
	 */
	private class ObjectReaderBuilder {
		// A value that is a `FieldReader` is a terminal leaf; a `MutableMap` is a sub-object still being built.
		private val fields: MutableMap<String, Any> = LinkedHashMap()

		fun addLeaf(path: String, reader: FieldReader) {
			val parts = path.split(NESTED_SUB_OBJECT_SEPARATOR)
			addLeafInternal(parts, 0, reader, fields)
		}

		fun build(): ObjectReader = ObjectReader(buildChildren(fields))

		private fun addLeafInternal(parts: List<String>, index: Int, leaf: FieldReader, into: MutableMap<String, Any>) {
			val part = parts[index]
			if (index == parts.lastIndex) {
				if (part in into) {
					error("column-path collision at '$part': duplicate leaf")
				}
				into[part] = leaf
				return
			}
			val nested: MutableMap<String, Any> = when (val existing = into[part]) {
				null -> LinkedHashMap<String, Any>().also { into[part] = it }
				is MutableMap<*, *> -> @Suppress("UNCHECKED_CAST") (existing as MutableMap<String, Any>)
				else -> error("column-path collision at '$part': a leaf reader already present, cannot nest below it")
			}
			addLeafInternal(parts, index + 1, leaf, nested)
		}

		private fun buildChildren(map: Map<String, Any>): Map<String, FieldReader> =
			map.mapValues { (_, v) ->
				when (v) {
					is FieldReader -> v
					is Map<*, *> -> @Suppress("UNCHECKED_CAST") ObjectReader(buildChildren(v as Map<String, Any>))
					else -> error("unexpected builder node type: ${v::class}")
				}
			}
	}

	companion object {
		/**
		 * Assemble the [StreamReader] for [meta]. Slot layout:
		 * - `[0 .. pkCount)` → CURRENT pk values (in `currentPkMappings` order)
		 * - `[pkCount .. pkCount+columnCount)` → simple column values (in `simpleColumnMappings` order)
		 * - `[pkCount+columnCount .. width)` → array-subtable buckets, each a `List<RecordRow>`
		 *
		 * [translateValues] toggles scalar coercion. When `false`, every scalar column uses
		 * [UntypedScalarReader] so the row carries raw JSON-typed values — matches the legacy
		 * `config.translateValues=false` path.
		 */
		fun from(meta: SourceMeta, translateValues: Boolean): StreamReader {
			val currentPks: List<PkMap> = meta.pkMappings.filter { it.pkType == PKType.CURRENT }
			val pkCount = currentPks.size
			val columnCount = meta.simpleColumnMappings.size
			val subtableCount = meta.children.size
			val width = pkCount + columnCount + subtableCount

			// Scalar-root stream: the single simple column is an unnamed value (e.g. array-of-string subtable),
			// so the record itself IS the scalar — there is no enclosing object.
			if (columnCount == 1 && meta.simpleColumnMappings.single().prop == null && pkCount == 0 && subtableCount == 0) {
				val col = meta.simpleColumnMappings.single()
				return StreamReader(1, null, scalarReaderFor(0, col.schemaType, col.nestedArray, translateValues))
			}

			val builder = ObjectReaderBuilder()
			currentPks.forEachIndexed { i, pk ->
				builder.addLeaf(pk.prop, scalarReaderFor(i, pk.schemaType, pk.nestedArray, translateValues))
			}
			meta.simpleColumnMappings.forEachIndexed { i, col ->
				col.prop?.let {
					builder.addLeaf(it, scalarReaderFor(pkCount + i, col.schemaType, col.nestedArray, translateValues))
				}
			}
			meta.children.forEachIndexed { i, child ->
				builder.addLeaf(child.prop, SubtableReader(pkCount + columnCount + i, from(child, translateValues)))
			}

			return StreamReader(
				width,
				computeCleaningSlot(meta.cleaningColumn, currentPks, meta.simpleColumnMappings, pkCount),
				builder.build(),
			)
		}

		private fun computeCleaningSlot(
			cleaningColumn: String?,
			currentPks: List<PkMap>,
			simpleColumns: List<ColumnMap>,
			pkCount: Int,
		): Int? {
			if (cleaningColumn == null) return null
			val pkIdx = currentPks.indexOfFirst { it.prop == cleaningColumn }
			if (pkIdx >= 0) return pkIdx
			val colIdx = simpleColumns.indexOfFirst { it.prop == cleaningColumn }
			return if (colIdx >= 0) pkCount + colIdx else null
		}

		private fun scalarReaderFor(slot: Int, schemaType: String?, nestedArray: Boolean, translateValues: Boolean): FieldReader {
			if (!translateValues || nestedArray) return UntypedScalarReader(slot)
			return when (schemaType) {
				"string" -> StringReader(slot)
				"integer" -> IntegerReader(slot)
				"number" -> NumberReader(slot)
				"boolean" -> BooleanReader(slot)
				else -> UntypedScalarReader(slot)
			}
		}

		private fun readUntyped(parser: JsonParser): Any? = when (parser.currentToken) {
			JsonToken.VALUE_NULL -> null
			JsonToken.VALUE_STRING -> parser.text
			JsonToken.VALUE_NUMBER_INT -> parser.numberValue
			JsonToken.VALUE_NUMBER_FLOAT -> parser.doubleValue
			JsonToken.VALUE_TRUE -> true
			JsonToken.VALUE_FALSE -> false
			JsonToken.START_OBJECT, JsonToken.START_ARRAY -> parser.readValueAs(Any::class.java)
			else -> null
		}
	}
}
