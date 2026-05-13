package com.biron.singerTargetClickhouse

import com.fasterxml.jackson.module.kotlin.jsonMapper
import com.fasterxml.jackson.module.kotlin.kotlinModule
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.datatest.withData
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain

class StreamReaderTest : ShouldSpec({

	val mapper = jsonMapper { addModule(kotlinModule()) }

	fun readJson(meta: SourceMeta, json: String, translateValues: Boolean = true): RecordRow {
		val reader = StreamReader.from(meta, translateValues)
		mapper.factory.createParser(json).apply { codec = mapper }.use { p ->
			p.nextToken()
			return reader.read(p)
		}
	}

	fun objectMetaWithSimpleColumn(prop: String, schemaType: String): SourceMeta = SourceMeta(
		prop = "t",
		sqlTableName = "`t`",
		pkMappings = emptyList(),
		simpleColumnMappings = listOf(
			ColumnMap(
				prop = prop,
				sqlIdentifier = "`$prop`",
				chType = null,
				valueExtractor = { (it as? Map<*, *>)?.get(prop) },
				schemaType = schemaType,
				typeFormat = null,
				nullable = true,
				lowCardinality = false,
				nestedArray = false,
			),
		),
		children = emptyList(),
	)

	context("StringReader") {
		val meta = objectMetaWithSimpleColumn("v", "string")

		should("returns null on JSON null") {
			readJson(meta, """{"v":null}""").toList() shouldBe listOf(null)
		}

		should("reads string values verbatim") {
			readJson(meta, """{"v":"hello"}""").toList() shouldBe listOf("hello")
		}

		should("coerces numbers and booleans via the parser's text representation") {
			readJson(meta, """{"v":42}""").toList() shouldBe listOf("42")
			readJson(meta, """{"v":true}""").toList() shouldBe listOf("true")
			readJson(meta, """{"v":3.14}""").toList() shouldBe listOf("3.14")
		}

		should("stringifies arrays and objects via Jackson's default toString") {
			val arr = readJson(meta, """{"v":[1,2,3]}""").toList().single() as String
			arr shouldContain "1"
			arr shouldContain "2"
			val obj = readJson(meta, """{"v":{"a":1}}""").toList().single() as String
			obj shouldContain "a"
		}
	}

	context("IntegerReader") {
		val meta = objectMetaWithSimpleColumn("v", "integer")

		should("reads integers as Long") {
			readJson(meta, """{"v":42}""").toList() shouldBe listOf(42L)
		}

		should("truncates floats to Long") {
			readJson(meta, """{"v":3.9}""").toList() shouldBe listOf(3L)
		}

		should("parses numeric strings as Long") {
			readJson(meta, """{"v":"123"}""").toList() shouldBe listOf(123L)
			readJson(meta, """{"v":"  77 "}""").toList() shouldBe listOf(77L)
		}

		should("falls back to Double parsing for floating-point strings") {
			readJson(meta, """{"v":"7.9"}""").toList() shouldBe listOf(7L)
		}

		should("returns null for unparseable string") {
			readJson(meta, """{"v":"not-a-number"}""").toList() shouldBe listOf(null)
		}

		should("returns null on JSON null") {
			readJson(meta, """{"v":null}""").toList() shouldBe listOf(null)
		}

		should("returns null on objects and arrays after skipping the body") {
			readJson(meta, """{"v":{"a":1}}""").toList() shouldBe listOf(null)
			readJson(meta, """{"v":[1,2,3]}""").toList() shouldBe listOf(null)
		}

		should("returns null on booleans") {
			readJson(meta, """{"v":true}""").toList() shouldBe listOf(null)
			readJson(meta, """{"v":false}""").toList() shouldBe listOf(null)
		}
	}

	context("NumberReader") {
		val meta = objectMetaWithSimpleColumn("v", "number")

		should("reads doubles directly") {
			readJson(meta, """{"v":3.14}""").toList() shouldBe listOf(3.14)
		}

		should("widens integer JSON numbers to Double") {
			readJson(meta, """{"v":42}""").toList() shouldBe listOf(42.0)
		}

		should("parses numeric strings as Double") {
			readJson(meta, """{"v":"  9.5 "}""").toList() shouldBe listOf(9.5)
		}

		should("returns null for unparseable string") {
			readJson(meta, """{"v":"abc"}""").toList() shouldBe listOf(null)
		}

		should("returns null on JSON null and booleans") {
			readJson(meta, """{"v":null}""").toList() shouldBe listOf(null)
			readJson(meta, """{"v":true}""").toList() shouldBe listOf(null)
		}

		should("returns null and skips on objects and arrays") {
			readJson(meta, """{"v":{"a":1}}""").toList() shouldBe listOf(null)
			readJson(meta, """{"v":[1]}""").toList() shouldBe listOf(null)
		}
	}

	context("BooleanReader") {
		val meta = objectMetaWithSimpleColumn("v", "boolean")

		should("emits 1 for true and 0 for false") {
			readJson(meta, """{"v":true}""").toList() shouldBe listOf(1)
			readJson(meta, """{"v":false}""").toList() shouldBe listOf(0)
		}

		should("emits 1 for the string 'true' and 0 for any other string") {
			readJson(meta, """{"v":"true"}""").toList() shouldBe listOf(1)
			readJson(meta, """{"v":"false"}""").toList() shouldBe listOf(0)
			readJson(meta, """{"v":"yes"}""").toList() shouldBe listOf(0)
		}

		should("emits 1 for integer 1 and 0 otherwise") {
			readJson(meta, """{"v":1}""").toList() shouldBe listOf(1)
			readJson(meta, """{"v":0}""").toList() shouldBe listOf(0)
			readJson(meta, """{"v":2}""").toList() shouldBe listOf(0)
		}

		should("emits 1 for float 1.0 and 0 otherwise") {
			readJson(meta, """{"v":1.0}""").toList() shouldBe listOf(1)
			readJson(meta, """{"v":0.5}""").toList() shouldBe listOf(0)
		}

		should("returns null on JSON null") {
			readJson(meta, """{"v":null}""").toList() shouldBe listOf(null)
		}

		should("emits 0 on objects and arrays after skipping the body") {
			readJson(meta, """{"v":{"a":1}}""").toList() shouldBe listOf(0)
			readJson(meta, """{"v":[1,2]}""").toList() shouldBe listOf(0)
		}
	}

	context("UntypedScalarReader") {
		// translateValues=false → every scalar is read raw, regardless of schemaType.
		val meta = objectMetaWithSimpleColumn("v", "string")

		context("preserves raw JSON types") {
			withData(
				mapOf(
					"integer" to ("""{"v":42}""" to 42),
					"float" to ("""{"v":3.14}""" to 3.14),
					"true" to ("""{"v":true}""" to true),
					"false" to ("""{"v":false}""" to false),
					"string" to ("""{"v":"abc"}""" to "abc"),
					"null" to ("""{"v":null}""" to null),
				),
			) { (json, expected) ->
				readJson(meta, json, translateValues = false).toList() shouldBe listOf(expected)
			}
		}

		should("preserves arrays and objects as their parsed Kotlin shapes") {
			val arr = readJson(meta, """{"v":[1,2]}""", translateValues = false).toList().single()
			arr shouldBe listOf(1, 2)
			val obj = readJson(meta, """{"v":{"a":1}}""", translateValues = false).toList().single()
			obj shouldBe mapOf("a" to 1)
		}
	}

	context("ObjectReader dispatch") {
		val meta = SourceMeta(
			prop = "t",
			sqlTableName = "`t`",
			pkMappings = emptyList(),
			simpleColumnMappings = listOf(
				ColumnMap(
					prop = "id", sqlIdentifier = "`id`", chType = null,
					valueExtractor = { (it as? Map<*, *>)?.get("id") },
					schemaType = "integer", typeFormat = null,
					nullable = false, lowCardinality = false, nestedArray = false,
				),
				ColumnMap(
					prop = "name", sqlIdentifier = "`name`", chType = null,
					valueExtractor = { (it as? Map<*, *>)?.get("name") },
					schemaType = "string", typeFormat = null,
					nullable = true, lowCardinality = false, nestedArray = false,
				),
			),
			children = emptyList(),
		)

		should("leaves all slots null when the record itself is null") {
			readJson(meta, "null").toList() shouldBe listOf(null, null)
		}

		should("skips unknown fields and reads known ones") {
			readJson(meta, """{"unknown":[1,2],"id":7,"name":"bob"}""").toList() shouldBe listOf(7L, "bob")
		}

		should("falls back to skipChildren on a non-object record body") {
			readJson(meta, "42").toList() shouldBe listOf(null, null)
		}
	}

	context("SubtableReader") {
		val meta = SourceMeta(
			prop = "t", sqlTableName = "`t`",
			pkMappings = listOf(
				PkMap(
					prop = "id", sqlIdentifier = "`id`", chType = null,
					valueExtractor = { (it as Map<*, *>)["id"] },
					schemaType = "integer", typeFormat = null,
					nullable = false, lowCardinality = false, nestedArray = false,
					pkType = PKType.CURRENT,
				),
			),
			simpleColumnMappings = emptyList(),
			children = listOf(
				SourceMeta(
					prop = "tags", sqlTableName = "`t__tags`",
					pkMappings = emptyList(),
					simpleColumnMappings = listOf(
						ColumnMap(
							prop = "name", sqlIdentifier = "`name`", chType = null,
							valueExtractor = { (it as? Map<*, *>)?.get("name") },
							schemaType = "string", typeFormat = null,
							nullable = false, lowCardinality = false, nestedArray = false,
						),
					),
					children = emptyList(),
				),
			),
		)

		should("decodes an array of subtable rows") {
			val row = readJson(meta, """{"id":3,"tags":[{"name":"a"},{"name":"b"}]}""")
			row.toList()[0] shouldBe 3L
			@Suppress("UNCHECKED_CAST")
			val sub = row[1] as List<RecordRow>
			sub shouldHaveSize 2
			sub[0].toList() shouldBe listOf("a")
			sub[1].toList() shouldBe listOf("b")
		}

		should("treats a null subtable field as a missing list") {
			val row = readJson(meta, """{"id":1,"tags":null}""")
			row[1] shouldBe null
		}

		should("wraps a non-array subtable value into a singleton list") {
			val row = readJson(meta, """{"id":4,"tags":{"name":"only"}}""")
			@Suppress("UNCHECKED_CAST") val sub = row[1] as List<RecordRow>
			sub shouldHaveSize 1
			sub[0].toList() shouldBe listOf("only")
		}
	}

	context("nested object flattening") {
		// `nested$%€£color` is the flattened path used internally for `nested.color`.
		val meta = SourceMeta(
			prop = "t", sqlTableName = "`t`",
			pkMappings = emptyList(),
			simpleColumnMappings = listOf(
				ColumnMap(
					prop = "nested${NESTED_SUB_OBJECT_SEPARATOR}color",
					sqlIdentifier = "`nested__color`", chType = null,
					valueExtractor = { (it as? Map<*, *>)?.let { m -> (m["nested"] as? Map<*, *>)?.get("color") } },
					schemaType = "string", typeFormat = null,
					nullable = true, lowCardinality = false, nestedArray = false,
				),
			),
			children = emptyList(),
		)

		should("descends into nested objects to find leaf scalars") {
			readJson(meta, """{"nested":{"color":"red"}}""").toList() shouldBe listOf("red")
		}

		should("produces null when the nested object is JSON null") {
			readJson(meta, """{"nested":null}""").toList() shouldBe listOf(null)
		}
	}

	context("StreamReader.from path collision") {
		fun col(prop: String) = ColumnMap(
			prop = prop, sqlIdentifier = "`$prop`", chType = null,
			valueExtractor = { it },
			schemaType = "string", typeFormat = null,
			nullable = true, lowCardinality = false, nestedArray = false,
		)

		should("rejects a leaf-vs-nested collision") {
			val meta = SourceMeta(
				prop = "t", sqlTableName = "`t`",
				pkMappings = emptyList(),
				simpleColumnMappings = listOf(
					col("a"),
					col("a${NESTED_SUB_OBJECT_SEPARATOR}b"),
				),
				children = emptyList(),
			)
			shouldThrow<IllegalStateException> {
				StreamReader.from(meta, translateValues = true)
			}.message shouldContain "column-path collision"
		}

		should("rejects a duplicate leaf at the same depth") {
			val meta = SourceMeta(
				prop = "t", sqlTableName = "`t`",
				pkMappings = emptyList(),
				simpleColumnMappings = listOf(col("a"), col("a")),
				children = emptyList(),
			)
			shouldThrow<IllegalStateException> {
				StreamReader.from(meta, translateValues = true)
			}.message shouldContain "duplicate leaf"
		}
	}

	context("scalar-root stream") {
		// A child whose only column is the unnamed scalar `value`.
		val meta = SourceMeta(
			prop = "t", sqlTableName = "`t`",
			pkMappings = emptyList(),
			simpleColumnMappings = listOf(
				ColumnMap(
					prop = null, sqlIdentifier = "`value`", chType = null,
					valueExtractor = { it },
					schemaType = "integer", typeFormat = null,
					nullable = false, lowCardinality = false, nestedArray = false,
				),
			),
			children = emptyList(),
		)

		should("reads the scalar root directly into slot 0 with translation") {
			readJson(meta, "42").toList() shouldBe listOf(42L)
		}

		should("reads the scalar root raw without translation") {
			readJson(meta, "42", translateValues = false).toList() shouldBe listOf(42)
		}
	}

	context("scalarReaderFor short-circuits on nestedArray columns") {
		// translateValues=true + nestedArray=true → falls through to UntypedScalarReader so the
		// list value is preserved verbatim instead of being coerced through StringReader.
		val meta = SourceMeta(
			prop = "t", sqlTableName = "`t`",
			pkMappings = emptyList(),
			simpleColumnMappings = listOf(
				ColumnMap(
					prop = "tags", sqlIdentifier = "`tags`", chType = "Array(String)",
					valueExtractor = { (it as? Map<*, *>)?.get("tags") },
					schemaType = "string", typeFormat = null,
					nullable = true, lowCardinality = false, nestedArray = true,
				),
			),
			children = emptyList(),
		)

		should("preserves the array as a list when translateValues=true") {
			val row = readJson(meta, """{"tags":["a","b","c"]}""", translateValues = true)
			row.toList() shouldBe listOf(listOf("a", "b", "c"))
		}
	}

	context("StreamReader cleaningColumnSlot") {
		fun pk(prop: String) = PkMap(
			prop = prop, sqlIdentifier = "`$prop`", chType = null,
			valueExtractor = { (it as Map<*, *>)[prop] },
			schemaType = "integer", typeFormat = null,
			nullable = false, lowCardinality = false, nestedArray = false,
			pkType = PKType.CURRENT,
		)

		fun col(prop: String) = ColumnMap(
			prop = prop, sqlIdentifier = "`$prop`", chType = null,
			valueExtractor = { (it as? Map<*, *>)?.get(prop) },
			schemaType = "string", typeFormat = null,
			nullable = true, lowCardinality = false, nestedArray = false,
		)

		should("returns the PK slot when the cleaning column is a current PK") {
			val meta = SourceMeta(
				prop = "t", sqlTableName = "`t`",
				pkMappings = listOf(pk("id"), pk("ts")),
				simpleColumnMappings = listOf(col("name")),
				children = emptyList(),
				cleaningColumn = "ts",
			)
			val reader = StreamReader.from(meta, translateValues = true)
			reader.cleaningColumnSlot shouldBe 1
		}

		should("returns the pkCount + column-index slot when the cleaning column is a simple column") {
			val meta = SourceMeta(
				prop = "t", sqlTableName = "`t`",
				pkMappings = listOf(pk("id")),
				simpleColumnMappings = listOf(col("name"), col("deleted_at")),
				children = emptyList(),
				cleaningColumn = "deleted_at",
			)
			val reader = StreamReader.from(meta, translateValues = true)
			// pkCount=1 + index 1 (deleted_at) = 2
			reader.cleaningColumnSlot shouldBe 2
		}

		should("returns null when the cleaning column is not on the meta") {
			val meta = SourceMeta(
				prop = "t", sqlTableName = "`t`",
				pkMappings = listOf(pk("id")),
				simpleColumnMappings = listOf(col("name")),
				children = emptyList(),
				cleaningColumn = "nope",
			)
			val reader = StreamReader.from(meta, translateValues = true)
			reader.cleaningColumnSlot shouldBe null
		}

		should("returns null when no cleaning column is set") {
			val meta = SourceMeta(
				prop = "t", sqlTableName = "`t`",
				pkMappings = listOf(pk("id")),
				simpleColumnMappings = listOf(col("name")),
				children = emptyList(),
			)
			val reader = StreamReader.from(meta, translateValues = true)
			reader.cleaningColumnSlot shouldBe null
		}
	}
})
