package com.biron.singerTargetClickhouse

import com.biron.singer.core.domain.JsonSchema
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.datatest.withData
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain

class JsonSchemaInspectorTest : ShouldSpec({

	val simpleSchema = JsonSchema(
		type = listOf("null", "object"),
		properties = mapOf(
			"author_id" to JsonSchema(type = listOf("null", "string")),
			"id" to JsonSchema(type = listOf("null", "integer")),
			"created_at" to JsonSchema(type = listOf("string"), format = "date-time"),
			"ticket_id" to JsonSchema(type = listOf("null", "integer")),
		),
	)

	val nestedObjectSchema = JsonSchema(
		type = listOf("null", "object"),
		properties = mapOf(
			"id" to JsonSchema(type = listOf("null", "integer")),
			"nested" to JsonSchema(
				type = listOf("null", "object"),
				properties = mapOf("color" to JsonSchema(type = listOf("string"))),
			),
		),
	)

	val arrayScalarSchema = JsonSchema(
		type = listOf("null", "object"),
		properties = mapOf(
			"collaborator_ids" to JsonSchema(
				type = listOf("null", "array"),
				items = JsonSchema(type = listOf("null", "integer")),
			),
			"id" to JsonSchema(type = listOf("null", "integer")),
		),
	)

	val arrayObjectSchema = JsonSchema(
		type = listOf("null", "object"),
		properties = mapOf(
			"custom_fields" to JsonSchema(
				type = listOf("null", "array"),
				items = JsonSchema(
					type = listOf("null", "object"),
					properties = mapOf(
						"field" to JsonSchema(type = listOf("null", "integer")),
						"value" to JsonSchema(), // {} → no type; will be ignored
					),
				),
			),
			"id" to JsonSchema(type = listOf("null", "integer")),
		),
	)

	val nestedObjectWithArraysSchema = JsonSchema(
		type = listOf("null", "object"),
		properties = mapOf(
			"id" to JsonSchema(type = listOf("null", "integer")),
			"nested" to JsonSchema(
				type = listOf("null", "object"),
				properties = mapOf(
					"color" to JsonSchema(type = listOf("string")),
					"tags" to JsonSchema(
						type = listOf("array"),
						items = JsonSchema(
							type = listOf("object"),
							properties = mapOf("value" to JsonSchema(type = listOf("integer"))),
						),
					),
				),
			),
		),
	)

	val deepNestedArrayObjectSchema = JsonSchema(
		type = listOf("object"),
		properties = mapOf(
			"id" to JsonSchema(type = listOf("integer")),
			"bill_fields" to JsonSchema(
				type = listOf("array"),
				items = JsonSchema(
					type = listOf("object"),
					properties = mapOf(
						"bill_id" to JsonSchema(type = listOf("number")),
						"john_fields" to JsonSchema(
							type = listOf("array"),
							items = JsonSchema(
								type = listOf("object"),
								properties = mapOf(
									"john_id" to JsonSchema(type = listOf("number")),
									"name" to JsonSchema(type = listOf("string")),
									"jack_fields" to JsonSchema(
										type = listOf("array"),
										items = JsonSchema(
											type = listOf("object"),
											properties = mapOf("jack_value" to JsonSchema(type = listOf("number"))),
										),
									),
								),
							),
						),
					),
				),
			),
		),
	)

	val nestedValueArraySchema = JsonSchema(
		type = listOf("null", "object"),
		properties = mapOf(
			"id" to JsonSchema(type = listOf("string")),
			"events" to JsonSchema(
				type = listOf("null", "array"),
				items = JsonSchema(
					type = listOf("null", "object"),
					properties = mapOf(
						"previous_value" to JsonSchema(
							type = listOf("null", "array", "string"),
							items = JsonSchema(type = listOf("null", "string")),
						),
					),
				),
			),
		),
	)

	context("getSimpleColumnSqlType") {
		val ctx = JsonSchemaInspectorContext("audits", simpleSchema, emptyList())

		should("handles simple stream") {
			getSimpleColumnSqlType(ctx, JsonSchema(type = listOf("null", "integer"))) shouldBe "Int64"
		}

		context("maps string formats to ClickHouse types") {
			withData(
				mapOf(
					"no format" to (JsonSchema(type = listOf("string")) to "String"),
					"date" to (JsonSchema(type = listOf("string"), format = "date") to "Date"),
					"x-excel-date" to (JsonSchema(type = listOf("string"), format = "x-excel-date") to "Date"),
					"date-time" to (JsonSchema(type = listOf("string"), format = "date-time") to "DateTime"),
					"date-time64" to (JsonSchema(type = listOf("string"), format = "date-time64") to "DateTime64"),
					"uuid" to (JsonSchema(type = listOf("string"), format = "uuid") to "UUID"),
					"unknown-format falls back to String" to (JsonSchema(type = listOf("string"), format = "unknown-format") to "String"),
				),
			) { (schema, expected) -> getSimpleColumnSqlType(ctx, schema) shouldBe expected }
		}

		context("maps integer formats to ClickHouse types") {
			withData(
				mapOf(
					"no format" to (JsonSchema(type = listOf("integer")) to "Int64"),
					"int128" to (JsonSchema(type = listOf("integer"), format = "int128") to "Int128"),
					"int64" to (JsonSchema(type = listOf("integer"), format = "int64") to "Int64"),
					"int32" to (JsonSchema(type = listOf("integer"), format = "int32") to "Int32"),
					"int16" to (JsonSchema(type = listOf("integer"), format = "int16") to "Int16"),
					"int8" to (JsonSchema(type = listOf("integer"), format = "int8") to "Int8"),
				),
			) { (schema, expected) -> getSimpleColumnSqlType(ctx, schema) shouldBe expected }
		}

		should("throws on unsupported integer format") {
			shouldThrow<IllegalStateException> {
				getSimpleColumnSqlType(ctx, JsonSchema(type = listOf("integer"), format = "boom"))
			}.message shouldContain "unsupported integer format"
		}

		context("maps number formats to ClickHouse types") {
			withData(
				mapOf(
					"no format defaults to Decimal(16, 2)" to (JsonSchema(type = listOf("number")) to "Decimal(16, 2)"),
					"explicit precision/decimals" to (JsonSchema(type = listOf("number"), precision = 30, decimals = 6) to "Decimal(30, 6)"),
					"float64" to (JsonSchema(type = listOf("number"), format = "float64") to "Float64"),
					"float32" to (JsonSchema(type = listOf("number"), format = "float32") to "Float32"),
				),
			) { (schema, expected) -> getSimpleColumnSqlType(ctx, schema) shouldBe expected }
		}

		should("throws on unsupported number format") {
			shouldThrow<IllegalStateException> {
				getSimpleColumnSqlType(ctx, JsonSchema(type = listOf("number"), format = "boom"))
			}.message shouldContain "unsupported number format"
		}

		should("maps boolean to UInt8 and rejects unknown formats") {
			getSimpleColumnSqlType(ctx, JsonSchema(type = listOf("boolean"))) shouldBe "UInt8"
			shouldThrow<IllegalStateException> {
				getSimpleColumnSqlType(ctx, JsonSchema(type = listOf("boolean"), format = "boom"))
			}.message shouldContain "[boom]"
		}

		should("returns null on unsupported top-level type") {
			getSimpleColumnSqlType(ctx, JsonSchema(type = listOf("array"))) shouldBe null
			getSimpleColumnSqlType(ctx, JsonSchema(type = emptyList())) shouldBe null
		}

		// TS treats `!format` as "no format" — empty string falls back to the default branch
		// rather than the unsupported-format throw. Mirror that for TS-version parity.
		should("treats empty format string as no format (TS `!format` parity)") {
			getSimpleColumnSqlType(ctx, JsonSchema(type = listOf("integer"), format = "")) shouldBe "Int64"
			getSimpleColumnSqlType(ctx, JsonSchema(type = listOf("number"), format = "")) shouldBe "Decimal(16, 2)"
			getSimpleColumnSqlType(ctx, JsonSchema(type = listOf("boolean"), format = "")) shouldBe "UInt8"
		}

		// TS-version uses `precision || 16` / `decimals || 2`, so 0 (falsy) falls back to the default.
		should("treats precision/decimals = 0 as missing for Decimal default (TS `||` parity)") {
			getSimpleColumnSqlType(ctx, JsonSchema(type = listOf("number"), precision = 0, decimals = 0)) shouldBe "Decimal(16, 2)"
		}

		should("includes nested ancestry alias in error messages") {
			val parent = JsonSchemaInspectorContext("root", simpleSchema, listOf("id"))
			val child = JsonSchemaInspectorContext("audits", simpleSchema, listOf("id"), parentCtx = parent, level = 1)
			val msg = shouldThrow<IllegalStateException> {
				getSimpleColumnSqlType(child, JsonSchema(type = listOf("integer"), format = "boom"), key = "ts")
			}.message!!
			msg shouldContain "root.audits"
			msg shouldContain "ts"
			msg shouldContain "[boom]"
		}
	}

	context("buildMeta") {
		should("handles simple schema") {
			val meta = buildMeta(JsonSchemaInspectorContext("audits", simpleSchema, listOf("id")))
			meta.sqlTableName shouldBe "`audits`"
			meta.pkMappings shouldHaveSize 1
			meta.pkMappings[0].chType shouldBe "Int64"
			meta.simpleColumnMappings shouldHaveSize 3
			meta.simpleColumnMappings.find { it.prop == "created_at" }?.nullable shouldBe false
		}

		should("handles array scalar") {
			val meta = buildMeta(JsonSchemaInspectorContext("audits", arrayScalarSchema, listOf("id")))
			meta.children shouldHaveSize 1

			val child = meta.children[0]
			child.sqlTableName shouldBe "`audits__collaborator_ids`"
			child.pkMappings.map { it.prop to it.chType } shouldBe listOf(
				"id" to "Int64",
				"_level_0_index" to "Int32",
			)
			child.pkMappings.last().nullable shouldBe false
		}

		should("handles nested object (flattened)") {
			val meta = buildMeta(JsonSchemaInspectorContext("audits", nestedObjectSchema, listOf("id")))
			meta.children shouldHaveSize 0
			meta.simpleColumnMappings shouldHaveSize 1
			meta.pkMappings shouldHaveSize 1
			meta.simpleColumnMappings[0].sqlIdentifier shouldBe "`nested__color`"
			meta.simpleColumnMappings[0].chType shouldBe "String"
		}

		should("handles array of nested object") {
			val meta = buildMeta(JsonSchemaInspectorContext("audits", arrayObjectSchema, listOf("id")))
			val child = meta.children[0]
			child.sqlTableName shouldBe "`audits__custom_fields`"
			child.simpleColumnMappings.map { it.sqlIdentifier } shouldBe listOf("`field`")
			child.pkMappings.map { it.sqlIdentifier } shouldBe listOf("`_root_id`", "`_level_0_index`")
		}

		should("handles array of nested object with children PK") {
			val allKeyProps = SchemaKeyProperties(
				props = listOf("id"),
				children = mapOf("custom_fields" to SchemaKeyProperties(emptyList(), emptyMap())),
			)
			val meta = buildMeta(
				JsonSchemaInspectorContext(
					alias = "audits",
					schema = arrayObjectSchema,
					keyProperties = listOf("id"),
					allKeyProperties = allKeyProps,
				),
			)

			val child = meta.children[0]
			child.sqlTableName shouldBe "`audits__custom_fields`"
			child.simpleColumnMappings.map { it.sqlIdentifier } shouldBe listOf("`field`")
			child.pkMappings.map { it.sqlIdentifier } shouldBe listOf(
				"`_root_id`", "`_parent_id`", "`_level_0_index`",
			)
		}

		should("handles deep nested array of nested object with children PK") {
			val allKeyProperties = SchemaKeyProperties(
				props = listOf("id"),
				children = mapOf(
					"bill_fields" to SchemaKeyProperties(
						props = listOf("bill_id"),
						children = mapOf(
							"john_fields" to SchemaKeyProperties(
								props = listOf("john_id"),
								children = emptyMap(),
							),
						),
					),
				),
			)

			val meta = buildMeta(
				JsonSchemaInspectorContext(
					alias = "audits",
					schema = deepNestedArrayObjectSchema,
					keyProperties = listOf("id"),
					allKeyProperties = allKeyProperties,
				),
			)

			val billFields = meta.children[0]
			billFields.sqlTableName shouldBe "`audits__bill_fields`"
			billFields.pkMappings.map { it.sqlIdentifier } shouldBe listOf(
				"`_root_id`", "`_parent_id`", "`bill_id`", "`_level_0_index`",
			)

			val johnFields = billFields.children[0]
			johnFields.sqlTableName shouldBe "`audits__bill_fields__john_fields`"
			johnFields.pkMappings.map { it.sqlIdentifier } shouldBe listOf(
				"`_root_id`", "`_parent_bill_id`", "`john_id`", "`_level_0_index`", "`_level_1_index`",
			)

			// PK should not be in simple columns
			johnFields.simpleColumnMappings.find { it.prop == "john_id" } shouldBe null
			johnFields.simpleColumnMappings.find { it.prop == "name" }.shouldNotBeNull()

			val jackFields = johnFields.children[0]
			jackFields.sqlTableName shouldBe "`audits__bill_fields__john_fields__jack_fields`"
			jackFields.pkMappings.map { it.sqlIdentifier } shouldBe listOf(
				"`_root_id`", "`_parent_john_id`", "`_level_0_index`", "`_level_1_index`", "`_level_2_index`",
			)
		}

		should("handles nested object with arrays") {
			val meta = buildMeta(JsonSchemaInspectorContext("audits", nestedObjectWithArraysSchema, listOf("id")))
			meta.children shouldHaveSize 1
			meta.children[0].sqlTableName shouldBe "`audits__nested__tags`"
			meta.children[0].simpleColumnMappings shouldHaveSize 1
			meta.pkMappings[0].valueExtractor(mapOf("id" to 3)) shouldBe 3
			meta.simpleColumnMappings[0].valueExtractor(mapOf("nested" to mapOf("color" to "blue"))) shouldBe "blue"
			meta.children[0].simpleColumnMappings[0].valueExtractor(mapOf("value" to 10)) shouldBe 10
		}

		should("handles nested value array schema") {
			val meta = buildMeta(JsonSchemaInspectorContext("audits", nestedValueArraySchema, listOf("id")))

			meta.prop shouldBe "audits"
			meta.sqlTableName shouldBe "`audits`"
			meta.pkMappings shouldHaveSize 1
			meta.pkMappings[0].prop shouldBe "id"
			meta.pkMappings[0].chType shouldBe "String"
			meta.pkMappings[0].pkType shouldBe PKType.CURRENT
			meta.simpleColumnMappings shouldBe emptyList()

			val events = meta.children[0]
			events.prop shouldBe "events"
			events.sqlTableName shouldBe "`audits__events`"
			events.pkMappings.map { it.sqlIdentifier to it.pkType } shouldBe listOf(
				"`_root_id`" to PKType.ROOT,
				"`_level_0_index`" to PKType.LEVEL,
			)

			val prevValue = events.children[0]
			prevValue.prop shouldBe "previous_value"
			prevValue.sqlTableName shouldBe "`audits__events__previous_value`"
			prevValue.pkMappings.map { it.sqlIdentifier } shouldBe listOf(
				"`_root_id`", "`_level_0_index`", "`_level_1_index`",
			)

			prevValue.simpleColumnMappings shouldHaveSize 1
			prevValue.simpleColumnMappings[0].sqlIdentifier shouldBe "`value`"
			prevValue.simpleColumnMappings[0].chType shouldBe "String"
			prevValue.simpleColumnMappings[0].nullable shouldBe true
			prevValue.simpleColumnMappings[0].valueExtractor("tartempion") shouldBe "tartempion"
		}

		should("throws when an array child is declared without a root key property") {
			val schema = JsonSchema(
				type = listOf("object"),
				properties = mapOf(
					"id" to JsonSchema(type = listOf("integer")),
					"children" to JsonSchema(
						type = listOf("array"),
						items = JsonSchema(type = listOf("object"), properties = mapOf("x" to JsonSchema(type = listOf("integer")))),
					),
				),
			)
			shouldThrow<IllegalStateException> {
				buildMeta(JsonSchemaInspectorContext("audits", schema, emptyList()))
			}.message shouldContain "array child with no root key properties"
		}

		should("skips properties with an empty (`{}`) definition with a warning") {
			val schema = JsonSchema(
				type = listOf("object"),
				properties = mapOf(
					"id" to JsonSchema(type = listOf("integer")),
					"empty" to JsonSchema(),
				),
			)
			val meta = buildMeta(JsonSchemaInspectorContext("audits", schema, listOf("id")))
			meta.simpleColumnMappings.map { it.prop } shouldBe emptyList()
			meta.pkMappings.single().prop shouldBe "id"
		}

		should("skips nested empty (`{}`) property definitions inside a nested object") {
			val schema = JsonSchema(
				type = listOf("object"),
				properties = mapOf(
					"id" to JsonSchema(type = listOf("integer")),
					"meta" to JsonSchema(
						type = listOf("object"),
						properties = mapOf(
							"present" to JsonSchema(type = listOf("string")),
							"missing" to JsonSchema(),
						),
					),
				),
			)
			val meta = buildMeta(JsonSchemaInspectorContext("audits", schema, listOf("id")))
			meta.simpleColumnMappings.map { it.sqlIdentifier } shouldBe listOf("`meta__present`")
		}

		should("warns and skips columns whose type can't be resolved") {
			val schema = JsonSchema(
				type = listOf("object"),
				properties = mapOf(
					"id" to JsonSchema(type = listOf("integer")),
					"weird" to JsonSchema(type = listOf("totally-unknown")),
				),
			)
			val meta = buildMeta(JsonSchemaInspectorContext("audits", schema, listOf("id")))
			meta.simpleColumnMappings shouldBe emptyList()
		}

		should("treats array+format='nested' as a nested-array scalar column") {
			val schema = JsonSchema(
				type = listOf("object"),
				properties = mapOf(
					"id" to JsonSchema(type = listOf("integer")),
					"tags" to JsonSchema(
						type = listOf("array"),
						format = "nested",
						items = JsonSchema(type = listOf("string")),
					),
				),
			)
			val meta = buildMeta(JsonSchemaInspectorContext("audits", schema, listOf("id")))
			meta.children shouldBe emptyList()
			val tags = meta.simpleColumnMappings.single { it.prop == "tags" }
			tags.nestedArray shouldBe true
			tags.chType shouldBe "String"
		}

		should("preserves lowCardinality flag from schema") {
			val schema = JsonSchema(
				type = listOf("object"),
				properties = mapOf(
					"id" to JsonSchema(type = listOf("integer")),
					"status" to JsonSchema(type = listOf("string"), lowCardinality = true),
				),
			)
			val meta = buildMeta(JsonSchemaInspectorContext("audits", schema, listOf("id")))
			meta.simpleColumnMappings.single { it.prop == "status" }.lowCardinality shouldBe true
		}

		should("returns no columns for a scalar root with empty type list") {
			val meta = buildMeta(JsonSchemaInspectorContext("audits", JsonSchema(type = emptyList()), emptyList()))
			meta.simpleColumnMappings shouldBe emptyList()
			meta.pkMappings shouldBe emptyList()
		}

		should("builds a single-column scalar root from a primitive schema") {
			val meta = buildMeta(JsonSchemaInspectorContext("audits", JsonSchema(type = listOf("string")), emptyList()))
			meta.simpleColumnMappings.single().sqlIdentifier shouldBe "`value`"
			meta.simpleColumnMappings.single().chType shouldBe "String"
		}

		should("composes nested PARENT pk columns when allKeyProperties is set on parent only") {
			// `_parent_X` columns are emitted only when the parent has non-empty all_key_properties.
			val allKeyProps = SchemaKeyProperties(
				props = listOf("id"),
				children = mapOf("custom_fields" to SchemaKeyProperties(props = listOf("field"), children = emptyMap())),
			)
			val meta = buildMeta(
				JsonSchemaInspectorContext(
					alias = "audits",
					schema = arrayObjectSchema,
					keyProperties = listOf("id"),
					allKeyProperties = allKeyProps,
				),
			)
			val child = meta.children.single()
			child.pkMappings.map { it.pkType } shouldBe listOf(
				PKType.ROOT, PKType.PARENT, PKType.CURRENT, PKType.LEVEL,
			)
		}
	}

	context("escapeIdentifier nested ancestry") {
		should("makes truncated identifier deterministic for the same input") {
			val long = "a".repeat(100)
			escapeIdentifier(long) shouldBe escapeIdentifier(long)
		}

		should("uses the configured subtable separator when expanding nested identifiers") {
			escapeIdentifier("x${NESTED_SUB_OBJECT_SEPARATOR}y", subtableSeparator = "::") shouldBe "`x::y`"
		}
	}

	context("formatRootPKColumn") {
		should("prefixes the property name with _root_") {
			formatRootPKColumn("id") shouldBe "_root_id"
		}
	}

	context("JsonSchemaInspectorContext") {
		should("isRoot is true when parentCtx is null") {
			JsonSchemaInspectorContext("audits", simpleSchema, listOf("id")).isRoot() shouldBe true
		}

		should("isRoot is false when a parent context is set") {
			val parent = JsonSchemaInspectorContext("root", simpleSchema, listOf("id"))
			val child = JsonSchemaInspectorContext("audits", simpleSchema, listOf("id"), parentCtx = parent)
			child.isRoot() shouldBe false
		}

		should("rootCtx walks the parent chain") {
			val grand = JsonSchemaInspectorContext("a", simpleSchema, emptyList())
			val parent = JsonSchemaInspectorContext("b", simpleSchema, emptyList(), parentCtx = grand)
			val child = JsonSchemaInspectorContext("c", simpleSchema, emptyList(), parentCtx = parent)
			child.rootCtx shouldBe grand
		}
	}

	context("createSubTable / nested object edges") {
		should("falls back to a string-typed item schema when array items is missing") {
			val schema = JsonSchema(
				type = listOf("object"),
				properties = mapOf(
					"id" to JsonSchema(type = listOf("integer")),
					"things" to JsonSchema(type = listOf("array")),
				),
			)
			val meta = buildMeta(JsonSchemaInspectorContext("root", schema, listOf("id")))
			val child = meta.children.single { it.prop == "things" }
			val valueCol = child.simpleColumnMappings.single()
			valueCol.sqlIdentifier shouldBe "`value`"
			valueCol.chType shouldBe "String"
		}

		should("flattens an object whose properties map is null without crashing") {
			val schema = JsonSchema(
				type = listOf("object"),
				properties = mapOf(
					"id" to JsonSchema(type = listOf("integer")),
					"meta" to JsonSchema(type = listOf("object")),
				),
			)
			val meta = buildMeta(JsonSchemaInspectorContext("root", schema, listOf("id")))
			meta.simpleColumnMappings.map { it.prop } shouldBe emptyList()
		}
	}

	context("nested array+nested format combinations") {
		should("preserves nested-array typing while traversing PK derivation") {
			val schema = JsonSchema(
				type = listOf("object"),
				properties = mapOf(
					"id" to JsonSchema(type = listOf("integer")),
					"floats" to JsonSchema(
						type = listOf("array"),
						format = "nested",
						items = JsonSchema(type = listOf("number"), format = "float64"),
					),
				),
			)
			val meta = buildMeta(JsonSchemaInspectorContext("root", schema, listOf("id")))
			val floats = meta.simpleColumnMappings.single { it.prop == "floats" }
			floats.nestedArray shouldBe true
			floats.chType shouldBe "Float64"
		}
	}

	context("escapeIdentifier") {
		should("wraps short identifiers in backticks") {
			escapeIdentifier("id") shouldBe "`id`"
		}
		should("replaces nested separator with subtable separator") {
			escapeIdentifier("a${NESTED_SUB_OBJECT_SEPARATOR}b") shouldBe "`a__b`"
		}
		should("truncates and hashes long identifiers") {
			val long = "x".repeat(80)
			val escaped = escapeIdentifier(long)
			escaped.length shouldBe 66 // backticks + 64-char body
			escaped.startsWith("`") shouldBe true
			escaped.endsWith("`") shouldBe true
		}
	}
})
