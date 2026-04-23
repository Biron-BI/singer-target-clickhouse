package com.biron.singerTargetClickhouse

import com.biron.singer.core.domain.JsonSchema
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.DescribeSpec
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe

class JsonSchemaInspectorTest : DescribeSpec({

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

	describe("getSimpleColumnSqlType") {
		it("handles simple stream") {
			getSimpleColumnSqlType(
				JsonSchemaInspectorContext("audits", simpleSchema, emptyList()),
				JsonSchema(type = listOf("null", "integer")),
			) shouldBe "Int64"
		}
	}

	describe("buildMeta") {
		it("handles simple schema") {
			val meta = buildMeta(JsonSchemaInspectorContext("audits", simpleSchema, listOf("id")))
			meta.sqlTableName shouldBe "`audits`"
			meta.pkMappings shouldHaveSize 1
			meta.pkMappings[0].chType shouldBe "Int64"
			meta.simpleColumnMappings shouldHaveSize 3
			meta.simpleColumnMappings.find { it.prop == "created_at" }?.nullable shouldBe false
		}

		it("handles array scalar") {
			val meta = buildMeta(JsonSchemaInspectorContext("audits", arrayScalarSchema, listOf("id")))
			meta.children shouldHaveSize 1

			val child = meta.children[0]
			child.sqlTableName shouldBe "`audits__collaborator_ids`"
			child.pkMappings shouldHaveSize 2
			child.pkMappings[0].prop shouldBe "id"
			child.pkMappings[0].chType shouldBe "Int64"
			child.pkMappings[1].prop shouldBe "_level_0_index"
			child.pkMappings[1].chType shouldBe "Int32"
			child.pkMappings[1].nullable shouldBe false
		}

		it("handles nested object (flattened)") {
			val meta = buildMeta(JsonSchemaInspectorContext("audits", nestedObjectSchema, listOf("id")))
			meta.children shouldHaveSize 0
			meta.simpleColumnMappings shouldHaveSize 1
			meta.pkMappings shouldHaveSize 1
			meta.simpleColumnMappings[0].sqlIdentifier shouldBe "`nested__color`"
			meta.simpleColumnMappings[0].chType shouldBe "String"
		}

		it("handles array of nested object") {
			val meta = buildMeta(JsonSchemaInspectorContext("audits", arrayObjectSchema, listOf("id")))
			val child = meta.children[0]
			child.sqlTableName shouldBe "`audits__custom_fields`"
			child.simpleColumnMappings shouldHaveSize 1
			child.simpleColumnMappings[0].sqlIdentifier shouldBe "`field`"
			child.pkMappings shouldHaveSize 2
			child.pkMappings[0].sqlIdentifier shouldBe "`_root_id`"
			child.pkMappings[1].sqlIdentifier shouldBe "`_level_0_index`"
		}

		it("handles array of nested object with children PK") {
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
			child.simpleColumnMappings shouldHaveSize 1
			child.simpleColumnMappings[0].sqlIdentifier shouldBe "`field`"
			child.pkMappings shouldHaveSize 3
			child.pkMappings[0].sqlIdentifier shouldBe "`_root_id`"
			child.pkMappings[1].sqlIdentifier shouldBe "`_parent_id`"
			child.pkMappings[2].sqlIdentifier shouldBe "`_level_0_index`"
		}

		it("handles deep nested array of nested object with children PK") {
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
			billFields.pkMappings[0].sqlIdentifier shouldBe "`_root_id`"
			billFields.pkMappings[1].sqlIdentifier shouldBe "`_parent_id`"
			billFields.pkMappings[2].sqlIdentifier shouldBe "`bill_id`"
			billFields.pkMappings[3].sqlIdentifier shouldBe "`_level_0_index`"

			val johnFields = billFields.children[0]
			johnFields.sqlTableName shouldBe "`audits__bill_fields__john_fields`"
			johnFields.pkMappings[0].sqlIdentifier shouldBe "`_root_id`"
			johnFields.pkMappings[1].sqlIdentifier shouldBe "`_parent_bill_id`"
			johnFields.pkMappings[2].sqlIdentifier shouldBe "`john_id`"
			johnFields.pkMappings[3].sqlIdentifier shouldBe "`_level_0_index`"
			johnFields.pkMappings[4].sqlIdentifier shouldBe "`_level_1_index`"

			// PK should not be in simple columns
			johnFields.simpleColumnMappings.find { it.prop == "john_id" } shouldBe null
			johnFields.simpleColumnMappings.find { it.prop == "name" }.shouldNotBeNull()

			val jackFields = johnFields.children[0]
			jackFields.sqlTableName shouldBe "`audits__bill_fields__john_fields__jack_fields`"
			jackFields.pkMappings[0].sqlIdentifier shouldBe "`_root_id`"
			jackFields.pkMappings[1].sqlIdentifier shouldBe "`_parent_john_id`"
			jackFields.pkMappings[2].sqlIdentifier shouldBe "`_level_0_index`"
			jackFields.pkMappings[3].sqlIdentifier shouldBe "`_level_1_index`"
			jackFields.pkMappings[4].sqlIdentifier shouldBe "`_level_2_index`"
		}

		it("handles nested object with arrays") {
			val meta = buildMeta(JsonSchemaInspectorContext("audits", nestedObjectWithArraysSchema, listOf("id")))
			meta.children shouldHaveSize 1
			meta.children[0].sqlTableName shouldBe "`audits__nested__tags`"
			meta.children[0].simpleColumnMappings shouldHaveSize 1
			meta.pkMappings[0].valueExtractor(mapOf("id" to 3)) shouldBe 3
			meta.simpleColumnMappings[0].valueExtractor(mapOf("nested" to mapOf("color" to "blue"))) shouldBe "blue"
			meta.children[0].simpleColumnMappings[0].valueExtractor(mapOf("value" to 10)) shouldBe 10
		}

		it("handles nested value array schema") {
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
			events.pkMappings shouldHaveSize 2
			events.pkMappings[0].sqlIdentifier shouldBe "`_root_id`"
			events.pkMappings[0].pkType shouldBe PKType.ROOT
			events.pkMappings[1].pkType shouldBe PKType.LEVEL

			val prevValue = events.children[0]
			prevValue.prop shouldBe "previous_value"
			prevValue.sqlTableName shouldBe "`audits__events__previous_value`"
			prevValue.pkMappings shouldHaveSize 3
			prevValue.pkMappings[0].sqlIdentifier shouldBe "`_root_id`"
			prevValue.pkMappings[1].sqlIdentifier shouldBe "`_level_0_index`"
			prevValue.pkMappings[2].sqlIdentifier shouldBe "`_level_1_index`"

			prevValue.simpleColumnMappings shouldHaveSize 1
			prevValue.simpleColumnMappings[0].sqlIdentifier shouldBe "`value`"
			prevValue.simpleColumnMappings[0].chType shouldBe "String"
			prevValue.simpleColumnMappings[0].nullable shouldBe true
			prevValue.simpleColumnMappings[0].valueExtractor("tartempion") shouldBe "tartempion"
		}

		it("throws when an array child is declared without a root key property") {
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
			}
		}
	}

	describe("escapeIdentifier") {
		it("wraps short identifiers in backticks") {
			escapeIdentifier("id") shouldBe "`id`"
		}
		it("replaces nested separator with subtable separator") {
			escapeIdentifier("a${NESTED_SUB_OBJECT_SEPARATOR}b") shouldBe "`a__b`"
		}
		it("truncates and hashes long identifiers") {
			val long = "x".repeat(80)
			val escaped = escapeIdentifier(long)
			escaped.length shouldBe 66 // backticks + 64-char body
			escaped.startsWith("`") shouldBe true
			escaped.endsWith("`") shouldBe true
		}
	}
})
