package com.biron.singerTargetClickhouse

import com.biron.singer.core.domain.JsonSchema
import com.fasterxml.jackson.module.kotlin.jsonMapper
import com.fasterxml.jackson.module.kotlin.kotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe

class JsonSchemaDeserializationTest : ShouldSpec({

	val mapper = jsonMapper { addModule(kotlinModule()) }
	fun parse(json: String) = mapper.readValue<JsonSchema>(json)

	context("type field") {
		should("accept scalar string and wrap into a single-element list") {
			parse("""{"type":"integer"}""") shouldBe JsonSchema(type = listOf("integer"))
		}

		should("accept array as-is") {
			parse("""{"type":["integer"]}""") shouldBe JsonSchema(type = listOf("integer"))
		}

		should("accept multi-element array (e.g. nullable types)") {
			parse("""{"type":["null","integer"]}""") shouldBe JsonSchema(type = listOf("null", "integer"))
		}

		should("default to empty list when missing") {
			parse("""{}""") shouldBe JsonSchema()
			parse("""{"format":"date-time"}""") shouldBe JsonSchema(type = emptyList(), format = "date-time")
		}
	}

	context("optional fields") {
		should("populate format / precision / decimals / lowCardinality (camelCase preserved)") {
			parse("""{"type":"number","precision":30,"decimals":6,"lowCardinality":true}""") shouldBe
				JsonSchema(type = listOf("number"), precision = 30, decimals = 6, lowCardinality = true)
			parse("""{"type":"string","format":"uuid"}""") shouldBe
				JsonSchema(type = listOf("string"), format = "uuid")
		}

		should("ignore unknown keys") {
			parse("""{"type":"integer","title":"x","description":"y"}""") shouldBe
				JsonSchema(type = listOf("integer"))
		}
	}

	context("nested structures") {
		should("recurse into properties") {
			parse("""{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":["null","string"]}}}""") shouldBe
				JsonSchema(
					type = listOf("object"),
					properties = mapOf(
						"id" to JsonSchema(type = listOf("integer")),
						"name" to JsonSchema(type = listOf("null", "string")),
					),
				)
		}

		should("treat empty `{}` property as default JsonSchema (no CustomMapDeserializer mapping to null)") {
			parse("""{"type":"object","properties":{"empty":{}}}""") shouldBe
				JsonSchema(
					type = listOf("object"),
					properties = mapOf("empty" to JsonSchema()),
				)
		}

		should("recurse into items") {
			parse("""{"type":"array","items":{"type":"integer"}}""") shouldBe
				JsonSchema(type = listOf("array"), items = JsonSchema(type = listOf("integer")))
		}
	}
})
