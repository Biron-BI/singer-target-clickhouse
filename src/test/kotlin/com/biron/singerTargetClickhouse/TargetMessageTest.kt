package com.biron.singerTargetClickhouse

import com.fasterxml.jackson.databind.node.TextNode
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf

class TargetMessageTest : StringSpec({
	"parses SCHEMA message with minimal fields" {
		val msg = TargetMessageParser.parse(
			"""{"type":"SCHEMA","stream":"users","schema":{"type":["null","object"],"properties":{"id":{"type":"integer"}}},"key_properties":["id"]}"""
		).shouldBeInstanceOf<TargetMessage.Schema>()

		msg.stream shouldBe "users"
		msg.keyProperties shouldBe listOf("id")
		msg.cleanFirst shouldBe false
		msg.cleaningColumn shouldBe null
		msg.allKeyProperties shouldBe SchemaKeyProperties.empty
		msg.schema.type shouldBe listOf("null", "object")
		msg.schema.properties?.keys shouldBe setOf("id")
	}

	"parses SCHEMA message with clean_first, cleaning_column, all_key_properties" {
		val msg = TargetMessageParser.parse(
			"""{"type":"SCHEMA","stream":"users","schema":{"type":"object"},"key_properties":["id"],
			   "clean_first":true,"cleaning_column":"deleted_at",
			   "all_key_properties":{"props":["id"],"children":{"audits":{"props":["a"],"children":{}}}}}"""
		).shouldBeInstanceOf<TargetMessage.Schema>()

		msg.cleanFirst shouldBe true
		msg.cleaningColumn shouldBe "deleted_at"
		msg.allKeyProperties shouldBe SchemaKeyProperties(
			props = listOf("id"),
			children = mapOf("audits" to SchemaKeyProperties(props = listOf("a"), children = emptyMap())),
		)
	}

	"parses RECORD message" {
		val msg = TargetMessageParser.parse(
			"""{"type":"RECORD","stream":"users","record":{"id":7,"name":"bob"}}"""
		).shouldBeInstanceOf<TargetMessage.Record>()

		msg.stream shouldBe "users"
		msg.record shouldBe mapOf("id" to 7, "name" to "bob")
	}

	"parses DELETED_RECORD message" {
		val msg = TargetMessageParser.parse(
			"""{"type":"DELETED_RECORD","stream":"users","record":{"id":9}}"""
		).shouldBeInstanceOf<TargetMessage.DeletedRecord>()

		msg.stream shouldBe "users"
		msg.record shouldBe mapOf("id" to 9)
	}

	"parses STATE message preserving JsonNode" {
		val msg = TargetMessageParser.parse(
			"""{"type":"STATE","value":{"bookmarks":{"a":"b"}}}"""
		).shouldBeInstanceOf<TargetMessage.State>()

		msg.value["bookmarks"]["a"] shouldBe TextNode("b")
	}

	"parses ACTIVE_STREAMS message" {
		val msg = TargetMessageParser.parse(
			"""{"type":"ACTIVE_STREAMS","streams":["a","b"]}"""
		).shouldBeInstanceOf<TargetMessage.ActiveStreams>()

		msg.streams shouldBe listOf("a", "b")
	}

	"returns Unknown for unrecognized type" {
		TargetMessageParser.parse("""{"type":"MYSTERY","x":1}""")
			.shouldBeInstanceOf<TargetMessage.Unknown>()
	}

	"returns Unknown for malformed json" {
		TargetMessageParser.parse("""not-json""").shouldBeInstanceOf<TargetMessage.Unknown>()
	}

	"returns null for blank line" {
		TargetMessageParser.parse("   ") shouldBe null
	}
})
