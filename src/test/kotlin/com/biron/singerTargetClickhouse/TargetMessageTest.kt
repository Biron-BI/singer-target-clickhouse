package com.biron.singerTargetClickhouse

import com.fasterxml.jackson.core.JsonParseException
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.types.shouldBeInstanceOf

class TargetMessageTest : ShouldSpec({
	fun aUnderTest(translateValues: Boolean = false): TargetMessageParser =
		TargetMessageParser(subtableSeparator = "__", translateValues = translateValues)

	fun TargetMessageParser.readSingle(line: String): TargetMessage? =
		createParser(line.byteInputStream()).use { readNext(it) }

	val userSchemaLine =
		"""{"type":"SCHEMA","stream":"users","schema":{"type":["null","object"],"properties":{"id":{"type":"integer"},"name":{"type":"string"}}},"key_properties":["id"]}"""

	should("parses SCHEMA message with minimal fields") {
		val msg = aUnderTest().readSingle(
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

	should("parses SCHEMA message with clean_first, cleaning_column, all_key_properties") {
		val msg = aUnderTest().readSingle(
			"""{"type":"SCHEMA","stream":"users",
			   "schema":{"type":"object","properties":{"id":{"type":"integer"},"deleted_at":{"type":"string"}}},
			   "key_properties":["id"],
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

	should("parses RECORD message using the registered stream reader") {
		val underTest = aUnderTest(translateValues = true)
		underTest.readSingle(userSchemaLine)  // registers the reader for "users"

		val msg = underTest.readSingle(
			"""{"type":"RECORD","stream":"users","record":{"id":7,"name":"bob"}}"""
		).shouldBeInstanceOf<TargetMessage.Record>()

		msg.stream shouldBe "users"
		// Layout: [id (current PK), name (simple column)]
		msg.row.toList() shouldBe listOf(7L, "bob")
	}

	should("parses DELETED_RECORD message (PK-only body)") {
		val underTest = aUnderTest(translateValues = true)
		underTest.readSingle(userSchemaLine)

		val msg = underTest.readSingle(
			"""{"type":"DELETED_RECORD","stream":"users","record":{"id":9}}"""
		).shouldBeInstanceOf<TargetMessage.DeletedRecord>()

		msg.stream shouldBe "users"
		// Non-PK slots are left null — see TargetMessage.DeletedRecord kdoc.
		msg.row.toList() shouldBe listOf(9L, null)
	}

	should("parses STATE message preserving value tree") {
		val msg = aUnderTest().readSingle(
			"""{"type":"STATE","value":{"bookmarks":{"a":"b"}}}"""
		).shouldBeInstanceOf<TargetMessage.State>()

		msg.value shouldBe mapOf("bookmarks" to mapOf("a" to "b"))
	}

	should("parses ACTIVE_STREAMS message") {
		val msg = aUnderTest().readSingle(
			"""{"type":"ACTIVE_STREAMS","streams":["a","b"]}"""
		).shouldBeInstanceOf<TargetMessage.ActiveStreams>()

		msg.streams shouldBe listOf("a", "b")
	}

	should("returns Unknown for unrecognized type") {
		aUnderTest().readSingle("""{"type":"MYSTERY","x":1}""")
			.shouldBeInstanceOf<TargetMessage.Unknown>()
	}

	should("throws on malformed json") {
		shouldThrow<JsonParseException> {
			aUnderTest().readSingle("""not-json""")
		}
	}

	should("returns null on blank input (EOF)") {
		aUnderTest().readSingle("   ") shouldBe null
	}

	should("treats top-level arrays as Unknown") {
		aUnderTest().readSingle("[1,2,3]")
			.shouldBeInstanceOf<TargetMessage.Unknown>()
			.raw shouldBe "<non-object top-level token: START_ARRAY>"
	}

	should("treats top-level scalars as Unknown") {
		aUnderTest().readSingle("42")
			.shouldBeInstanceOf<TargetMessage.Unknown>()
	}

	should("returns Unknown when type field is missing") {
		aUnderTest().readSingle("""{"foo":"bar"}""")
			.shouldBeInstanceOf<TargetMessage.Unknown>()
			.type shouldBe "UNKNOWN"
	}

	should("STATE without value field carries null") {
		val msg = aUnderTest().readSingle("""{"type":"STATE"}""")
			.shouldBeInstanceOf<TargetMessage.State>()
		msg.value shouldBe null
	}

	should("STATE with explicit null value preserves the null") {
		val msg = aUnderTest().readSingle("""{"type":"STATE","value":null}""")
			.shouldBeInstanceOf<TargetMessage.State>()
		msg.value shouldBe null
	}

	should("ACTIVE_STREAMS without streams field defaults to empty list") {
		aUnderTest().readSingle("""{"type":"ACTIVE_STREAMS"}""")
			.shouldBeInstanceOf<TargetMessage.ActiveStreams>()
			.streams shouldBe emptyList()
	}

	should("ACTIVE_STREAMS with non-array streams ignores the field") {
		aUnderTest().readSingle("""{"type":"ACTIVE_STREAMS","streams":"oops"}""")
			.shouldBeInstanceOf<TargetMessage.ActiveStreams>()
			.streams shouldBe emptyList()
	}

	should("SCHEMA without schema field still produces a Schema with empty schema") {
		val msg = aUnderTest().readSingle("""{"type":"SCHEMA","stream":"u","key_properties":[]}""")
			.shouldBeInstanceOf<TargetMessage.Schema>()
		msg.stream shouldBe "u"
		msg.schema.type shouldBe emptyList()
	}

	should("SCHEMA with non-array key_properties treats them as empty") {
		val msg = aUnderTest().readSingle(
			"""{"type":"SCHEMA","stream":"u","schema":{"type":["object"],"properties":{"id":{"type":"integer"}}},"key_properties":"id"}"""
		).shouldBeInstanceOf<TargetMessage.Schema>()
		msg.keyProperties shouldBe emptyList()
	}

	should("SCHEMA with non-map all_key_properties falls back to empty") {
		val msg = aUnderTest().readSingle(
			"""{"type":"SCHEMA","stream":"u","schema":{"type":["object"],"properties":{"id":{"type":"integer"}}},"key_properties":["id"],"all_key_properties":42}"""
		).shouldBeInstanceOf<TargetMessage.Schema>()
		msg.allKeyProperties shouldBe SchemaKeyProperties.empty
	}

	should("SCHEMA with cleaning_column=null reads the null") {
		val msg = aUnderTest().readSingle(
			"""{"type":"SCHEMA","stream":"u","schema":{"type":["object"],"properties":{"id":{"type":"integer"}}},"key_properties":["id"],"cleaning_column":null}"""
		).shouldBeInstanceOf<TargetMessage.Schema>()
		msg.cleaningColumn shouldBe null
	}

	should("RECORD before SCHEMA throws an error referencing the stream name") {
		shouldThrow<IllegalStateException> {
			aUnderTest().readSingle("""{"type":"RECORD","stream":"orphans","record":{"id":1}}""")
		}.message shouldContain "before Schema is defined for stream=orphans"
	}

	should("DELETED_RECORD before SCHEMA throws") {
		shouldThrow<IllegalStateException> {
			aUnderTest().readSingle("""{"type":"DELETED_RECORD","stream":"orphans","record":{"id":1}}""")
		}.message shouldContain "DELETED_RECORD received before Schema"
	}

	should("RECORD with type before stream still throws when stream missing") {
		val underTest = aUnderTest()
		underTest.readSingle(userSchemaLine)
		shouldThrow<IllegalStateException> {
			underTest.readSingle("""{"type":"RECORD","record":{"id":1}}""")
		}.message shouldContain "must emit 'stream' before 'record'"
	}

	should("SCHEMA without stream field rejects message construction") {
		shouldThrow<IllegalStateException> {
			aUnderTest().readSingle("""{"type":"SCHEMA","schema":{"type":["object"]}}""")
		}.message shouldContain "requires a [stream] field"
	}

	should("non-record message that carries a record field is ignored on the record body") {
		val msg = aUnderTest().readSingle(
			"""{"type":"STATE","record":{"id":1},"value":{"bookmark":"x"}}"""
		).shouldBeInstanceOf<TargetMessage.State>()
		msg.value shouldBe mapOf("bookmark" to "x")
	}

	should("ignores unknown envelope fields") {
		val msg = aUnderTest().readSingle(
			"""{"type":"STATE","value":{"a":1},"unknown_field":{"x":[1,2,3]}}"""
		).shouldBeInstanceOf<TargetMessage.State>()
		msg.value shouldBe mapOf("a" to 1)
	}

	should("parses nested all_key_properties recursively") {
		val msg = aUnderTest().readSingle(
			"""{"type":"SCHEMA","stream":"u",
			   "schema":{"type":["object"],"properties":{"id":{"type":"integer"}}},
			   "key_properties":["id"],
			   "all_key_properties":{"props":["id"],"children":{"a":{"props":["x"],"children":{"b":{"props":[],"children":{}}}}}}}"""
		).shouldBeInstanceOf<TargetMessage.Schema>()

		val a = msg.allKeyProperties.children.getValue("a")
		a.props shouldBe listOf("x")
		val b = a.children.getValue("b")
		b.props shouldBe emptyList()
		b.children shouldBe emptyMap()
	}

	should("accepts RECORD with a string field larger than Jackson's default 20MB StreamReadConstraints limit") {
		val underTest = aUnderTest(translateValues = true)
		underTest.readSingle(userSchemaLine)

		// Default Jackson StreamReadConstraints.maxStringLength is 20_000_000; go just past it.
		val bigString = "a".repeat(20_000_500)
		val msg = underTest.readSingle(
			"""{"type":"RECORD","stream":"users","record":{"id":1,"name":"$bigString"}}"""
		).shouldBeInstanceOf<TargetMessage.Record>()

		(msg.row[1] as String).length shouldBe bigString.length
	}

	should("re-registers the StreamReader on a second SCHEMA for the same stream") {
		val underTest = aUnderTest(translateValues = true)
		underTest.readSingle(userSchemaLine)
		// new SCHEMA: rename `name`→`label`. RECORDs must use the new layout.
		underTest.readSingle(
			"""{"type":"SCHEMA","stream":"users","schema":{"type":["object"],"properties":{"id":{"type":"integer"},"label":{"type":"string"}}},"key_properties":["id"]}"""
		)
		val r = underTest.readSingle("""{"type":"RECORD","stream":"users","record":{"id":1,"label":"hi"}}""")
			.shouldBeInstanceOf<TargetMessage.Record>()
		r.row.toList() shouldBe listOf(1L, "hi")
	}
})
