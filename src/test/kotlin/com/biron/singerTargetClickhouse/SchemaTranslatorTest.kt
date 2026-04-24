package com.biron.singerTargetClickhouse

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe

class SchemaTranslatorTest : StringSpec({
	"null type passes value through unchanged" {
		translateValue(null, "foo") shouldBe "foo"
		translateValue(null, 42) shouldBe 42
	}

	"string type coerces to String" {
		translateValue("string", 42) shouldBe "42"
		translateValue("string", true) shouldBe "true"
	}

	"boolean type emits 1 for truthy variants, 0 otherwise" {
		translateValue("boolean", true) shouldBe 1
		translateValue("boolean", "true") shouldBe 1
		translateValue("boolean", 1) shouldBe 1
		translateValue("boolean", 1L) shouldBe 1
		translateValue("boolean", 1.0) shouldBe 1
		translateValue("boolean", 0) shouldBe 0
		translateValue("boolean", "false") shouldBe 0
		translateValue("boolean", "random") shouldBe 0
	}

	"integer type parses numbers and numeric strings" {
		translateValue("integer", 42) shouldBe 42L
		translateValue("integer", "42") shouldBe 42L
		translateValue("integer", 3.9) shouldBe 3L
		translateValue("integer", "3.9") shouldBe 3L
		translateValue("integer", "boom") shouldBe null
	}

	"number type parses numbers and numeric strings" {
		translateValue("number", 3.14) shouldBe 3.14
		translateValue("number", "3.14") shouldBe 3.14
		translateValue("number", 42) shouldBe 42.0
		translateValue("number", "NaN-ish") shouldBe null
	}

	"null input passes through untouched" {
		listOf("string", "integer", "number", "boolean", null).forEach {
			translateValue(it, null) shouldBe null
		}
	}
})
