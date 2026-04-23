package com.biron.singerTargetClickhouse

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe

class SchemaTranslatorTest : StringSpec({
	"null type returns identity translator" {
		val t = SchemaTranslator.buildTranslator(null)
		t("foo") shouldBe "foo"
		t(42) shouldBe 42
	}

	"string translator coerces to String" {
		val t = SchemaTranslator.buildTranslator("string")
		t(42) shouldBe "42"
		t(true) shouldBe "true"
	}

	"boolean translator emits 1 for truthy variants, 0 otherwise" {
		val t = SchemaTranslator.buildTranslator("boolean")
		t(true) shouldBe 1
		t("true") shouldBe 1
		t(1) shouldBe 1
		t(1L) shouldBe 1
		t(1.0) shouldBe 1
		t(0) shouldBe 0
		t("false") shouldBe 0
		t("random") shouldBe 0
	}

	"integer translator parses numbers and numeric strings" {
		val t = SchemaTranslator.buildTranslator("integer")
		t(42) shouldBe 42L
		t("42") shouldBe 42L
		t(3.9) shouldBe 3L
		t("3.9") shouldBe 3L
		t("boom") shouldBe null
	}

	"number translator parses numbers and numeric strings" {
		val t = SchemaTranslator.buildTranslator("number")
		t(3.14) shouldBe 3.14
		t("3.14") shouldBe 3.14
		t(42) shouldBe 42.0
		t("NaN-ish") shouldBe null
	}

	"null input passes through untouched" {
		listOf("string", "integer", "number", "boolean", null).forEach {
			SchemaTranslator.buildTranslator(it)(null) shouldBe null
		}
	}
})
