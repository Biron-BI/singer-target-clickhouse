package com.biron.singerTargetClickhouse

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe

class UtilsTest : StringSpec({
	"escapeValue doubles delimiter with backslashes" {
		escapeValue("bob's stuff") shouldBe "bob\\'\\s stuff"
	}

	"escapeValue returns input unchanged when delimiter absent" {
		escapeValue("no quotes here") shouldBe "no quotes here"
	}

	"escapeValue works with custom delimiter" {
		escapeValue("a\"b", delimiter = "\"") shouldBe "a\\\"\\b"
	}
})
