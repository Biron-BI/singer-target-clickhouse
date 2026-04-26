package com.biron.singerTargetClickhouse

import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe

class UtilsTest : ShouldSpec({
	context("escapeValue") {
		should("doubles delimiter with backslashes") {
			escapeValue("bob's stuff") shouldBe "bob\\'\\s stuff"
		}

		should("returns input unchanged when delimiter absent") {
			escapeValue("no quotes here") shouldBe "no quotes here"
		}

		should("works with custom delimiter") {
			escapeValue("a\"b", delimiter = "\"") shouldBe "a\\\"\\b"
		}
	}
})
