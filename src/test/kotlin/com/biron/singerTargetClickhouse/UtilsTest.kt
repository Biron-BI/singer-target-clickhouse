package com.biron.singerTargetClickhouse

import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe

class UtilsTest : ShouldSpec({
	context("sqlStringLiteral") {
		should("wraps the value in single quotes") {
			sqlStringLiteral("toYYYYMM(ts)") shouldBe "'toYYYYMM(ts)'"
		}

		should("escapes single quotes and backslashes") {
			sqlStringLiteral("""formatDateTime(ts, '%Y') || '\'""") shouldBe """'formatDateTime(ts, \'%Y\') || \'\\\''"""
		}
	}
})
