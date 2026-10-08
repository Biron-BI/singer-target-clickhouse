package com.biron.singerTargetClickhouse

/** [value] as a single-quoted ClickHouse string literal. */
fun sqlStringLiteral(value: String): String =
	"'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
