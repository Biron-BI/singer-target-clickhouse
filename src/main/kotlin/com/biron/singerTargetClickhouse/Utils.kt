package com.biron.singerTargetClickhouse

fun escapeValue(value: String, delimiter: String = "'"): String =
	value.split(delimiter).joinToString("\\$delimiter\\")
