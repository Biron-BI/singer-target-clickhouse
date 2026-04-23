package com.biron.singerTargetClickhouse

fun <T> T.asList(): List<T> = listOf(this)

fun escapeValue(value: String, delimiter: String = "'"): String =
	value.split(delimiter).joinToString("\\$delimiter\\")
