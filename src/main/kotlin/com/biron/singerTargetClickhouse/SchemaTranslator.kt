package com.biron.singerTargetClickhouse

/**
 * Coerce a raw value into the Kotlin shape expected for [schemaType]. Null stays null.
 * Unknown types pass through unchanged.
 *
 * The hot ingestion path reads values straight from the JSON token stream in specialized
 * readers (see [StreamReader]) — this function stays on the cold paths (DeletedRecord,
 * cleaningColumn handling) where values arrive pre-materialized as [Any?].
 */
fun translateValue(schemaType: String?, v: Any?): Any? {
	if (v == null) return null
	return when (schemaType) {
		"string" -> v.toString()
		"boolean" -> toBooleanFlag(v)
		"integer" -> toInteger(v)
		"number" -> toNumber(v)
		else -> v
	}
}

private fun toBooleanFlag(v: Any): Int = when {
	v == true || v == "true" -> 1
	v is Number && v.toDouble() == 1.0 -> 1
	else -> 0
}

private fun toInteger(v: Any): Long? = when (v) {
	is Number -> v.toLong()
	is String -> v.trim().toLongOrNull() ?: v.trim().toDoubleOrNull()?.toLong()
	else -> null
}

private fun toNumber(v: Any): Double? = when (v) {
	is Number -> v.toDouble()
	is String -> v.trim().toDoubleOrNull()
	else -> null
}
