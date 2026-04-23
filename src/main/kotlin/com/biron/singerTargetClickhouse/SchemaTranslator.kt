package com.biron.singerTargetClickhouse

typealias ValueTranslator = (Any?) -> Any?

object SchemaTranslator {
	fun buildTranslator(type: String?): ValueTranslator {
		val inner = translatorFor(type)
		return { v ->
			if (v == null || inner == null) v else inner(v)
		}
	}

	private fun translatorFor(type: String?): ((Any) -> Any?)? = when (type) {
		"string" -> ::toStringValue
		"boolean" -> ::toBooleanFlag
		"integer" -> ::toInteger
		"number" -> ::toNumber
		else -> null
	}
}

private fun toStringValue(v: Any): String = v.toString()

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
