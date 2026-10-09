package com.biron.singerTargetClickhouse

/**
 * `partition_by` of a SCHEMA message: the root table is partitioned by a date derived from one of
 * the root record's properties. That value must never change for a given key, as ReplacingMergeTree
 * only deduplicates within a partition (see docs/partitioning.md).
 */
data class PartitionSpec(
	/** Path to the property in the root record, e.g. `["attributes", "timestamp"]`. */
	val property: List<String>,
	val type: Type,
	val converter: Converter,
) {
	enum class Type(val jsonValue: String) {
		/** Integer Unix timestamp, in seconds. */
		TIMESTAMP("timestamp"),

		/** Date column: string with format `date` (or `x-excel-date`). */
		DATE("date"),

		/** DateTime or DateTime64 column: string with format `date-time` (or `date-time64`). */
		DATE_TIME("date-time"),
	}

	enum class Converter(val function: String) {
		YYYYMM("toYYYYMM"),
		YYYY("toYear"),
	}

	companion object {
		fun parse(stream: String, raw: Any?): PartitionSpec {
			val fail: (String) -> Nothing = { error("[$stream]: invalid partition_by $raw: $it") }
			val spec = raw as? Map<*, *> ?: fail("expected an object")
			val property = (spec["property"] as? List<*>)
				?.takeIf { it.isNotEmpty() && it.all { part -> part is String } }
				?.map { it as String }
				?: fail("property must be a non-empty array of strings")
			val type = Type.entries.find { it.jsonValue == spec["type"] }
				?: fail("type must be one of ${Type.entries.map { it.jsonValue }}")
			val converter = Converter.entries.find { it.name == spec["converter"] }
				?: fail("converter must be one of ${Converter.entries.map { it.name }}")
			return PartitionSpec(property, type, converter)
		}
	}
}

/** [PartitionSpec] resolved against the root table: the name of the column it reads and the `PARTITION BY` expression. */
data class PartitionBy(val column: String, val expression: String)

/**
 * Only root columns can drive the partition: child tables do not have them. A timestamp is converted
 * in UTC so that the partition of a row does not depend on the server timezone. Date and DateTime
 * columns are used as they are: a DateTime is read in the server timezone, as ClickHouse displays it.
 */
internal fun resolvePartitionBy(
	stream: String,
	spec: PartitionSpec,
	pkMappings: List<PkMap>,
	columns: List<ColumnMap>,
): PartitionBy {
	val prop = spec.property.joinToString(NESTED_SUB_OBJECT_SEPARATOR)
	val path = spec.property.joinToString(".")
	val column =
		pkMappings.firstOrNull { it.prop == prop }?.let { PartitionColumn(it.sqlIdentifier, it.chType, it.schemaType, it.typeFormat) }
			?: columns.firstOrNull { it.prop == prop && !it.nestedArray }
				?.let { PartitionColumn(it.sqlIdentifier, it.chType, it.schemaType, it.typeFormat) }
			?: error("[$stream]: partition_by property [$path] is not a column of the root table (properties inside arrays are not supported)")
	val date = when (spec.type) {
		PartitionSpec.Type.TIMESTAMP -> {
			check(column.schemaType == "integer") {
				"[$stream]: partition_by property [$path] must be an integer to be used as a timestamp, found ${column.schemaType ?: "no type"}"
			}
			"toDateTime(${column.sqlIdentifier}, 'UTC')"
		}

		PartitionSpec.Type.DATE -> {
			check(column.chType == "Date") {
				"[$stream]: partition_by property [$path] must be a string with format date to be used as a date, found ${column.declaredType()}"
			}
			column.sqlIdentifier
		}

		PartitionSpec.Type.DATE_TIME -> {
			check(column.chType == "DateTime" || column.chType == "DateTime64") {
				"[$stream]: partition_by property [$path] must be a string with format date-time or date-time64 to be used as a date-time, " +
						"found ${column.declaredType()}"
			}
			column.sqlIdentifier
		}
	}
	return PartitionBy(column.sqlIdentifier.removeSurrounding("`"), "${spec.converter.function}($date)")
}

private data class PartitionColumn(val sqlIdentifier: String, val chType: String?, val schemaType: String?, val typeFormat: String?) {
	/** The type as the SCHEMA message declares it, e.g. `string with format date-time`. */
	fun declaredType(): String = (schemaType ?: "no type") + typeFormat?.let { " with format $it" }.orEmpty()
}
