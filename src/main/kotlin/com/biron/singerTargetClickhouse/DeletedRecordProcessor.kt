package com.biron.singerTargetClickhouse

import io.github.oshai.kotlinlogging.KotlinLogging

private val logger = KotlinLogging.logger {}

data class DeletedRecordProcessorConfig(
	val batchSize: Int,
	val translateValues: Boolean,
)

class DeletedRecordProcessor(
	private val meta: SourceMeta,
	private val clickhouse: TargetConnection,
	private val config: DeletedRecordProcessorConfig,
) {
	private val currentPkMappings: List<PkMap> = meta.pkMappings.filter { it.pkType == PKType.CURRENT }
	private val buffered: MutableList<List<String>> = mutableListOf()

	fun pushDeletedRecord(data: Map<String, Any?>) {
		check(currentPkMappings.isNotEmpty()) {
			"[${meta.prop}] cannot push deleted record to a stream without pk mapping"
		}
		val pkValues = currentPkMappings.map { extractValue(data, it, config.translateValues) }
		buffered.add(pkValues.mapIndexed { i, v -> formatForSql(v, currentPkMappings[i]) })
		if (buffered.size >= config.batchSize) deleteBufferedData()
	}

	fun deleteBufferedData() {
		if (buffered.isEmpty()) return
		logger.info { "[${meta.prop}] deleting ${buffered.size} records" }
		val pkIds = currentPkMappings.joinToString(",") { it.sqlIdentifier }
		val values = buffered.joinToString(",") { "(${it.joinToString(",")})" }
		clickhouse.runQuery(
			"""DELETE FROM ${meta.sqlTableName}
			   WHERE ($pkIds) IN ($values)
			   SETTINGS mutations_sync=2""".trimIndent(),
		)
		buffered.clear()
	}

	private fun formatForSql(value: Any?, mapping: PkMap): String =
		if (mapping.chType in QUOTE_TYPES) "'$value'" else value.toString()

	companion object {
		private val QUOTE_TYPES = setOf("String", "FixedString", "DateTime", "Date", "DateTime64", "Date32", "UUID")
	}
}
