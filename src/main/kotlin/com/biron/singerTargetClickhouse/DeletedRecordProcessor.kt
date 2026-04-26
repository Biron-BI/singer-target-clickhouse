package com.biron.singerTargetClickhouse

import io.github.oshai.kotlinlogging.KotlinLogging

private val logger = KotlinLogging.logger {}

data class DeletedRecordProcessorConfig(
	val batchSize: Int,
	val translateValues: Boolean,
)

/**
 * Buffers DELETED_RECORD messages and issues a single SQL DELETE per batch. A Singer
 * DELETED_RECORD carries only the current-level PK fields in its body — so the [RecordRow]
 * fed in has its PK slots `[0, pkCount)` populated and every other slot left null. This
 * processor reads those PK slots directly; it does not touch simple-column or subtable
 * slots.
 */
class DeletedRecordProcessor(
	private val meta: SourceMeta,
	private val clickhouse: TargetConnection,
	private val config: DeletedRecordProcessorConfig,
) {
	private val currentPkMappings: List<PkMap> = meta.pkMappings.filter { it.pkType == PKType.CURRENT }
	private val buffered: MutableList<List<String>> = mutableListOf()

	fun pushDeletedRecord(row: RecordRow) {
		check(currentPkMappings.isNotEmpty()) {
			"[${meta.prop}] cannot push deleted record to a stream without pk mapping"
		}
		val pkValues = List(currentPkMappings.size) { row[it] }
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
