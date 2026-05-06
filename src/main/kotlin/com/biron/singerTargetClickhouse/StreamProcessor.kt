package com.biron.singerTargetClickhouse

import io.github.oshai.kotlinlogging.KotlinLogging

private val logger = KotlinLogging.logger {}

/**
 * One per Singer stream. Owns the live insert path (records, deletions, batch commits) and
 * the post-stream finalize step (ReplacingMergeTree optimize, child orphan cleanup, PK
 * integrity check).
 */
interface StreamProcessor {
	fun processRecord(row: RecordRow, messageCount: Int, abort: (Throwable) -> Unit)
	fun processDeletedRecord(row: RecordRow)
	fun commitPendingChanges()
	fun finalizeProcessing()

	companion object {
		fun create(
			ch: TargetConnection,
			meta: SourceMeta,
			config: TargetConfig,
			cleanFirst: Boolean,
			existingTables: List<String>,
			cleaningColumnSlot: Int?,
		): StreamProcessor =
			DefaultStreamProcessor.create(ch, meta, config, cleanFirst, existingTables, cleaningColumnSlot)
	}
}

typealias StreamProcessorFactory = (
	ch: TargetConnection,
	meta: SourceMeta,
	config: TargetConfig,
	cleanFirst: Boolean,
	existingTables: List<String>,
	cleaningColumnSlot: Int?,
) -> StreamProcessor

internal class DefaultStreamProcessor private constructor(
	private val clickhouse: TargetConnection,
	private val meta: SourceMeta,
	private val startedClean: Boolean,
	private val recordProcessor: RecordProcessor,
	private val deletedRecordProcessor: DeletedRecordProcessor,
	private val cleaningColumnSlot: Int?,
	initialMaxVer: Long,
) : StreamProcessor {
	private var maxVer: Long = initialMaxVer
	private var pendingRowCount: Int = 0
	private val cleaningValuesSeen: MutableSet<String> = mutableSetOf()

	override fun processRecord(row: RecordRow, messageCount: Int, abort: (Throwable) -> Unit) {
		applyCleaningColumnIfPresent(row)
		maxVer++
		recordProcessor.pushRecord(row, abort, maxVer, messageCount = messageCount)
		pendingRowCount++
	}

	override fun processDeletedRecord(row: RecordRow) {
		deletedRecordProcessor.pushDeletedRecord(row)
	}

	override fun commitPendingChanges() {
		if (pendingRowCount > 0) {
			logger.info { "[${meta.prop}]: ending batch ingestion for $pendingRowCount rows" }
			recordProcessor.endIngestion()
			pendingRowCount = 0
			maxVer++
		}
		deletedRecordProcessor.deleteBufferedData()
	}

	override fun finalizeProcessing() {
		try {
			commitPendingChanges()
		} catch (e: Throwable) {
			throw IllegalStateException("could not save new records", e)
		}
		logger.info { "[${meta.prop}]: finalizing processing" }
		if (startedClean) return

		if (meta.isReplacingMergeTree)
			optimizeReplacingMergeTree()
		logger.info { "[${meta.prop}]: ensuring PK integrity is maintained" }
		assertPkIntegrity(meta)
	}

	private fun applyCleaningColumnIfPresent(row: RecordRow) {
		if (startedClean || cleaningColumnSlot == null) return
		val value = row[cleaningColumnSlot]?.toString()
		if (value.isNullOrEmpty() || value in cleaningValuesSeen) return
		deleteCleaningValue(value)
		cleaningValuesSeen += value
	}

	private fun optimizeReplacingMergeTree() {
		logger.info { "[${meta.prop}]: removing root duplicates" }
		clickhouse.runQuery("OPTIMIZE TABLE ${meta.sqlTableName} FINAL")
		if (!recordProcessor.hasChildren) return
		logger.info { "[${meta.prop}]: removing children orphans" }
		meta.children.forEach { deleteChildDuplicates(it) }
	}

	private fun deleteCleaningValue(value: String) {
		val cleaningColumn = meta.cleaningColumn ?: run {
			logger.warn { "[${meta.prop}]: unexpected request to clean values: cleaning column undefined" }
			return
		}
		validateCleaningColumnTyped(cleaningColumn)
		logger.info { "[${meta.prop}]: cleaning column: deleting based on $value" }
		clickhouse.runQuery(
			"""ALTER TABLE ${meta.sqlTableName} DELETE
			   WHERE `$cleaningColumn` = '${escapeValue(value)}'
			   SETTINGS mutations_sync=2""".trimIndent(),
		)
	}

	private fun validateCleaningColumnTyped(cleaningColumn: String) {
		val resolved = (meta.simpleColumnMappings.map { it.prop to it.schemaType } +
				meta.pkMappings.map { it.prop to it.schemaType })
			.firstOrNull { (prop, _) -> prop == cleaningColumn }
			?: error("[${meta.prop}] could not resolve cleaning column meta (looking for $cleaningColumn)")
		if (resolved.second == null) {
			error("[${meta.prop}] could not be used as cleaning column: no typed schema for [$cleaningColumn]")
		}
	}

	private fun deleteChildDuplicates(currentNode: SourceMeta) {
		val parentColumns = meta.pkMappings.map { escapeIdentifier(formatRootPKColumn(it.prop)) } + "_root_ver"
		val rootColumns = meta.pkMappings.map { it.sqlIdentifier } + "_ver"
		clickhouse.runQuery(
			"""ALTER TABLE ${currentNode.sqlTableName} DELETE
			   WHERE (${parentColumns.joinToString(",")})
			     NOT IN (SELECT ${rootColumns.joinToString(",")} FROM ${meta.sqlTableName})
			   SETTINGS mutations_sync=2""".trimIndent(),
		)
		currentNode.children.forEach { deleteChildDuplicates(it) }
	}

	private fun assertPkIntegrity(current: SourceMeta) {
		current.children.forEach { assertPkIntegrity(it) }
		if (current.pkMappings.isEmpty()) return
		val pks = current.pkMappings.joinToString(",") { it.sqlIdentifier }
		val res = clickhouse.runQuery(
			"""SELECT $pks
			   FROM (SELECT $pks, ROW_NUMBER() OVER (PARTITION BY $pks) AS row_number FROM ${current.sqlTableName})
			   WHERE row_number > 1 LIMIT 1""".trimIndent(),
		)
		if (res.rows > 0) {
			error("Duplicate key on table ${current.sqlTableName}, data: ${res.data}, aborting process")
		}
	}

	companion object {
		fun create(
			ch: TargetConnection,
			meta: SourceMeta,
			config: TargetConfig,
			cleanFirst: Boolean,
			existingTables: List<String>,
			cleaningColumnSlot: Int?,
		): DefaultStreamProcessor {
			applySchema(ch, meta, cleanFirst, existingTables)
			val maxVer = initialMaxVer(ch, meta, cleanFirst)
			logger.info { "[${meta.prop}]: initial max version is [$maxVer]" }
			return buildProcessor(ch, meta, config, cleanFirst, cleaningColumnSlot, maxVer)
		}

		private fun applySchema(ch: TargetConnection, meta: SourceMeta, cleanFirst: Boolean, existingTables: List<String>) {
			val rootAlreadyExists = if (cleanFirst) {
				dropStreamTablesQueries(meta).forEach { ch.runQuery(it) }
				false
			} else {
				existingTables.any { meta.sqlTableName == escapeIdentifier(it) }
			}
			if (rootAlreadyExists) {
				updateSchema(meta, ch, existingTables)
			} else {
				logger.info { "[${meta.prop}]: creating tables" }
				translateCH(ch.getDatabase(), meta, recursive = true).forEach { ch.runQuery(it) }
			}
		}

		private fun initialMaxVer(ch: TargetConnection, meta: SourceMeta, cleanFirst: Boolean): Long {
			if (cleanFirst || !meta.isReplacingMergeTree) return 0L
			return ch.runQuery("SELECT max(_ver) FROM ${meta.sqlTableName}").data
				.firstOrNull()?.firstOrNull()?.toString()?.toLongOrNull() ?: 0L
		}

		private fun buildProcessor(
			clickhouse: TargetConnection,
			meta: SourceMeta,
			config: TargetConfig,
			cleanFirst: Boolean,
			cleaningColumnSlot: Int?,
			initialMaxVer: Long
		) =
			DefaultStreamProcessor(
				clickhouse,
				meta,
				cleanFirst,
				RecordProcessor(
					meta = meta,
					clickhouse = clickhouse,
					config = RecordProcessorConfig(
						batchSize = config.batchSize,
						translateValues = config.translateValues,
						autoEndTimeoutMs = autoEndTimeoutMs(config.insertStreamTimeoutSec),
					),
				),
				DeletedRecordProcessor(
					meta = meta,
					clickhouse = clickhouse,
					config = DeletedRecordProcessorConfig(
						batchSize = config.deletionBatchSize,
						translateValues = config.translateValues,
					),
				),
				cleaningColumnSlot,
				initialMaxVer,
			)

		/** True when this root has CURRENT-level PKs and is therefore stored as ReplacingMergeTree. */
		private val SourceMeta.isReplacingMergeTree: Boolean get() = pkMappings.isNotEmpty()

		/**
		 * Auto-end fires before the server's `http_receive_timeout` (= `insertStreamTimeoutSec`)
		 * cuts an idle insert connection. The 5s margin we used to apply was too tight under
		 * load; we now leave a 30s margin whenever the timeout is large enough to afford it,
		 * falling back to half the timeout for very small configured values so the auto-end
		 * still has time to actually fire.
		 */
		internal fun autoEndTimeoutMs(insertStreamTimeoutSec: Int): Long {
			val sec = maxOf(insertStreamTimeoutSec - 30, insertStreamTimeoutSec / 2)
			return sec.coerceAtLeast(1) * 1000L
		}
	}
}
