package com.biron.singerTargetClickhouse

import io.github.oshai.kotlinlogging.KotlinLogging

private val logger = KotlinLogging.logger {}

/** @return true iff [meta] is a root whose CURRENT PKs promote the table to ReplacingMergeTree. */
private fun metaRepresentsReplacingMergeTree(meta: SourceMeta): Boolean = meta.pkMappings.isNotEmpty()

class StreamProcessor private constructor(
	private val clickhouse: TargetConnection,
	private val meta: SourceMeta,
	private val startedClean: Boolean,
	private val recordProcessor: RecordProcessor,
	private val deletedRecordProcessor: DeletedRecordProcessor,
	val streamReader: StreamReader,
) {
	private var maxVer: Long = 0
	private var noPendingRows: Int = 0
	private val cleaningValues: MutableSet<String> = mutableSetOf()

	fun processRecord(row: RecordRow, messageCount: Int, abort: (Throwable) -> Unit) {
		if (!startedClean && streamReader.cleaningColumnSlot >= 0) {
			val cleaningValue = row[streamReader.cleaningColumnSlot]?.toString()
			if (!cleaningValue.isNullOrEmpty() && cleaningValue !in cleaningValues) {
				deleteCleaningValue(cleaningValue)
				cleaningValues += cleaningValue
			}
		}
		recordProcessor.pushRecord(row, abort, maxVer, messageCount = messageCount)
		maxVer++
		noPendingRows++
	}

	fun processDeletedRecord(record: Map<String, Any?>) {
		deletedRecordProcessor.pushDeletedRecord(record)
	}

	fun commitPendingChanges() {
		if (noPendingRows > 0) {
			logger.info { "[${meta.prop}]: ending batch ingestion for $noPendingRows rows" }
			recordProcessor.endIngestion()
			noPendingRows = 0
			maxVer++
		}
		deletedRecordProcessor.deleteBufferedData()
	}

	fun finalizeProcessing() {
		try {
			commitPendingChanges()
		} catch (e: Throwable) {
			throw IllegalStateException("could not save new records", e)
		}
		logger.info { "[${meta.prop}]: finalizing processing" }

		if (!startedClean) {
			if (isReplacingMergeTree()) {
				logger.info { "[${meta.prop}]: removing root duplicates" }
				clickhouse.runQuery("OPTIMIZE TABLE ${meta.sqlTableName} FINAL")

				if (recordProcessor.hasChildren) {
					logger.info { "[${meta.prop}]: removing children orphans" }
					meta.children.forEach { deleteChildDuplicates(it) }
				}
			}

			logger.info { "[${meta.prop}]: ensuring PK integrity is maintained" }
			assertPKIntegrity(meta)
		}
	}

	private fun clearTables() {
		buildDropTablesQueries(meta).forEach { clickhouse.runQuery(it) }
	}

	private fun deleteCleaningValue(value: String) {
		val cleaningColumn = meta.cleaningColumn ?: run {
			logger.warn { "[${meta.prop}]: unexpected request to clean values: cleaning column undefined" }
			return
		}
		val cleaningColumnMeta = (meta.simpleColumnMappings.map { it.prop to it.schemaType } +
			meta.pkMappings.map { it.prop to it.schemaType })
			.firstOrNull { (prop, _) -> prop == cleaningColumn }
			?: throw IllegalStateException(
				"[${meta.prop}] could not resolve cleaning column meta (looking for $cleaningColumn)",
			)
		val schemaType = cleaningColumnMeta.second
			?: throw IllegalStateException(
				"[${meta.prop}] could not be used as cleaning column as it do not have a translator",
			)
		val resolvedValue = translateValue(schemaType, value)
		logger.info { "[${meta.prop}]: cleaning column: deleting based on $resolvedValue" }

		clickhouse.runQuery(
			"""ALTER TABLE ${meta.sqlTableName} DELETE
			   WHERE `$cleaningColumn` = '${escapeValue(value)}'
			   SETTINGS mutations_sync=2""".trimIndent(),
		)
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

	private fun isReplacingMergeTree(): Boolean = metaRepresentsReplacingMergeTree(meta)

	private fun assertPKIntegrity(current: SourceMeta) {
		current.children.forEach { assertPKIntegrity(it) }
		if (current.pkMappings.isEmpty()) return
		val pks = current.pkMappings.joinToString(",") { it.sqlIdentifier }
		val res = clickhouse.runQuery(
			"""SELECT $pks
			   FROM (SELECT $pks, ROW_NUMBER() OVER (PARTITION BY $pks) AS row_number FROM ${current.sqlTableName})
			   WHERE row_number > 1 LIMIT 1""".trimIndent(),
		)
		if (res.rows > 0) {
			throw IllegalStateException(
				"Duplicate key on table ${current.sqlTableName}, data: ${res.data}, aborting process",
			)
		}
	}

	companion object {
		fun create(
			ch: TargetConnection,
			meta: SourceMeta,
			config: TargetConfig,
			cleanFirst: Boolean,
			existingTables: List<String>,
		): StreamProcessor {
			val processor = StreamProcessor(
				clickhouse = ch,
				meta = meta,
				startedClean = cleanFirst,
				recordProcessor = RecordProcessor(
					meta = meta,
					clickhouse = ch,
					config = RecordProcessorConfig(
						batchSize = config.batchSize,
						translateValues = config.translateValues,
						autoEndTimeoutMs = ((config.insertStreamTimeoutSec - 5).coerceAtLeast(1)) * 1000L,
					),
				),
				deletedRecordProcessor = DeletedRecordProcessor(
					meta = meta,
					clickhouse = ch,
					config = DeletedRecordProcessorConfig(
						batchSize = config.deletionBatchSize,
						translateValues = config.translateValues,
					),
				),
				streamReader = buildStreamReader(meta, config.translateValues),
			)

			val rootAlreadyExists: Boolean = if (cleanFirst) {
				processor.clearTables()
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

			processor.maxVer = if (cleanFirst || !metaRepresentsReplacingMergeTree(meta)) {
				processor.maxVer
			} else {
				ch.runQuery("SELECT max(_ver) FROM ${meta.sqlTableName}").data
					.firstOrNull()?.firstOrNull()?.toString()?.toLongOrNull() ?: 0L
			}

			logger.info { "[${meta.prop}]: initial max version is [${processor.maxVer}]" }
			return processor
		}
	}
}

private fun buildDropTablesQueries(meta: SourceMeta): List<String> =
	listOf("DROP TABLE IF EXISTS ${meta.sqlTableName}") + meta.children.flatMap(::buildDropTablesQueries)
