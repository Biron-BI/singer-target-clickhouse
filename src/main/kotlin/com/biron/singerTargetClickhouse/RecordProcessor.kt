package com.biron.singerTargetClickhouse

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jsonMapper
import com.fasterxml.jackson.module.kotlin.kotlinModule
import io.github.oshai.kotlinlogging.KotlinLogging
import java.io.ByteArrayOutputStream
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

private val logger = KotlinLogging.logger {}

private val jsonMapper: ObjectMapper = jsonMapper { addModule(kotlinModule()) }

data class RecordProcessorConfig(
	val batchSize: Int,
	val translateValues: Boolean,
	val autoEndTimeoutMs: Long,
)

/**
 * Ingests and stores data values. Tree structure mirrors the precomputed source meta:
 * one node per table. Each node keeps its own insert stream open until the batch size
 * is reached or the auto-end timeout fires, then flushes and reopens on the next push.
 *
 * All mutations of [buffered]/[ingestion] take [lock], so the scheduler-thread-driven
 * auto-end cannot race with the caller thread.
 *
 * Rows arrive as [RecordRow]s — slot layout `[currentPks..., simpleColumns..., subtables...]`
 * determined by the matching [StreamReader]. The processor reads its data from the row by
 * slot index; translation (config.translateValues) already happened inside the reader.
 */
class RecordProcessor(
	private val meta: SourceMeta,
	private val clickhouse: TargetConnection,
	private val config: RecordProcessorConfig,
	private val level: Int = 0,
) {
	private val isRoot: Boolean = level == 0
	private val isWithParentPK: Boolean = !isRoot && meta.pkMappings.any { it.pkType == PKType.PARENT }
	val hasChildren: Boolean = meta.children.isNotEmpty()
	private val currentPkMappings: List<PkMap> = meta.pkMappings.filter { it.pkType == PKType.CURRENT }
	private val pkCount: Int = currentPkMappings.size
	private val columnCount: Int = meta.simpleColumnMappings.size
	private val children: Map<String, RecordProcessor> = meta.children
		.associateBy { it.sqlTableName }
		.mapValues { RecordProcessor(it.value, clickhouse, config, level + 1) }

	private val lock = Any()
	private var ingestion: Ingestion? = null
	private val buffered: MutableList<List<Any?>> = mutableListOf()

	data class SourceMetaPK internal constructor(
		val values: List<Any?>,
		val rootValues: List<Any?>?,
		val parentValues: List<Any?>?,
		val levelValues: List<Any?>?,
	)

	private class Ingestion(
		val writer: RowWriter,
		var timeoutFuture: ScheduledFuture<*>?,
	)

	fun pushRecord(
		row: RecordRow,
		abort: (Throwable) -> Unit,
		maxVer: Long,
		parentMeta: SourceMetaPK? = null,
		rootVer: Long? = null,
		indexInParent: Int = -1,
		messageCount: Int = 0,
	) {
		val (sourcePk, resolvedRootVer) = synchronized(lock) {
			if (ingestion == null) startIngestion(messageCount, abort)

			val rootVersion = if (isRoot && meta.pkMappings.isNotEmpty()) maxVer else rootVer
			val srcPk = buildSourcePk(row, parentMeta, indexInParent)
			val pkValues = composePkValues(srcPk)

			buffered.add(buildInsertValues(row, pkValues, rootVersion))
			if (buffered.size >= config.batchSize) {
				refreshTimeout(abort)
				flushBuffered()
			}

			srcPk to rootVersion
		}

		if (hasChildren) dispatchToChildren(row, sourcePk, resolvedRootVer, maxVer, messageCount, abort)
	}

	private fun buildSourcePk(row: RecordRow, parentMeta: SourceMetaPK?, indexInParent: Int): SourceMetaPK {
		val currentPkValues: List<Any?> = List(pkCount) { row[it] }
		return SourceMetaPK(
			values = currentPkValues,
			rootValues = if (isRoot) null else parentMeta?.rootValues ?: parentMeta?.values,
			parentValues = if (isRoot) null else parentMeta?.values,
			levelValues = if (isRoot) null else (parentMeta?.levelValues ?: emptyList()) + indexInParent,
		)
	}

	private fun composePkValues(srcPk: SourceMetaPK): List<Any?> {
		if (isRoot) return srcPk.values
		return buildList {
			addAll(srcPk.rootValues!!)
			if (isWithParentPK) addAll(srcPk.parentValues!!)
			addAll(srcPk.values)
			addAll(srcPk.levelValues!!)
		}
	}

	private fun dispatchToChildren(
		row: RecordRow,
		sourcePk: SourceMetaPK,
		rootVer: Long?,
		maxVer: Long,
		messageCount: Int,
		abort: (Throwable) -> Unit,
	) {
		meta.children.forEachIndexed { i, child ->
			val processor = children.getValue(child.sqlTableName)
			@Suppress("UNCHECKED_CAST")
			val childRows = row[pkCount + columnCount + i] as? List<RecordRow> ?: return@forEachIndexed
			childRows.forEachIndexed { idx, childRow ->
				processor.pushRecord(
					row = childRow,
					abort = abort,
					maxVer = maxVer,
					parentMeta = sourcePk,
					rootVer = rootVer,
					indexInParent = idx,
					messageCount = messageCount,
				)
			}
		}
	}

	fun endIngestion() {
		synchronized(lock) {
			val ctx = ingestion ?: return@synchronized
			logger.debug { "[${meta.prop}] closing stream to insert data" }
			ctx.timeoutFuture?.cancel(false)
			flushBuffered()
			try {
				ctx.writer.close()
			} finally {
				ingestion = null
			}
		}
		children.values.forEach { it.endIngestion() }
	}

	internal fun buildSQLInsertField(): List<String> {
		val noRootPk = meta.pkMappings.none { it.pkType == PKType.ROOT }
		return meta.pkMappings.map { it.sqlIdentifier } +
			meta.simpleColumnMappings.map { it.sqlIdentifier } +
			when {
				noRootPk && meta.pkMappings.isNotEmpty() -> listOf("`_ver`")
				noRootPk -> emptyList()
				else -> listOf("`_root_ver`")
			}
	}

	private fun flushBuffered() {
		if (buffered.isEmpty()) return
		val ctx = ingestion ?: error("ingestion not started but buffered data present")
		// Write rows through a UTF-8 JsonGenerator directly into a byte buffer — avoids the
		// per-row StringWriter+StringBuilder round-trip and the final String.getBytes encode
		// that together dominated flushBuffered in the profile.
		val baos = ByteArrayOutputStream(8 * 1024)
		jsonMapper.factory.createGenerator(baos).use { gen ->
			// Jackson's default inserts " " between successive root-level values; disable that
			// so consecutive rows are separated only by the '\n' we write below.
			gen.setRootValueSeparator(null)
			buffered.forEach { row ->
				jsonMapper.writeValue(gen, row)
				gen.writeRaw('\n')
			}
		}
		ctx.writer.write(baos.toByteArray())
		buffered.clear()
	}

	private fun startIngestion(messageCount: Int, abort: (Throwable) -> Unit) {
		val insertQuery = "INSERT INTO ${meta.sqlTableName} (${buildSQLInsertField().joinToString(",")}) FORMAT JSONCompactEachRow"
		if (isRoot) logger.info { "[${meta.prop}] handling lines starting at $messageCount" }
		val writer = clickhouse.openRowWriter(insertQuery)
		val ctx = Ingestion(writer, timeoutFuture = null)
		ctx.timeoutFuture = scheduleAutoEnd(ctx, abort)
		ingestion = ctx
	}

	private fun refreshTimeout(abort: (Throwable) -> Unit) {
		val ctx = ingestion ?: return
		ctx.timeoutFuture?.cancel(false)
		ctx.timeoutFuture = scheduleAutoEnd(ctx, abort)
	}

	/**
	 * Schedule auto-end for [ctx]. The callback itself re-acquires [lock] and re-checks that
	 * the ingestion is still current: if another push replaced the timer (or closed the
	 * ingestion) between scheduling and firing, the callback becomes a no-op.
	 */
	private fun scheduleAutoEnd(ctx: Ingestion, abort: (Throwable) -> Unit): ScheduledFuture<*> {
		val taskRef = java.util.concurrent.atomic.AtomicReference<ScheduledFuture<*>?>(null)
		val task = defaultScheduler.schedule(
			{
				synchronized(lock) {
					if (ingestion !== ctx || ctx.timeoutFuture !== taskRef.get()) return@synchronized
					logger.debug { "[${meta.prop}] auto closing stream to insert data due to inactivity" }
					try {
						endIngestionUnlocked()
					} catch (e: Throwable) {
						abort(e)
					}
				}
			},
			config.autoEndTimeoutMs,
			TimeUnit.MILLISECONDS,
		)
		taskRef.set(task)
		return task
	}

	/** Close the current ingestion assuming [lock] is already held (or we are on the lock-thread). */
	private fun endIngestionUnlocked() {
		val ctx = ingestion ?: return
		flushBuffered()
		try {
			ctx.writer.close()
		} finally {
			ingestion = null
		}
	}

	private fun buildInsertValues(
		row: RecordRow,
		pkValues: List<Any?>,
		version: Long?,
	): List<Any?> = buildList(pkValues.size + columnCount + (if (version != null) 1 else 0)) {
		addAll(pkValues)
		for (i in 0 until columnCount) add(row[pkCount + i])
		if (version != null) add(version)
	}

	companion object {
		private val defaultScheduler: ScheduledExecutorService =
			Executors.newSingleThreadScheduledExecutor { r ->
				Thread(r, "record-processor-auto-end").apply { isDaemon = true }
			}
	}
}
