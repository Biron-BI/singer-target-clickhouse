package com.biron.singerTargetClickhouse

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jsonMapper
import com.fasterxml.jackson.module.kotlin.kotlinModule
import io.github.oshai.kotlinlogging.KotlinLogging
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
 */
class RecordProcessor(
	private val meta: SourceMeta,
	private val clickhouse: TargetConnection,
	private val config: RecordProcessorConfig,
	private val level: Int = 0,
	private val scheduler: ScheduledExecutorService = defaultScheduler,
) {
	private val isRoot: Boolean = level == 0
	private val isWithParentPK: Boolean = !isRoot && meta.pkMappings.any { it.pkType == PKType.PARENT }
	val hasChildren: Boolean = meta.children.isNotEmpty()
	private val currentPkMappings: List<PkMap> = meta.pkMappings.filter { it.pkType == PKType.CURRENT }
	private val children: Map<String, RecordProcessor> = meta.children
		.associateBy { it.sqlTableName }
		.mapValues { RecordProcessor(it.value, clickhouse, config, level + 1, scheduler) }

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
		data: Any?,
		abort: (Throwable) -> Unit,
		maxVer: Long,
		parentMeta: SourceMetaPK? = null,
		rootVer: Long? = null,
		indexInParent: Int = -1,
		messageCount: Int = 0,
	) {
		val (sourcePk, resolvedRootVer) = synchronized(lock) {
			if (ingestion == null) startIngestion(messageCount, abort)

			val rootVersion = if (isRoot && meta.pkMappings.isNotEmpty()) maxVer + 1 else rootVer

			val currentPkValues = currentPkMappings.map { extractValue(data, it, config.translateValues) }
			val srcPk = SourceMetaPK(
				values = currentPkValues,
				rootValues = if (isRoot) null else parentMeta?.rootValues ?: parentMeta?.values,
				parentValues = if (isRoot) null else parentMeta?.values,
				levelValues = if (isRoot) null else (parentMeta?.levelValues ?: emptyList()) + indexInParent,
			)

			val pkValues: List<Any?> = if (isRoot) currentPkValues else buildList {
				addAll(srcPk.rootValues!!)
				if (isWithParentPK) addAll(srcPk.parentValues!!)
				addAll(currentPkValues)
				addAll(srcPk.levelValues!!)
			}

			buffered.add(buildInsertValues(data, pkValues, rootVersion))
			if (buffered.size >= config.batchSize) {
				refreshTimeout(abort)
				flushBuffered()
			}

			srcPk to rootVersion
		}

		if (hasChildren) {
			for (child in meta.children) {
				val processor = children.getValue(child.sqlTableName)
				val childDataRaw = extractChildData(data, child.prop)
				val childArray: List<Any?> = when (childDataRaw) {
					null -> emptyList()
					is List<*> -> childDataRaw
					else -> listOf(childDataRaw)
				}
				childArray.forEachIndexed { idx, childData ->
					processor.pushRecord(
						data = childData,
						abort = abort,
						maxVer = maxVer,
						parentMeta = sourcePk,
						rootVer = resolvedRootVer,
						indexInParent = idx,
						messageCount = messageCount,
					)
				}
			}
		}
	}

	private fun extractChildData(data: Any?, prop: String): Any? {
		val parts = prop.split(NESTED_SUB_OBJECT_SEPARATOR)
		return parts.fold(data) { acc, part -> (acc as? Map<*, *>)?.get(part) }
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
		val ctx = ingestion ?: throw IllegalStateException("ingestion not started but buffered data present")
		val payload = buildString {
			buffered.forEach { row ->
				append(jsonMapper.writeValueAsString(row))
				append('\n')
			}
		}.toByteArray(Charsets.UTF_8)
		ctx.writer.write(payload)
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
		val task = scheduler.schedule(
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
		data: Any?,
		pkValues: List<Any?>,
		version: Long?,
	): List<Any?> = buildList(pkValues.size + meta.simpleColumnMappings.size + (if (version != null) 1 else 0)) {
		addAll(pkValues)
		meta.simpleColumnMappings.forEach { add(extractValue(data, it, config.translateValues)) }
		if (version != null) add(version)
	}

	companion object {
		private val defaultScheduler: ScheduledExecutorService =
			Executors.newSingleThreadScheduledExecutor { r ->
				Thread(r, "record-processor-auto-end").apply { isDaemon = true }
			}
	}
}
