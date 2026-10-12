package com.biron.singerTargetClickhouse

import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.Locale

private val logger = KotlinLogging.logger {}

/** A table with parts holding deleted rows, and its rows on disk, deleted ones included. */
internal data class MaskedTable(val name: String, val rowsOnDisk: Long)

/**
 * Lightweight deletes (cleaning column, DELETED_RECORD messages, child orphans) only mask rows,
 * which stay on disk until their part is rewritten. Once every stream is finalized, a table whose
 * deleted rows make up at least [minRatio] of its rows on disk gets `APPLY DELETED MASK`, which
 * rewrites its masked parts only. Below that ratio, the rows wait for a merge or for a later run
 * that deletes more: they may take up to [minRatio] of a table, which spares rewriting large parts
 * for a few rows.
 *
 * The ratio is over the whole table, not over its masked parts: deletes concentrated in a few
 * small parts would trigger rewrites that the next merge of those parts makes for free.
 *
 * The masks are read from the tables rather than tracked in memory, so the ones left by a run that
 * stopped before its end are reclaimed too.
 */
internal class DeletedRowsReclaimer(
	private val clickhouse: TargetConnection,
	private val minRatio: Double,
) {
	/** The tables among [tables] with at least one part holding deleted rows. */
	fun tablesWithDeletedRows(tables: Collection<String>): List<MaskedTable> {
		if (tables.isEmpty()) return emptyList()
		return clickhouse.runQuery(
			"""SELECT table, sum(rows) FROM system.parts
			   WHERE database = currentDatabase() AND active
			     AND table IN (${tables.joinToString(", ") { sqlStringLiteral(it) }})
			   GROUP BY table
			   HAVING countIf(has_lightweight_delete) > 0""".trimIndent(),
		).data.map { (name, rows) -> MaskedTable(name.toString(), rows.toString().toLong()) }
	}

	fun reclaim(table: MaskedTable) {
		val deleted = deletedRows(table.name)
		if (deleted == 0L) return
		val ratio = deleted.toDouble() / table.rowsOnDisk
		if (ratio < minRatio) {
			logger.info { "[${table.name}]: deleted rows ratio ${format(ratio)} below ${format(minRatio)}, left on disk" }
			return
		}
		logger.info { "[${table.name}]: deleted rows ratio ${format(ratio)}, removing them from disk" }
		clickhouse.runQuery("ALTER TABLE `${table.name}` APPLY DELETED MASK SETTINGS mutations_sync = 2")
	}

	/**
	 * Deleted rows still on disk, 0 when their parts were merged in the meantime. Only reads the
	 * deleted-rows mask of the parts holding some.
	 */
	private fun deletedRows(table: String): Long =
		clickhouse.runQuery(
			"""SELECT countIf(NOT _row_exists)
			   FROM `$table`
			   WHERE _part IN (SELECT name FROM system.parts
			                   WHERE database = currentDatabase() AND table = ${sqlStringLiteral(table)} AND active AND has_lightweight_delete)
			   SETTINGS apply_deleted_mask = 0""".trimIndent(),
		).data.single().single().toString().toLong()

	private fun format(ratio: Double): String = String.format(Locale.ROOT, "%.3f", ratio)
}
