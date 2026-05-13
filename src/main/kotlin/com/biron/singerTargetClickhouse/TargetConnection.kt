package com.biron.singerTargetClickhouse

import arrow.core.Either

data class Column(
	val name: String,
	val type: String,
	val isInSortingKey: Boolean,
)

data class QueryResult(
	val data: List<List<Any?>>,
	val rows: Int,
)

data class AddColumnError(val newCol: Column, val error: Throwable)
data class RemoveColumnError(val existing: Column, val error: Throwable)
data class UpdateColumnError(val existing: Column, val newCol: Column, val error: Throwable)

/**
 * Writes rows to an open insert stream. Write raw bytes already formatted as the
 * insert payload (e.g. JSONCompactEachRow). close() closes the stream and awaits
 * server confirmation; it throws if the server reported an error.
 */
interface RowWriter : AutoCloseable {
	fun write(bytes: ByteArray)
	override fun close()
}

interface TargetConnection {
	fun getDatabase(): String
	fun runQuery(query: String, retries: Int = 2): QueryResult
	fun listTables(): List<String>
	fun listColumns(table: String): List<Column>
	fun addColumn(table: String, newCol: Column): Either<AddColumnError, Unit>
	fun removeColumn(table: String, existing: Column): Either<RemoveColumnError, Unit>
	fun updateColumn(table: String, existing: Column, newCol: Column): Either<UpdateColumnError, Unit>
	fun renameObsoleteTable(table: String): QueryResult
	fun openRowWriter(query: String): RowWriter

	companion object {
		const val DROPPED_TABLE_PREFIX = "_dropped_"
		const val ARCHIVED_TABLE_PREFIX = "_archived_"
	}
}
