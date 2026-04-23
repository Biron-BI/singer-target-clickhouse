package com.biron.singerTargetClickhouse

import arrow.core.Either
import arrow.core.right
import java.io.ByteArrayOutputStream

/**
 * In-memory TargetConnection used by unit tests to capture what the processors
 * write/query. Query execution returns empty results unless `runQueryStub` is set.
 */
internal class FakeTargetConnection(private val database: String = "testdb") : TargetConnection {
	val streams: MutableList<CapturedStream> = mutableListOf()
	val runQueryLog: MutableList<String> = mutableListOf()
	var runQueryStub: ((String) -> QueryResult)? = null

	override fun getDatabase(): String = database

	override fun runQuery(query: String, retries: Int): QueryResult {
		runQueryLog += query
		return runQueryStub?.invoke(query) ?: QueryResult(emptyList(), 0)
	}

	override fun listTables(): List<String> = emptyList()
	override fun listColumns(table: String): List<Column> = emptyList()
	override fun addColumn(table: String, newCol: Column): Either<AddColumnError, Unit> = Unit.right()
	override fun removeColumn(table: String, existing: Column): Either<RemoveColumnError, Unit> = Unit.right()
	override fun updateColumn(table: String, existing: Column, newCol: Column): Either<UpdateColumnError, Unit> = Unit.right()
	override fun renameObsoleteColumn(table: String): QueryResult {
		runQueryLog += "RENAME TABLE $table"
		return QueryResult(emptyList(), 0)
	}

	override fun openRowWriter(query: String): RowWriter = CapturedStream(query).also { streams += it }

	internal class CapturedStream(val query: String) : RowWriter {
		private val buffer = ByteArrayOutputStream()
		var closed: Boolean = false
			private set

		override fun write(bytes: ByteArray) {
			buffer.write(bytes)
		}

		override fun close() {
			closed = true
		}

		val data: String get() = String(buffer.toByteArray(), Charsets.UTF_8)
	}
}
