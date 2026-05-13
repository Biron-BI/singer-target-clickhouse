package com.biron.singerTargetClickhouse

import io.mockk.checkUnnecessaryStub
import io.mockk.clearAllMocks
import io.mockk.every
import java.io.ByteArrayOutputStream

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

internal class RowWriterCapture {
	val streams: MutableList<CapturedStream> = mutableListOf()
}

internal fun TargetConnection.captureRowWriters(): RowWriterCapture {
	val captures = RowWriterCapture()
	every { openRowWriter(any()) } answers {
		CapturedStream(firstArg<String>()).also { captures.streams += it }
	}
	return captures
}

internal class RunQueryCapture {
	val queries: MutableList<String> = mutableListOf()
}

/**
 * Catches every `runQuery(...)` call for inspection. The optional [answer] computes the
 * returned [QueryResult] from the SQL string, mirroring the previous fake's `runQueryStub`.
 */
internal fun TargetConnection.captureRunQueries(
	answer: (String) -> QueryResult = { QueryResult(emptyList(), 0) },
): RunQueryCapture {
	val capture = RunQueryCapture()
	every { runQuery(any(), any()) } answers {
		val q = firstArg<String>()
		capture.queries += q
		answer(q)
	}
	return capture
}

/**
 * Mirrors the project-wide `checkAndClearAllMocks()` convention: any `every { }` that wasn't
 * consumed during the test fails the spec, then mock state is cleared between tests.
 */
internal fun checkAndClearAllMocks() {
	checkUnnecessaryStub()
	clearAllMocks()
}
