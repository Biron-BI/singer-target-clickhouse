package com.biron.singerTargetClickhouse

internal class RecordingStreamProcessorFactory {
	val invocations: MutableList<Invocation> = mutableListOf()
	val produced: MutableList<FakeStreamProcessor> = mutableListOf()
	var onProduced: (FakeStreamProcessor) -> Unit = {}
	fun asFactory(): StreamProcessorFactory = { _, meta, _, cleanFirst, existingTables, _ ->
		invocations += Invocation(meta.prop, cleanFirst, existingTables.toList())
		FakeStreamProcessor(meta.prop).also {
			produced += it
			onProduced(it)
		}
	}

	data class Invocation(val stream: String, val cleanFirst: Boolean, val existingTables: List<String>)

	internal class FakeStreamProcessor(
		val stream: String,
	) : StreamProcessor {
		val recordedRecords: MutableList<RecordedRecord> = mutableListOf()
		var commitCount: Int = 0
			private set
		var finalizeCount: Int = 0
			private set

		var processRecordError: Throwable? = null

		override fun processRecord(row: RecordRow, messageCount: Int, abort: (Throwable) -> Unit) {
			recordedRecords += RecordedRecord(row, messageCount)
			processRecordError?.let { throw it }
		}

		override fun processDeletedRecord(row: RecordRow) {}

		override fun commitPendingChanges() {
			commitCount++
		}

		override fun finalizeProcessing() {
			finalizeCount++
		}

		data class RecordedRecord(val row: RecordRow, val messageCount: Int)
	}
}
