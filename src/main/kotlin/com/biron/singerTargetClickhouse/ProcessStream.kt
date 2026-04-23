package com.biron.singerTargetClickhouse

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jsonMapper
import com.fasterxml.jackson.module.kotlin.kotlinModule
import io.github.oshai.kotlinlogging.KotlinLogging
import java.io.BufferedReader
import java.io.Reader
import java.io.Writer
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext

private val logger = KotlinLogging.logger {}
private val outMapper: ObjectMapper = jsonMapper { addModule(kotlinModule()) }

/**
 * Read the Singer-formatted message stream from [input] and write state messages (and
 * any other passthrough output) to [output]. Dispatches each message to the right
 * handler; finalizes all stream processors in parallel at the end.
 *
 * Mirrors the TS `processStream` entry point.
 */
fun processStream(
	input: Reader,
	config: TargetConfig,
	output: Writer,
	streamsToReplace: List<String> = emptyList(),
) {
	val ch = ClickhouseConnection(config)
	processStream(input, config, output, streamsToReplace, ch)
}

internal fun processStream(
	input: Reader,
	config: TargetConfig,
	output: Writer,
	streamsToReplace: List<String>,
	ch: TargetConnection,
) {
	val state = ProcessingState(streamsToReplace.toMutableList(), ch.listTables().toMutableList())
	val streamProcessors = linkedMapOf<String, StreamProcessor>()
	var encounteredErr: Throwable? = null
	val abort: (Throwable) -> Unit = { err ->
		encounteredErr = err
		logger.error { err.message }
	}

	BufferedReader(input).useLines { lines ->
		lines.withIndex().forEach { (index, line) ->
			if (encounteredErr != null) return@forEach
			val message = TargetMessageParser.parse(line) ?: return@forEach
			try {
				processLine(message, config, ch, streamProcessors, state, index, output, abort)
			} catch (e: Throwable) {
				abort(e)
			}
		}
	}
	output.flush()
	logger.info { "done reading lines" }

	encounteredErr?.let { throw it }

	runBlocking(Dispatchers.Default) {
		val concurrency = config.finalizeConcurrency.coerceAtLeast(1)
		val semaphore = Semaphore(concurrency)
		coroutineScope {
			streamProcessors.values.map { processor ->
				async {
					semaphore.withPermit {
						withContext(Dispatchers.IO) { processor.finalizeProcessing() }
					}
				}
			}.awaitAll()
		}
	}
}

private fun processLine(
	msg: TargetMessage,
	config: TargetConfig,
	ch: TargetConnection,
	streamProcessors: MutableMap<String, StreamProcessor>,
	state: ProcessingState,
	lineCount: Int,
	output: Writer,
	abort: (Throwable) -> Unit,
) {
	when (msg) {
		is TargetMessage.Schema -> {
			streamProcessors[msg.stream]?.commitPendingChanges()
			logger.info { "[${msg.stream}]: Received schema message." }
			streamProcessors[msg.stream] = processSchemaMessage(msg, config, ch, state)
		}

		is TargetMessage.Record -> {
			val processor = streamProcessors[msg.stream]
				?: throw IllegalStateException("Record message received before Schema is defined")
			processor.processRecord(msg.record, lineCount, abort)
		}

		is TargetMessage.DeletedRecord -> {
			val processor = streamProcessors[msg.stream]
				?: throw IllegalStateException("Record message received before Schema is defined")
			processor.processDeletedRecord(msg.record)
		}

		is TargetMessage.State -> {
			logger.info { "Received state message. Commit pending changes..." }
			streamProcessors.values.forEach { it.commitPendingChanges() }
			output.write(outMapper.writeValueAsString(msg.value))
			output.write("\n")
			output.flush()
		}

		is TargetMessage.ActiveStreams -> processActiveStreamsMessage(msg, config, ch)

		is TargetMessage.Unknown -> logger.warn {
			"Message type not handled at line $lineCount starting with [${msg.raw.take(50)}]"
		}
	}
}

private fun processSchemaMessage(
	msg: TargetMessage.Schema,
	config: TargetConfig,
	ch: TargetConnection,
	state: ProcessingState,
): StreamProcessor {
	val meta = buildMeta(
		JsonSchemaInspectorContext(
			alias = msg.stream,
			schema = msg.schema,
			keyProperties = msg.keyProperties,
			subtableSeparator = config.subtableSeparator,
			cleaningColumn = msg.cleaningColumn,
			allKeyProperties = msg.allKeyProperties,
		),
	)

	val replaceIdx = state.streamsToReplace.indexOf(meta.prop)
	if (replaceIdx > -1) {
		logger.info { "[${meta.prop}]: dropping root and children tables" }
		dropStreamTablesQueries(meta).forEach { ch.runQuery(it) }
		state.streamsToReplace.removeAt(replaceIdx)
		state.existingTables.clear()
		state.existingTables.addAll(ch.listTables())
	}

	val streamProcessor = StreamProcessor.create(ch, meta, config, msg.cleanFirst, state.existingTables)
	state.existingTables.clear()
	state.existingTables.addAll(ch.listTables())
	return streamProcessor
}

private fun processActiveStreamsMessage(
	msg: TargetMessage.ActiveStreams,
	config: TargetConfig,
	ch: TargetConnection,
) {
	ch.listTables().forEach { table ->
		if (tableShouldBeDropped(table, msg.streams, config.subtableSeparator, config.extraActiveTables)) {
			ch.renameObsoleteColumn(table)
		}
	}
}

private fun tableShouldBeDropped(
	table: String,
	activeStreams: List<String>,
	subtableSeparator: String,
	extraActiveTables: List<String>,
): Boolean {
	val matchesActive = (activeStreams + extraActiveTables).any { active ->
		table == active || table.startsWith(active + subtableSeparator)
	}
	val alreadyDropped = table.startsWith(TargetConnection.DROPPED_TABLE_PREFIX)
	val isArchived = table.startsWith(TargetConnection.ARCHIVED_TABLE_PREFIX)
	return !matchesActive && !alreadyDropped && !isArchived
}

private class ProcessingState(
	val streamsToReplace: MutableList<String>,
	val existingTables: MutableList<String>,
)
