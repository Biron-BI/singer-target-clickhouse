package com.biron.singerTargetClickhouse

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jsonMapper
import com.fasterxml.jackson.module.kotlin.kotlinModule
import io.github.oshai.kotlinlogging.KotlinLogging
import java.io.InputStream
import java.io.Writer
import java.util.concurrent.ArrayBlockingQueue
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
 * Queue capacity between the parser thread and the main consumer. Large enough to absorb
 * normal bursts and bridge STATE-barrier / HTTP-close waits on the consumer side; small
 * enough to bound peak memory if the consumer stalls.
 */
private const val PARSE_QUEUE_CAPACITY = 1024

private sealed class ParseSignal {
	class Msg(val message: TargetMessage) : ParseSignal()
	class Err(val cause: Throwable) : ParseSignal()
	object Eof : ParseSignal()
}

/**
 * Read the Singer-formatted message stream from [input] and write state messages (and
 * any other passthrough output) to [output]. Dispatches each message to the right
 * handler; finalizes all stream processors in parallel at the end.
 *
 * Mirrors the TS `processStream` entry point.
 */
fun processStream(
	input: InputStream,
	config: TargetConfig,
	output: Writer,
	streamsToReplace: List<String> = emptyList(),
) {
	val ch = ClickhouseConnection(config)
	processStream(input, config, output, streamsToReplace, ch)
}

internal fun processStream(
	input: InputStream,
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

	// Run the Jackson parser on a dedicated producer thread: while we're blocked synchronously
	// on flushes / STATE commits on the main thread, the producer keeps parsing and filling
	// the queue. Bounded capacity gives backpressure so the producer can't outrun the consumer
	// unboundedly.
	val queue = ArrayBlockingQueue<ParseSignal>(PARSE_QUEUE_CAPACITY)
	val streamingParser = StreamingMessageParser(
		subtableSeparator = config.subtableSeparator,
		translateValues = config.translateValues,
	)
	val producerThread = Thread({
		try {
			streamingParser.createParser(input).use { parser ->
				while (!Thread.currentThread().isInterrupted) {
					val msg = streamingParser.readNext(parser)
					if (msg == null) {
						queue.put(ParseSignal.Eof)
						return@use
					}
					queue.put(ParseSignal.Msg(msg))
				}
			}
		} catch (_: InterruptedException) {
			// Consumer asked us to stop — drop out cleanly.
		} catch (e: Throwable) {
			try {
				queue.put(ParseSignal.Err(e))
			} catch (_: InterruptedException) {
				// Consumer is gone — nothing to do.
			}
		}
	}, "singer-parser").apply { isDaemon = true }
	producerThread.start()

	var lineCount = 0
	try {
		loop@ while (encounteredErr == null) {
			when (val sig = queue.take()) {
				is ParseSignal.Msg -> {
					try {
						processLine(sig.message, config, ch, streamProcessors, state, lineCount, output, abort)
					} catch (e: Throwable) {
						abort(e)
					}
					lineCount++
				}

				is ParseSignal.Err -> abort(sig.cause)
				ParseSignal.Eof -> break@loop
			}
		}
	} finally {
		producerThread.interrupt()
		producerThread.join(5000)
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

		is TargetMessage.TypedRecord -> {
			val processor = streamProcessors[msg.stream]
				?: throw IllegalStateException("Record message received before Schema is defined")
			processor.processRecord(msg.row, lineCount, abort)
		}

		is TargetMessage.Record -> {
			// Only the legacy line-based parser produces this variant (not the streaming path).
			// In production we always get TypedRecord from StreamingMessageParser.
			throw IllegalStateException("Unexpected map-based Record on production path for stream=${msg.stream}")
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
