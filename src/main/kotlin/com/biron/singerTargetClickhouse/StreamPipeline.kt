package com.biron.singerTargetClickhouse

import arrow.fx.coroutines.parMap
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jsonMapper
import com.fasterxml.jackson.module.kotlin.kotlinModule
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import java.io.InputStream
import java.io.Writer
import java.util.concurrent.ArrayBlockingQueue

private val logger = KotlinLogging.logger {}
private val outMapper: ObjectMapper = jsonMapper { addModule(kotlinModule()) }

// Sized to absorb normal bursts and bridge STATE-barrier / HTTP-close waits without ballooning peak memory if the consumer stalls.
private const val PARSE_QUEUE_CAPACITY = 1024
private const val PARSER_THREAD_JOIN_TIMEOUT_MS = 5_000L

private sealed class ParseSignal {
	data class Msg(val message: TargetMessage) : ParseSignal()
	data class Err(val cause: Throwable) : ParseSignal()
	data object Eof : ParseSignal()
}

/**
 * Reads a Singer-formatted message stream and dispatches each message to the right handler.
 *
 * The Jackson parser runs on a dedicated producer thread: while the consumer blocks on
 * flushes / STATE commits, the producer keeps parsing into a bounded queue, which gives
 * backpressure so the producer cannot outrun the consumer.
 */
class StreamPipeline private constructor(
	private val config: TargetConfig,
	private val ch: TargetConnection,
	private val streamProcessorFactory: StreamProcessorFactory,
) {

	companion object {
		fun forConfig(
			config: TargetConfig,
			connectionFactory: (TargetConfig) -> TargetConnection = ::ClickhouseConnection,
			streamProcessorFactory: StreamProcessorFactory = StreamProcessor.Companion::create,
		): StreamPipeline = StreamPipeline(config, connectionFactory(config), streamProcessorFactory)
	}

	fun run(input: InputStream, output: Writer, streamsToReplace: List<String> = emptyList()) {
		val state = ProcessingState(streamsToReplace.toMutableList(), ch.listTables().toMutableList())
		val processors = linkedMapOf<String, StreamProcessor>()
		val errSink = ErrorSink()
		val queue = ArrayBlockingQueue<ParseSignal>(PARSE_QUEUE_CAPACITY)
		val producerThread = startParserThread(input, queue)

		try {
			consumeMessages(queue, errSink) { msg, lineCount ->
				processLine(msg, processors, state, lineCount, output, errSink::abort)
			}
		} finally {
			producerThread.apply {
				interrupt()
				join(PARSER_THREAD_JOIN_TIMEOUT_MS)
			}
		}
		output.flush()
		logger.info { "done reading lines" }

		errSink.thrownOrNull()?.let { throw it }
		finalizeAllInParallel(processors.values, config.finalizeConcurrency)
	}

	private fun startParserThread(input: InputStream, queue: ArrayBlockingQueue<ParseSignal>): Thread {
		val parser = TargetMessageParser(
			subtableSeparator = config.subtableSeparator,
			translateValues = config.translateValues,
		)
		return Thread({
			try {
				parser.createParser(input).use { p ->
					while (!Thread.currentThread().isInterrupted) {
						val msg = parser.readNext(p)
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
		}, "singer-parser").apply {
			isDaemon = true
			start()
		}
	}

	private fun consumeMessages(
		queue: ArrayBlockingQueue<ParseSignal>,
		errSink: ErrorSink,
		dispatch: (TargetMessage, Int) -> Unit,
	) {
		var lineCount = 0
		while (errSink.thrownOrNull() == null) {
			when (val sig = queue.take()) {
				is ParseSignal.Msg -> {
					try {
						dispatch(sig.message, lineCount)
					} catch (e: Throwable) {
						errSink.abort(e)
					}
					lineCount++
				}

				is ParseSignal.Err -> errSink.abort(sig.cause)
				is ParseSignal.Eof -> return
			}
		}
	}

	private fun finalizeAllInParallel(processors: Collection<StreamProcessor>, configuredConcurrency: Int) {
		if (processors.isEmpty()) return
		runBlocking(Dispatchers.Default) {
			coroutineScope {
				processors.parMap(context = Dispatchers.IO, concurrency = configuredConcurrency.coerceAtLeast(1)) { processor ->
					processor.finalizeProcessing()
				}
			}
		}
	}

	private fun processLine(
		msg: TargetMessage,
		processors: MutableMap<String, StreamProcessor>,
		state: ProcessingState,
		lineCount: Int,
		output: Writer,
		abort: (Throwable) -> Unit,
	) {
		when (msg) {
			is TargetMessage.Schema -> handleSchema(msg, processors, state)
			is TargetMessage.Record -> requireProcessor(processors, msg.stream).processRecord(msg.row, lineCount, abort)
			is TargetMessage.DeletedRecord -> requireProcessor(processors, msg.stream).processDeletedRecord(msg.row)
			is TargetMessage.State -> handleState(msg, processors, output)
			is TargetMessage.ActiveStreams -> handleActiveStreams(msg)
			is TargetMessage.Unknown -> logger.warn {
				"Message type not handled at line $lineCount starting with [${msg.raw.take(50)}]"
			}
		}
	}

	private fun requireProcessor(processors: Map<String, StreamProcessor>, stream: String): StreamProcessor =
		processors[stream] ?: error("Record message received before Schema is defined for stream [$stream]")

	private fun handleSchema(
		msg: TargetMessage.Schema,
		processors: MutableMap<String, StreamProcessor>,
		state: ProcessingState,
	) {
		processors[msg.stream]?.commitPendingChanges()
		logger.info { "[${msg.stream}]: Received schema message." }
		val meta = msg.meta
		if (state.consumeReplaceFor(meta.prop)) {
			logger.info { "[${meta.prop}]: dropping root and children tables" }
			dropStreamTablesQueries(meta).forEach { ch.runQuery(it) }
			state.refreshExistingTables(ch)
		}
		processors[msg.stream] = streamProcessorFactory(ch, meta, config, msg.cleanFirst, state.existingTables, msg.reader.cleaningColumnSlot)
		state.refreshExistingTables(ch)
	}

	private fun handleState(
		msg: TargetMessage.State,
		processors: MutableMap<String, StreamProcessor>,
		output: Writer,
	) {
		logger.info { "Received state message. Commit pending changes..." }
		processors.values.forEach { it.commitPendingChanges() }
		output.write(outMapper.writeValueAsString(msg.value))
		output.write("\n")
		output.flush()
	}

	private fun handleActiveStreams(msg: TargetMessage.ActiveStreams) {
		ch.listTables().forEach { table ->
			if (table.startsWith(TargetConnection.DROPPED_TABLE_PREFIX)) return@forEach
			if (table.startsWith(TargetConnection.ARCHIVED_TABLE_PREFIX)) return@forEach
			if (table in config.extraActiveTables) return@forEach
			if (table.substringBefore(config.subtableSeparator) in msg.streams) return@forEach
			ch.renameObsoleteTable(table)
		}
	}
}

private class ErrorSink {
	@Volatile
	private var thrown: Throwable? = null
	fun abort(err: Throwable) {
		thrown = err
		logger.error { err.message }
	}

	fun thrownOrNull(): Throwable? = thrown
}

private class ProcessingState(
	private val streamsToReplace: MutableList<String>,
	val existingTables: MutableList<String>,
) {
	fun consumeReplaceFor(stream: String): Boolean =
		streamsToReplace.remove(stream)

	fun refreshExistingTables(ch: TargetConnection) {
		existingTables.clear()
		existingTables.addAll(ch.listTables())
	}
}

