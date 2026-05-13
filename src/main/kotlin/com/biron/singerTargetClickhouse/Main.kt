package com.biron.singerTargetClickhouse

import com.biron.singer.core.logging.LoggingConfigurer
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.ProgramResult
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.multiple
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.types.*
import io.github.oshai.kotlinlogging.KotlinLogging
import java.io.BufferedWriter
import java.io.InputStream
import java.io.OutputStreamWriter
import java.io.Writer
import java.nio.charset.StandardCharsets
import kotlin.io.path.reader

private val logger = KotlinLogging.logger {}

/**
 * Runs a configured pipeline against the given input/output. Defaulted on [RootCommand] to the
 * real `StreamPipeline.forConfig(...).run(...)`; tests substitute a recording fake.
 */
internal typealias PipelineRunner = (TargetConfig, InputStream, Writer, List<String>) -> Unit

class RootCommand internal constructor(
	private val pipelineRunner: PipelineRunner,
) : CliktCommand(name = "target-clickhouse") {

	constructor() : this({ config: TargetConfig, input: InputStream, writer: Writer, streams: List<String> ->
		StreamPipeline.forConfig(config).run(input, writer, streams)
	})

	private val configPath by option("-c", "--config")
		.path(mustExist = true, canBeDir = false, mustBeReadable = true)
		.required()

	private val inputStream by option("--input", help = "An alternate file to read from, instead of STDIN")
		.inputStream()
		.defaultStdin()

	private val outputStream by option("--output", help = "An alternate file to write to, instead of STDOUT")
		.outputStream(truncateExisting = true)
		.defaultStdout()

	private val updateStreams by option(
		"-u", "--update-streams",
		help = "Schema whose root and children tables will be dropped / recreated on SCHEMA messages",
	).multiple()

	/** no-op flag kept for TS-CLI parity; log verbosity is driven by config.logging_level */
	@Suppress("unused")
	private val verbose by option("--verbose").flag()

	override fun run() {
		val config = configPath.reader(StandardCharsets.UTF_8).use { TargetConfig.fromJson(it) }
		loggingConfigurer.reconfigure(!outputStream.isCliktParameterDefaultStdout, config.logLevel)

		val writer = BufferedWriter(OutputStreamWriter(outputStream, StandardCharsets.UTF_8))

		try {
			inputStream.use {
				writer.use {
					pipelineRunner(config, inputStream, writer, updateStreams)
				}
			}
			logger.info { "Stream processing done" }
		} catch (e: Throwable) {
			logger.error(e) { e.message }
			throw ProgramResult(1)
		}
	}

	companion object {
		internal val loggingConfigurer = LoggingConfigurer("com.biron.singerTargetClickhouse")
	}
}

fun main(args: Array<String>) = RootCommand().main(args)
