package com.biron.singerTargetClickhouse

import com.biron.singer.core.logging.LoggingConfigurer
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.ProgramResult
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.multiple
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.types.inputStream
import com.github.ajalt.clikt.parameters.types.outputStream
import com.github.ajalt.clikt.parameters.types.path
import io.github.oshai.kotlinlogging.KotlinLogging
import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets
import kotlin.io.path.reader

private val logger = KotlinLogging.logger {}

class RootCommand : CliktCommand(name = "target-clickhouse") {
	private val configPath by option("--config")
		.path(mustExist = true, canBeDir = false, mustBeReadable = true)
		.required()

	private val inputStream by option("--input", help = "An alternate file to read from, instead of STDIN")
		.inputStream()

	private val outputStream by option("--output", help = "An alternate file to write to, instead of STDOUT")
		.outputStream(truncateExisting = true)

	private val updateStreams by option(
		"-u", "--update-streams",
		help = "Schema whose root and children tables will be dropped / recreated on SCHEMA messages",
	).multiple()

	/** no-op flag kept for TS-CLI parity; log verbosity is driven by config.logging_level */
	@Suppress("unused")
	private val verbose by option("--verbose").flag()

	override fun run() {
		val config = configPath.reader(StandardCharsets.UTF_8).use { TargetConfig.fromJson(it) }
		loggingConfigurer.reconfigure(useStdoutInsteadOfStdErr = false, basePackageLogLevel = config.logLevel)

		val stdin = inputStream ?: System.`in`
		val stdout = outputStream ?: System.out

		val reader = BufferedReader(InputStreamReader(stdin, StandardCharsets.UTF_8))
		val writer = BufferedWriter(OutputStreamWriter(stdout, StandardCharsets.UTF_8))

		try {
			reader.use {
				writer.use {
					processStream(reader, config, writer, updateStreams)
				}
			}
			logger.info { "Stream processing done" }
		} catch (e: Throwable) {
			logger.error(e) { "${e.message}" }
			throw ProgramResult(1)
		}
	}

	companion object {
		internal val loggingConfigurer = LoggingConfigurer("com.biron.singerTargetClickhouse")
	}
}

fun main(args: Array<String>) = RootCommand().main(args)
