package com.biron.singerTargetClickhouse

import com.github.ajalt.clikt.core.ProgramResult
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.PrintStream
import kotlin.io.path.createTempDirectory
import kotlin.io.path.writeText

class MainTest : ShouldSpec({

	val configJson = """
		{"host":"h","port":8123,"username":"u","password":"p","database":"d"}
	""".trimIndent()

	fun writeConfigFile(json: String = configJson): File {
		val dir = createTempDirectory("singer-config-")
		val cfg = dir.resolve("config.json")
		cfg.writeText(json)
		return cfg.toFile()
	}

	data class Recorded(val config: TargetConfig, val input: String, val streams: List<String>)

	class RecordingRunner {
		val calls: MutableList<Recorded> = mutableListOf()
		var onCall: () -> Unit = {}

		val asRunner: PipelineRunner = { config, input, writer, streams ->
			val payload = input.readBytes().toString(Charsets.UTF_8)
			calls += Recorded(config, payload, streams)
			writer.write("recorded:$payload")
			onCall()
		}
	}

	context("CLI option parsing") {
		should("invokes the pipeline with the parsed config and update-streams") {
			val cfg = writeConfigFile()
			val runner = RecordingRunner()
			val cmd = RootCommand(runner.asRunner)

			cmd.parse(
				arrayOf(
					"--config", cfg.absolutePath,
					"--input", makeInputFile("hello").absolutePath,
					"--output", makeOutputFile().absolutePath,
					"-u", "users",
					"--update-streams", "orders",
				),
			)

			runner.calls.size shouldBe 1
			val call = runner.calls.single()
			call.config.host shouldBe "h"
			call.config.database shouldBe "d"
			call.input shouldBe "hello"
			call.streams shouldContainExactly listOf("users", "orders")
		}

		should("accepts -c as a short alias for --config") {
			val cfg = writeConfigFile()
			val runner = RecordingRunner()
			RootCommand(runner.asRunner).parse(
				arrayOf(
					"-c", cfg.absolutePath,
					"--input", makeInputFile("").absolutePath,
					"--output", makeOutputFile().absolutePath,
				),
			)
			runner.calls.single().config.host shouldBe "h"
		}

		should("falls back to System.in / System.out when --input / --output omitted") {
			val cfg = writeConfigFile()
			val runner = RecordingRunner()

			val originalIn = System.`in`
			val originalOut = System.out
			val captured = ByteArrayOutputStream()
			try {
				System.setIn(ByteArrayInputStream("from-stdin".toByteArray()))
				System.setOut(PrintStream(captured))

				RootCommand(runner.asRunner).parse(arrayOf("--config", cfg.absolutePath))
			} finally {
				System.setIn(originalIn)
				System.setOut(originalOut)
			}

			runner.calls.single().input shouldBe "from-stdin"
			captured.toString(Charsets.UTF_8) shouldBe "recorded:from-stdin"
		}

		should("accepts the no-op --verbose flag") {
			val cfg = writeConfigFile()
			val runner = RecordingRunner()
			RootCommand(runner.asRunner).parse(
				arrayOf(
					"--config", cfg.absolutePath,
					"--input", makeInputFile("").absolutePath,
					"--output", makeOutputFile().absolutePath,
					"--verbose",
				),
			)
			runner.calls.size shouldBe 1
		}

		should("defaults update-streams to empty when not provided") {
			val cfg = writeConfigFile()
			val runner = RecordingRunner()
			RootCommand(runner.asRunner).parse(
				arrayOf(
					"--config", cfg.absolutePath,
					"--input", makeInputFile("").absolutePath,
					"--output", makeOutputFile().absolutePath,
				),
			)
			runner.calls.single().streams shouldBe emptyList()
		}

		should("re-reads the config snake_case fields") {
			val richConfig = """
				{"host":"h","port":8123,"username":"u","password":"p","database":"d",
				 "batch_size":42,"translate_values":true}
			""".trimIndent()
			val cfg = writeConfigFile(richConfig)
			val runner = RecordingRunner()
			RootCommand(runner.asRunner).parse(
				arrayOf(
					"--config", cfg.absolutePath,
					"--input", makeInputFile("").absolutePath,
					"--output", makeOutputFile().absolutePath,
				),
			)
			runner.calls.single().config.batchSize shouldBe 42
			runner.calls.single().config.translateValues shouldBe true
		}
	}

	context("error path") {
		should("wraps a pipeline failure into ProgramResult(1)") {
			val cfg = writeConfigFile()
			val runner = RecordingRunner().apply {
				onCall = { throw IllegalStateException("synthetic pipeline failure") }
			}
			val cmd = RootCommand(runner.asRunner)

			val result = shouldThrow<ProgramResult> {
				cmd.parse(
					arrayOf(
						"--config", cfg.absolutePath,
						"--input", makeInputFile("").absolutePath,
						"--output", makeOutputFile().absolutePath,
					),
				)
			}
			result.statusCode shouldBe 1
		}
	}

	context("Clikt option validation") {
		should("rejects a missing --config") {
			val ex = shouldThrow<Exception> {
				RootCommand(RecordingRunner().asRunner).parse(emptyArray())
			}
			// Clikt throws a com.github.ajalt.clikt.core.MissingOption (a UsageError subclass).
			ex.shouldBeInstanceOf<com.github.ajalt.clikt.core.UsageError>()
		}

		should("rejects a non-existent --config path") {
			val ex = shouldThrow<Exception> {
				RootCommand(RecordingRunner().asRunner).parse(arrayOf("--config", "/no/such/file"))
			}
			ex.shouldBeInstanceOf<com.github.ajalt.clikt.core.UsageError>()
		}
	}

	context("default constructor") {
		should("constructs a RootCommand wired to the real pipeline runner") {
			// Just verify construction doesn't throw — exercising the default-arg path.
			RootCommand()
		}
	}
})

private fun makeInputFile(content: String): File {
	val dir = createTempDirectory("singer-in-")
	val f = dir.resolve("input.jsonl")
	f.writeText(content)
	return f.toFile()
}

private fun makeOutputFile(): File {
	val dir = createTempDirectory("singer-out-")
	return dir.resolve("output.jsonl").toFile()
}
