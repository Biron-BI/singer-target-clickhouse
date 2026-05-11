@file:Suppress("SqlNoDataSourceInspection")

package com.biron.singerTargetClickhouse

import arrow.core.right
import com.biron.singerTargetClickhouse.ClickhouseConnection.*
import com.biron.singerTargetClickhouse.ClickhouseConnection.Companion.withRetries
import io.kotest.assertions.arrow.core.shouldBeLeft
import io.kotest.assertions.arrow.core.shouldBeRight
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.types.shouldBeInstanceOf
import io.kotest.matchers.types.shouldBeSameInstanceAs
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import org.springframework.jdbc.core.JdbcTemplate
import java.net.http.HttpResponse
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger

class ClickhouseConnectionTest : ShouldSpec({

	afterTest { checkAndClearAllMocks() }

	val stubCfg = TargetConfig(host = "h", port = 1, username = "u", password = "p", database = "db")
	val jdbc = mockk<JdbcTemplate>() // helpers pass it through; never invoked in these tests

	fun aUnderTest(
		cfg: TargetConfig = stubCfg,
		runQuery: QueryRunner = mockk(),
		addColumn: ColumnAdder = mockk(),
		removeColumn: ColumnRemover = mockk(),
		updateColumn: ColumnUpdater = mockk(),
		listColumnsParser: ListColumnsResultParser = mockk(),
		rowWriterFactory: RowWriterFactory = mockk(),
	): ClickhouseConnection = ClickhouseConnection(
		cfg, runQuery, addColumn, removeColumn, updateColumn, listColumnsParser, rowWriterFactory,
	)

	// ─────────────────────── wiring ───────────────────────
	// Verifies that each TargetConnection method delegates to the injected collaborator with
	// the right arguments. The connection's real jdbc/httpClient are constructed but never
	// invoked, because every collaborator is a mock that ignores them.

	context("wiring") {
		should("runQuery delegates to QueryRunner with the connection's JdbcTemplate, query, retries") {
			val runQuery = mockk<QueryRunner>()
			every { runQuery(any(), "SELECT 42", 3) } returns QueryResult(listOf(listOf(42)), 1)

			aUnderTest(runQuery = runQuery).runQuery("SELECT 42", 3) shouldBe
					QueryResult(listOf(listOf(42)), 1)
		}

		should("listTables runs SHOW TABLES via QueryRunner with retries=2 and maps the first column") {
			val runQuery = mockk<QueryRunner>()
			every { runQuery(any(), "SHOW TABLES", 2) } returns QueryResult(listOf(listOf("a"), listOf("b"), listOf("c")), 3)
			aUnderTest(runQuery = runQuery).listTables() shouldBe listOf("a", "b", "c")
		}

		should("listColumns delegates the result of the system.columns query to ListColumnsResultParser") {
			val rawResult = QueryResult(listOf(listOf("id", "Int32", true)), 1)
			val runQuery = mockk<QueryRunner>()
			every { runQuery(any(), match { it.contains("system.columns") && it.contains("'box'") }, 2) } returns rawResult

			val parser = mockk<ListColumnsResultParser>()
			every { parser(rawResult) } returns listOf(Column("id", "Int32", isInSortingKey = true))

			aUnderTest(runQuery = runQuery, listColumnsParser = parser).listColumns("box") shouldBe
					listOf(Column("id", "Int32", isInSortingKey = true))
		}

		should("listColumns escapes single quotes in the table name before injecting it into the SQL") {
			val captured = slot<String>()
			val runQuery = mockk<QueryRunner>()
			every { runQuery(any(), capture(captured), 2) } returns QueryResult(emptyList(), 0)
			val parser = mockk<ListColumnsResultParser>()
			every { parser(any()) } returns emptyList()

			aUnderTest(runQuery = runQuery, listColumnsParser = parser).listColumns("o'brien")

			// escapeValue doubles the apostrophe with backslashes — proves the table arg flows through that path.
			captured.captured shouldContain """'o\'\brien'"""
		}

		should("addColumn delegates to ColumnAdder with the injected QueryRunner, jdbc, table, newCol") {
			val runQuery = mockk<QueryRunner>()
			val capturedRunner = slot<QueryRunner>()
			val capturedJdbc = slot<JdbcTemplate>()
			val addColumn = mockk<ColumnAdder>()
			val newCol = Column("name", "Nullable(String)", isInSortingKey = false)
			every { addColumn(capture(capturedRunner), capture(capturedJdbc), "box", newCol) } returns Unit.right()

			aUnderTest(runQuery = runQuery, addColumn = addColumn).addColumn("box", newCol).shouldBeRight()

			capturedRunner.captured shouldBeSameInstanceAs runQuery
			capturedJdbc.isCaptured shouldBe true
		}

		should("removeColumn delegates to ColumnRemover with the injected QueryRunner, jdbc, table, existing") {
			val runQuery = mockk<QueryRunner>()
			val capturedRunner = slot<QueryRunner>()
			val removeColumn = mockk<ColumnRemover>()
			val existing = Column("gone", "String", isInSortingKey = false)
			every { removeColumn(capture(capturedRunner), any(), "box", existing) } returns Unit.right()

			aUnderTest(runQuery = runQuery, removeColumn = removeColumn)
				.removeColumn("box", existing).shouldBeRight()

			capturedRunner.captured shouldBeSameInstanceAs runQuery
		}

		should("updateColumn delegates to ColumnUpdater with the injected QueryRunner, jdbc, table, existing, newCol") {
			val runQuery = mockk<QueryRunner>()
			val capturedRunner = slot<QueryRunner>()
			val updateColumn = mockk<ColumnUpdater>()
			val existing = Column("name", "String", isInSortingKey = false)
			val newCol = Column("name", "Nullable(String)", isInSortingKey = false)
			every { updateColumn(capture(capturedRunner), any(), "box", existing, newCol) } returns Unit.right()

			aUnderTest(runQuery = runQuery, updateColumn = updateColumn)
				.updateColumn("box", existing, newCol).shouldBeRight()

			capturedRunner.captured shouldBeSameInstanceAs runQuery
		}

		should("renameObsoleteTable runs the prefixed RENAME via QueryRunner") {
			val runQuery = mockk<QueryRunner>()
			every {
				runQuery(any(), "RENAME TABLE `box` TO `_dropped_box`", 2)
			} returns QueryResult(emptyList(), 0)

			aUnderTest(runQuery = runQuery).renameObsoleteTable("box") shouldBe
					QueryResult(emptyList(), 0)
		}

		should("openRowWriter delegates to RowWriterFactory with the connection's HttpClient, an INSERT URL, and the auth header") {
			val expectedWriter = mockk<RowWriter>()
			val capturedUrl = slot<java.net.URI>()
			val capturedAuth = slot<String>()
			val rowWriterFactory = mockk<RowWriterFactory>()
			every {
				rowWriterFactory(any(), capture(capturedUrl), capture(capturedAuth))
			} returns expectedWriter

			val underTest = aUnderTest(rowWriterFactory = rowWriterFactory)
			underTest.openRowWriter("INSERT INTO box FORMAT JSONCompactEachRow") shouldBeSameInstanceAs expectedWriter

			val urlString = capturedUrl.captured.toString()
			urlString shouldContain "http://h:1/?"
			urlString shouldContain "database=db"
			urlString shouldContain "INSERT+INTO+box"
			urlString shouldContain "input_format_null_as_default=0"
			urlString shouldContain "http_receive_timeout=180"

			// `Basic ` + base64("u:p")
			capturedAuth.captured shouldBe "Basic ${java.util.Base64.getEncoder().encodeToString("u:p".toByteArray())}"
		}

		should("openRowWriter URL-encodes config.insertStreamTimeoutSec when it is overridden") {
			val capturedUrl = slot<java.net.URI>()
			val rowWriterFactory = mockk<RowWriterFactory>()
			every { rowWriterFactory(any(), capture(capturedUrl), any()) } returns mockk()

			aUnderTest(
				cfg = stubCfg.copy(insertStreamTimeoutSec = 90),
				rowWriterFactory = rowWriterFactory,
			).openRowWriter("INSERT INTO x FORMAT JSONCompactEachRow")

			capturedUrl.captured.toString() shouldContain "http_receive_timeout=90"
		}

		should("getDatabase returns the configured database name without consulting any collaborator") {
			val runQuery = mockk<QueryRunner>() // strict mock — would fail if used
			aUnderTest(runQuery = runQuery).getDatabase() shouldBe "db"
		}
	}

	context("withRetries") {
		should("returns immediately on first success") {
			val attempts = AtomicInteger()
			val sleeps = mutableListOf<Long>()
			val result = withRetries(retries = 3, sleeper = { sleeps += it }) {
				attempts.incrementAndGet()
				"ok"
			}
			result shouldBe "ok"
			attempts.get() shouldBe 1
			sleeps shouldBe emptyList()
		}

		should("retries until the block succeeds and applies exponential backoff") {
			val attempts = AtomicInteger()
			val sleeps = mutableListOf<Long>()
			val result = withRetries(retries = 3, factor = 2, minTimeoutMs = 100, sleeper = { sleeps += it }) {
				if (attempts.incrementAndGet() < 3) error("transient")
				"recovered"
			}
			result shouldBe "recovered"
			attempts.get() shouldBe 3
			// First failure → sleep(100); second failure → sleep(200).
			sleeps shouldContainExactly listOf(100L, 200L)
		}

		should("throws the last error after exhausting retries") {
			val attempts = AtomicInteger()
			val sleeps = mutableListOf<Long>()
			val ex = shouldThrow<IllegalStateException> {
				withRetries(retries = 2, factor = 4, minTimeoutMs = 50, sleeper = { sleeps += it }) {
					attempts.incrementAndGet()
					error("boom #${attempts.get()}")
				}
			}
			ex.message shouldBe "boom #3"
			attempts.get() shouldBe 3 // initial + 2 retries
			sleeps shouldContainExactly listOf(50L, 200L) // 50*4^0, 50*4^1
		}

		should("retries=0 makes a single attempt and rethrows") {
			val attempts = AtomicInteger()
			val sleeps = mutableListOf<Long>()
			shouldThrow<IllegalStateException> {
				withRetries(retries = 0, sleeper = { sleeps += it }) {
					attempts.incrementAndGet()
					error("nope")
				}
			}
			attempts.get() shouldBe 1
			sleeps shouldBe emptyList()
		}
	}

	context("DefaultListColumnsResultParser") {
		val underTest = DefaultListColumnsResultParser

		fun row(name: String, type: String, isInSorting: Any?) = listOf<Any?>(name, type, isInSorting)

		should("treats Boolean true/false as the sorting flag") {
			underTest(
				QueryResult(
					listOf(
						row("id", "Int32", true),
						row("name", "String", false),
					), 2
				)
			) shouldContainExactly listOf(
				Column("id", "Int32", isInSortingKey = true),
				Column("name", "String", isInSortingKey = false),
			)
		}

		should("treats Number 0 as false and any other number as true") {
			underTest(
				QueryResult(
					listOf(
						row("a", "Int32", 1),
						row("b", "Int32", 0),
						row("c", "Int32", 42L),
						row("d", "Int32", 0.toShort()),
					), 4
				)
			).map { it.isInSortingKey } shouldContainExactly listOf(true, false, true, false)
		}

		should("treats null as not-in-sorting-key") {
			underTest(QueryResult(listOf(row("id", "Int32", null)), 1)).single().isInSortingKey shouldBe false
		}

		should("parses string values via toBoolean()") {
			underTest(
				QueryResult(
					listOf(
						row("a", "Int32", "true"),
						row("b", "Int32", "false"),
						row("c", "Int32", "TRUE"),
						row("d", "Int32", "anything-else"),
					), 4
				)
			).map { it.isInSortingKey } shouldContainExactly listOf(true, false, true, false)
		}

		should("converts non-string column data to strings") {
			underTest(QueryResult(listOf(listOf<Any?>(123, 456, true)), 1)) shouldContainExactly listOf(
				Column("123", "456", isInSortingKey = true),
			)
		}

		should("returns empty for empty data") {
			underTest(QueryResult(emptyList(), 0)) shouldBe emptyList()
		}
	}

	context("DefaultColumnAdder") {
		val underTest = DefaultColumnAdder

		should("returns Right and runs the ADD COLUMN with retries=2 on success") {
			val calls = mutableListOf<Triple<JdbcTemplate, String, Int>>()
			val runner = QueryRunner { db, sql, retries ->
				calls += Triple(db, sql, retries); QueryResult(emptyList(), 0)
			}
			underTest(runner, jdbc, "tbl", Column("name", "String", isInSortingKey = false)).shouldBeRight()
			calls.single() shouldBe Triple(jdbc, "ALTER TABLE tbl ADD COLUMN `name` String", 2)
		}

		should("returns Left wrapping the underlying error") {
			val runner = QueryRunner { _, _, _ -> error("denied") }
			val err = underTest(runner, jdbc, "tbl", Column("x", "Int32", isInSortingKey = false)).shouldBeLeft()
			err.newCol.name shouldBe "x"
			err.error.message shouldBe "denied"
		}
	}

	context("DefaultColumnRemover") {
		val underTest = DefaultColumnRemover

		should("returns Right and runs the DROP COLUMN on success") {
			val calls = mutableListOf<String>()
			val runner = QueryRunner { _, sql, _ -> calls += sql; QueryResult(emptyList(), 0) }
			underTest(runner, jdbc, "tbl", Column("legacy", "String", isInSortingKey = false)).shouldBeRight()
			calls.single() shouldBe "ALTER TABLE tbl DROP COLUMN `legacy`"
		}

		should("returns Left on failure") {
			val runner = QueryRunner { _, _, _ -> error("locked") }
			val err = underTest(runner, jdbc, "tbl", Column("legacy", "String", isInSortingKey = false)).shouldBeLeft()
			err.existing.name shouldBe "legacy"
		}
	}

	context("DefaultColumnUpdater") {
		val underTest = DefaultColumnUpdater
		val existing = Column("name", "String", isInSortingKey = false)
		val newCol = Column("name", "Nullable(String)", isInSortingKey = false)

		should("returns Right and only issues the MODIFY query on success") {
			val calls = mutableListOf<Pair<String, Int>>()
			val runner = QueryRunner { _, sql, retries -> calls += sql to retries; QueryResult(emptyList(), 0) }
			underTest(runner, jdbc, "tbl", existing, newCol).shouldBeRight()
			calls.single() shouldBe ("ALTER TABLE tbl MODIFY COLUMN `name` Nullable(String)" to 0)
		}

		should("issues the revert query when the modify fails and still returns Left") {
			val calls = mutableListOf<Pair<String, Int>>()
			val runner = QueryRunner { _, sql, retries ->
				calls += sql to retries
				if (sql.contains("Nullable")) error("modify failed")
				QueryResult(emptyList(), 0)
			}
			val err = underTest(runner, jdbc, "tbl", existing, newCol).shouldBeLeft()

			err.existing shouldBe existing
			err.newCol shouldBe newCol
			err.error.message shouldBe "modify failed"
			calls shouldHaveSize 2
			calls[0] shouldBe ("ALTER TABLE tbl MODIFY COLUMN `name` Nullable(String)" to 0)
			calls[1] shouldBe ("ALTER TABLE tbl MODIFY COLUMN `name` String" to 2)
		}

		should("swallows revert errors and still returns Left from the original failure") {
			val calls = mutableListOf<String>()
			val runner = QueryRunner { _, sql, _ -> calls += sql; error("everything is on fire") }
			val err = underTest(runner, jdbc, "tbl", existing, newCol).shouldBeLeft()
			err.error.message shouldBe "everything is on fire"
			calls shouldHaveSize 2
		}
	}

	context("DefaultRowWriterFactory") {
		should("delegates to HttpStreamingRowWriter.open") {
			// Use a real HttpClient that won't actually connect — the factory just dispatches the
			// request asynchronously. We're verifying the factory wires (url, auth, client) through
			// without throwing.
			val client = java.net.http.HttpClient.newHttpClient()
			val writer = DefaultRowWriterFactory(client, java.net.URI.create("http://127.0.0.1:1/insert"), "Basic test")
			writer.shouldBeInstanceOf<HttpStreamingRowWriter>()
		}
	}

	context("HttpStreamingRowWriter") {
		should("close() throws when the response status is non-2xx") {
			val body = BlockingQueueInputStream()
			val future = CompletableFuture.completedFuture(mockResponse(statusCode = 500, body = "internal error"))
			val underTest = HttpStreamingRowWriter(body, future)

			shouldThrow<IllegalStateException> { underTest.close() }
				.message shouldContain "ClickHouse insert failed (500)"
		}

		should("close() returns silently for a successful 2xx response") {
			val body = BlockingQueueInputStream()
			val future = CompletableFuture.completedFuture(mockResponse(statusCode = 200, body = "ok"))
			val underTest = HttpStreamingRowWriter(body, future)
			underTest.close()
		}

		should("close() is idempotent") {
			val body = BlockingQueueInputStream()
			val future = CompletableFuture.completedFuture(mockResponse(statusCode = 204, body = ""))
			val underTest = HttpStreamingRowWriter(body, future)
			underTest.close()
			underTest.close() // second call should be a no-op, not throw
		}

		should("close() wraps an ExecutionException as 'ClickHouse insert failed'") {
			val body = BlockingQueueInputStream()
			val failed = CompletableFuture<HttpResponse<String>>()
			failed.completeExceptionally(RuntimeException("network glitch"))
			val underTest = HttpStreamingRowWriter(body, failed)

			shouldThrow<IllegalStateException> { underTest.close() }.apply {
				message shouldContain "ClickHouse insert failed"
				cause?.message shouldBe "network glitch"
			}
		}

		should("write() detects mid-stream rejection by a completed bad-status future") {
			val body = BlockingQueueInputStream()
			val future = CompletableFuture.completedFuture(mockResponse(statusCode = 400, body = "rejected"))
			val underTest = HttpStreamingRowWriter(body, future)

			shouldThrow<IllegalStateException> { underTest.write("row\n".toByteArray()) }
				.message shouldContain "ClickHouse insert completed prematurely (400)"
		}

		should("write() surfaces an ExecutionException as 'mid-stream' failure") {
			val body = BlockingQueueInputStream()
			val failed = CompletableFuture<HttpResponse<String>>()
			failed.completeExceptionally(RuntimeException("connection reset"))
			val underTest = HttpStreamingRowWriter(body, failed)

			shouldThrow<IllegalStateException> { underTest.write("row\n".toByteArray()) }.apply {
				message shouldContain "mid-stream"
				cause?.message shouldBe "connection reset"
			}
		}

		should("write() forwards bytes to the body queue when the future is still in-flight") {
			val body = BlockingQueueInputStream()
			val future = CompletableFuture<HttpResponse<String>>() // never completes
			val underTest = HttpStreamingRowWriter(body, future)

			underTest.write("hello\n".toByteArray())
			underTest.write("world\n".toByteArray())

			body.complete()
			val sink = java.io.ByteArrayOutputStream()
			body.copyTo(sink)
			sink.toString(Charsets.UTF_8) shouldBe "hello\nworld\n"
		}

		should("close() invokes onClose exactly once on a successful response") {
			val body = BlockingQueueInputStream()
			val future = CompletableFuture.completedFuture(mockResponse(statusCode = 200, body = "ok"))
			val onCloseCalls = AtomicInteger()
			val underTest = HttpStreamingRowWriter(body, future, onClose = { onCloseCalls.incrementAndGet() })

			underTest.close()

			onCloseCalls.get() shouldBe 1
		}

		should("close() invokes onClose even when the response status is non-2xx") {
			val body = BlockingQueueInputStream()
			val future = CompletableFuture.completedFuture(mockResponse(statusCode = 500, body = "internal error"))
			val onCloseCalls = AtomicInteger()
			val underTest = HttpStreamingRowWriter(body, future, onClose = { onCloseCalls.incrementAndGet() })

			shouldThrow<IllegalStateException> { underTest.close() }

			onCloseCalls.get() shouldBe 1
		}

		should("close() invokes onClose even when the response future failed exceptionally") {
			val body = BlockingQueueInputStream()
			val failed = CompletableFuture<HttpResponse<String>>()
			failed.completeExceptionally(RuntimeException("network glitch"))
			val onCloseCalls = AtomicInteger()
			val underTest = HttpStreamingRowWriter(body, failed, onClose = { onCloseCalls.incrementAndGet() })

			shouldThrow<IllegalStateException> { underTest.close() }

			onCloseCalls.get() shouldBe 1
		}

		should("close() does not invoke onClose a second time when called twice") {
			val body = BlockingQueueInputStream()
			val future = CompletableFuture.completedFuture(mockResponse(statusCode = 204, body = ""))
			val onCloseCalls = AtomicInteger()
			val underTest = HttpStreamingRowWriter(body, future, onClose = { onCloseCalls.incrementAndGet() })

			underTest.close()
			underTest.close()

			onCloseCalls.get() shouldBe 1
		}
	}

	context("BlockingQueueInputStream") {
		should("returns -1 once complete() is called and the queue is drained") {
			val underTest = BlockingQueueInputStream()
			underTest.put("ab".toByteArray())
			underTest.complete()
			underTest.read() shouldBe 'a'.code
			underTest.read() shouldBe 'b'.code
			underTest.read() shouldBe -1
		}

		should("supports the (b, off, len) read variant") {
			val underTest = BlockingQueueInputStream()
			underTest.put("abcdef".toByteArray())
			underTest.complete()
			val buf = ByteArray(10)
			val n1 = underTest.read(buf, 2, 4)
			n1 shouldBe 4
			String(buf, 2, n1) shouldBe "abcd"
		}

		should("read(buf, _, 0) returns 0 without consuming") {
			BlockingQueueInputStream().read(ByteArray(4), 0, 0) shouldBe 0
		}

		should("ignores empty puts") {
			val underTest = BlockingQueueInputStream()
			underTest.put(ByteArray(0))
			underTest.complete()
			underTest.read() shouldBe -1
		}

		should("ignores puts after complete") {
			val underTest = BlockingQueueInputStream()
			underTest.complete()
			underTest.put("ignored".toByteArray())
			underTest.read() shouldBe -1
		}

		should("complete() called twice is a no-op") {
			val underTest = BlockingQueueInputStream()
			underTest.complete()
			underTest.complete()
			underTest.read() shouldBe -1
		}
	}
})

private fun mockResponse(statusCode: Int, body: String): HttpResponse<String> = object : HttpResponse<String> {
	override fun statusCode() = statusCode
	override fun request() = throw UnsupportedOperationException()
	override fun previousResponse() = java.util.Optional.empty<HttpResponse<String>>()
	override fun headers() = java.net.http.HttpHeaders.of(emptyMap()) { _, _ -> true }
	override fun body() = body
	override fun sslSession() = java.util.Optional.empty<javax.net.ssl.SSLSession>()
	override fun uri() = java.net.URI.create("http://test")
	override fun version() = java.net.http.HttpClient.Version.HTTP_1_1
}
