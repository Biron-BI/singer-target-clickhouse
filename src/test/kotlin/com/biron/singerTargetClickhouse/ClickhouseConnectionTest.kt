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
import io.kotest.matchers.longs.shouldBeGreaterThan
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
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread
import kotlin.system.measureTimeMillis

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

			captured.captured shouldContain """table = 'o\'brien'"""
		}

		should("getPartitionKey reads system.tables for the configured database and table") {
			val runQuery = mockk<QueryRunner>()
			every {
				runQuery(any(), match { it.contains("system.tables") && it.contains("database = 'db'") && it.contains("name = 'box'") }, 2)
			} returns QueryResult(listOf(listOf("toYYYYMM(ts)")), 1)

			aUnderTest(runQuery = runQuery).getPartitionKey("box") shouldBe "toYYYYMM(ts)"
		}

		should("getPartitionKey returns an empty key when the table is unknown") {
			val runQuery = mockk<QueryRunner>()
			every { runQuery(any(), any(), 2) } returns QueryResult(emptyList(), 0)

			aUnderTest(runQuery = runQuery).getPartitionKey("box") shouldBe ""
		}

		should("formatExpression formats the expression as a quoted SELECT and strips the SELECT keyword") {
			val runQuery = mockk<QueryRunner>()
			every {
				runQuery(any(), """SELECT formatQuerySingleLine('SELECT formatDateTime(ts, \'%Y\')')""", 2)
			} returns QueryResult(listOf(listOf("SELECT formatDateTime(ts, '%Y')")), 1)

			aUnderTest(runQuery = runQuery).formatExpression("formatDateTime(ts, '%Y')") shouldBe "formatDateTime(ts, '%Y')"
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
			urlString shouldContain "max_partitions_per_insert_block=1000"

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

		should("reads the partition key flag from the fourth column, absent meaning false") {
			underTest(
				QueryResult(
					listOf(
						listOf<Any?>("ts", "Int64", 0, 1),
						listOf<Any?>("id", "String", 1, false),
						row("name", "String", false),
					), 3
				)
			).map { it.isInPartitionKey } shouldContainExactly listOf(true, false, false)
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
			val writer = DefaultRowWriterFactory(InsertBodyBudget(1024))(client, java.net.URI.create("http://127.0.0.1:1/insert"), "Basic test")
			writer.shouldBeInstanceOf<HttpStreamingRowWriter>()
		}
	}

	context("InsertBodyBudget") {
		should("takes 1/8 of the max heap, capped at 64 MiB") {
			InsertBodyBudget.forMaxHeap(256L * MIB).capacityBytes shouldBe 32L * MIB
			InsertBodyBudget.forMaxHeap(4096L * MIB).capacityBytes shouldBe 64L * MIB
		}

		should("refuses a reservation that does not fit until bytes are released") {
			val underTest = InsertBodyBudget(10)
			underTest.tryReserve(8, 0) shouldBe true
			underTest.tryReserve(4, 50) shouldBe false
			underTest.release(8)
			underTest.tryReserve(4, 0) shouldBe true
		}

		should("always grants a reservation when nothing is queued, even above capacity") {
			InsertBodyBudget(10).tryReserve(100, 0) shouldBe true
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

		should("close() gives up and aborts the request once the upload makes no progress for the idle timeout") {
			val body = BlockingQueueInputStream()
			body.put("never read".toByteArray())
			val onCloseCalls = AtomicInteger()
			val onAbortCalls = AtomicInteger()
			val underTest = HttpStreamingRowWriter(
				body,
				CompletableFuture<HttpResponse<String>>(), // never answers
				onClose = { onCloseCalls.incrementAndGet() },
				onAbort = { onAbortCalls.incrementAndGet() },
				closeIdleTimeoutMs = 200,
			)

			shouldThrow<IllegalStateException> { underTest.close() }.apply {
				message shouldContain "before server responded"
				cause?.message shouldBe "no progress for 200 ms"
			}
			onAbortCalls.get() shouldBe 1
			onCloseCalls.get() shouldBe 0
		}

		should("close() keeps waiting past the idle timeout while the upload makes progress") {
			val body = BlockingQueueInputStream()
			body.put(ByteArray(8))
			val pending = CompletableFuture<HttpResponse<String>>()
			val onAbortCalls = AtomicInteger()
			val underTest = HttpStreamingRowWriter(body, pending, onAbort = { onAbortCalls.incrementAndGet() }, closeIdleTimeoutMs = 300)
			// A slow link: one byte every 100 ms, then the server answers once it has read the whole body.
			thread {
				val buf = ByteArray(1)
				while (body.read(buf, 0, 1) != -1) Thread.sleep(100)
				pending.complete(mockResponse(statusCode = 200, body = ""))
			}

			val elapsedMs = measureTimeMillis { underTest.close() }

			elapsedMs shouldBeGreaterThan 300L
			onAbortCalls.get() shouldBe 0
		}

		should("gives back its budget as soon as the request fails, so other streams can write") {
			val budget = InsertBodyBudget(4)
			val pending = CompletableFuture<HttpResponse<String>>()
			val underTest = HttpStreamingRowWriter(BlockingQueueInputStream(budget), pending)
			underTest.write("abcd".toByteArray())
			budget.tryReserve(4, 0) shouldBe false

			pending.completeExceptionally(RuntimeException("connection reset"))

			budget.tryReserve(4, 0) shouldBe true
		}

		should("write() waiting on a full budget fails once its own request ends") {
			val budget = InsertBodyBudget(4)
			budget.tryReserve(4, 0) shouldBe true // another stream holds the whole budget
			val pending = CompletableFuture<HttpResponse<String>>()
			val underTest = HttpStreamingRowWriter(BlockingQueueInputStream(budget), pending)
			val blockedWrite = CompletableFuture.runAsync { underTest.write("ab".toByteArray()) }
			Thread.sleep(200)
			blockedWrite.isDone shouldBe false

			pending.completeExceptionally(RuntimeException("connection reset"))

			shouldThrow<ExecutionException> { blockedWrite.get(5, TimeUnit.SECONDS) }
				.cause?.message shouldContain "mid-stream"
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

		should("put() blocks while the budget is full, until the reader takes the queued bytes") {
			val underTest = BlockingQueueInputStream(InsertBodyBudget(4))
			underTest.put("abcd".toByteArray())
			val blockedPut = CompletableFuture.runAsync { underTest.put("ef".toByteArray()) }
			Thread.sleep(200)
			blockedPut.isDone shouldBe false

			underTest.read() shouldBe 'a'.code

			blockedPut.get(5, TimeUnit.SECONDS)
			underTest.complete()
			String(underTest.readAllBytes()) shouldBe "bcdef"
		}

		should("put() checks that the request is alive while it waits") {
			val underTest = BlockingQueueInputStream(InsertBodyBudget(1))
			underTest.put("a".toByteArray())
			shouldThrow<IllegalStateException> { underTest.put("b".toByteArray()) { error("request ended") } }
				.message shouldBe "request ended"
		}

		should("abandon() gives back the budget of unread bytes and ends the stream") {
			val budget = InsertBodyBudget(10)
			val underTest = BlockingQueueInputStream(budget)
			underTest.put("abcdef".toByteArray())

			underTest.abandon()

			budget.tryReserve(10, 0) shouldBe true
			underTest.read() shouldBe -1
		}
	}
})

private const val MIB = 1024L * 1024

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
