package com.biron.singerTargetClickhouse

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import java.io.InputStream
import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.math.pow

private val logger = KotlinLogging.logger {}

class ClickhouseConnection internal constructor(
	private val config: TargetConfig,
	private val runQuery: QueryRunner,
	private val addColumnOp: ColumnAdder,
	private val removeColumnOp: ColumnRemover,
	private val updateColumnOp: ColumnUpdater,
	private val listColumnsParser: ListColumnsResultParser,
	private val rowWriterFactory: RowWriterFactory,
) : TargetConnection {

	constructor(config: TargetConfig) : this(
		config = config,
		runQuery = DefaultQueryRunner(),
		addColumnOp = DefaultColumnAdder,
		removeColumnOp = DefaultColumnRemover,
		updateColumnOp = DefaultColumnUpdater,
		listColumnsParser = DefaultListColumnsResultParser,
		rowWriterFactory = DefaultRowWriterFactory(InsertBodyBudget.forMaxHeap()),
	)

	private val dataSource: DriverManagerDataSource = DriverManagerDataSource(
		buildJdbcUrl(config),
		config.username,
		config.password,
	)
	private val jdbc: JdbcTemplate = JdbcTemplate(dataSource)

	// Shared across the throwaway HttpClient built for each insert — cheap to reuse, expensive
	// to recreate. The HttpClient itself is not shared: see openRowWriter for why.
	private val httpExecutor = Executors.newCachedThreadPool { r ->
		Thread(r, "ch-http-worker-${threadSeq.getAndIncrement()}").apply { isDaemon = true }
	}

	private val baseUrl = "http://${config.host}:${config.port}"
	private val authHeader = "Basic " + Base64.getEncoder()
		.encodeToString("${config.username}:${config.password}".toByteArray(StandardCharsets.UTF_8))

	override fun getDatabase(): String = config.database

	override fun runQuery(query: String, retries: Int): QueryResult = runQuery(jdbc, query, retries)

	override fun listTables(): List<String> =
		runQuery(jdbc, "SHOW TABLES", 2).data.map { it[0].toString() }

	override fun listColumns(table: String): List<Column> = listColumnsParser(
		runQuery(
			jdbc,
			"""SELECT name, type, is_in_sorting_key, is_in_partition_key
			   FROM system.columns
			   WHERE database = ${sqlStringLiteral(config.database)} AND table = ${sqlStringLiteral(table)}""".trimIndent(),
			2,
		),
	)

	override fun getPartitionKey(table: String): String =
		runQuery(
			jdbc,
			"""SELECT partition_key
			   FROM system.tables
			   WHERE database = ${sqlStringLiteral(config.database)} AND name = ${sqlStringLiteral(table)}""".trimIndent(),
			2,
		).data.firstOrNull()?.firstOrNull()?.toString().orEmpty()

	// Formatting `SELECT <expression>` yields the same text as system.tables uses for keys
	// (e.g. `INTERVAL 1 MONTH` becomes `toIntervalMonth(1)`, superfluous backquotes are dropped).
	override fun formatExpression(expression: String): String =
		runQuery(jdbc, "SELECT formatQuerySingleLine(${sqlStringLiteral("SELECT $expression")})", 2)
			.data.single().single().toString().removePrefix("SELECT ")

	override fun addColumn(table: String, newCol: Column): Either<AddColumnError, Unit> =
		addColumnOp(runQuery, jdbc, table, newCol)

	override fun removeColumn(table: String, existing: Column): Either<RemoveColumnError, Unit> =
		removeColumnOp(runQuery, jdbc, table, existing)

	override fun updateColumn(table: String, existing: Column, newCol: Column): Either<UpdateColumnError, Unit> =
		updateColumnOp(runQuery, jdbc, table, existing, newCol)

	override fun renameObsoleteTable(table: String): QueryResult {
		logger.info { "[$table] Renaming obsolete table $table" }
		return runQuery(jdbc, "RENAME TABLE `$table` TO `${TargetConnection.DROPPED_TABLE_PREFIX}$table`", 2)
	}

	// Each insert gets its own HttpClient, owned by the returned writer and closed with it.
	// Reasoning: JDK HttpClient pools HTTP/1.1 connections without probing liveness, while
	// ClickHouse closes idle keep-alive sockets after ~10s. A shared pool would eventually
	// hand out a stale socket and fail with `IOException: HTTP/1.1 header parser received
	// no bytes`. Per-call clients side-step the pool entirely.
	override fun openRowWriter(query: String): RowWriter {
		val httpClient = HttpClient.newBuilder()
			.executor(httpExecutor)
			.connectTimeout(Duration.ofSeconds(30))
			.build()
		return rowWriterFactory(httpClient, insertUrl(query), authHeader)
	}

	private fun insertUrl(query: String): URI {
		val params = listOf(
			"database" to config.database,
			"query" to query,
			"mutations_sync" to "2",
			"date_time_input_format" to "best_effort",
			"insert_null_as_default" to "0",
			"input_format_null_as_default" to "0",
			"input_format_defaults_for_omitted_fields" to "0",
			"http_receive_timeout" to config.insertStreamTimeoutSec.toString(),
			// A full-history load into a table partitioned by month can put more than the default 100
			// months in one insert block; 1000 months is never reached. No effect on unpartitioned tables.
			"max_partitions_per_insert_block" to "1000",
		)
		val qs = params.joinToString("&") { (k, v) -> "${encode(k)}=${encode(v)}" }
		return URI.create("$baseUrl/?$qs")
	}

	private fun encode(v: String): String = URLEncoder.encode(v, StandardCharsets.UTF_8)

	// ─────────────────────────── collaborators ───────────────────────────
	/**
	 * Executes a single SQL statement with up to `retries` retries on failure. Stateless:
	 * callers thread their own [JdbcTemplate] through, so the same instance can be reused
	 * across connections (or substituted by tests).
	 */
	internal fun interface QueryRunner {
		operator fun invoke(jdbc: JdbcTemplate, query: String, retries: Int): QueryResult
	}

	internal fun interface ColumnAdder {
		operator fun invoke(
			runQuery: QueryRunner,
			jdbc: JdbcTemplate,
			table: String,
			newCol: Column,
		): Either<AddColumnError, Unit>
	}

	internal fun interface ColumnRemover {
		operator fun invoke(
			runQuery: QueryRunner,
			jdbc: JdbcTemplate,
			table: String,
			existing: Column,
		): Either<RemoveColumnError, Unit>
	}

	internal fun interface ColumnUpdater {
		operator fun invoke(
			runQuery: QueryRunner,
			jdbc: JdbcTemplate,
			table: String,
			existing: Column,
			newCol: Column,
		): Either<UpdateColumnError, Unit>
	}

	internal fun interface ListColumnsResultParser {
		operator fun invoke(result: QueryResult): List<Column>
	}

	internal fun interface RowWriterFactory {
		operator fun invoke(httpClient: HttpClient, url: URI, authHeader: String): RowWriter
	}

	internal class DefaultQueryRunner(
		private val sleeper: (Long) -> Unit = Thread::sleep,
	) : QueryRunner {
		override fun invoke(jdbc: JdbcTemplate, query: String, retries: Int): QueryResult =
			withRetries(retries, sleeper = sleeper) {
				logger.debug { "query sql [$query]" }
				jdbc.execute { conn: java.sql.Connection ->
					conn.createStatement().use { stmt ->
						if (!stmt.execute(query)) QueryResult(emptyList(), 0)
						else readResultSet(stmt.resultSet)
					}
				}!!
			}

		private fun readResultSet(rs: java.sql.ResultSet): QueryResult = rs.use {
			val cols = rs.metaData.columnCount
			val data = buildList {
				while (rs.next()) add(List<Any?>(cols) { rs.getObject(it + 1) })
			}
			QueryResult(data = data, rows = data.size)
		}
	}

	internal object DefaultColumnAdder : ColumnAdder {
		override fun invoke(
			runQuery: QueryRunner,
			jdbc: JdbcTemplate,
			table: String,
			newCol: Column,
		): Either<AddColumnError, Unit> = try {
			logger.info { "[$table] Adding column $table.${newCol.name} ${newCol.type}" }
			runQuery(jdbc, "ALTER TABLE $table ADD COLUMN `${newCol.name}` ${newCol.type}", 2)
			Unit.right()
		} catch (e: Throwable) {
			AddColumnError(newCol, e).left()
		}
	}

	internal object DefaultColumnRemover : ColumnRemover {
		override fun invoke(
			runQuery: QueryRunner,
			jdbc: JdbcTemplate,
			table: String,
			existing: Column,
		): Either<RemoveColumnError, Unit> = try {
			logger.info { "[$table] Removing column $table.${existing.name}" }
			runQuery(jdbc, "ALTER TABLE $table DROP COLUMN `${existing.name}`", 2)
			Unit.right()
		} catch (e: Throwable) {
			RemoveColumnError(existing, e).left()
		}
	}

	internal object DefaultColumnUpdater : ColumnUpdater {
		override fun invoke(
			runQuery: QueryRunner,
			jdbc: JdbcTemplate,
			table: String,
			existing: Column,
			newCol: Column,
		): Either<UpdateColumnError, Unit> = try {
			logger.info { "[$table] Updating column $table.${existing.name} from ${existing.type} to ${newCol.type}" }
			runQuery(jdbc, "ALTER TABLE $table MODIFY COLUMN `${newCol.name}` ${newCol.type}", 0)
			Unit.right()
		} catch (e: Throwable) {
			// Clickhouse may leave the column in a corrupt intermediate state if the mutation cannot apply;
			// revert the definition so we don't poison the table for future runs.
			try {
				runQuery(jdbc, "ALTER TABLE $table MODIFY COLUMN `${existing.name}` ${existing.type}", 2)
			} catch (revertError: Throwable) {
				logger.error(revertError) { "could not revert update" }
			}
			UpdateColumnError(existing, newCol, e).left()
		}
	}

	/**
	 * Decodes a `system.columns` result row into a [Column]. The `is_in_*_key` values can come
	 * back as Boolean, Number, null, or a String depending on the JDBC driver version, so we
	 * normalize each shape here.
	 */
	internal object DefaultListColumnsResultParser : ListColumnsResultParser {
		override fun invoke(result: QueryResult): List<Column> = result.data.map { row ->
			Column(
				name = row[0].toString(),
				type = row[1].toString(),
				isInSortingKey = toBoolean(row[2]),
				isInPartitionKey = toBoolean(row.getOrNull(3)),
			)
		}

		private fun toBoolean(v: Any?): Boolean = when (v) {
			is Boolean -> v
			is Number -> v.toLong() != 0L
			null -> false
			else -> v.toString().toBoolean()
		}
	}

	/** Every writer it opens shares [budget], so the cap holds whatever the number of open insert streams. */
	internal class DefaultRowWriterFactory(private val budget: InsertBodyBudget) : RowWriterFactory {
		override fun invoke(httpClient: HttpClient, url: URI, authHeader: String): RowWriter =
			HttpStreamingRowWriter.open(url = url, authHeader = authHeader, httpClient = httpClient, budget = budget)
	}

	/**
	 * Caps the insert-body bytes queued but not yet taken by the HTTP client, summed over all
	 * open insert streams. Without it nothing makes the target wait for ClickHouse: a tap dumping
	 * a burst faster than ClickHouse ingests piles the backlog up on the heap until
	 * OutOfMemoryError. Once the budget is full, writes block, which in turn stalls the parser
	 * and then the tap on its output pipe, so everything moves at ClickHouse's pace.
	 */
	internal class InsertBodyBudget(val capacityBytes: Long) {
		private val lock = ReentrantLock()
		private val released = lock.newCondition()
		private var queuedBytes = 0L

		/**
		 * Reserves [size] bytes, waiting at most [timeoutMs] for room. Always granted when nothing
		 * is queued, so a chunk larger than the whole budget cannot wait forever.
		 */
		fun tryReserve(size: Int, timeoutMs: Long): Boolean {
			lock.withLock {
				var remainingNs = TimeUnit.MILLISECONDS.toNanos(timeoutMs)
				while (queuedBytes > 0 && queuedBytes + size > capacityBytes) {
					if (remainingNs <= 0) return false
					remainingNs = released.awaitNanos(remainingNs)
				}
				queuedBytes += size
				return true
			}
		}

		fun release(size: Int) {
			lock.withLock {
				queuedBytes -= size
				released.signalAll()
			}
		}

		companion object {
			private const val MAX_CAPACITY_BYTES = 64L * 1024 * 1024

			/**
			 * 1/8 of the max heap, leaving the rest to the parse queue, the row batches and their
			 * serialization. Capped because a bigger buffer buys no throughput: once the target
			 * outruns ClickHouse the buffer stays full either way, and it only has to absorb jitter.
			 */
			fun forMaxHeap(maxHeapBytes: Long = Runtime.getRuntime().maxMemory()): InsertBodyBudget =
				InsertBodyBudget(minOf(maxHeapBytes / 8, MAX_CAPACITY_BYTES))
					.also { logger.info { "insert buffers capped at ${it.capacityBytes / (1024 * 1024)} MiB" } }
		}
	}

	/**
	 * InputStream backed by a blocking queue of byte arrays. Unlike PipedInputStream,
	 * it does **not** track reader-thread identity, which matters because
	 * HttpClient.BodyPublishers.ofInputStream pulls from arbitrary executor threads
	 * and recycles them between batches — PipedInputStream would then raise
	 * "Read end dead" on the next write after the original read thread died.
	 *
	 * Queued bytes count against [budget] until the reader takes them.
	 */
	internal class BlockingQueueInputStream(
		private val budget: InsertBodyBudget = InsertBodyBudget(Long.MAX_VALUE),
	) : InputStream() {
		private val queue = LinkedBlockingQueue<ByteArray>()
		private val enqueueLock = Any()
		private var current: ByteArray = EMPTY
		private var pos: Int = 0

		@Volatile
		private var completed: Boolean = false

		/** [System.nanoTime] of the last read call that returned: how [HttpStreamingRowWriter.close] tells a slow upload from a stalled one. */
		@Volatile
		var lastReadNanos: Long = System.nanoTime()
			private set

		/**
		 * Blocks while [budget] is full. [checkAlive] runs between waits so that a request the
		 * server already ended surfaces as an error instead of a hang.
		 */
		fun put(bytes: ByteArray, checkAlive: () -> Unit = {}) {
			if (completed || bytes.isEmpty()) return
			while (!budget.tryReserve(bytes.size, RESERVE_WAIT_MS)) checkAlive()
			synchronized(enqueueLock) {
				if (completed) budget.release(bytes.size) else queue.put(bytes)
			}
		}

		fun complete() {
			synchronized(enqueueLock) {
				if (completed) return
				completed = true
				queue.put(EOF)
			}
		}

		/**
		 * Ends the stream and gives back the budget of everything still queued. Only once the
		 * request is over: dropping bytes a live request still reads would truncate the insert.
		 */
		fun abandon() {
			synchronized(enqueueLock) {
				completed = true
				while (true) {
					val bytes = queue.poll() ?: break
					if (bytes !== EOF) budget.release(bytes.size)
				}
				// Wakes up a reader still parked in take().
				queue.put(EOF)
			}
		}

		override fun read(): Int {
			val byte = if (ensureAvailable()) current[pos++].toInt() and 0xFF else -1
			lastReadNanos = System.nanoTime()
			return byte
		}

		override fun read(b: ByteArray, off: Int, len: Int): Int {
			if (len == 0) return 0
			val n = if (ensureAvailable()) minOf(len, current.size - pos) else -1
			if (n > 0) {
				System.arraycopy(current, pos, b, off, n)
				pos += n
			}
			lastReadNanos = System.nanoTime()
			return n
		}

		private fun ensureAvailable(): Boolean {
			while (pos >= current.size) {
				val next = queue.take()
				if (next === EOF) return false
				budget.release(next.size)
				current = next
				pos = 0
			}
			return true
		}

		companion object {
			private const val RESERVE_WAIT_MS = 1_000L
			private val EMPTY = ByteArray(0)
			private val EOF = ByteArray(0)
		}
	}

	/**
	 * [onClose] releases the HTTP client once the request is over. [onAbort] cancels a request
	 * still running when [close] gives up on it: `HttpClient.close()` would wait for it to
	 * complete instead, with no time limit.
	 */
	internal class HttpStreamingRowWriter internal constructor(
		private val body: BlockingQueueInputStream,
		private val responseFuture: CompletableFuture<HttpResponse<String>>,
		private val onClose: () -> Unit = {},
		private val onAbort: () -> Unit = {},
		private val closeIdleTimeoutMs: Long = CLOSE_IDLE_TIMEOUT_MS,
	) : RowWriter {

		private var closed = false

		init {
			// Once the request is over nobody reads the body anymore: give back its budget right
			// away, otherwise a failed stream would block the writes of every other stream.
			responseFuture.whenComplete { _, _ -> body.abandon() }
		}

		companion object {
			private const val CLOSE_IDLE_TIMEOUT_MS = 30_000L

			// No per-request timeout: one insert stream can stay open for the whole ingestion
			// of a stream (millions of rows). Idle protection is handled on the caller side by
			// RecordProcessor's auto-end timeout, which closes the stream after inactivity.
			fun open(url: URI, authHeader: String, httpClient: HttpClient, budget: InsertBodyBudget): HttpStreamingRowWriter {
				val body = BlockingQueueInputStream(budget)
				val request = HttpRequest.newBuilder(url)
					.header("Authorization", authHeader)
					.header("Content-Type", "application/octet-stream")
					.POST(HttpRequest.BodyPublishers.ofInputStream { body })
					.build()
				val responseFuture = httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
				return HttpStreamingRowWriter(body, responseFuture, onClose = httpClient::close, onAbort = httpClient::shutdownNow)
			}
		}

		override fun write(bytes: ByteArray) {
			failIfRequestEnded()
			body.put(bytes, checkAlive = ::failIfRequestEnded)
		}

		// If the server rejected the request mid-stream, surface the error now instead of
		// silently dropping rows into a queue nobody is draining.
		private fun failIfRequestEnded() {
			if (responseFuture.isDone) {
				try {
					val resp = responseFuture.get(0, TimeUnit.SECONDS)
					error("ClickHouse insert completed prematurely (${resp.statusCode()}): ${resp.body()}")
				} catch (e: ExecutionException) {
					throw IllegalStateException("ClickHouse insert failed mid-stream", e.cause ?: e)
				}
			}
		}

		override fun close() {
			if (closed) return
			closed = true
			body.complete()
			val response = try {
				awaitResponse()
			} catch (e: ExecutionException) {
				onClose()
				throw IllegalStateException("ClickHouse insert failed", e.cause ?: e)
			} catch (e: Throwable) {
				onAbort()
				throw IllegalStateException("ClickHouse insert failed before server responded", e)
			}
			onClose()
			if (response.statusCode() !in 200..299) {
				error("ClickHouse insert failed (${response.statusCode()}): ${response.body()}")
			}
		}

		/**
		 * Waits as long as the request makes progress: the queued rows still have to be uploaded,
		 * which takes as long as the link needs. Gives up only after [closeIdleTimeoutMs] without
		 * the HTTP client reading anything, i.e. a stalled upload, or a server that does not
		 * answer after the last byte. This bounds how long an auto-end worker can stay parked
		 * here if the server stops responding entirely.
		 */
		private fun awaitResponse(): HttpResponse<String> {
			val closeStartedNanos = System.nanoTime()
			val idleTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(closeIdleTimeoutMs)
			while (true) {
				val idleSinceNanos = maxOf(body.lastReadNanos, closeStartedNanos)
				val remainingNanos = idleSinceNanos + idleTimeoutNanos - System.nanoTime()
				if (remainingNanos <= 0) {
					throw TimeoutException("no progress for $closeIdleTimeoutMs ms")
				}
				try {
					return responseFuture.get(remainingNanos, TimeUnit.NANOSECONDS)
				} catch (_: TimeoutException) {
					// Re-checked against the reader's latest progress: only a full idle period gives up.
				}
			}
		}
	}

	companion object {
		private val threadSeq = java.util.concurrent.atomic.AtomicLong(0)

		/**
		 * `mutations_sync=2` ensures ALTER … DELETE returns only once the mutation has fully
		 * applied on all replicas. The `*_null_as_default=0` trio preserves NULL values
		 * literally (otherwise ClickHouse would substitute column defaults).
		 * `date_time_input_format=best_effort` accepts the variety of date formats Singer taps
		 * emit.
		 *
		 * The v2 JDBC driver routes URL parameters through `ClientConfigProperties`, which
		 * logs a warning for any key it doesn't recognize as a *client* property. Server-side
		 * ClickHouse settings must therefore be prefixed with `clickhouse_setting_` to be
		 * forwarded as query settings instead of being flagged as unknown.
		 */
		private fun buildJdbcUrl(cfg: TargetConfig): String {
			val params = listOf(
				"mutations_sync" to "2",
				"date_time_input_format" to "best_effort",
				"insert_null_as_default" to "0",
				"input_format_null_as_default" to "0",
				"input_format_defaults_for_omitted_fields" to "0",
			).joinToString("&") { (k, v) -> "clickhouse_setting_$k=$v" }
			return "jdbc:clickhouse://${cfg.host}:${cfg.port}/${cfg.database}?$params"
		}

		/**
		 * Run [block] with up to [retries] retries on failure, with exponential backoff. The [sleeper]
		 * seam lets tests inject a no-op sleep — the real call site uses `Thread::sleep`.
		 */
		internal fun <T> withRetries(
			retries: Int,
			factor: Int = 4,
			minTimeoutMs: Long = 1000,
			sleeper: (Long) -> Unit = Thread::sleep,
			block: () -> T,
		): T {
			var lastError: Throwable? = null
			for (attempt in 0..retries) {
				try {
					return block()
				} catch (e: Throwable) {
					lastError = e
					if (attempt < retries) {
						val delay = (minTimeoutMs * factor.toDouble().pow(attempt)).toLong()
						logger.warn { "query failed, retrying after ${delay}ms: ${e.message}" }
						sleeper(delay)
					}
				}
			}
			throw lastError!!
		}
	}
}
