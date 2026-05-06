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
		rowWriterFactory = DefaultRowWriterFactory,
	)

	private val dataSource: DriverManagerDataSource = DriverManagerDataSource(
		buildJdbcUrl(config),
		config.username,
		config.password,
	)
	private val jdbc: JdbcTemplate = JdbcTemplate(dataSource)

	private val httpClient: HttpClient = HttpClient.newBuilder()
		.executor(Executors.newCachedThreadPool { r ->
			Thread(r, "ch-http-worker-${threadSeq.getAndIncrement()}").apply { isDaemon = true }
		})
		.connectTimeout(Duration.ofSeconds(30))
		.build()

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
			"""SELECT name, type, is_in_sorting_key
			   FROM system.columns
			   WHERE database = '${escapeValue(config.database)}' AND table = '${escapeValue(table)}'""".trimIndent(),
			2,
		),
	)

	override fun addColumn(table: String, newCol: Column): Either<AddColumnError, Unit> =
		addColumnOp(runQuery, jdbc, table, newCol)

	override fun removeColumn(table: String, existing: Column): Either<RemoveColumnError, Unit> =
		removeColumnOp(runQuery, jdbc, table, existing)

	override fun updateColumn(table: String, existing: Column, newCol: Column): Either<UpdateColumnError, Unit> =
		updateColumnOp(runQuery, jdbc, table, existing, newCol)

	override fun renameObsoleteTable(table: String): QueryResult {
		logger.info { "[$table] Renaming table $table" }
		return runQuery(jdbc, "RENAME TABLE `$table` TO `${TargetConnection.DROPPED_TABLE_PREFIX}$table`", 2)
	}

	override fun openRowWriter(query: String): RowWriter =
		rowWriterFactory(httpClient, insertUrl(query), authHeader)

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
	 * Decodes a `system.columns` result row into a [Column]. The `is_in_sorting_key` value can come
	 * back as Boolean, Number, null, or a String depending on the JDBC driver version, so we
	 * normalize each shape here.
	 */
	internal object DefaultListColumnsResultParser : ListColumnsResultParser {
		override fun invoke(result: QueryResult): List<Column> = result.data.map { row ->
			Column(
				name = row[0].toString(),
				type = row[1].toString(),
				isInSortingKey = when (val v = row[2]) {
					is Boolean -> v
					is Number -> v.toLong() != 0L
					null -> false
					else -> v.toString().toBoolean()
				},
			)
		}
	}

	internal object DefaultRowWriterFactory : RowWriterFactory {
		override fun invoke(httpClient: HttpClient, url: URI, authHeader: String): RowWriter =
			HttpStreamingRowWriter.open(url = url, authHeader = authHeader, httpClient = httpClient)
	}

	/**
	 * InputStream backed by a blocking queue of byte arrays. Unlike PipedInputStream,
	 * it does **not** track reader-thread identity, which matters because
	 * HttpClient.BodyPublishers.ofInputStream pulls from arbitrary executor threads
	 * and recycles them between batches — PipedInputStream would then raise
	 * "Read end dead" on the next write after the original read thread died.
	 */
	internal class BlockingQueueInputStream : InputStream() {
		private val queue = LinkedBlockingQueue<ByteArray>()
		private var current: ByteArray = EMPTY
		private var pos: Int = 0

		@Volatile
		private var completed: Boolean = false

		fun put(bytes: ByteArray) {
			if (completed || bytes.isEmpty()) return
			queue.put(bytes)
		}

		fun complete() {
			if (completed) return
			completed = true
			queue.put(EOF)
		}

		override fun read(): Int {
			if (!ensureAvailable()) return -1
			return current[pos++].toInt() and 0xFF
		}

		override fun read(b: ByteArray, off: Int, len: Int): Int {
			if (len == 0) return 0
			if (!ensureAvailable()) return -1
			val n = minOf(len, current.size - pos)
			System.arraycopy(current, pos, b, off, n)
			pos += n
			return n
		}

		private fun ensureAvailable(): Boolean {
			while (pos >= current.size) {
				val next = queue.take()
				if (next === EOF) return false
				current = next
				pos = 0
			}
			return true
		}

		companion object {
			private val EMPTY = ByteArray(0)
			private val EOF = ByteArray(0)
		}
	}

	internal class HttpStreamingRowWriter internal constructor(
		private val body: BlockingQueueInputStream,
		private val responseFuture: CompletableFuture<HttpResponse<String>>,
	) : RowWriter {

		private var closed = false

		companion object {
			// No per-request timeout: one insert stream can stay open for the whole ingestion
			// of a stream (millions of rows). Idle protection is handled on the caller side by
			// RecordProcessor's auto-end timeout, which closes the stream after inactivity.
			fun open(url: URI, authHeader: String, httpClient: HttpClient): HttpStreamingRowWriter {
				val body = BlockingQueueInputStream()
				val request = HttpRequest.newBuilder(url)
					.header("Authorization", authHeader)
					.header("Content-Type", "application/octet-stream")
					.POST(HttpRequest.BodyPublishers.ofInputStream { body })
					.build()
				val responseFuture = httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
				return HttpStreamingRowWriter(body, responseFuture)
			}
		}

		override fun write(bytes: ByteArray) {
			// If the server rejected the request mid-stream, surface the error now instead of
			// silently dropping rows into a queue nobody is draining.
			if (responseFuture.isDone) {
				try {
					val resp = responseFuture.get(0, TimeUnit.SECONDS)
					error("ClickHouse insert completed prematurely (${resp.statusCode()}): ${resp.body()}")
				} catch (e: ExecutionException) {
					throw IllegalStateException("ClickHouse insert failed mid-stream", e.cause ?: e)
				}
			}
			body.put(bytes)
		}

		override fun close() {
			if (closed) return
			closed = true
			body.complete()
			val response = try {
				responseFuture.get(5, TimeUnit.MINUTES)
			} catch (e: ExecutionException) {
				throw IllegalStateException("ClickHouse insert failed", e.cause ?: e)
			} catch (e: Throwable) {
				throw IllegalStateException("ClickHouse insert failed before server responded", e)
			}
			if (response.statusCode() !in 200..299) {
				error("ClickHouse insert failed (${response.statusCode()}): ${response.body()}")
			}
		}
	}

	companion object {
		private val threadSeq = java.util.concurrent.atomic.AtomicLong(0)

		/**
		 * `mutations_sync=2` ensures ALTER … DELETE returns only once the mutation has fully
		 * applied, matching TS's `queryOptions.mutations_sync=2`. The `*_null_as_default=0`
		 * trio preserves NULL values literally (otherwise ClickHouse would substitute column
		 * defaults). `date_time_input_format=best_effort` accepts the variety of date formats
		 * Singer taps emit.
		 */
		private fun buildJdbcUrl(cfg: TargetConfig): String {
			val params = listOf(
				"mutations_sync" to "2",
				"date_time_input_format" to "best_effort",
				"insert_null_as_default" to "0",
				"input_format_null_as_default" to "0",
				"input_format_defaults_for_omitted_fields" to "0",
			).joinToString("&") { (k, v) -> "$k=$v" }
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
