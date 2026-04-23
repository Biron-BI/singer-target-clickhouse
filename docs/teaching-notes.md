# From TypeScript to Kotlin: rewriting a Singer target for ClickHouse

*A teaching walkthrough — for students in their 2nd year.*

---

## Before we start: what you will learn today

By the end of this session, you should understand:

1. What a **data pipeline** is, and why companies need tools to move data around.
2. What **Singer** is — a small, pragmatic *specification* for how pipeline tools talk to each other.
3. What a **target** does, and specifically what our target (`target-clickhouse`) is responsible for.
4. How the original **TypeScript implementation** is structured, step by step.
5. **Why that implementation is slow** — and here we'll actually look under the hood at JavaScript, V8, and the Node.js runtime.
6. What we **rewrote in Kotlin**, and why that gives us a faster baseline — while being honest about which gains come "for free" and which we still have to design for.
7. How we used **Claude** to drive the migration: the process, what went well, what needed correction.
8. Where to look next if we want to make the Kotlin version *much* faster than a 1:1 port.

I'll try to answer questions before you ask them. If something is still not clear, stop me — that's the whole point of being here.

Let's go.

---

## 1. The data pipeline problem

Every company that stores data ends up with the same situation:

- **Customers, transactions, events, logs** are produced everywhere: a Postgres database for the website backend, a CSV export from a finance tool, a MongoDB for the mobile app, a Stripe account for payments, a Zendesk for the support team, a Google Analytics account for the marketing team.
- **Decisions have to be made on top of all of that together**: "which customers are most profitable?", "did the marketing campaign work?", "how many tickets does the support team close per day by product?".

You cannot answer those questions from the operational databases. They are designed for low-latency reads and writes on a single row at a time, not for "scan a billion rows and aggregate". You need a second database — a **data warehouse** (Snowflake, BigQuery, ClickHouse, Redshift…) — that is designed for *analytical* queries.

So you need to continuously move data out of the operational sources and into the warehouse. That is the **data pipeline** problem, and the classical shape of the solution is called **ETL** or **ELT**:

```
[ source A ] ─┐
              ├─── Extract ── Transform ── Load ──> [ warehouse ]
[ source B ] ─┘
```

- **Extract**: read data from the source (database, API, file).
- **Transform**: clean, normalize, deduplicate, join.
- **Load**: write the result to the warehouse.

In the modern "ELT" variant, we load first and transform inside the warehouse, because warehouses are now very good at SQL-based transformations. That's the shape we care about today. The piece we are going to focus on is the **L** — loading data into ClickHouse — but to do that we first need to understand what format the data arrives in.

### Why not just write a script per source?

That's exactly what the industry used to do, and it was terrible. Every team re-implements "read from Stripe, transform, write to warehouse" with its own conventions, bugs, retry logic, schema handling. No code is reused between `stripe → warehouse`, `zendesk → warehouse`, `postgres → warehouse`.

So people started to think: *what if we standardize the format that flows between "the thing that reads from the source" and "the thing that writes to the warehouse"?* If we agree on a format, then a single writer ("write to ClickHouse") can consume data from **any** reader, as long as the reader emits the agreed format. And vice-versa.

That's the idea that became **Singer**.

---

## 2. Meet Singer

Singer is an open specification published by Stitch (now part of Talend) around 2017. It defines:

1. A **line-based protocol** where each line is one JSON object on stdout.
2. Three core **message types**: `SCHEMA`, `RECORD`, `STATE`.
3. Two kinds of programs: **taps** (read from a source, print Singer messages to stdout) and **targets** (read Singer messages from stdin, write them somewhere).

The whole specification fits on a single page. That's its genius: it is *almost laughably simple*, and that simplicity is what makes it work in practice.

### The wire format

When a tap runs, it prints something like this to stdout:

```jsonl
{"type":"SCHEMA","stream":"customers","schema":{"type":"object","properties":{"id":{"type":"integer"},"email":{"type":"string"}}},"key_properties":["id"]}
{"type":"RECORD","stream":"customers","record":{"id":1,"email":"alice@example.com"}}
{"type":"RECORD","stream":"customers","record":{"id":2,"email":"bob@example.com"}}
{"type":"STATE","value":{"bookmarks":{"customers":"2024-05-10T08:30:00Z"}}}
```

Let's read that carefully:

- **SCHEMA**: "Here is the shape of a stream called `customers`. Each record will have an integer `id` and a string `email`. The primary key is `id`."
- **RECORD**: "Here is one row of that stream."
- **STATE**: "I have finished processing through this point. If you restart me with this state, I'll resume from here."

The schema uses a subset of **JSON Schema** to describe types. That's important — it means the data format is *self-describing*. The target doesn't need to know in advance what the columns of `customers` are; the tap will tell it.

### The genius: stdin / stdout

The tap writes messages to its standard output. The target reads messages from its standard input. That's it. You connect them with a pipe:

```bash
tap-stripe --config tap_config.json | target-clickhouse --config target_config.json
```

Any tap can talk to any target. The Unix philosophy of small, composable programs, applied to data integration.

### What Singer explicitly does NOT do

Singer is *not*:

- A scheduling system (use cron, Airflow, Dagster…).
- A transformation system (use dbt, SQL inside the warehouse…).
- A connection pooling / HTTP retry / auth framework.

Singer is a **data interchange protocol**. Nothing more. The tap is responsible for talking to Stripe; the target is responsible for talking to ClickHouse. The protocol just says how they hand data to each other.

---

## 3. Taps and targets, side by side

A **tap** is a program whose job is:

1. Connect to some source (API, database, file).
2. Figure out what streams (= tables / collections) exist.
3. Read data out of them.
4. For each stream, emit a SCHEMA message, then emit one RECORD per row.
5. Periodically emit STATE messages so that work can resume after a crash.

A **target** is a program whose job is:

1. Read Singer messages from stdin.
2. When it sees a SCHEMA, make sure the destination has a compatible table (create / alter it as needed).
3. When it sees RECORDs, insert them into that table.
4. When it sees STATE, flush pending writes and echo the STATE to stdout (so that the driver — cron, Airflow, whatever — can store it and restart from there next time).

There are Singer-compatible taps for hundreds of sources: Stripe, Zendesk, Salesforce, Postgres, MySQL, Google Sheets… And targets for most warehouses: Snowflake, BigQuery, Postgres, and — our subject today — **ClickHouse**.

### An everyday analogy

Imagine a library that wants to digitize all its books:

- The tap is the **scanner operator**: they walk through the shelves, open each book, scan each page, and read the results out loud into a microphone. They don't care who is listening. They just describe each book ("Here is a novel, by this author, published this year — now page 1 says…, now page 2 says…"), and they keep a bookmark so they can resume tomorrow.
- The target is the **transcriptionist**: they listen through headphones, type into a computer, and organize the result into the digital library. They don't care where the voice comes from. They just receive descriptions and copy them into the right shelves.
- The Singer spec is the **language they agreed to use** so that scanner operators and transcriptionists can be hired independently.

If tomorrow we get a new scanner for audiobooks, we write a new tap. The target doesn't change. If we change from a digital library to a different one, we replace the target. The scanners don't change.

This is why Singer is called a **"data integration framework"**: it's not one tool, it's a convention that lets an ecosystem of tools compose.

---

## 4. Our specific target: `target-clickhouse`

Now let's zoom into our case. **ClickHouse** is an open-source columnar analytical database — think "very fast SQL on billions of rows, optimized for append-mostly workloads". It's particularly good at:

- **Bulk inserts** (millions of rows per second if you feed it right).
- **Analytical queries** (aggregations over large ranges).
- **Columnar compression** (rows with similar values in one column compress beautifully).

It is **not** as good at:

- Random updates to individual rows (there's `ALTER TABLE … UPDATE` but it's an async mutation, not a row-level update).
- Deletions (same — `ALTER … DELETE` is a mutation).

Our `target-clickhouse` has to translate what Singer gives it (arbitrary streams of JSON records with arbitrary schemas) into **ClickHouse tables with the right engine choice and the right column types**, and then stream the records into those tables as efficiently as possible.

### What makes this non-trivial?

Several things:

1. **Nested JSON → flat columns.** Singer records can contain nested objects and arrays. ClickHouse is columnar and tables are mostly flat. So if we see `{"id":1, "address": {"street":"…","city":"…"}}`, we have to decide: do we flatten into `address__street`, `address__city`? Or do we create a sub-table? The target does both, depending on whether the nested structure is a single object (flatten) or an array of objects (sub-table, with a foreign-key-like link).

2. **Primary keys and versioning.** Some streams have primary keys. When a record with a known ID is updated, we want the latest version to "win". ClickHouse has a `ReplacingMergeTree` engine that keeps the row with the highest value in a designated version column. The target has to detect PK'd streams, use that engine, and assign a monotonically increasing `_ver` to each record.

3. **Schema drift.** The tap may emit a SCHEMA message with a column that didn't exist before, or with a different type. The target has to `ALTER TABLE` on the fly — add columns, modify types — while preserving existing data.

4. **Deletions.** Some taps emit `DELETED_RECORD` messages (it's an extension over plain Singer). The target has to translate those into `DELETE FROM … WHERE (pk_cols) IN (…)`.

5. **Cleaning columns.** A "cleaning column" is a special pattern used when the tap re-emits a batch of records identified by some column (e.g., `batch_id`). Before inserting, the target has to delete all existing rows with that batch id to avoid duplicates.

6. **Active streams.** At the end of a sync, the tap can emit an `ACTIVE_STREAMS` message listing which streams still exist. Any table in ClickHouse that corresponds to a stream no longer in that list is renamed with a `_dropped_` prefix (so data is archived, not deleted).

So the target is not a dumb "insert this row" loop. It contains a **full JSON-schema-to-SQL translator, a schema-diff engine, a streaming insert path, a deduplication mechanism, and a lifecycle manager for tables**. Each of those is an interesting piece of engineering on its own.

---

## 5. Anatomy of the TypeScript implementation

Let's now walk through the code. I'm going to describe it by responsibility, not file by file — the goal is to build a mental model, not memorize a codebase.

### 5.1 The entry point

The program is started as a CLI:

```bash
target-clickhouse --config config.json [--input file.jsonl] [--output state.jsonl] [-u table1 table2]
```

- `--config` points to a JSON file with connection info and tunables (`host`, `port`, `batch_size`, etc.).
- `--input` is an optional file instead of stdin (useful for tests).
- `--output` is an optional file instead of stdout (that's where STATE messages get echoed).
- `-u` / `--update-streams` forces recreation of some tables.

In the TS code this is done in `src/index.ts`: it parses args, reads config, opens the input stream, and calls the main `processStream` function.

### 5.2 The read loop

Conceptually:

```
for each line on input:
  parse the line as a JSON Singer message
  dispatch on message.type:
    SCHEMA → make sure the table exists / is compatible
    RECORD → buffer the row, flush when batch is full
    DELETED_RECORD → buffer a deletion, flush when batch is full
    STATE → flush everything, echo the state JSON
    ACTIVE_STREAMS → rename obsolete tables
```

In TypeScript, this is built around:

- `readline.createInterface({input: stdin})` — splits the input stream by newlines.
- `async function processLine(line, …)` — parses and dispatches.
- `await processLinePromise` before dispatching the next line — ensures serial ordering despite the async machinery.

At the end of the stream, all the stream processors are finalized in parallel (up to `finalize_concurrency`). Finalization means: commit remaining rows, run `OPTIMIZE TABLE … FINAL` to collapse duplicate primary keys, delete orphan children, and check PK integrity.

### 5.3 Schema handling

When a SCHEMA message arrives, the code runs a `JsonSchemaInspector`. Its job is to turn a JSON Schema like:

```json
{
  "type": "object",
  "properties": {
    "id": {"type": "integer"},
    "name": {"type": ["null","string"]},
    "addresses": {
      "type": "array",
      "items": {"type": "object", "properties": {"street": {"type": "string"}}}
    }
  }
}
```

into an internal `SourceMeta` tree that looks like:

```
SourceMeta(
  prop = "customers",
  sqlTableName = `customers`,
  pkMappings = [id CURRENT Int64],
  simpleColumnMappings = [name Nullable(String)],
  children = [
    SourceMeta(
      prop = "addresses",
      sqlTableName = `customers__addresses`,
      pkMappings = [_root_id ROOT Int64, _level_0_index LEVEL Int32],
      simpleColumnMappings = [street String],
      children = [],
    )
  ]
)
```

Why the `_root_id`, `_level_0_index` magic? Because SQL is relational: a row in `customers__addresses` needs to know which parent row it belongs to. `_root_id` is the parent's primary key; `_level_0_index` is the position in the parent's array (useful when the child doesn't itself have a natural key). That pair together forms the child's primary key.

From the `SourceMeta`, another module (`JsonSchemaTranslator`) generates the `CREATE TABLE` statements: it chooses `MergeTree` or `ReplacingMergeTree` depending on whether there are primary keys, decides on `ORDER BY`, wraps types in `Nullable(...)`, `LowCardinality(...)`, `Array(...)` as needed.

If the table already exists, the code runs `SHOW CREATE TABLE`, compares column by column, and emits `ALTER TABLE ADD/DROP/MODIFY COLUMN` to make the schema match. This is `updateSchema`.

### 5.4 Record ingestion

Once the schema is handled, each RECORD message causes:

1. Extract primary key values and simple column values from the record.
2. Build a JSON array of the form `[pk_values…, col_values…, version]`.
3. Serialize it with `JSON.stringify`.
4. Append the serialized line + `\n` to a buffer.
5. When the buffer reaches `batch_size` rows, flush it to ClickHouse.

Flushing works like this in TS: when the first record arrives, the code opens an HTTP **Writable stream** to ClickHouse with a query:

```
INSERT INTO customers (id, name, _ver) FORMAT JSONCompactEachRow
```

Every row we buffer is just one line in the JSONCompactEachRow format (an array: `[1,"alice",1]`). We write those lines into the HTTP stream as they arrive; ClickHouse reads them from the request body as a chunked stream. When we close the stream, ClickHouse commits the batch.

Two interesting details:

- **Children** are fed recursively. When the record has an array, each item becomes a child record, fed to a child `RecordProcessor` (same mechanism, different table). The parent's primary key and the array index are propagated to the child.
- **Auto-end timeout**: if no new row arrives within `insert_stream_timeout_sec - 5` seconds, the current batch is closed automatically. This prevents an HTTP stream from staying open indefinitely on slow taps.

### 5.5 Finalization

At the end of the whole input (or on every STATE message), the code:

1. Closes all open insert streams, flushing buffers.
2. For tables with primary keys (ReplacingMergeTree), runs `OPTIMIZE TABLE … FINAL` — this forces ClickHouse to merge all parts and keep only the latest version per key.
3. For each such table's children, deletes orphaned rows (child rows whose root/version doesn't exist in the root anymore).
4. Runs a PK integrity check: `SELECT pk_cols, ROW_NUMBER() OVER (PARTITION BY pk_cols) WHERE row_number > 1` — if any row is found, fails loudly. (We never want to silently corrupt the warehouse.)

### 5.6 A mental model

If you squint, the whole target is a small **interpreter for a mini-protocol**:

```
 SCHEMA ─── JsonSchemaInspector ─── JsonSchemaTranslator ─── CREATE/ALTER TABLE
 RECORD ─── RecordProcessor ────── buffer ──── JSONCompactEachRow ──── HTTP stream
 DELETE ─── DeletedRecordProcessor ── buffer ──── DELETE FROM …
 STATE  ─── flush all ──── echo to --output
 ACTIVE ─── rename obsolete ──── _dropped_ prefix
```

That's ~1700 lines of TypeScript, split across ~15 files. It has been in production for years at Biron.

---

## 6. Why is the TypeScript target slow?

This is where it gets interesting — and where we need to understand **how JavaScript actually runs**.

Before I list the specific reasons, let me make an honest preamble: the TS target is not *broken* or *badly written*. It's a correct, maintainable implementation. "Slow" is relative — in practice it ingests on the order of **thousands to tens of thousands of rows per second**. For many workloads that's fine. The question is: if we need millions of rows per second (for example to catch up after an outage, or to handle larger customers), can we do better? And the answer is: **yes, quite a lot better — but not because the TS code is dumb. Because the runtime and the patterns it uses have inherent overheads.**

OK, let's dig in.

### 6.1 Node.js is single-threaded in user code

Node.js runs a single JavaScript thread. Everything your code does — parsing, object creation, promise resolution — competes for that one thread. I/O (reading stdin, HTTP) is handled by a thread pool under the hood (`libuv`), which is good, but the moment data comes back to JavaScript-land, it's a single thread again.

So if your code is CPU-bound (parsing JSON is CPU-bound!), you cannot use more than one core. No matter how many cores your machine has.

**Consequence for our target**: if we want to saturate a ClickHouse insert (which can easily absorb 1 GB/s of JSON on good hardware), one CPU core of Node is not going to be enough. We can fork workers, but that's complex and not what the TS target does.

The JVM, by contrast, has real threads and can trivially use multiple cores. That's one (not the only!) reason the Kotlin version has more headroom.

### 6.2 JSON parsing is expensive — and we pay the full cost per record

When the TS code does `JSON.parse(line)`:

1. V8's C++ parser reads the bytes and builds a full tree of JavaScript objects.
2. Every key becomes a string (interned in V8's string table when possible).
3. Every number becomes either a small integer (SMI) or a heap-allocated double.
4. Every object has a "hidden class" (a shape descriptor) — V8 tries to reuse hidden classes when objects have identical shapes, but with arbitrary tap output this is brittle.
5. Every nested array / object means more allocations.

`JSON.parse` is written in C++ and is genuinely fast — but "fast" is relative to the amount of work it has to do, and the amount of work scales with the *whole record*, not with *the fields we care about*.

Consider a Stripe invoice record. It has maybe 60 fields. Our target only uses, say, 10 of them for a given stream. But `JSON.parse` parses all 60 every time.

Streaming parsers (like `jackson-core` in Java with the tree/streaming API) can skip entire fields without materializing them. The TS target doesn't use a streaming parser — `JSON.parse` is all-or-nothing.

**Rule of thumb**: for a 1 KB JSON line, `JSON.parse` costs roughly a microsecond on modern hardware. That sounds tiny, but at 100k rows per second you're spending 100 ms per second *just on JSON parsing*. If you also have to re-serialize into JSONCompactEachRow for the insert, double that. Suddenly 20% of your CPU is gone before you do any real work.

### 6.3 String handling in V8 is sneaky

V8 has several internal representations of strings:

- Small strings fit into a single heap object.
- Concatenations (`"a" + "b"`) are often represented as **ConsStrings** — a tree of references, not a real flattened buffer.
- Slices (`"hello".substring(1, 4)`) are **SlicedStrings** — references into a parent.

This is usually good. It makes operations cheap. But when you then pass the string to something that needs a real flat buffer (the network, the file system, a regex engine), V8 has to flatten it, which is a hidden cost.

Our target does lots of string manipulation:

- Building SQL queries by concatenation.
- Splitting property names on a separator (`"a$%€£b".split(...)`) to walk nested objects.
- Stringifying rows with `JSON.stringify(array)` for every single record.

Each of those is fast in isolation, but they add up — and because of ConsStrings and SlicedStrings, the actual memory footprint can grow in non-obvious ways.

### 6.4 Garbage collection pressure

This is the big one.

Every single RECORD produces, very roughly:

- 1 string (the input line)
- 1 JS object from `JSON.parse` + N nested objects / arrays
- M primary key and column values (maybe primitive, maybe strings)
- 1 array to represent the row for JSONCompactEachRow
- 1 string from `JSON.stringify(row)`
- 1 Buffer (Node's binary buffer) to write to the HTTP stream

That's dozens of short-lived allocations per record. V8 allocates them in the **young generation** (also called the "new space" or "nursery"), which is small (typically 16 MB by default) and compacting. Every time the nursery fills up, V8 runs a **minor GC** — fast, but not free: it scans the nursery, promotes survivors, and resets.

At high throughput (say 100k rows/s), the nursery fills in a fraction of a second. You get dozens of minor GCs per second. Each one pauses the event loop for ~1-5 ms. That adds up to maybe **100 ms per second in GC pauses** — a 10% tax — plus CPU work that is not directly productive.

If some objects survive and get promoted to the **old generation**, they eventually trigger **major GCs** which are much more expensive (tens of milliseconds). You can tune V8 (`--max-old-space-size`, `--max-semi-space-size`) but you can't eliminate this entirely.

**Key insight**: in JavaScript, it's very hard to avoid allocations, because the language doesn't give you primitive types or stack allocation. Every number you put in an array is either a small integer (inlined as a tagged pointer) or a heap-allocated double. Every object you create is a heap object. Every function call produces a closure if it captures anything. The language is optimized for expressiveness, not for *predictably not allocating*.

### 6.5 The async machinery adds overhead

Look at this fragment from `processStream.ts`:

```typescript
let processLinePromise = Promise.resolve()
await forAwaitOnMacroTaskQueue(rl[Symbol.asyncIterator](), async line => {
  await processLinePromise
  processLinePromise = processLine(line, config, ch, streamProcessors, state, lineCount++, abort)
})
```

This is *correct*, but it has a hidden cost. Every `async function` is syntactic sugar for a state machine that returns a Promise. Every `await` creates a microtask. Every chain is a graph of Promise objects.

In V8, each of those is a heap allocation. Each resumption is a dispatch through the microtask queue. For a function called once a second, this is free. For a function called 100k times per second, it is **another 100k-200k allocations per second** just to manage "control flow".

Node also uses `setImmediate` and a custom "macrotask queue" trick in this code (`forAwaitOnMacroTaskQueue`) to allow I/O to interleave with processing. That's clever, but it's a workaround for the fact that a naive `for await` loop on a readable stream can starve I/O on tight CPU.

### 6.6 `readline` on stdin is not free

The TS target reads stdin via:

```typescript
const rl = readline.createInterface({input: stdin})
```

This pipes the raw bytes of stdin into a StringDecoder, then into a line splitter. Each decoded chunk produces a string. Every line is emitted as a JS string.

Two subtleties:

- **UTF-8 decoding** is done in JS/C++. If the tap writes ASCII, that's fast. If it writes UTF-8 with multi-byte chars, the decoder has to handle chunk boundaries carefully.
- **The line buffer grows dynamically**. Long lines (a single big record over many KB) cause reallocations.

For comparison, reading from a file with `fs.createReadStream` gives you raw buffers and you decode yourself — sometimes faster, but more code. The target uses the convenient abstraction, which is always slower than the raw primitive.

### 6.7 Polymorphism kills the JIT

V8 has a powerful optimizing compiler (TurboFan). It works best on *monomorphic* code — where a given site sees objects of exactly one shape, variables of exactly one type, calls to exactly one function.

Our code has a function `extractValue(data, mapping, translateValue)` that is called millions of times. But `data` can be any object shape (depending on what the tap emits). `mapping.valueExtractor` can be different closures for different columns. `mapping.valueTranslator` is either undefined or one of several functions.

TurboFan sees this as **polymorphic** (or megamorphic if >4 shapes). It cannot inline `valueExtractor`. It has to go through a **polymorphic inline cache** (PIC) — an indirect table lookup. That's not catastrophic (it's still fast), but it is meaningfully slower than a monomorphic call, especially compared to Java/Kotlin where type information is static.

### 6.8 Streams, Buffers, and Writable are layered deeply

The HTTP library (`@apla/clickhouse`) exposes a Writable stream. Under the hood:

1. Our JS code creates a `Buffer` from a string.
2. The Writable's `write()` queues it in an internal buffer.
3. The underlying `http.ClientRequest` writes chunks to a TCP socket.
4. libuv's uv_write pushes bytes into the kernel send buffer.

Each step has its own buffer, its own event callbacks, its own backpressure mechanism. If any level gets full, you block. If any level does a copy (and they usually do, because Writable has no zero-copy story), you pay.

### 6.9 Summary — where does the time go?

If we profile a typical run ingesting millions of rows, we'd see roughly:

- 30-40% in `JSON.parse` and value extraction
- 15-20% in building rows and `JSON.stringify`
- 10-15% in Promise / await plumbing
- 10-15% in stream I/O and buffer copies
- 10-15% in GC
- ~15% in actual "useful work"

That last number is depressing. More than 80% of the CPU is spent on runtime overhead, not on "what this program is for". And this is **normal** for an idiomatic Node.js program — it's what you trade for the developer productivity that JavaScript gives you.

Again: this is not a criticism of the code. The code is idiomatic and well-structured. The ceiling is a property of the runtime, not the code.

---

## 7. Why rewrite in Kotlin (on the JVM)?

Kotlin is a statically-typed language that compiles to JVM bytecode. On the JVM, we get:

1. **Real multi-threading**. If we need to parallelize parsing, serialization, or finalize steps, threads are a normal tool (not a big ceremony like `worker_threads` in Node).
2. **Static types → monomorphic dispatch**. When we call a method, the JIT usually knows the exact callee and can inline it. That's free.
3. **Primitive types**. An `Int` in Kotlin is a 32-bit value on the stack; a `Long` is 64 bits; arrays of primitives don't box. Compare to JS where every number in an array is a tagged pointer or a heap double.
4. **Streaming JSON parser** (Jackson, specifically its `JsonParser`/`JsonGenerator` streaming API). We can walk a JSON document token-by-token and skip fields we don't need. No full-tree materialization.
5. **Mature GC choices** (G1, ZGC, Shenandoah) that scale to large heaps with sub-millisecond pauses.
6. **A better HTTP story**: `java.net.http.HttpClient` supports HTTP/2 and streaming request bodies from an `InputStream` — in our case, the output side of a `PipedOutputStream` we write rows into. We get true chunked streaming without a heavy third-party library.
7. **Easier profiling**. `async-profiler`, JFR, JMX — decades of tooling.

But a word of caution: **the JVM is not automatically faster**. You can easily write JVM code that's slower than Node, by allocating lots of short-lived objects, using reflection, using boxed collections instead of primitive arrays, or triggering lots of autoboxing in hot paths.

What the JVM gives us is a **higher ceiling** and **better tools to measure**. It's up to us (and to you, as the next engineers of this codebase) to use them well.

### What we deliberately mirrored from the TS version

For the first cut, we kept the architecture identical:

- Same module names and responsibilities (`JsonSchemaInspector`, `JsonSchemaTranslator`, `RecordProcessor`, `DeletedRecordProcessor`, `StreamProcessor`, `processStream`).
- Same data flow: line-based stdin, per-stream buffers, per-record JSONCompactEachRow serialization, OPTIMIZE + orphan cleanup at the end.
- Same semantics for edge cases (cleaning columns, active streams, update streams, insert stream timeout).

This is a **deliberate choice**: a 1:1 port means we can use the existing integration test suite unchanged as the acceptance test. If the Kotlin target produces the same ClickHouse state as the TS target for the same fixtures, we know we didn't regress anything. Only then do we go optimize.

### What already got a natural win

Even without any optimization work, the port picked up a few things for free:

- **Static type checking**. Many classes of bug the TS code has to handle at runtime (e.g., `boolean propDef not supported`) became compile-time impossibilities or explicit sealed types.
- **Arrow's `Either`** replaces the hand-rolled TS `Either` type, with better ergonomics (`mapLeft`, `bind`, …).
- **Coroutines + `Semaphore`** replace `@supercharge/promise-pool` for bounded concurrency in the finalize step.
- **The JDBC v2 driver** handles connection pooling, retries, compression, and ClickHouse-specific settings (mutations_sync, date_time_input_format) through connection URL parameters.
- **HTTP chunked body from `PipedInputStream`** gives us the same TS "write rows as they come, close to commit" semantics, with less overhead.

These are not huge wins in isolation — but they're meaningful.

---

## 8. How we used Claude to drive the migration

This is the part you came for: "they said an AI did this, show me how".

Claude is a large language model that can read files, write files, run shell commands, and hold long conversations. It is **not magical** — it produces good output when you give it good context and good instructions, and bad output when you don't. The interesting engineering question is: *what does "good context" and "good instructions" look like for a real migration?*

Here's roughly how we did it.

### 8.1 Setting the stage

Before asking Claude to write any Kotlin, we did two things:

1. **Wrote a detailed `CLAUDE.md`** at the root of the project. It says what the project's goal is, which TS repo is the source of truth, and lists *quirks the port must preserve* (e.g., "`--update-streams` is CLI-only, never in config.json", "ClickHouse JDBC v2 arrays are `java.sql.Array`, not `ClickHouseArray`").
2. **Ported the integration test suite first, and made it pass against the existing TS Docker image.** This gave us a black-box acceptance test: any Kotlin implementation that produces the same ClickHouse state as the TS image passes.

These two steps took most of the initial time. The lesson: **the quality of the output is bounded by the quality of the context**. Claude can produce a Kotlin port, but without a concrete correctness oracle (the test suite), there's no way to know whether the port is right. Without the quirks document, the port would have drifted on the edge cases.

### 8.2 Planning before coding

When we asked Claude to do the rewrite, the first response was a **plan** — list of modules in dependency order, with tests gating each step:

```
Step 1: Gradle setup
Step 2: Pure leaves (Utils, Config, TargetMessage, SchemaTranslator) + unit tests
Step 3: JsonSchemaInspector + port the TS spec tests
Step 4: JsonSchemaTranslator + tests
Step 5: ClickhouseConnection + testcontainer integration test
Step 6: Record/DeletedRecord processors with mockk
Step 7: StreamProcessor / processStream
Step 8: Main.kt CLI
Step 9: Wire the full integration suite to the Kotlin runtime
Step 10: Dockerfile
```

This is the same plan a senior engineer would write. Claude's role is then to execute each step, compile, run tests, and only move on when the step is green.

The key pedagogical point: **use Claude as a faster keyboard, not as an oracle**. The architect still has to think; Claude can just type much faster and read the whole codebase at once.

### 8.3 Iterating on failures

Real migrations never go straight to green. We hit three concrete failures during this port — a concurrency race, a deserializer corner case, and an async-mutation defaulting issue — and we'll walk through each of them live. The general pattern is: the test suite catches the regression, the diff is small, the lesson is out of proportion with the fix.

### 8.4 What Claude did well, what it didn't

Honest assessment from this migration:

**Did well:**

- Read the entire TS codebase and understood the architecture quickly.
- Kept the module structure faithful to the original.
- Wrote idiomatic Kotlin (data classes, sealed types, Arrow's `Either`, coroutines).
- Wrote unit tests alongside each module.
- Found subtle bugs (like the race condition) by running the tests.
- Kept commits logically separable (we haven't committed yet, but each step is a clean chunk).

**Needed supervision:**

- The initial `RecordProcessor` was not thread-safe — I (the human) had to point to the concurrency model and ask for explicit synchronization.
- The HTTP streaming insert had a timeout that wasn't quite right the first time (I had to ask for the timeout to include a `+10s` grace period above the tap timeout).
- Knowing *which* Biron internal libraries to reuse (`biron-singer-kotlin`, `clikt`, `arrow-core`) required pointing Claude at the sibling `singer-tap-csv` project.

The pattern is: **Claude is excellent at local, well-specified tasks, and weaker at global design decisions and cross-project knowledge**. Give it the specs and the acceptance tests, and it delivers. Ask it to invent an architecture from scratch, and you'll get something generic.

### 8.5 The economics

We spent roughly a day of elapsed time (probably 2-3 hours of concentrated human attention) to produce ~2000 lines of Kotlin + 1000 lines of tests, all green against a 36-case integration suite and 11 unit specs. That's not a typical week of "port 1700 lines of TS to Kotlin"; it's noticeably faster.

But — and this is the important caveat — **the productivity gain comes from the preparation**, not from Claude alone. Without the integration tests as the acceptance oracle, Claude would have produced plausible-looking code that was subtly wrong, and we'd have spent a week debugging it. With the tests, every step is verified immediately.

So the takeaway for you, as future engineers: **invest heavily in tests, specs, and documentation before you use AI to do anything serious**. The AI is a force multiplier on whatever foundation you give it — including the bad foundations.

---

## 9. Where to go next — performance work for the presentation

We now have a 1:1 Kotlin port. The integration tests pass. The unit tests pass. We've matched semantics.

What do we do to make it fast enough to ingest millions of rows per second?

Here are concrete optimization directions, roughly in order of expected payoff. This is the material you and your students can work on during the session.

### 9.1 Measure before you optimize

**Rule zero of performance work**: do not guess. Measure.

Tools:

- **JMH** (Java Microbenchmark Harness) for measuring a function in isolation.
- **async-profiler** for sampled CPU profiling — produces flame graphs.
- **Java Flight Recorder (JFR)** for a full multi-aspect profile (CPU, GC, locks, I/O).
- **`/proc/self/status` and `vmstat`** to watch GC and RSS from the outside.

Suggested exercise for the session: ingest `covidtracker.jsonl` and `clickhouse_query_log.jsonl` in a loop, record the throughput in rows/second, and produce a flame graph. The first pass will tell you where the time goes. Resist the temptation to optimize before you see the graph.

### 9.2 Enlarge the batch size

The current default `batchSize = 100` is very conservative. ClickHouse performs best with batches in the **thousands to hundreds of thousands** of rows — the per-insert overhead (parsing the INSERT query, writing to MergeTree parts) amortizes over a larger batch.

Try `batchSize = 10000`. Measure. Try `batchSize = 100000`. Measure. Look for the knee of the curve where memory pressure balances insert latency.

Be careful: large batches raise memory usage on the target side (we're buffering them) and latency per batch on the ClickHouse side. There's a trade-off.

### 9.3 Avoid the intermediate string allocation in record buffering

In `RecordProcessor.flushBuffered`, we currently do:

```kotlin
val payload = buildString {
  buffered.forEach { row ->
    append(jsonMapper.writeValueAsString(row))
    append('\n')
  }
}.toByteArray(Charsets.UTF_8)
ctx.writer.write(payload)
```

This:

1. Serializes each row to a `String`.
2. Concatenates N strings into one via `StringBuilder`.
3. Encodes the whole thing to UTF-8 bytes.
4. Writes the bytes to the `RowWriter`.

Three full materializations (row → String → StringBuilder → ByteArray). A more efficient path is to write a `JsonGenerator` directly into an `OutputStream`:

```kotlin
jsonMapper.writer().createGenerator(outputStream).use { g ->
  buffered.forEach { row ->
    g.writeStartArray()
    row.forEach { g.writeObject(it) }
    g.writeEndArray()
    g.writeRaw('\n')
  }
}
```

This writes bytes directly to the pipe. No intermediate `String`, no `StringBuilder`, no `ByteArray`. At high throughput this alone can be a 2x improvement for the insert path.

### 9.4 Avoid a `List<Any?>` per row entirely

Going further: why build a `List<Any?>` per row at all? That's a Kotlin allocation, boxing every number. We could write the row straight from the input record:

```kotlin
g.writeStartArray()
pkMappings.forEach { pk -> writeExtractedValue(g, data, pk) }
simpleMappings.forEach { col -> writeExtractedValue(g, data, col) }
version?.let { g.writeNumber(it) }
g.writeEndArray()
```

Where `writeExtractedValue` pulls the value from `data` and writes it into the generator without ever creating an intermediate `Any?`. This removes all per-row boxing from the hot path.

### 9.5 Parse input with a streaming JSON parser

The current port uses `ObjectMapper.readTree(line)` to parse each input line. That builds a full `JsonNode` tree. Jackson also offers a streaming API (`JsonParser`) that returns tokens without materializing the tree.

Strategy: walk the tokens once, dispatch based on `type` field, parse the `record` into whatever shape we actually need. For the hot RECORD path this can avoid creating the outer wrapper object entirely.

### 9.6 Why we do *not* process records in parallel

This is the idea that looks most tempting — "multiple cores, why not fan out?" — and it's the idea we have to push back on hardest. Let me explain.

Singer records inside a stream have an **implicit ordering contract**. A tap may emit two RECORDs with the same primary key: the second one is an *update*, and it must win over the first. If we reorder them, or apply them in parallel, we can end up with the older row in ClickHouse — a correctness bug that is very hard to notice because it's data-dependent.

Concretely, with a `ReplacingMergeTree`, "winning" means having the highest `_ver`. Our code assigns `_ver` monotonically as we see records. If two workers parse and insert out of order, `_ver` assignment is still monotone in wall-clock order, but the rows can land in ClickHouse parts in a different order, and subsequent `OPTIMIZE … FINAL` merges could pick the wrong version if there's a tie-breaking ambiguity. You really don't want to introduce any possibility of that.

So **records within a stream must be applied in the order the tap emitted them**. Full stop.

What about parallelism across streams? Worth looking at — ClickHouse can absorb concurrent inserts into different tables just fine. But there's a pragmatic reason it's not a big prize: **most Singer taps are sequential**. They finish one stream, then move on to the next. In practice, at any given moment the target is only being fed one stream. Parallelizing across streams would let us ingest multiple streams *when the tap happens to interleave*, which is uncommon, at the cost of significant extra complexity (per-stream queues, a global STATE barrier, back-pressure across unbalanced streams).

The better place to spend CPU is to **make the single-stream path faster** (sections 9.3, 9.4, 9.5) so that one worker can keep ClickHouse saturated. Concurrency we *do* already exploit:

- The JSONCompactEachRow HTTP body is streamed — while we're building the next batch, the previous batch is already flying over the wire to ClickHouse and being parsed server-side.
- The finalize step (OPTIMIZE, orphan cleanup, PK check) runs in parallel across streams via `config.finalize_concurrency`. Ordering doesn't matter there.

So the rule is: **preserve per-stream ordering; add concurrency only where correctness doesn't depend on order**.

### 9.7 Use native compression on the HTTP body

ClickHouse supports compressed HTTP inserts (`Content-Encoding: lz4` or `zstd`). If your network link is the bottleneck, compressing the JSONCompactEachRow body before sending can improve end-to-end throughput. Zstd at level 1 is essentially free on modern CPUs and gives ~3x reduction on JSON.

### 9.8 Skip value translation when not needed

The `translate_values` flag tries to coerce `"42"` (string) into `42` (integer) for typed columns. Useful when the tap is sloppy, wasteful when the tap is already sending typed JSON. Measure the cost of the translator calls and consider bypassing them if the column already receives the right type.

### 9.9 Profile and tune GC

For a JVM process ingesting a lot, G1 (default) is OK but often **ZGC** or **Shenandoah** is better: sub-ms pauses, scales with heap size. Try:

```
-XX:+UseZGC -Xmx8g
```

Measure allocation rate with JFR. If it's very high (hundreds of MB/s), that's a sign there are allocations on the hot path we should eliminate (see 9.3, 9.4).

### 9.10 Things that probably won't help

For completeness, here are ideas that sound good but likely won't move the needle in our setting:

- **Rewriting in Rust/C++**: will be faster, but the Kotlin code with the above optimizations will already be within a small factor of what hand-tuned C could do. The maintenance cost is not worth it until throughput is genuinely the bottleneck.
- **Moving to gRPC instead of HTTP**: ClickHouse's HTTP interface is already very efficient; gRPC wouldn't change much.
- **Caching parsed schemas**: schemas change rarely and parsing them is already cheap; this is premature optimization.

---

## 10. Bigger picture: what we want you to take away

A few things that I want to underline before we go hands-on:

1. **Understand the domain before touching the code.** Half of the value of this session is learning what Singer is, what a target does, what ClickHouse cares about. Without that, you'd be "porting code from language A to language B" and you wouldn't notice when the port is subtly wrong.

2. **Tests are the spec.** The TS target has been in production for years; its behavior *is* the spec. Without the integration test suite, we had no way to know that our Kotlin port is correct. Your first move on any new codebase should be to find (or write) the tests. Only then do you refactor, optimize, or rewrite.

3. **The runtime matters.** V8 and the JVM are both excellent — they just have different strengths. If your workload is I/O-bound and bursty, Node is fine. If it's CPU-heavy and long-running, the JVM has a higher ceiling. Pick the tool that fits the shape of the work, not the trendy one.

4. **AI helps you execute faster, not think better.** Claude was a massive accelerator for the migration. But every non-trivial decision — architecture, concurrency model, where to cut corners, which tests to write first — had to come from a human who understood the problem. If you outsource understanding, you ship plausible-looking code that's wrong in ways you don't see.

5. **Measure before you optimize. Re-measure after.** "This loop allocates a lot, let me optimize it" without a profile means you might spend a day making a 3% part of the code 20% faster, for a total gain of <1%. That happens **all the time** in performance work. Instrument first.

6. **Simplicity compounds.** Singer is successful because it's almost laughably simple. You could understand the whole protocol in 10 minutes; you can write a working target in a weekend. If you design your own data-interchange format someday, err on the side of too simple. You can always add complexity later; you can never remove it.

---

## 11. Questions you might have (FAQ)

**"Why not just use a commercial ETL tool like Fivetran or Airbyte?"**

For many cases you should. But when you need full control over schema handling, cost, or data residency — or when you're a small company whose bill would explode at Fivetran scale — rolling your own Singer target is a reasonable, well-worn path. Biron has built its ingestion layer on Singer for years for exactly these reasons.

**"Why ClickHouse specifically?"**

ClickHouse is a great fit for append-heavy, analytical workloads. It's fast, open-source, has a great community, and scales reasonably. For the kind of ingestion volume the target has to handle (many streams, many records, rapid schema evolution), ClickHouse's columnar design + fast bulk inserts is an ideal target database.

**"Why not rewrite the TS target incrementally, module by module?"**

Because there's no clean seam. The modules are interdependent in ways that would force a bridge layer that speaks both JS and JVM — doable, but much more complex than just porting. A clean reimplementation is simpler and lets us run the two side-by-side during migration.

**"How confident are we that the Kotlin version is really equivalent?"**

Very confident — but not 100%. The 36-case integration suite covers all the scenarios the TS code was historically broken on and then fixed for. Any difference in ClickHouse state between TS and Kotlin would fail the suite. There are still edge cases that aren't in the suite (malformed tap output, weird Unicode in stream names, …) that we'd have to add tests for before fully decommissioning the TS version in production.

**"Could we use Kotlin Multiplatform and share the implementation with a JS target?"**

Technically yes, but it would defeat the point. The main gains of the Kotlin version come from JVM-specific things (real threads, streaming JSON parsers, primitive types, ZGC). Retargeting to JS would give us a slower version back.

**"How long did it take?"**

About a day of elapsed time from "no Kotlin code" to "full integration suite green in-process". See §8.5 for the breakdown and caveats.

**"Will this replace the TS target in production?"**

Eventually, yes. Not tomorrow. The plan is: run both in shadow mode (both ingesting the same data into parallel databases, comparing results) for a few weeks, fix any drift, then cut over.

**"What happens if the tap emits invalid JSON?"**

The TS target logs a warning and continues. Our Kotlin port does the same via the `TargetMessage.Unknown` variant. Neither crashes — that's a deliberate choice: a single bad line shouldn't abort a multi-million-row sync.

---

## 12. Workshop plan (if we have the time)

If we do the hands-on session, here's a rough sequence:

1. **15 min**: bring up the codebase, run the full test suite (make sure your laptop has Docker + a recent JDK).
2. **30 min**: take a flame graph of the `covidtracker.jsonl` ingestion. Everyone compares.
3. **30 min**: implement 9.3 (avoid the intermediate string allocation). Re-measure.
4. **30 min**: implement 9.4 (avoid per-row List allocation). Re-measure.
5. **30 min**: discuss what you saw: was the optimization worth it? What was the cost (readability, maintenance)?
6. **15 min**: wrap up, talk about what to do next (parallelism, streaming parser, …).

Objective is not to land the fastest implementation; it's to teach the habit of *measure, change one thing, re-measure, keep or revert*.

---

That's it. Thank you for listening. Let's go look at the code.
