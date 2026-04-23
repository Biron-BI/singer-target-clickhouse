# Performance enhancement plan

This document is a working log for making the Kotlin target fast enough to be a
drop-in replacement for the TS one, and then some. Every entry below should be
validated with `scripts/benchmark.sh` before and after.

## 1. Baseline — 10 M rows

Input: `/home/sestienney/biron/tap_output-birondemo-10M.jsonl.gz`
(≈10 M Singer `RECORD` messages once decompressed, 18 tables).
Host: Linux, cgroups v2, single ClickHouse on the loopback interface.
Both implementations run under Docker, with `--network=host`.

```
impl         iter   wall_ms    cpu_ms     eff_cores  rows       tables
---------------------------------------------------------------------------
kotlin       1      76262      88361      1.16       9999964    18
typescript   1      133409     169263     1.27       9999964    18
kotlin       2      83396      96741      1.16       9999964    18
typescript   2      133860     169396     1.27       9999964    18
kotlin       3      82231      95659      1.16       9999964    18
typescript   3      136035     171364     1.26       9999964    18

kotlin       median: wall=82231 ms  cpu=95659 ms  eff_cores=1.16
typescript   median: wall=133860 ms  cpu=169396 ms  eff_cores=1.27
wall speedup (ts / kotlin): 1.63x
cpu  ratio   (ts / kotlin): 1.77x   (<1 means kotlin burns more CPU to get its wall-time win)
row-count parity: OK (both 9999964)
```

### What this tells us

- Kotlin is **1.63×** faster on wall time and spends **1.77×** *less* CPU than
  TS. Both runtimes are effectively single-threaded for the ingestion path
  (eff_cores ≈ 1.2). The small overhang above 1.0 is the HTTP body being sent
  by an IO-executor thread while the main thread builds the next batch.
- The Kotlin implementation is **already CPU-bound** in a single thread:
  `cpu_ms ≈ wall_ms × eff_cores`. Network/disk are not the bottleneck on this
  host, and neither is ClickHouse parsing on the server side.
- Throughput: Kotlin ≈ **121 K rows/s**, TS ≈ **75 K rows/s**.

The implication: to go faster we need to **reduce per-record CPU work on the
main thread**. Pushing more cores at it is the wrong lever (see §4).

### 1.1 batch_size = 1000 experiment (kotlin only)

Same input, 2 iterations, only the default `batch_size` changed from 100 → 1000:

```
impl         iter   wall_ms    cpu_ms     eff_cores  rows       tables
---------------------------------------------------------------------------
kotlin       1      84775      95700      1.13       9999964    18
kotlin       2      88417      100028     1.13       9999964    18

kotlin       median: wall=86596 ms  cpu=97864 ms  eff_cores=1.13
```

Compared to the 100-batch baseline (wall 82.2 s, cpu 95.7 s): **flat**. A 10×
reduction in the number of HTTP flushes + MergeTree parts produced *no*
measurable wall-time change (and cpu_ms actually edged up by ~2 %, within
noise).

**What this falsifies.** Per-batch overhead — HTTP chunk setup, ClickHouse-side
parsing per batch, MergeTree part creation per batch — is **not** the bottleneck
on this workload. The hot path is strictly per-record CPU, which `batch_size`
does not touch. See §3.1 below for the revised call on that entry.

## 2. Where the CPU goes, per record

Walk the hot path once, per `RECORD` message, from stdin to HTTP body:

1. `BufferedReader.readLine()` — pulls one line from stdin.
2. `TargetMessageParser.parse(line)` — `ObjectMapper.readTree(line)` builds
   a `JsonNode` tree for the whole message.
3. `asMap(node["record"])` — `objectMapper.convertValue(node, Map::class.java)`
   walks the tree again and allocates a `LinkedHashMap<String, Any?>` of boxed
   values. **Every record allocates a fresh map.**
4. Construct `TargetMessage.Record(stream, record)` — sealed-class allocation.
5. `processRecord` → `RecordProcessor.pushRecord`:
   - Acquire `synchronized(lock)`.
   - Allocate `List<Any?>` for `currentPkValues` (+ boxed Longs).
   - Allocate another `List<Any?>` for the full row via `buildList {}`.
   - Append to `buffered: MutableList<List<Any?>>`.
   - If batch reached: `flushBuffered()`.
6. `flushBuffered()`:
   - For each buffered row, `jsonMapper.writeValueAsString(row)` — Jackson
     writes to an internal `SegmentedStringWriter`, then `toString()` copies.
   - `buildString { … }` concatenates N row strings via `StringBuilder`.
   - `.toByteArray(Charsets.UTF_8)` allocates another array and runs the
     UTF-8 encoder over the full buffer.
   - `ctx.writer.write(payload)` enqueues the buffer for the HTTP thread.
7. Children (nested arrays) recurse through steps 5–6 for each subtable.

Per-record allocation budget, rough order of magnitude:
- 1× input `String` (from `BufferedReader`)
- ~N tree nodes where N = fields in the message
- 1× `Map<String, Any?>` with boxed values (~2–5× field size)
- 2× `List<Any?>` for pkValues + row (with boxed numbers)
- 1× JSON `String` per record
- Fragments in `StringBuilder` + final `String` per flush
- 1× `ByteArray` per flush

That is a lot of young-gen garbage for what is structurally a byte-to-byte
transform. Most optimizations below boil down to **removing one or more of
those materializations**.

## 3. Optimizations, ranked by expected impact

Ordering reflects estimated wall-time gain on the current CPU-bound profile,
not implementation cost. Apply top-down, re-benchmark between each change,
back out anything that doesn't move the needle.

### Tier 1 — the big ones (per-record CPU reductions)

The §1.1 experiment showed that anything not touching per-record CPU is a
distraction. These three entries are the real Tier 1 now; the former §3.1
has been demoted and lives at the bottom of this section.

#### 3.1 Raise the default `batch_size` — ~~expected big~~ falsified, keep low

- **Previous hypothesis**: Each flush produces an HTTP chunk and triggers a
  CH-side MergeTree part creation + background merges, so batching ~100×
  more rows would save a proportional chunk of fixed overhead — estimated
  1.5–3× wall-time.
- **Measured (§1.1, 2026-04-23)**: 100 → 1 000 moved **nothing** on wall or
  CPU (and `eff_cores` actually dropped slightly, 1.16 → 1.13). In a
  CPU-bound per-record profile, per-batch overhead is lost in the noise.
- **Revised plan**: Still worth a single extra data point at **10 000**
  and **100 000** to confirm the curve stays flat at the larger end; if
  they come out ≥ baseline as well, close the question and leave the
  default at 100. Do **not** treat this as a prerequisite for Tier 1.
- **Why this matters as documentation**: It's a textbook "guess wrong
  once, measure, reprioritize" — future-you should not redo this.
- **Where to edit (if we do keep a larger default)**: `TargetConfig.kt`,
  plus `deletion_batch_size`.
- **Risk** (unchanged): with a larger batch, crash mid-stream loses more
  un-committed rows; Singer STATE discipline handles recovery.

#### 3.2 Kill the `Map<String, Any?>` per record

- **Current**: `TargetMessageParser.asMap` calls `convertValue(node, Map::class)`
  which walks the JsonNode subtree and allocates a boxed map.
  `ValueExtractor` is `(Any?) -> Any?` and assumes a `Map`.
- **What to do**: Make `TargetMessage.Record.record` a `JsonNode` instead of
  `Map<String, Any?>`. Change `ValueExtractor` to `(JsonNode?) -> JsonNode?`
  (or `(JsonNode?) -> Any?` if we still want to return native types for the
  non-translate-values path). Rewrite `buildValueExtractor` to navigate the
  tree with `node.get(part)`.
- **Why it wins**: One fewer full walk of the record, one fewer map allocation
  with boxed numbers, all string keys stay interned. JsonNode is Jackson's
  native data shape; extracting from it is a hash lookup and no box/unbox.
- **Expected**: **1.3–1.8×** standalone, more in combination with §3.3.
- **Where to edit**: `TargetMessage.kt`, `JsonSchemaInspector.kt`
  (`buildValueExtractor`), `JsonSchemaTranslator.kt` (`extractValue`),
  `RecordProcessor.kt` (`extractChildData`), `StreamProcessor.kt`
  (cleaning-column read), `DeletedRecordProcessor.kt`. Tests need the same
  adjustment — but they're unit-sized, easy to update.
- **Risk**: API-breaking internally; no external-facing change. Correctness
  verified by the existing integration suite.

#### 3.3 Write rows straight to the HTTP body via `JsonGenerator`

- **Current**: `RecordProcessor.flushBuffered` does
  `buildString { buffered.forEach { jsonMapper.writeValueAsString(row); … } }.toByteArray()`.
  Three full materializations: `Any? → String → StringBuilder → ByteArray`.
- **What to do**: In `HttpStreamingRowWriter`, expose an `OutputStream` view
  of the body. In `flushBuffered`, obtain a `JsonGenerator` bound to that
  stream and, per row:
  ```kotlin
  gen.writeStartArray()
  pkMappings.forEach { gen.writeValue(pk.extractor(data)) }
  simpleMappings.forEach { gen.writeValue(col.extractor(data)) }
  version?.let { gen.writeNumber(it) }
  gen.writeEndArray()
  gen.writeRaw('\n')
  ```
  For the JsonNode path (§3.2), `gen.writeTree(node)` is a direct copy.
- **Why it wins**: Zero intermediate strings, zero intermediate byte arrays,
  zero UTF-8 re-encoding. The HTTP body streams through `JsonGenerator` →
  `OutputStream` → `BlockingQueueInputStream` → HTTP IO, with one byte copy.
- **Expected**: **1.3–1.8×** on its own, **~2×** when stacked with §3.2.
- **Where to edit**: Add `OutputStream openBodyStream()` to `RowWriter`
  (or change the contract entirely: `fun writeRows(block: (JsonGenerator) -> Unit)`).
  Rewrite `HttpStreamingRowWriter` to expose its `BlockingQueueInputStream`'s
  dual `OutputStream` (ByteArrayOutputStream that self-flushes on ~64 KB).
- **Risk**: Need to handle `generator.flush()` points vs. HTTP chunking.
  Unit tests already assert the byte-for-byte payload shape — they'll catch
  any regression.

#### 3.4 Stream-parse the input, don't `readTree` every line

- **Current**: `TargetMessageParser.parse` does `objectMapper.readTree(trimmed)`
  which materializes the whole JsonNode tree before we even know it's a RECORD.
- **What to do**: Use `JsonFactory.createParser(line)`, walk the top-level
  object once, dispatch on `"type"`, and for `RECORD`/`DELETED_RECORD` either
  (a) stop at the `record` field and return the parser pointing at that
  sub-object (§3.2 then reads directly from the parser token stream), or (b)
  read the sub-object to a `JsonNode` only from that offset. Either way we
  skip parsing the outer wrapper into a tree.
- **Why it wins**: Jackson's token parser is ~2× faster than `readTree` for the
  same input; combined with §3.2/§3.3 it removes the "parse-to-tree, then
  walk the tree again" double work.
- **Expected**: **1.2–1.4×** on its own.
- **Where to edit**: `TargetMessage.kt` (`TargetMessageParser`).
- **Risk**: Parser state machine is easy to mis-sequence. Unit tests already
  cover all five message types with malformed / partial variants — keep them.

### Tier 2 — likely 1.1×–1.3× each

#### 3.5 Enable HTTP body compression

- **Current**: plain JSON body, `Content-Encoding` unset.
- **What to do**: Set `Content-Encoding: zstd` and wrap the body writer in
  a zstd output stream (`com.github.luben:zstd-jni`). ClickHouse accepts
  `decompress=1` or the `Content-Encoding` header; pick the one the server
  build supports.
- **Why it wins**: JSONCompactEachRow compresses ~3–5×; zstd level 1 is
  typically ≤5 % CPU overhead on modern hardware.
- **Caveat**: On the current loopback benchmark, the body is never the
  bottleneck — so expect **~1.0×** here and **1.2–1.5×** over real networks
  (prod scenario: tap in one region, CH in another).
- **Where to edit**: `ClickhouseConnection.openRowWriter` / `insertUrl`.
- **Risk**: New dependency, new failure mode (bad zstd frame → CH rejects).

#### 3.6 Widen the BufferedReader and preallocate

- **Current**: default `BufferedReader` (8 KB buffer).
- **What to do**: `BufferedReader(reader, 64 * 1024)` at the input site in
  `processStream`. One fewer syscall per 8× more bytes.
- **Expected**: **1.02–1.05×** — small but free.
- **Where to edit**: `ProcessStream.kt` line 54.

#### 3.7 Precompute per-record write plans

- **Current**: per record we walk `meta.pkMappings` and `meta.simpleColumnMappings`
  as `List`, calling `extractValue` for each.
- **What to do**: At stream init, precompute an `Array<(Source) -> Unit>` of
  direct "extract-and-write" closures bound to the `JsonGenerator`. Iteration
  over arrays (not lists) with monomorphic callsites plays better with the
  JIT.
- **Expected**: **1.05–1.15×**. Most of the win comes only if §3.3 lands.
- **Where to edit**: `RecordProcessor.kt`.

#### 3.8 GC tuning (ZGC or Shenandoah)

- **Current**: default G1 on JDK 21, default heap.
- **What to do**: Add `-XX:+UseZGC -Xmx2g` (or 4g/8g depending on `batch_size`).
  Also consider `-XX:+UseStringDeduplication` — all the stream/table names
  repeat across records.
- **Why it matters**: At current allocation rate (rough guess: 100–300 MB/s
  young-gen), G1 pauses are sub-10 ms but cumulative. ZGC has sub-ms pauses
  and scales to large heaps without stop-the-world.
- **Expected**: **1.0–1.1×** once allocations are already trimmed by §3.2/§3.3;
  larger if you skip those and rely on bigger heaps instead.
- **Where to edit**: `Dockerfile` (JVM args) + optionally a `JAVA_TOOL_OPTIONS`
  env var.

#### 3.9 Skip the `translate_values` fast-path when the tap sends typed JSON

- **Current**: `translate_values=false` by default already bypasses the
  per-value `SchemaTranslator.buildTranslator(...)` call. Good.
- **What to do**: Advertise it in docs; for taps that emit `"42"` strings for
  integer columns, measure whether the translator cost is worth the later
  CH-side coercion.
- **Expected**: **~1.0–1.1×** when enabled-but-unused is eliminated; zero
  (already off by default) otherwise.
- **Where to edit**: docs + benchmark harness.

### Tier 3 — marginal, apply only if profile says so

#### 3.10 Coalesce small byte payloads before enqueuing to HTTP body

- **Current**: every batch-flush puts one ByteArray onto the
  `BlockingQueueInputStream`. At 10 K-row batches this is large (~MBs) and
  fine. At 100-row batches (old default) it's fragmented.
- **What to do**: Only relevant if we keep small batches. Buffer into a
  reusable ByteBuffer of ~256 KB before enqueuing.
- **Expected**: **1.02–1.05×** at tiny batch sizes, **0%** once §3.1 lands.

#### 3.11 Drop the per-row `List<Any?>` (write directly from JsonNode)

Covered implicitly by §3.2 + §3.3. Listed here as an explicit reminder: once
both land, remove `buildInsertValues` entirely. The generator writes straight
from the extracted `JsonNode` values, bypassing the boxed-list materialization.

#### 3.12 Lock-free ingestion path

- **Current**: `synchronized(lock)` in `pushRecord` / `endIngestion` / timer
  callback. Uncontended on one ingestion thread — cost is a couple of ns per
  call.
- **Expected**: **<1.01×** standalone. Skip unless a profiler flags it.

#### 3.13 `StringDeduplication` for stream/table names

Already covered by §3.8 (`-XX:+UseStringDeduplication`). Mentioned here so
readers don't miss it.

#### 3.14 mmap the input when it's a seekable file

- **Current**: `BufferedReader(InputStreamReader(in))`, syscall-driven.
- **What to do**: When `--input <file>` is given, `FileChannel.map(READ_ONLY, …)`
  and wrap in a `ByteArrayInputStream` / direct parser. Not available when
  reading from stdin (the common CLI use case).
- **Expected**: **1.05–1.1×** for file inputs; **0%** for stdin.

## 4. Things to not do

From chapter 9 of the teaching notes, plus what I'd add:

- **Per-stream-parallel ingestion** of records within a stream. Breaks the
  tap's emission order, races on `_ver`, can silently corrupt a
  `ReplacingMergeTree`. Don't.
- **Fan-out across streams at the top level**. Singer taps are usually
  sequential per-stream — we'd pay coordination cost for gains that only
  materialize when the tap interleaves, which is rare. The finalize step
  *already* runs in parallel across streams (`finalize_concurrency`), and
  that's the place where order genuinely doesn't matter.
- **Rewrite in Rust/C++**. The above optimizations likely close most of the
  remaining gap to hand-rolled code. Revisit only when we've exhausted §3.
- **Switch to gRPC**. The HTTP insert interface is already efficient; the
  protocol is not the bottleneck.
- **Cache parsed JSON schemas**. Schemas arrive once per stream and parsing
  them is microseconds — classic premature optimization.

## 5. Measurement methodology

- Always use `scripts/benchmark.sh -n 3 <file>` so you get three iterations
  and a median. Wall-time variance is usually ±2 %; anything smaller than
  5 % is noise.
- Report both `wall_ms` *and* `cpu_ms`. A change that makes wall time worse
  but CPU much better is still useful for multi-instance throughput.
  Likewise a change that wins wall time by burning two more cores has a
  cost you want to be aware of.
- For fine-grained per-function analysis, attach **async-profiler** in
  `cpu` mode to the running container:
  ```
  docker run -d --name bench --network=host -v $PROFILER:/profiler … <image>
  docker exec bench /profiler/profiler.sh -d 30 -f /profiler/flame.html <pid>
  ```
  Flame graphs will show the Jackson/convertValue slice first — that's §3.2.
- Keep baseline numbers at the top of this doc after each round; don't
  rewrite history. That way we can answer "what did §3.1 buy us?" three
  months from now.

## 6. Suggested sequencing

Updated after §1.1 showed batch_size isn't the lever we thought it was.

1. **§3.2 + §3.3 together** — the real Tier 1 entry point. Both share the
   `ValueExtractor` signature change, so a single refactor lands both wins.
   Benchmark before merging — expect the first meaningful wall-time move
   to come from here.
2. **§3.4 streaming parser** — only after §3.2/§3.3, to avoid doing the
   work twice.
3. **§3.8 GC flags** — cheap, measure with and without.
4. **§3.5 compression** — only if the benchmark moves off the loopback and
   onto a real network.
5. **§3.1 batch_size at 10 K and 100 K** — one more sweep for completeness,
   purely to close the question; don't plan for a win here.
6. **Everything in Tier 3** — only if a profile says so.

Expected cumulative improvement if §3.2 + §3.3 + §3.4 land as described:
**2.5–4×** on wall time from today's baseline, at broadly the same CPU
consumption per row (so still ≈1.2 effective cores). That puts us at
~300–500 K rows/s on the 10 M benchmark above, i.e., a 10 M-row ingestion
in ~20–35 seconds. The earlier "3–5×" guess baked in a §3.1 contribution
that §1.1 just falsified — revise down accordingly.

## 7. History

- **2026-04-23** — Initial baseline captured (see §1). No optimizations
  applied yet beyond the defaults produced by the port.
- **2026-04-23** — §3.1 `batch_size` 100 → 1 000 trialled (§1.1); flat
  wall time, flat CPU. Reclassified §3.1 from "Tier 1 biggest payoff" to
  "keep for completeness, don't block on it". Tier 1 re-sorted to lead
  with §3.2 + §3.3 (the per-record allocation cuts).
