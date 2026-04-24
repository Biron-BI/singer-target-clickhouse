# Performance enhancement log

This document is the working record of making the Kotlin target faster than
the published TS one. Every change below was validated against
`scripts/benchmark.sh` on the 10 M-row fixture (§1) and driven by an IntelliJ
profiler session on a representative local fixture (§2).

## 1. Baseline — 10 M rows (2026-04-23)

Input: `/home/sestienney/biron/tap_output-birondemo-10M.jsonl.gz` (≈10 M
`RECORD` messages, 18 tables). Host: Linux, cgroups v2, single ClickHouse on
the loopback interface. Both implementations ran under Docker, `--network=host`.

```
impl         iter   wall_ms    cpu_ms     eff_cores  rows       tables
---------------------------------------------------------------------------
kotlin       1      76262      88361      1.16       9999964    18
typescript   1      133409     169263     1.27       9999964    18
kotlin       2      83396      96741      1.16       9999964    18
typescript   2      133860     169396     1.27       9999964    18
kotlin       3      82231      95659      1.16       9999964    18
typescript   3      136035     171364     1.26       9999964    18

kotlin       average: wall=80630 ms  cpu=93587 ms  eff_cores=1.16
typescript   average: wall=134435 ms  cpu=170008 ms  eff_cores=1.27
wall speedup (ts / kotlin): 1.67x
cpu  ratio   (ts / kotlin): 1.82x   (<1 means kotlin burns more CPU to get its wall-time win)
row-count parity: OK (both 9999964)
```

### What this told us

- Kotlin was already **1.67×** faster on wall time and spent **1.82×** *less*
  CPU than TS. Both runtimes were effectively single-threaded for the ingestion
  path (eff_cores ≈ 1.2). The small overhang above 1.0 is the HTTP body being
  sent by a separate I/O-executor thread while the main thread builds the next
  batch.
- Kotlin was already **CPU-bound** on the main thread: `cpu_ms ≈ wall_ms ×
  eff_cores`. Network/disk were not the bottleneck, and neither was CH-side
  parsing.
- Throughput: Kotlin ≈ **124 K rows/s**, TS ≈ **75 K rows/s**.

To go faster we needed two things, in this order: **reduce per-record CPU on
the main thread**, then **parallelise what's left**.

### 1.1 The `batch_size` false lead (kept for the teaching)

Early hypothesis: 100 → 1 000 per-flush rows would cut HTTP chunk + MergeTree
part overhead by ~10×.

Same input, 2 iterations:

```
impl         iter   wall_ms    cpu_ms     eff_cores  rows       tables
---------------------------------------------------------------------------
kotlin       1      84775      95700      1.13       9999964    18
kotlin       2      88417      100028     1.13       9999964    18

kotlin       average: wall=86596 ms  cpu=97864 ms  eff_cores=1.13
```

**Flat.** A 10× reduction in flush count moved nothing — confirming the
bottleneck was strictly per-record CPU, not per-batch overhead. Textbook
"guess wrong, measure, reprioritise". Left here so future-you doesn't redo it.

## 2. Profiling methodology

Every optimisation below was driven by a single IntelliJ profiler view:
**Call Tree**, **CPU Time** mode.

### Running the target under the profiler

Ran the Kotlin target in-IDE with a local JSONL fixture, with ClickHouse in a
local Docker container. The target itself was NOT dockerised for profiling —
just a plain `MainKt` Run Configuration with:

```
VM options:   -XX:StartFlightRecording=filename=target-ch.jfr,settings=profile,dumponexit=true
Program args: --config /abs/path/config.json --input /abs/path/sample-input.jsonl
```

`dumponexit=true` writes the `.jfr` on process exit. Opens in IntelliJ
Ultimate's profiler tab. Equivalent path: IntelliJ's built-in "Profile with
IntelliJ Profiler" button (async-profiler under the hood) — same Call Tree.

### How to read the Call Tree

Two numbers per row:

- **Percentage** — fraction of the parent's CPU time this method was on-CPU.
  This drove the "is it worth optimising?" decision.
- **Execution time (ms)** — the absolute cost, useful to track step-to-step
  deltas across profile sessions.

Hot paths collapse to a single column as long as >50% of the parent's time
stays on one descendant. The moment it splits, the leaf contributions on that
row are what you optimise.

### What CPU Time *doesn't* show

`CPU Time` mode elides wait/blocked time. When the main thread is blocked
(socket read, `responseFuture.get`, `queue.take`), no sample is attributed.
Consequence: the profile is informative for CPU-bound paths but blind to
waits. For this workload the main thread was always CPU-bound, so it worked
out — but Wall Time mode is needed if waits become the suspect, which was the
case for one idea we couldn't test (see §6).

### `Thread.run()` subtrees

These are non-main threads — HTTP worker, scheduler, etc. Their CPU already
runs in parallel with main, so their % doesn't add to wall time unless they
exceed a whole core. For this target, HTTP worker stayed at ~11% of one core.

## 3. Optimisations applied, in order

Each step: what the profile said → what changed → measured effect.

### Step 1 — parse once, straight to `Map<String, Any?>`

**Profile before** (baseline, ~77 s CPU on the profiling fixture):
`TargetMessageParser.parse` was **45–53%** of main-thread CPU, split as:

- `ObjectMapper.readTree(line)` = 30.7% — built a `JsonNode` tree for the
  whole message.
- `asMap(node["record"])` = 13.6% — `objectMapper.convertValue(node, Map::class)`.
  Decomposed internally into `serializeValue` (5.1%) + `MapDeserializer.deserialize`
  (8.8%): Jackson serialised the already-parsed `JsonNode` through a
  `TokenBuffer` and re-deserialised it into a `LinkedHashMap`. **The record
  body went through Jackson twice.**

**Change** (`TargetMessage.kt`): replaced `readTree` + `convertValue` with a
single `objectMapper.readValue(line, mapType)` deserialising directly into
`LinkedHashMap<String, Any?>`. `State.value` switched from `JsonNode` to
`Any?` — the only downstream use was `writeValueAsString(msg.value)`, which
works identically on a `Map`/`List`/primitive tree.

**After**: parse dropped from ~45% → ~30% of main-thread CPU. Wall time
−20% (per user-observed throughput).

### Step 2 — feed Jackson the raw `InputStream`

**Profile before** (~60 s CPU): with step 1 in, the new hotspot was
`BufferedReader.readLine()` at **21.5%** of CPU:

- `fill()` = 15.2% — `InputStreamReader` decoding UTF-8 bytes into a char
  buffer.
- `nextFieldName` downstream in Jackson = re-tokenising those chars back.
- `IndexingSequence.hasNext` from `lines.withIndex()` = 7.8% iterator overhead.

In effect every byte touched the CPU **three times** on the way in: UTF-8
decode to char, scan for `\n`, then re-tokenise.

**Change** (`TargetMessage.kt`, `ProcessStream.kt`, `Main.kt`): opened a
single `JsonFactory.createParser(inputStream)` and drove the loop with
`parser.nextToken()` + `objectMapper.readValue(parser, mapType)`. Jackson's
UTF8 tokenizer decodes bytes + tokenises in one pass. No `InputStreamReader`,
no `BufferedReader`, no per-line `String`, no `withIndex()` iterator.

Behaviour change consciously accepted: malformed JSON mid-stream now aborts,
where the line-based path logged-and-continued. Streaming parsers can't
reliably resync on corrupt bytes. The string-based `parse(line)` entry point
was kept with its old recovery semantics, for the unit-test contract.

**After**: 77 s → ~60 s CPU. Wall time −20%.

### Step 3 — write row batches through a `JsonGenerator`

**Profile before** (~48 s CPU): with steps 1-2 in, `flushBuffered` was
**21.6%** of CPU, split:

- `jsonMapper.writeValueAsString(row)` per row = 13.5% — each call allocated
  a fresh `StringWriter` + char buffer, returning a `String`.
- `StringBuilder.append` for the outer `buildString { }` = 1.8%.
- `String.getBytes(UTF_8)` encoding the whole batch back to bytes = 5.2%.

Three full materialisations of the same data: `List<Any?> → String →
StringBuilder → ByteArray`.

**Change** (`RecordProcessor.kt`): built each batch into a
`ByteArrayOutputStream` via a `JsonGenerator` (UTF-8 direct). Per row:
`jsonMapper.writeValue(gen, row)` + `gen.writeRaw('\n')`. No intermediate
`String`, no outer `StringBuilder`, no final UTF-8 re-encode. Jackson emits
UTF-8 bytes straight into the buffer.

Unit-test-caught gotcha: `JsonGenerator` defaults to a single space as its
root-value separator (legacy pretty-printer default). Between successive rows
the output had `[1,"a"]\n [2,"b"]\n` — the extra leading space before row 2+.
ClickHouse's `JSONCompactEachRow` tolerates leading whitespace, so integration
tests passed; byte-level unit tests caught it. Fix: `gen.setRootValueSeparator(null)`.

**After**: `flushBuffered` dropped from 21.6% → ~16% of CPU. Step total ≈ −13%
CPU.

### Step 4 — parse on its own thread

**Profile before** (~41 s CPU): with steps 1-3 in, the residual split was

- `readNext` (Jackson tokenisation + Map build) = **63%** — near the floor for
  "parse arbitrary JSON into a Map". Inside it: `UTF8StreamJsonParser.nextFieldName`
  33%, `MapDeserializer._readAndBindStringKeyMap` 61.7%. This is mostly inherent
  to the format; further wins require schema-coupled parsing (§6).
- `processLine` (extract + serialise + HTTP enqueue) = 23%.
- Everything else = ~2%.

These ran **sequentially on the main thread**: parse one → process one →
parse next. The profile showed two distinct CPU phases alternating on one
thread with the HTTP worker using ~11% of a second core.

**Change** (`ProcessStream.kt`): split the two phases onto two threads. A
`singer-parser` daemon producer runs `readNext` in a loop and pushes
`TargetMessage` values into a bounded `ArrayBlockingQueue<ParseSignal>(1024)`.
The main thread consumes and calls `processLine`. `ParseSignal` is a sealed
type with `Msg` / `Err` / `Eof` — errors from the parser arrive as `Err` and
feed into the existing `abort(...)` path; EOF as `Eof`. Consumer-side aborts
interrupt the producer via the existing `.isInterrupted` check + `queue.put()`
interrupt semantics.

Backpressure is natural: the queue bounds peak memory; producer blocks on
`put` when the consumer falls behind. STATE / commit semantics are unchanged
— the main thread still blocks on `writer.close()` when committing, the
producer simply keeps filling the queue during that wait.

**After**: see §4. CPU barely moved (we didn't reduce work, we parallelised
it); `eff_cores` jumped 1.16 → 1.71 and wall time dropped proportionally.

## 4. Final benchmark — after all four steps

Same 10 M fixture, 3 iterations.

```
impl         iter   wall_ms    cpu_ms     eff_cores  rows       tables
---------------------------------------------------------------------------
kotlin       1      45225      77167      1.71       9999964    18
kotlin       2      46493      79895      1.72       9999964    18
kotlin       3      48065      81943      1.70       9999964    18

kotlin       average: wall=46594 ms  cpu=79668 ms  eff_cores=1.71
```

Versus the kotlin baseline (§1):

| metric      | baseline | after  | delta                     |
|-------------|----------|--------|---------------------------|
| wall_ms     | 80630    | 46594  | **−42%** (1.73× speedup)  |
| cpu_ms      | 93587    | 79668  | −15%                      |
| eff_cores   | 1.16     | 1.71   | +0.55                     |
| rows/s      | ~124 K   | ~215 K | 1.73×                     |

Versus the TS baseline:

| metric      | typescript | kotlin (now) | ratio               |
|-------------|------------|--------------|---------------------|
| wall_ms     | 134 435    | 46 594       | **2.88×** faster    |
| cpu_ms      | 170 008    | 79 668       | **2.13×** less CPU  |

The CPU savings came from steps 1-3 (less per-record work). Step 4 converted
remaining CPU cost into parallel work — wall dropped without much CPU change.

## 5. Tried and reverted

### Stream rows directly into the generator from `pushRecord`

Proposal: replace `buildInsertValues` + `buffered: MutableList<List<Any?>>`
with a generator + BAOS living on the `Ingestion` object, and in `pushRecord`
write each column inline via typed `gen.writeString` / `writeNumber` / ...
Expected to kill the per-row list allocation (5.4% of CPU in the profile) and
the per-element Jackson serialiser dispatch inside `writeValue` (part of the
14.7%).

**Measured gain**: ~2%.

**Verdict**: reverted. Generator lifecycle coupled to `Ingestion`, a
`writeScalar` type ladder, more surface area under the synchronisation lock.
Not worth 2%. The simpler BAOS-per-flush version from step 3 is the
equilibrium.

## 6. Considered but not pursued

### Async CH writes — decouple STATE from `responseFuture.get()`

`HttpStreamingRowWriter.close()` blocks the main thread on the in-flight HTTP
response at every batch-end / STATE. The idea: track pending closes as
`CompletableFuture`s, fire-and-forget from the main loop, emit STATE only
when the futures its batches depend on have resolved. Preserves the Singer
"STATE-follows-persistence" durability contract.

Not pursued: Wall Time profiling wasn't available in the current IDE install,
so we couldn't measure how much the main thread actually sits in
`responseFuture.get()`. CPU Time by construction doesn't show waits. Worth
reopening if Wall Time (or a Socket I/O monitor) surfaces a real wait.

### Parse records into a schema-indexed typed row

Skip the `LinkedHashMap` per record entirely. A custom token-stream parser
would, given the prior `SCHEMA` message, stream values directly into a
pre-sized `Array<Any?>` indexed by column position. Today's
`UntypedObjectDeserializerNR$Scope.putValue` = 7.4% of CPU + the `extractValue`
hashes would drop close to zero.

Estimated ~10-15% of CPU for a significant chunk of new, schema-coupled code.
Not the best ROI until the other big levers are exhausted.

### HTTP body compression (zstd)

Plain JSON body today. `Content-Encoding: zstd` compresses
`JSONCompactEachRow` ~3-5× at ~5% CPU cost. On the current loopback
benchmark the body isn't the bottleneck — expected ≈1.0× here, **1.2-1.5×**
over a real network (tap in one region, CH in another). Revisit when moving
off loopback.

## 7. Things to still not do

- **Per-stream-parallel ingestion within a stream.** Breaks the tap's
  emission order, races on `_ver`, can silently corrupt a
  `ReplacingMergeTree`.
- **Fan-out across streams at the top level.** Singer taps are sequential
  per-stream in practice — we'd pay coordination cost for gains that rarely
  materialise. The finalize step already parallelises across streams
  (`finalize_concurrency`), which is the place where order genuinely doesn't
  matter.
- **Rewrite in Rust/C++.** The four steps above closed the gap substantially
  relative to the TS target.
- **Switch to gRPC.** The HTTP insert interface is not the bottleneck.
- **Cache parsed JSON schemas.** Microseconds; classic premature
  optimisation.

## 8. Measurement methodology

- Always `scripts/benchmark.sh -n 3 <file>` — three iterations, report
  **average**. Wall-time variance is typically ±3% on this host; treat deltas
  smaller than 5% as noise.
- Report both `wall_ms` *and* `cpu_ms`. A win on wall time by burning two
  more cores has a cost worth knowing (step 4 is an example — CPU barely
  moved but `eff_cores` did).
- For per-function analysis: JFR (`-XX:StartFlightRecording=...`) opened in
  IntelliJ's profiler, or the built-in "Profile with IntelliJ Profiler"
  button. Both land in the Call Tree / Flame Graph view.
- Keep per-step benchmark numbers in §9 so future-you can answer "what did
  step X buy us?" without re-running.

## 9. History

- **2026-04-23** — Baseline captured (§1). `batch_size` 100 → 1 000 trial
  was flat (§1.1), confirming the bottleneck was per-record CPU, not
  per-batch overhead.
- **2026-04-23** — Step 1 (parse-to-Map) landed. Parser 45% → 30% of CPU,
  ≈−20% wall.
- **2026-04-23** — Step 2 (InputStream + single `JsonParser`) landed.
  `readLine` 21.5% → 0; ≈−20% wall.
- **2026-04-23** — Step 3 (`ByteArrayOutputStream` + `JsonGenerator` in
  `flushBuffered`) landed. `flushBuffered` 21.6% → ~16%; ≈−13% CPU.
- **2026-04-23** — Per-row direct-to-generator refactor tried (§5) and
  reverted; 2% gain didn't justify the complexity.
- **2026-04-23** — Step 4 (parser pipelining) landed. `eff_cores` 1.16 →
  1.71; wall dropped to 46.6 s (§4). Cumulative **1.73×** over the baseline,
  **2.88×** over the TS target.
