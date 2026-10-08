# Partitioning root tables (`partition_by`)

`partition_by` is an optional field of SCHEMA messages that partitions the root
table of a stream by the month or the year of one of its properties. Its
purpose is to make the end-of-run deduplication rewrite only the partitions
that received rows, instead of the whole table.

Streams whose SCHEMA message has no `partition_by` behave exactly as before.

## 1. Why

When a stream declares `key_properties`, its root table is a
`ReplacingMergeTree(_ver)`. Every run inserts new versions of the rows it
receives and, once the stream is fully read, finalizes it:

1. `OPTIMIZE TABLE <root> FINAL` physically removes the outdated versions;
2. `ALTER TABLE <child> DELETE … NOT IN (SELECT … FROM <root>)` removes the
   child rows of the versions that were just removed;
3. a primary key integrity check makes sure no key is stored twice.

A table created without `PARTITION BY` is one single partition, so step 1
rewrites the whole table on every run, however few rows were inserted. While
the merge runs, ClickHouse needs as much free disk space as the table takes,
and the replaced parts are only deleted `old_parts_lifetime` (8 minutes by
default) after the merge ends.

Once the table is partitioned, step 1 only rewrites the partitions that
received rows (see [§5](#5-how-the-end-of-run-optimize-works)). The extra disk
space needed during a run drops to the size of those partitions.

## 2. Declaring it

```json
{
  "type": "SCHEMA",
  "stream": "events",
  "schema": {"…": "…"},
  "key_properties": ["id"],
  "partition_by": {"property": ["attributes", "timestamp"], "type": "timestamp", "converter": "YYYYMM"}
}
```

* `property`: path to a property of the root record. Nested objects are
  allowed. Properties inside arrays are not, since they are stored in child
  tables. The tap does not need to know how the target names the column:
  `["attributes", "timestamp"]` becomes `attributes__timestamp` or
  `attributes_timestamp` depending on `subtable_separator`.
* `type`: how to read the property. Only `timestamp` is supported: an integer
  Unix timestamp, in seconds.
* `converter`: the granularity of the partitions, `YYYYMM` (month) or `YYYY`
  (year).

The target generates the partition key, computed in UTC so that the partition
of a row never depends on the server timezone:

| converter | `PARTITION BY`                                         |
|-----------|--------------------------------------------------------|
| `YYYYMM`  | `toYYYYMM(toDateTime(attributes__timestamp, 'UTC'))`   |
| `YYYY`    | `toYear(toDateTime(attributes__timestamp, 'UTC'))`     |

Like key columns, the partition column is created non-nullable, since
ClickHouse refuses a Nullable partition key. A record without a value for it is
stored with `0`, so it lands in the `197001` (or `1970`) partition. This is
also what happens to a key column without a value: ClickHouse's JSON input
turns `null` into the column default for non-Nullable columns.

The run fails at the SCHEMA message if `partition_by` is malformed (unknown
`type` or `converter`, `property` not a non-empty array of strings) or if the
property is unknown, inside an array, or not an integer.

## 3. Choosing the property

### 3.1 Its value must never change for a given key

ReplacingMergeTree only deduplicates rows **within a partition**: merges never
combine parts of different partitions, and `OPTIMIZE … FINAL` merges each
partition on its own. If two versions of the same key have timestamps in
different months (or years), they end up in two partitions and no `OPTIMIZE`
will ever merge them.

* Good: the time an immutable event happened, a creation date.
* Bad: anything that can be updated, such as `updated_at` or the last
  activity date of a profile.

The target cannot check that a property is immutable. If this rule is broken,
the integrity check at the end of the run fails with an explicit diagnosis:

```
Duplicate key on table `events`, data: [[e1]], aborting process. Key [e1] of `events` is stored
in 2 partitions: its partition_by value [toYYYYMM(…)] changed between two versions, which
ReplacingMergeTree cannot deduplicate. …
```

The rows are already inserted at that point. Removing the stale versions
keeps the latest version of every key, as a whole-table `OPTIMIZE FINAL`
would. The orphan cleanup of the next run then removes their child rows:

```sql
DELETE FROM <database>.<stream>
WHERE (<key columns>, _ver) NOT IN (SELECT <key columns>, max(_ver) FROM <database>.<stream> GROUP BY <key columns>);
```

### 3.2 Granularity

`YYYYMM` suits most event streams. `YYYY` suits low volumes, where monthly
partitions would be tiny.

* A partition is the unit that gets rewritten: a run needs free disk space for
  the partitions it touches.
* ClickHouse refuses by default an insert block spanning more than 100
  partitions, which a full-history load by month can exceed. The target raises
  that limit (`max_partitions_per_insert_block`) to 1000 for its inserts, so a
  block mixing many months is accepted. ClickHouse then writes one small part per
  month, which background merges absorb.

## 4. Child tables are not partitioned

Child tables (the tables created for arrays, linked to the root by the root
key and `_root_ver`) keep their current DDL:

* They do not contain the root property the partition is computed from.
  Partitioning them the same way would require copying it into every child
  table.
* They are plain `MergeTree` tables and are never optimized. Their cleanup is
  the `ALTER TABLE … DELETE` mutation of step 2, which only rewrites the parts
  of a child table that contain rows to delete. The other parts are cloned
  through hard links.

## 5. How the end-of-run OPTIMIZE works

On a partitioned root table, step 1 becomes:

```sql
OPTIMIZE TABLE <root> FINAL SETTINGS optimize_skip_merged_partitions = 1
```

ClickHouse walks the partitions one after the other and skips every partition
made of a single part that is already the result of a merge (or, on recent
versions, of a deduplicated insert). Such a partition holds no duplicate. The
partitions that received rows contain several parts and get merged.

Steps 2 and 3 are unchanged, so the tables end up deduplicated exactly as
before.

This was preferred over computing the touched partitions with
`SELECT DISTINCT _partition_id FROM <root> WHERE _ver > <max _ver at start>`
followed by one `OPTIMIZE TABLE <root> PARTITION ID '…' FINAL` per partition:

* The setting only looks at the physical state of the table, so it also merges
  partitions left with duplicates by a run that died between its inserts and
  its `OPTIMIZE`. The `_ver`-based variant never revisits them: their rows
  have a `_ver` below the next run's starting point, so the integrity check
  would fail on every following run.
* A stream may receive several `SCHEMA` messages in one run. Each one rebuilds
  the stream processor with a new starting `_ver`, so the `_ver`-based variant
  would miss the rows inserted before the last `SCHEMA` message.
* It is a single statement, with no extra scan of the table.

## 6. Existing tables

ClickHouse cannot change the partition key of an existing table, so the
existing root table wins over the SCHEMA message. On every SCHEMA message, the
target reads the partition key of the existing root table
(`system.tables.partition_key`):

* **The table is not partitioned:** nothing changes compared to a stream
  without `partition_by`. If the SCHEMA message declares `partition_by`, a
  warning gives the expected `PARTITION BY` and the field is ignored: the
  partition column keeps its current type and the whole-table `OPTIMIZE FINAL`
  runs as before.
* **The table is partitioned:** its partitioning is kept, with or without
  `partition_by` in the SCHEMA message. The end-of-run OPTIMIZE is the
  partition-aware one, and the columns of the partition key are never made
  Nullable, even if the schema declares them nullable.
* **The table is partitioned by another key than `partition_by`:** the run
  fails before any change, for instance when `converter` changes from `YYYY`
  to `YYYYMM`.

The target quotes the column in the key it generates, and recognizes a table's
key as is whether ClickHouse prints that column with or without the backquotes.
Only a key spelled differently, e.g. a table partitioned by hand with extra
spaces, is normalized by ClickHouse before comparing (`formatQuerySingleLine`,
ClickHouse 23.10 or later).

Tables that are dropped and recreated (`clean_first: true` streams, or
`--update-streams <stream>`) are created from the SCHEMA message, so without
partitioning if it has no `partition_by`.
