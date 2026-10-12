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
2. `DELETE FROM <child> … NOT IN (SELECT … FROM <root>)` deletes the child
   rows of the versions that were just removed;
3. a primary key integrity check makes sure no key is stored twice.

Lightweight deletes only mask rows. Once every stream is finalized, the run
removes them from disk in the tables where they reach
`deleted_rows_reclaim_ratio` of the rows (see [§5](#5-how-the-end-of-run-optimize-works)).

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
* `type`: how to read the property. It must match the property's type in the
  schema:

  | `type`      | property                                            | column                    |
  |-------------|-----------------------------------------------------|---------------------------|
  | `timestamp` | `integer`: a Unix timestamp, in seconds             | `Int64`                   |
  | `date`      | `string` with format `date` (or `x-excel-date`)     | `Date`                    |
  | `date-time` | `string` with format `date-time` (or `date-time64`) | `DateTime` (`DateTime64`) |

* `converter`: the granularity of the partitions, `YYYYMM` (month) or `YYYY`
  (year).

The target generates the partition key:

| `type`                | converter | `PARTITION BY`                                       |
|-----------------------|-----------|------------------------------------------------------|
| `timestamp`           | `YYYYMM`  | `toYYYYMM(toDateTime(attributes__timestamp, 'UTC'))` |
| `timestamp`           | `YYYY`    | `toYear(toDateTime(attributes__timestamp, 'UTC'))`   |
| `date` or `date-time` | `YYYYMM`  | `toYYYYMM(created_at)`                               |
| `date` or `date-time` | `YYYY`    | `toYear(created_at)`                                 |

A timestamp is converted in UTC, so its partition never depends on the server
timezone. A `DateTime` is read in the server timezone, as ClickHouse displays
it: `2024-01-31T23:30:00Z` lands in `202402` on a server in `Europe/Paris`.
Changing the server timezone afterwards would move the new versions of
existing keys to other partitions (see [§3.1](#31-its-value-must-never-change-for-a-given-key)).
A `Date` has no timezone.

Like key columns, the partition column is created non-nullable, since
ClickHouse refuses a Nullable partition key. A record without a value for it:

* with `timestamp`, is stored with `0`, so it lands in the `197001` (or `1970`)
  partition. This is also what happens to a key column without a value:
  ClickHouse's JSON input turns `null` into the column default for non-Nullable
  integer columns.
* with `date` or `date-time`, is refused by ClickHouse (`Cannot parse input`),
  and the run fails with `could not save new records`.

The run fails at the SCHEMA message if `partition_by` is malformed (unknown
`type` or `converter`, `property` not a non-empty array of strings) or if the
property is unknown, inside an array, or its type does not match `type`.

## 3. Choosing the property

### 3.1 Its value must never change for a given key

ReplacingMergeTree only deduplicates rows **within a partition**: merges never
combine parts of different partitions, and `OPTIMIZE … FINAL` merges each
partition on its own. If two versions of the same key have values in
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
  the lightweight delete of step 2, which only writes a mask of the deleted
  rows in the parts that contain some. The other parts are cloned through hard
  links.

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

A skipped partition keeps the rows that a lightweight delete only masked
(cleaning column, `DELETED_RECORD` messages), and so do child tables (step 2):
only a merge or a mutation removes them from disk. Once every stream is
finalized, the run looks in `system.parts` for the tables of its streams, roots
and children, that still have parts with deleted rows
(`has_lightweight_delete`), whichever run deleted them: an earlier run that
stopped before its end leaves its masked rows to the next one. For each of
them, it counts the deleted rows, which only reads the mask of those parts, and
runs `ALTER TABLE <table> APPLY DELETED MASK` when they reach
`deleted_rows_reclaim_ratio` (5% by default) of the rows on disk of the whole
table. The `APPLY` only rewrites the masked parts: the partitions merged by the
OPTIMIZE already dropped theirs. Below the ratio, the rows wait for a merge or
a later run: they may take up to that share of the table, which spares
rewriting large parts to free a few rows. The ratio is over the whole table
rather than over the masked parts, which would rewrite small parts holding a
few deletes that their next merge drops for free.

## 6. Existing tables

ClickHouse cannot change the partition key of an existing table, so the
existing root table wins over the SCHEMA message. On every SCHEMA message, the
target reads the partition key of the existing root table
(`system.tables.partition_key`):

* **The table is not partitioned:** nothing changes compared to a stream
  without `partition_by`. If the SCHEMA message declares `partition_by`, a
  warning gives the expected `PARTITION BY` and the field is ignored: the
  partition column keeps its current type and the whole-table `OPTIMIZE FINAL`
  runs as before. To partition it, see [§7](#7-migrating-an-existing-table).
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

## 7. Migrating an existing table

ClickHouse cannot add a partition key to an existing table: its rows must be copied into a new, partitioned table, which then takes the
place of the old one. Child tables are not touched ([§4](#4-child-tables-are-not-partitioned)):
the copy keeps the `_ver` of every row, which their rows reference.

The examples migrate the root table `events` of the database `klaviyo`, with the `partition_by` of [§2](#2-declaring-it).

### 7.1 Before starting

* No run may write to the stream until the swap (step 5): rows inserted into the old table once the copy has started would be lost.
* Take the partition key from the target. On an unpartitioned table, every run logs the key it expects ([§6](#6-existing-tables)):

  ```
  [events]: table events is not partitioned, partition_by is ignored (expected PARTITION BY toYYYYMM(toDateTime(`attributes__timestamp`, 'UTC')))
  ```

### 7.2 Procedure

```sql
-- STEP 1 · partitioned copy. The `_archived_` prefix keeps the target from renaming it.
-- A short old_parts_lifetime frees the space of merged parts quickly during the migration.
CREATE TABLE klaviyo.`_archived_events_repartitioned` AS klaviyo.events
    ENGINE = ReplacingMergeTree
(
    _ver
)
    PARTITION BY toYYYYMM
(
    toDateTime
(
    attributes__timestamp,
    'UTC'
))
    ORDER BY id
    SETTINGS old_parts_lifetime = 30;

-- STEP 2 · copy the data (one block spans many months, hence the raised limit).
INSERT INTO klaviyo.`_archived_events_repartitioned`
SELECT *
FROM klaviyo.events SETTINGS max_partitions_per_insert_block = 1000;

-- STEP 3 · one merged part per partition, so later runs only rewrite the months they touch.
OPTIMIZE
TABLE klaviyo.`_archived_events_repartitioned` FINAL;

-- STEP 4 · both lines must be identical. Stop here otherwise.
SELECT 'old', count(), uniqExact(id), max(_ver)
FROM klaviyo.events
UNION ALL
SELECT 'new', count(), uniqExact(id), max(_ver)
FROM klaviyo.`_archived_events_repartitioned`;

-- STEP 5 · atomic swap, then restore the default old_parts_lifetime.
EXCHANGE
TABLES klaviyo.events AND klaviyo.`_archived_events_repartitioned`;
ALTER TABLE klaviyo.events RESET SETTING old_parts_lifetime;

-- STEP 6 · drop the old data once satisfied, before migrating the next table.
-- Until then, rollback = EXCHANGE TABLES again.
DROP TABLE klaviyo.`_archived_events_repartitioned` SYNC SETTINGS max_table_size_to_drop = 0;
```

### 7.3 Root tables without `key_properties`

Their engine is `MergeTree` and the target never runs `OPTIMIZE` on them:
create the new table with `ENGINE = MergeTree` and the `ORDER BY` of the current table, skip step 3, and compare only `count()` in step 4.
