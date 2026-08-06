# beacon-file-stats

Durable, column-addressable statistics for every file a Beacon instance knows
about. A query reads only the columns its `WHERE` clause names.

For diagrams of the write path, the read path, the segment layout, and the file
lifecycle, see [ARCHITECTURE.md](ARCHITECTURE.md).

## The problem

A Beacon node can hold a million files. Those files draw on 160 000 distinct
column names between them, and each file declares maybe fifty.

The obvious store is a table of files by columns. That table holds 1.6e11 cells.
It cannot be built, held, or written.

Only the pairs that exist may cost anything:

| Quantity | Value |
|---|---|
| Real cells, at ~50 columns per file | ~50 million |
| On disk, at the measured 40 bytes per cell | ~2 GB |
| Reads for a three-column predicate | 3 blocks per surviving segment |

Everything in this crate follows from that one constraint.

## The three layers

| Layer | Holds | Answers |
|---|---|---|
| `registry` | path ↔ `FileId`, name ↔ `ColumnId`, per-file summary | "what is this file's id, and its row count?" |
| `segment` | immutable per-batch blocks, one per column | "what are this column's ranges, per file?" |
| `manifest` | file id range and column ids per segment | "which segments need reading at all?" |

`store` puts the three behind one handle. `collector` fills them. `pruning`
(behind the `datafusion` feature) uses them.

### Why ids and not names

A segment references a file in 8 bytes. A 200-byte path repeated across 50
million cells costs 10 GB; the ids cost 400 MB.

Ids never shift. A delete sets a tombstone and keeps the slot, so a segment
written months ago still means what it said. Only compaction renumbers.

### Why the registry is a B-tree and the segments are objects

They answer different questions. "What id does this path have" is a point lookup
over a million keys, which belongs in a B-tree. "What are this column's ranges"
is a byte range over an immutable blob, which belongs in an object store.

Both live in the same `beacon.db`. The registry uses `RedbStore::database()`, the
segments use its `ObjectStore` face. Copy the one file and the statistics come
with it.

## A worked example

`examples/walkthrough.rs` runs the whole loop. Run it with:

```bash
cargo run -p beacon-file-stats --example walkthrough --features datafusion
```

### 1. Open the store

```rust
let registry = Arc::new(Registry::open(dir.join("registry.redb"))?);
let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
let store = Arc::new(
    FileStatsStore::open(registry, objects, Path::from("__file_stats__")).await?
);
```

In Beacon both arguments come from the one `RedbStore`.

### 2. Register what a listing found

Nothing is read here. This assigns ids and queues the files.

```rust
let ids = store.registry().intern_files(&discovered)?;
```

A path already known keeps its id. A file that changed underneath is marked
stale and goes back on the queue. An etag settles that when both sides carry
one; size and last-modified settle it otherwise. A doubtful match counts as
changed, because a wrong "unchanged" prunes real rows away.

### 3. Let the collector fill the store

```rust
let collector = StatsCollector::new(store.clone(), analyzer, CollectorConfig {
    batch_files: 1_000,
    concurrency: 4,
    prefix_depth: 2,
});
let report = collector.run_once().await?;
```

The collector groups its batch by path prefix and writes one segment per group.
`atlantic/2024` and `pacific/2024` become separate segments.

Reading a real file's statistics needs the format layer, which needs DataFusion,
which this crate does not depend on. So the collector takes a `FileAnalyzer`
trait and Beacon supplies one.

### 4. Ask a per-file question

```rust
let id = store.registry().file_id("atlantic/2024/3.nc")?.unwrap();
let record = store.registry().record(id)?.unwrap();
```

`num_rows` and `total_byte_size` live in the record on purpose. DataFusion asks
for those per file, so that question never touches a column block.

### 5. Prune

```rust
let predicate = binary(col("TEMP", &schema)?, Operator::Gt, lit(6.5f64), &schema)?;
let kept = prune_files(&store, &predicate, &schema, &ids).await;
```

### The output

```text
1. store opened
2. registered 10 files as ids 0..=9, all pending
3. collector: 10 analyzed, 0 failed, 2 groups -> 2 segments
4. registry: atlantic/2024/3.nc is id 3, 1000 rows, read by netcdf
5. WHERE TEMP > 6.5 keeps 1 of 10 files: [5]
     SKIP  id=0  atlantic/2024/0.nc     TEMP in [0, 2]
     SKIP  id=1  atlantic/2024/1.nc     TEMP in [1, 3]
     SKIP  id=2  atlantic/2024/2.nc     TEMP in [2, 4]
     SKIP  id=3  atlantic/2024/3.nc     TEMP in [3, 5]
     SKIP  id=4  atlantic/2024/4.nc     TEMP in [4, 6]
     keep  id=5  atlantic/2024/5.nc     TEMP in [5, 7]
     SKIP  id=6  pacific/2024/0.nc      TEMP in [0, 2]
     SKIP  id=7  pacific/2024/1.nc      TEMP in [1, 3]
     SKIP  id=8  pacific/2024/2.nc      TEMP in [2, 4]
     SKIP  id=9  pacific/2024/3.nc      TEMP in [3, 5]
6. WHERE PSAL > 40 keeps 4 of 10 files: [6, 7, 8, 9]
     the pacific files never declared PSAL, so they are not prunable on it
```

Step 6 is the interesting one. Only the atlantic files declare `PSAL`, and their
range is `[34.0, 35.5]`, so `PSAL > 40` rules them out. The pacific files carry
no `PSAL` statistic at all, so nothing rules them out and they survive.

That is the rule the whole crate obeys: **an absent statistic keeps a file**. A
file wrongly dropped is a silently wrong answer. A file wrongly kept is one scan
the optimizer would have skipped.

## When a file changes or disappears

Both keep the file's id, so old segments stay meaningful.

**Updated.** The listing notices a differing size, mtime, or etag and marks the
record `Stale`. Until the collector catches up, the segments still hold the old
range, and pruning on it would drop files the new content matches. The
`fs_suppressed` table names the ids whose statistics must not be trusted, and
`prune_files` treats those rows as absent, so the file is kept. Membership is
`stats_epoch > 0 && state != Analyzed`: the second half is the danger, the first
keeps the table empty through a first ingest.

After re-analysis the file has rows in two segments. Segments fold oldest first,
so the newest range wins.

**Deleted.** Registering can only add or update, because a listing reports what
is there and never what is gone. `Registry::reconcile_prefix(prefix, observed)`
does the comparison: it range-scans the path table over the prefix, and every
path the listing did not report becomes `Deleted` and suppressed. `observed` must
be the complete listing for that prefix, or it will tombstone whatever it left
out.

A tombstoned path that reappears is revived even byte-identical, because its
record still says `Deleted`.

Nothing is reclaimed by either case. Tombstoned records and superseded rows stay
until compaction, which is not built.

## What a read actually costs

`WHERE TEMP > 6.5` against a store with 160 000 column names:

1. **The manifest** names the segments that hold `TEMP` and cover the wanted file
   ids. No read. The manifest keeps a file id range and a sorted list of column
   ids per segment, which stays in the low megabytes for a whole store.
2. **The footer** of each surviving segment: one ranged read. It carries the type
   table and a sparse column index, one entry per 1024 columns.
3. **One index chunk**: one ranged read of 16 KB, binary-searched for the column.
4. **The block**: one ranged read.

Two reads per column lookup, and the count does not grow with the segment's
width. `tests/scale.rs` measures the same two reads at 100 columns and at 8000.

Nothing reads `PSAL`. Nothing reads a segment that holds no `TEMP`.

## The segment layout

```text
[MAGIC 8]
[block 0][block 1] ... [block N-1]      each starts 8-aligned
[column index]                          N fixed 16-byte records, sorted by column_id
[footer: rkyv SegmentFooter]
[footer_len: u32 LE][MAGIC 8]
```

One block per column, sorted ascending by file id:

| Field | Bytes | Note |
|---|---|---|
| `file_id` | 8 | ascending |
| `min` / `max` | the column's width | super-typed per block |
| `null_count` / `row_count` | 8 each | |

### Why buffers sit outside the rkyv metadata

rkyv aligns an archived region to its type's alignment, and `Vec<u8>` has
alignment 1. Arrow's `ScalarBuffer<T>` *asserts* alignment to `align_of::<T>()`.
A raw Arrow buffer nested inside an archived struct could land anywhere, and
every read would pay a realigning copy.

So the writer places every buffer itself, 8-byte aligned, and the metadata holds
only offsets. Alignment still cannot be guaranteed end to end, because the base
address of the `bytes::Bytes` an object store returns belongs to the allocator.
The reader checks the pointer and copies only when it must.

### Why types are per block, not per value

Files disagree. One declares `TEMP` as `Int16`, another as `Float32`. The
tempting fix is a tagged value enum, and it is the wrong trade at 50 million
cells: 56 bytes per cell against 40, no narrowing, no delta encoding, and a
`match` per value on read.

Instead each block is homogeneous. The builder casts to the super type at write
time, the block records its own Arrow type, and the reader casts to the type the
predicate compares against. Different segments may settle on different types for
the same column, and that is fine.

Two types with no common super type do not merge. The column is dropped from that
segment and logged. That costs pruning, never correctness.

## Two rules a caller must not break

**1. Push files in ascending id order.** That is what keeps blocks sorted without
a sort at finish.

**2. Batch a segment by path prefix, not by arrival order.** Files under one
prefix share columns, so a prefix-local segment is skipped outright by a
predicate on a column it does not hold. A segment batched by arrival order holds
a broad slice of the column space, matches nearly every query, and the manifest's
skip stops working.

The second rule is easy to get wrong because it looks fine at small scale. The
test measures it: for one column across four segments, prefix-local reads **1 of
4**, scattered reads **4 of 4**.

## At a million files

`examples/scale_million.rs` builds and queries the real target shape: 1M files,
100 families, ~160 000 distinct column names, 20 columns per file.

```bash
cargo run --release -p beacon-file-stats --example scale_million --features datafusion
```

```text
shape: 1000000 files, 100 families, ~160010 distinct columns, 20 columns/file

register : 1000000 files in 2.5s  (392972 files/s)
collect  : 1000000 analyzed, 100 segments in 8.1s  (123411 files/s)

registry :    514.0 MB
segments :    780.1 MB over 100 objects, 40.9 bytes/cell
manifest :      0.6 MB   <- the metadata that decides which segments to read
cells    : 20000000 real, against 160010000000 dense (8000x)
columns  : 160010 interned

registry lookup: 9.0 us each  (path -> id -> record, 10000 probes)

prune on a family column : 14 ms, keeps 999943 of 1000000
prune on a store-wide column : 58 ms, keeps 50000 of 1000000
prune on THREE store-wide columns : 105 ms, keeps 40000 of 1000000
prune the same family column over that family's 10k files only : 1 ms, keeps 9943 of 10000
```

Three columns cost 1.8x one column, not 3x. The predicate's columns are fetched
and packed together, and within each column the segments are read together.

Peak resident memory is ~1.0 GB for the whole run, ~810 MB of it in the build
phase. Nothing resident grows with the column count.

### The manifest holds up

0.6 MB. That was the part of the design most likely to fail: a manifest keeping
per-column min/max per segment would be 16M entries here, and the metadata would
cost more than the data it guards. Keeping only a file id range and a sorted
column id list per segment is what makes it a rounding error instead.

### Where pruning pays, and where it does not

Read the last three lines together. They say something the design cannot escape.

**Pruning power is bounded by how many of the candidate files declare the
column.** An absent statistic keeps a file, so a column that 0.6% of the
candidates declare can only ever prune 0.6% of them.

| Predicate | Candidates | Kept | Verdict |
|---|---|---|---|
| `core_3 > 9500`, declared by every file | 1 000 000 | 50 000 | 20x fewer files to open, in 57 ms |
| `fam7_var300 > 9000`, declared by ~62 files | 1 000 000 | 999 943 | correct, and useless |
| `fam7_var300 > 9000` | 10 000 (one family) | 9 943 | correct, cheap, still little use |

This is not a flaw to fix. It is what a sparse column space means. The win comes
from columns that are widely declared inside the set being scanned, which is
exactly the columns people filter on: time, latitude, depth. A column only a
handful of files declare will not prune, and no layout changes that.

The practical consequence is that **the candidate list should come from the table
being scanned, not from the whole instance.** Pruning a million candidates on a
column 62 of them declare is arithmetically correct and buys nothing.

### Two bugs this run found

Both were invisible below about 100 000 files.

**A redb commit is an fsync.** The collector called `mark_analyzed` per file, so
a million files meant a million transactions. The collect phase ran at ~250
files/s and would have taken over an hour. Batching the whole group into one
transaction took it to 8 seconds.

**A hash join where a merge join would do.** `prune_files` built a hash map over
every candidate to align segment rows onto output rows. Both sides are already
sorted by file id, so the map bought nothing and cost ~50 MB and an allocation
per entry. The merge join took the store-wide prune from 242 ms to 57 ms.

**Concurrency mistaken for parallelism, twice.** `buffer_unordered` polls every
future it holds from one task, so work that is CPU bound between awaits runs
single-threaded however high the limit. Reading a netCDF file's ranges is that
shape, and so is decoding a segment block. Measured against a bundled netCDF
file on eight cores: `buffer_unordered(8)` managed 296 files/s where serial
managed 287, and spawning the same work reached 1193. Both the collector's
analyses and the reader's segment fetches are now spawned, with
`buffer_unordered` kept only to bound how many run at once.

## Measured, not asserted

From `tests/scale.rs`:

```text
prefix-local : 64 columns,   40.2 bytes/cell   (the 40-byte payload floor)
scattered    : 4000 columns, 51.2 bytes/cell
framing      : ~112 bytes per block
lookup       : 2 ranged reads at both 100 and 8000 columns
manifest     : column 70 reads 1 of 4 prefix-local segments, 4 of 4 scattered
```

40 bytes per cell is today's floor: file id, min, max, null count, and row count,
all 8 bytes. Delta-packing the file ids and narrowing the counts to 32 bits would
lower it. Neither is built.

## Durability

Everything survives a restart and a vacuum. `tests/single_file.rs` covers both
against a real `beacon.db`.

The vacuum case needed a fix in `beacon-redb-store`. A rewrite copied only the
store's own three tables, so any tenant table was dropped with no error and no
warning. `vacuum` now copies tenant tables verbatim, and refuses to run while a
tenant still holds the database handle.

A tenant sharing the file must prefix its table names, declare tables as
`TableDefinition<&[u8], &[u8]>`, and encode integer keys big-endian. The byte
typing is what lets a vacuum copy a table whose types it cannot know. The
big-endian keys are what make redb's lexicographic ordering numeric, which the
collector's queue depends on.

## Dependencies

No DataFusion and no `beacon-binary-format` in the default graph. `cargo test -p
beacon-file-stats` compiles a small tree and runs in under a second.

The `datafusion` feature adds the pruning adapter and nothing else.

`beacon-redb-store` is a dev-dependency only, for the single-file tests.

## Not built yet

- A `FileAnalyzer` over Beacon's real format registry.
- Scan-time pruning inside `FileCollection::scan`.
- `ANALYZE` and a `beacon.system.file_stats` view.
- Compaction, tombstone collection, and zone maps inside a block.
- Backing `BeaconFileStatisticsCache` with this store.
