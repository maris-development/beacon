---
title: "Too big for memory: Beacon vs PostgreSQL, Trino, Presto & DuckDB at 250M rows"
description: "A benchmark of Beacon against PostgreSQL, Trino, Presto, and DuckDB on 250 million rows — querying files in place when the data no longer fits in RAM."
---

# Too big for memory: how Beacon stacks up at 250 million rows

*Beacon vs PostgreSQL, Trino, Presto, and DuckDB — querying files in place when the data no
longer fits in RAM.*

Most database benchmarks quietly cheat. They pick a dataset that fits in memory, run the
queries until everything is cached, and report the warm numbers. In that world a tuned
row-store looks unbeatable — it's just scanning RAM.

But the data that **[Beacon](https://github.com/maris-development/beacon)** is built for —
large scientific and environmental archives in Parquet, NetCDF, and Zarr — does *not* fit in
memory. So we built the opposite benchmark: a quarter-billion rows, a working set several
times larger than RAM, five engines, the same eight queries, one machine. This is the story
of what happened.

## The contenders

Beacon's whole pitch is that you shouldn't load your data anywhere — point it at your files
and query them where they live. We lined that claim up against the tools people actually
reach for:

| Engine | Version | How it queries the data |
|---|---|---|
| **Beacon** | 1.7.0 (1.7.x line) | Files in place, over HTTP SQL (Arrow/DataFusion) |
| **DuckDB** | 1.5 | Columnar; `read_parquet()` over the files — no ingest |
| **Trino** | 457 | Distributed SQL; Hive external tables over `file://` |
| **Presto** | 0.289 | Trino's predecessor; same Hive setup |
| **PostgreSQL** | 16 | Row store; data `COPY`-loaded into a table first |

Beacon and DuckDB read the files directly. Trino and Presto query them through a Hive
catalog. PostgreSQL is the odd one out — it has to ingest the data into its own tables
before it can answer anything. To keep it a fair fight, every engine — DuckDB included —
runs in its own container reading the same files, under the same caps.

## The setup

- **Same data for everyone:** 250 million synthetic ocean observations (time, latitude,
  longitude, depth, platform, and seven measured variables) — about **13 GB of zstd Parquet
  in 10 files**, sorted by time and a Morton (Z-order) code of lon/lat, with the Parquet
  page index written so engines can skip blocks that can't match.
- **Same budget for everyone:** each engine capped at **4 CPUs and 8 GB of memory**.
- **Real storage:** the files live on a native Docker volume (not a host bind mount, which
  on Docker Desktop reads through a slow translation layer) — so we measure engines, not the
  mount.
- **The crux:** loaded into PostgreSQL this data is a **~24 GB table**. The memory budget is
  8 GB. **Nothing fits in cache** — the exact moment most benchmarks avoid.
- **Fair measurement:** each query runs once cold, then five more; we report the warm median.
  Every query is wrapped as `SELECT count(*) FROM ( … )` so we time the engine's *execution*,
  not how fast it ships rows back — and that wrapped count doubles as a correctness check.
  All five engines returned identical row counts on every query.

## The queries

Eight analytical queries — counts, filters, an aggregation, a spatial box, a time window,
top-N, and distinct. Defined once and run verbatim on every engine; only the table reference
changes (`read_parquet('…/*.parquet')` for Beacon and DuckDB, `hive.bench.obs` for
Trino/Presto, `obs` for Postgres).

```sql
-- 1. full-scan count (touches a column, so it's a real scan not a metadata shortcut)
SELECT count(temperature) FROM <table>;

-- 2. single-column range filter
SELECT time, latitude, longitude, temperature FROM <table>
WHERE temperature BETWEEN 10 AND 12;

-- 3. multi-column filter
SELECT time, latitude, longitude, temperature, salinity FROM <table>
WHERE temperature BETWEEN 5 AND 15 AND latitude BETWEEN -10 AND 10;

-- 4. GROUP BY with aggregates
SELECT platform, avg(temperature), avg(salinity), count(*) AS n
FROM <table> GROUP BY platform ORDER BY n DESC;

-- 5. spatial bounding box
SELECT time, longitude, latitude, temperature FROM <table>
WHERE longitude BETWEEN -10 AND 10 AND latitude BETWEEN 30 AND 60;

-- 6. ~30-day time window (time is stored as epoch seconds)
SELECT time, latitude, longitude, temperature FROM <table>
WHERE time BETWEEN 1656625600 AND 1659217600;

-- 7. top-N by time
SELECT time, platform, temperature, salinity FROM <table>
ORDER BY time DESC LIMIT 1000;

-- 8. distinct values
SELECT DISTINCT platform FROM <table> ORDER BY platform;
```

## What happened

Warm query times, in milliseconds. Lower is better.

| Query | Beacon | DuckDB | Trino | Presto | PostgreSQL |
|---|--:|--:|--:|--:|--:|
| Full-scan count | **88** | 211 | 1,475 | 4,698 | 10,904 |
| Filter (temperature) | **300** | 597 | 1,611 | 4,616 | 10,997 |
| Filter (temp + latitude) | **496** | 1,011 | 1,909 | 7,639 | 10,725 |
| GROUP BY platform | 888 | **231** | 598 | 6,897 | 12,209 |
| Spatial bounding-box | **384** | 991 | 1,295 | 7,551 | 9,650 |
| Time-range window | 84 | **23** | 302 | 875 | 9,288 |
| Top-N by time | **88** | 626 | 2,132 | 5,436 | 10,958 |
| DISTINCT platform | 876 | **197** | 394 | 6,926 | 11,489 |
| **Average** | **400 ms** | 486 ms | 1,215 ms | 5,580 ms | 10,777 ms |

**Beacon came out on top** — a 400 ms average, fastest on five of the eight queries, and
never far off on the rest. It answered a count over 250 million rows in 88 milliseconds and a
month-long time slice in 84. And it did this on **297 MB of memory** with **zero time to get
ready** — no loading, no ingestion, just point and query.

## The pack behind it

**DuckDB is the one that keeps pace.** At a 486 ms average it's clearly the runner-up, and it
actually *beats* Beacon on the three aggregation-shaped queries — grouping, distinct, and the
time window, which it tore through in 23 ms. That's a confirmation more than a surprise: the
two columnar engines that read straight from the files, with no load step, pulled away from
everything else. The difference shows up elsewhere, though — DuckDB needed nearly 6 GB of
memory to do it, where Beacon stayed under 300 MB.

**Trino and Presto land in the middle.** Trino averages around 1.2 seconds — respectable, and
strong on aggregations — but it carries the weight of a distributed engine and a metastore
for what is, here, a single-node job. Presto, its older sibling, trails it across the board
at ~5.6 seconds.

**And then there's PostgreSQL.** At small scale, in our earlier tests, Postgres was a star —
when the table fit in memory it scanned RAM and won the filters outright. At 250 million rows
that advantage evaporates. It can't cache a 24 GB table in 8 GB, so every query becomes a
full read of the table off disk, and every query lands at roughly **ten seconds**. Before it
could answer anything, it spent **24 minutes loading the data** and then held 7 GB of memory.

## Why the gap is so wide

The filters tell the clearest story. Ask for "temperature between 10 and 12" and Postgres has
to drag all 24 GB off disk, because a row store reads whole rows whether you want one column
or twelve. Beacon reads only the column the question is about — a few hundred megabytes,
compressed, with whole chunks skipped because the file's own index says they can't match.
Same question, same data: ten seconds versus three hundred milliseconds.

| Filter | PostgreSQL | Beacon | Beacon advantage |
|---|--:|--:|--:|
| temperature | 10,997 ms | 300 ms | **37×** |
| temperature + latitude | 10,725 ms | 496 ms | **22×** |
| spatial box | 9,650 ms | 384 ms | **25×** |

There's a second thing in Beacon's column: its times barely move as the data grows. A count
is 88 ms; a time window is 84 ms — about what they'd be on a tenth the data. Because Beacon
touches only what a query needs, its work is set by the *question*, not the *size of the
archive*. Postgres, scanning the whole table every time, scales straight up with the row
count. Grow the data further and the gap only widens.

## Is it a fair fight?

Before anyone cries foul: the things a benchmark must control, we controlled — and checked.
All five engines run as containers in the **same Linux VM**, each pinned to exactly **4 CPUs
and 8 GB** (verified straight from the container configs, not just asked for). They read the
**same Parquet files** from the **same native volume**, run the **same SQL**, and — the part
that matters most — returned **identical row counts** on every query. DuckDB in particular is
containerized on that same volume under the same caps, not run loose on the host, so it's a
like-for-like comparison with Beacon.

What we *didn't* equalize is the part that's intrinsic to each engine, and we report it
openly rather than hide it:

- **PostgreSQL has to load the data first** — that 24-minute COPY and 24 GB heap are the
  price of being a row store, not a handicap we imposed. It also ran without secondary
  indexes; a B-tree would speed its selective filters (but not the full scans, and it'd cost
  build time and storage).
- **Each engine speaks its own protocol** to the harness, so for the very fastest queries a
  few milliseconds of client overhead are in the mix.
- It's **one machine, warm median of five runs** — directional, not a certified leaderboard.

None of that moves the headline: a 27× gap over PostgreSQL doesn't come from millisecond
protocol noise. The one result to read modestly is the close one — Beacon and DuckDB are in
the same class, with Beacon a step ahead and far lighter on memory (297 MB vs ~6 GB), rather
than a precise multiple apart.

## The takeaway

The headline isn't "Beacon wins a benchmark." It's that **at the scale and storage you
actually run in production, the engines that query files in place — Beacon and DuckDB — leave
the load-it-first databases behind.** Beacon led the field, DuckDB shadowed it, and the gap to
a row store that can't cache its table ran to 20–37× on the queries that matter most.

If you're sitting on large scientific or environmental archives in Parquet, NetCDF, or Zarr,
that's the choice in a nutshell: spend 24 minutes and 7 GB loading data into a database that
then crawls — or point Beacon at the files and get answers in milliseconds.

---

*Five engines · 250M rows (~13 GB Parquet on local storage) · 4 CPU / 8 GB each · warm median
of 5 runs · all engines verified to return identical row counts. Beacon 1.7.0 was the tested
image; the results apply to the 1.7.x line (including 1.7.1, the current source release). The
full harness and method are
[reproducible on GitHub](https://github.com/maris-development/beacon/tree/main/benchmarks).*
