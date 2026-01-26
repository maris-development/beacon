
# Zarr Support in Beacon 1.4

If Zarr is already part of your stack—or you’re considering it—Beacon 1.4 is built to help you get value fast. Beacon has supported reading Zarr stores since version 1.4, which means you can query cloud‑friendly, chunked datasets without converting formats or rewriting pipelines. This post shows how it works, how to combine many Zarr stores into one logical collection, and how to turn on pruning for faster results.

## What “Zarr support” means in Beacon

Zarr stores are folders that contain chunked data and metadata. Beacon can read these stores directly and expose them to queries. You do not need to write Rust code or change your datasets. Beacon does the heavy lifting: it discovers the schema, reads only the chunks it needs, and returns query results in the format you ask for (CSV, JSON, etc.). That means you can start querying immediately and iterate faster.

## Combine many Zarr stores into one logical collection

Most teams have many Zarr stores, not just one. Beacon lets you combine them into a single “logical table” (also called a collection). You give Beacon a list of paths or glob patterns, and it treats them as one queryable dataset. The win: one stable table name for a growing library of Zarr data.

This is useful when:

- You have daily or monthly Zarr stores and want to query them together.
- You have data split by region or sensor and need a unified view.
- You want one stable name for many underlying datasets.

At a high level, you create a logical table with `file_format: "zarr"` and a list of `paths` that point to the `zarr.json` metadata files. Beacon will then scan across all matching stores as if they were one table, so you can query once and get everything.

## Speeding up queries with statistics-based pruning

Zarr datasets are often large. Beacon includes an optional feature called “statistics‑based pruning” to reduce how much data is read—so you get results faster without extra effort.

In plain terms, Beacon can compute simple statistics (like min/max) for selected columns and use them to skip chunks that cannot match your filters. For example, if you filter on a time range, Beacon can skip chunks that fall entirely outside that range. Less I/O means faster queries and lower costs.

This works best for coordinate-like columns such as time, latitude, and longitude.

### How to enable statistics

You can enable statistics in two common ways:

1) **For a logical Zarr collection** (recommended for repeated queries)

You specify `statistics.columns` when creating the table. Beacon will compute and use statistics for those columns.

2) **For ad-hoc queries** (one-off reads)

You provide `statistics_columns` when calling `read_zarr`, and Beacon will use those columns to prune data during the query.

## When to use Zarr in Beacon

Zarr is a great fit when you want:

- Fast access to large, chunked arrays
- Cloud-native storage (object storage like S3)
- A single logical view across many datasets

If your workload includes repeated filters (time windows, geographic ranges, etc.), statistics‑based pruning can further reduce I/O and improve response time. This is where Beacon starts to feel “instant.”

## Takeaway

Beacon’s Zarr support makes it simple to read and query Zarr stores, combine many stores into one logical collection, and speed things up with statistics‑based pruning. If your data is in Zarr or you’re planning a Zarr migration, Beacon is ready—and it’s the fastest way to make those datasets useful right now.
