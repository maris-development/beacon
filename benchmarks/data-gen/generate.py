#!/usr/bin/env python3
"""Generate a synthetic oceanographic-observations dataset for the engine benchmark.

The same logical table is written to three formats so the benchmark can run both a
*fair* comparison (every engine reads identical Parquet) and an *ETL-overhead*
comparison (Beacon reads NetCDF/Zarr natively; competitors must first convert):

    data/parquet/obs_<NNN>.parquet   <- common format, queried by every engine
    data/netcdf/obs_<NNN>.nc         <- Beacon-native scientific format
    data/zarr/obs.zarr               <- Beacon-native scientific format

The schema mirrors Beacon's marine domain (see test-datasets/ and the API examples):
point observations with time / latitude / longitude / depth / platform plus a handful
of measured variables. Generation is chunked and deterministically seeded so runs are
reproducible and memory stays bounded regardless of scale.

Usage:
    python generate.py --scale small        # ~100 MB parquet
    python generate.py --scale medium       # ~2 GB parquet
    python generate.py --rows 50_000_000    # explicit row count
    python generate.py --scale small --formats parquet   # skip netcdf/zarr
"""

from __future__ import annotations

import argparse
import shutil
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# Rows per generated chunk = one parquet row-group. Bounds peak memory and sets the
# Morton-sort bucket / row-group granularity for pruning.
CHUNK_ROWS = 1_000_000

# Approximate rows that land at the target on-disk parquet size for each preset.
# Parquet here compresses to very roughly ~40 bytes/row with this schema.
SCALE_ROWS = {
    "small": 2_500_000,     # ~100 MB
    "medium": 50_000_000,   # ~2 GB
    "large": 250_000_000,   # ~10 GB
}

PLATFORMS = np.array(["SHIP", "BUOY", "FLOAT", "GLIDER", "MOORING"], dtype=object)

# Measured variables: (name, low, high, dtype). Kept float32 to match scientific data.
VARIABLES = [
    ("temperature", -2.0, 35.0, np.float32),
    ("salinity", 30.0, 40.0, np.float32),
    ("oxygen", 150.0, 400.0, np.float32),
    ("pressure", 0.0, 6000.0, np.float32),
    ("chlorophyll", 0.0, 30.0, np.float32),
    ("nitrate", 0.0, 45.0, np.float32),
    ("ph", 7.5, 8.4, np.float32),
]

# Epoch for the `time` column; observations span ~5 years from here.
EPOCH_S = np.int64(1_577_836_800)  # 2020-01-01T00:00:00Z
SPAN_S = np.int64(5 * 365 * 24 * 3600)


def arrow_schema() -> pa.Schema:
    # `time` is int64 epoch SECONDS, not a timestamp logical type. Parquet timestamp
    # types map inconsistently across Hive engines (Trino vs Presto handle micros
    # differently); a plain BIGINT is identical in every engine, keeping the
    # comparison apples-to-apples. Filter/sort on it numerically.
    fields = [
        ("time", pa.int64()),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
        ("depth", pa.float32()),
        ("platform", pa.string()),
        ("platform_id", pa.int32()),
    ]
    fields += [(name, pa.float32()) for name, _, _, _ in VARIABLES]
    return pa.schema(fields)


def morton2d(lon: np.ndarray, lat: np.ndarray, bits: int = 16) -> np.ndarray:
    """Vectorized Z-order (Morton) code of (lon, lat). Sorting by it clusters points
    that are spatially close, so Parquet row-groups built from sorted data get tight
    lat/lon min-max bounds and the reader can prune them on spatial filters."""
    x = np.clip((lon + 180.0) / 360.0 * ((1 << bits) - 1), 0, (1 << bits) - 1).astype(np.uint64)
    y = np.clip((lat + 90.0) / 180.0 * ((1 << bits) - 1), 0, (1 << bits) - 1).astype(np.uint64)

    def spread(v: np.ndarray) -> np.ndarray:  # interleave 16 bits with zeros
        v = v & np.uint64(0x0000FFFF)
        v = (v | (v << np.uint64(8))) & np.uint64(0x00FF00FF)
        v = (v | (v << np.uint64(4))) & np.uint64(0x0F0F0F0F)
        v = (v | (v << np.uint64(2))) & np.uint64(0x33333333)
        v = (v | (v << np.uint64(1))) & np.uint64(0x55555555)
        return v

    return spread(x) | (spread(y) << np.uint64(1))


def gen_chunk(
    rng: np.random.Generator,
    n: int,
    sort: str = "none",
    row_offset: int = 0,
    total_rows: int | None = None,
) -> dict[str, np.ndarray]:
    """Build one chunk of `n` rows as a dict of numpy arrays.

    sort:
      none     - random time, no ordering (worst case for row-group pruning).
      time     - time increases monotonically across the whole dataset, so files and
                 row-groups get tight, non-overlapping time ranges (helps time filters
                 and top-N-by-time).
      time-geo - as `time`, plus each chunk is reordered by a Morton code of (lon,lat),
                 so row-groups within a chunk also get tight spatial bounds.
    """
    cols: dict[str, np.ndarray] = {}
    if sort in ("time", "time-geo"):
        # Monotonic global time: row i maps to a fraction of the total span.
        total = total_rows or (row_offset + n)
        idx = (row_offset + np.arange(n, dtype=np.int64)).astype(np.float64)
        cols["time"] = (EPOCH_S + (idx / max(total, 1) * SPAN_S)).astype(np.int64)
    else:
        cols["time"] = EPOCH_S + rng.integers(0, SPAN_S, size=n, dtype=np.int64)
    cols["latitude"] = rng.uniform(-90.0, 90.0, size=n)
    cols["longitude"] = rng.uniform(-180.0, 180.0, size=n)
    cols["depth"] = rng.uniform(0.0, 6000.0, size=n).astype(np.float32)
    plat_idx = rng.integers(0, len(PLATFORMS), size=n)
    cols["platform"] = PLATFORMS[plat_idx]
    cols["platform_id"] = plat_idx.astype(np.int32)
    for name, lo, hi, dt in VARIABLES:
        cols[name] = rng.uniform(lo, hi, size=n).astype(dt)

    if sort == "time-geo":
        # Cluster the chunk spatially (the chunk already shares a narrow time window).
        perm = np.argsort(morton2d(cols["longitude"], cols["latitude"]), kind="stable")
        cols = {k: v[perm] for k, v in cols.items()}
    return cols


def chunk_to_table(cols: dict[str, np.ndarray], schema: pa.Schema) -> pa.Table:
    arrays = []
    for field in schema:
        arrays.append(pa.array(cols[field.name], type=field.type))
    return pa.Table.from_arrays(arrays, schema=schema)


def write_parquet(
    out_dir: Path, total_rows: int, seed: int, sort: str, row_group_size: int,
    compression: str, page_index: bool, max_files: int,
) -> tuple[int, float]:
    """Write at most `max_files` parquet files (larger files, many row-groups each)
    rather than one file per generation chunk. Generation stays chunked at CHUNK_ROWS so
    peak memory is bounded; chunks are appended as row-groups to a rolling ParquetWriter.
    Returns (file_count, elapsed_seconds)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    schema = arrow_schema()
    rng = np.random.default_rng(seed)
    # Rows per file to land at <= max_files, aligned up to CHUNK_ROWS so each generation
    # chunk (and its Morton-sorted bucket) stays whole within one file.
    target = (total_rows + max_files - 1) // max_files
    rows_per_file = max(CHUNK_ROWS, ((target + CHUNK_ROWS - 1) // CHUNK_ROWS) * CHUNK_ROWS)
    t0 = time.perf_counter()
    written = 0
    file_idx = 0
    rows_in_file = 0
    writer = None
    while written < total_rows:
        n = min(CHUNK_ROWS, total_rows - written)
        table = chunk_to_table(gen_chunk(rng, n, sort, written, total_rows), schema)
        if writer is None:
            # row_group_size sets pruning granularity; write_page_index adds the
            # column/offset index for page-level pruning (finer than row-groups).
            writer = pq.ParquetWriter(
                out_dir / f"obs_{file_idx:04d}.parquet", schema,
                compression=compression, write_page_index=page_index,
            )
            rows_in_file = 0
        writer.write_table(table, row_group_size=row_group_size)
        written += n
        rows_in_file += n
        print(f"  parquet: {written:,}/{total_rows:,} rows ({file_idx + 1} files)", end="\r")
        if rows_in_file >= rows_per_file and written < total_rows:
            writer.close()
            writer = None
            file_idx += 1
    if writer is not None:
        writer.close()
    print()
    return file_idx + 1, time.perf_counter() - t0


def write_netcdf(out_dir: Path, total_rows: int, seed: int, sort: str) -> tuple[int, float]:
    """Write NetCDF (one file per chunk) as 1-D point observations along an `obs` dim."""
    import xarray as xr

    out_dir.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(seed)  # same seed -> same logical data as parquet
    t0 = time.perf_counter()
    written = 0
    idx = 0
    while written < total_rows:
        n = min(CHUNK_ROWS, total_rows - written)
        cols = gen_chunk(rng, n, sort, written, total_rows)
        data_vars = {
            "latitude": ("obs", cols["latitude"]),
            "longitude": ("obs", cols["longitude"]),
            "depth": ("obs", cols["depth"]),
            "platform_id": ("obs", cols["platform_id"]),
            "platform": ("obs", cols["platform"].astype(str)),
        }
        for name, _, _, _ in VARIABLES:
            data_vars[name] = ("obs", cols[name])
        ds = xr.Dataset(
            data_vars=data_vars,
            coords={
                # CF time: seconds since epoch
                "time": ("obs", cols["time"], {"units": "seconds since 1970-01-01T00:00:00Z"}),
            },
        )
        path = out_dir / f"obs_{idx:04d}.nc"
        ds.to_netcdf(path, engine="netcdf4")
        written += n
        idx += 1
        print(f"  netcdf:  {written:,}/{total_rows:,} rows ({idx} files)", end="\r")
    print()
    return idx, time.perf_counter() - t0


def write_zarr(out_dir: Path, total_rows: int, seed: int, sort: str) -> tuple[int, float]:
    """Write a single Zarr store, appending chunk by chunk along the `obs` dim."""
    import xarray as xr

    store = out_dir / "obs.zarr"
    if store.exists():
        shutil.rmtree(store)
    rng = np.random.default_rng(seed)
    t0 = time.perf_counter()
    written = 0
    idx = 0
    while written < total_rows:
        n = min(CHUNK_ROWS, total_rows - written)
        cols = gen_chunk(rng, n, sort, written, total_rows)
        data_vars = {
            "latitude": ("obs", cols["latitude"]),
            "longitude": ("obs", cols["longitude"]),
            "depth": ("obs", cols["depth"]),
            "platform_id": ("obs", cols["platform_id"]),
        }
        for name, _, _, _ in VARIABLES:
            data_vars[name] = ("obs", cols[name])
        ds = xr.Dataset(
            data_vars=data_vars,
            coords={"time": ("obs", cols["time"], {"units": "seconds since 1970-01-01T00:00:00Z"})},
        )
        if idx == 0:
            ds.to_zarr(store, mode="w")
        else:
            ds.to_zarr(store, mode="a", append_dim="obs")
        written += n
        idx += 1
        print(f"  zarr:    {written:,}/{total_rows:,} rows ({idx} chunks)", end="\r")
    print()
    return 1, time.perf_counter() - t0


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scale", choices=list(SCALE_ROWS), default="small")
    ap.add_argument("--rows", type=int, default=None, help="explicit row count (overrides --scale)")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument(
        "--formats",
        default="parquet,netcdf",
        help=(
            "comma-separated subset of: parquet,netcdf,zarr. Default skips zarr because "
            "Beacon reads Zarr v3 while xarray+zarr<3 writes v2; add 'zarr' explicitly "
            "(with a v3-capable zarr) to include the native-Zarr query."
        ),
    )
    ap.add_argument(
        "--sort",
        choices=["none", "time", "time-geo"],
        default="none",
        help=(
            "row ordering. none=random (worst case for pruning); time=monotonic time "
            "(tight row-group time ranges); time-geo=time + Morton(lon,lat) within each "
            "chunk (tight time AND spatial ranges). Realistic ARCO data is sorted."
        ),
    )
    ap.add_argument(
        "--row-group-size",
        type=int,
        default=1_048_576,
        help="parquet row-group size in rows. Smaller -> finer pruning (try 131072 with --sort).",
    )
    ap.add_argument(
        "--compression",
        choices=["zstd", "snappy", "lz4", "gzip", "none"],
        default="zstd",
        help="parquet compression codec (default: zstd).",
    )
    ap.add_argument(
        "--page-index",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="write the parquet page index for page-level pruning (default: on).",
    )
    ap.add_argument(
        "--max-files",
        type=int,
        default=10,
        help="maximum number of parquet files to emit (larger files instead of many small).",
    )
    ap.add_argument(
        "--out",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "data",
        help="output root (default: benchmarks/data)",
    )
    args = ap.parse_args()

    total_rows = args.rows if args.rows is not None else SCALE_ROWS[args.scale]
    formats = [f.strip() for f in args.formats.split(",") if f.strip()]
    out: Path = args.out
    out.mkdir(parents=True, exist_ok=True)

    print(f"Generating {total_rows:,} rows (scale={args.scale}, sort={args.sort}, "
          f"compression={args.compression}) -> {out}")
    print(f"Formats: {', '.join(formats)}")

    timings = {}
    if "parquet" in formats:
        files, secs = write_parquet(
            out / "parquet", total_rows, args.seed, args.sort, args.row_group_size,
            args.compression, args.page_index, args.max_files,
        )
        timings["parquet"] = (files, secs)
    if "netcdf" in formats:
        files, secs = write_netcdf(out / "netcdf", total_rows, args.seed, args.sort)
        timings["netcdf"] = (files, secs)
    if "zarr" in formats:
        files, secs = write_zarr(out / "zarr", total_rows, args.seed, args.sort)
        timings["zarr"] = (files, secs)

    print("\nDone:")
    for fmt, (files, secs) in timings.items():
        print(f"  {fmt:8s} {files} file(s) in {secs:6.1f}s")
    # A manifest the harness reads to learn row count / layout without re-scanning.
    manifest = out / "manifest.txt"
    manifest.write_text(
        f"rows={total_rows}\nscale={args.scale}\nseed={args.seed}\nformats={','.join(formats)}\n"
        f"sort={args.sort}\nrow_group_size={args.row_group_size}\ncompression={args.compression}\n"
        f"page_index={args.page_index}\nmax_files={args.max_files}\n",
        encoding="utf-8",
    )
    print(f"  manifest -> {manifest}")


if __name__ == "__main__":
    main()
