#!/usr/bin/env python3
"""Benchmark orchestrator: Beacon vs PostgreSQL vs Trino vs Presto.

Phases per engine:
  1. Ingestion  — time to make the data query-ready (Postgres COPY, Trino/Presto
                  external-table registration, Beacon = 0 / queries files in place).
  2. Latency    — each query in queries.py run 1 cold + N warm; record cold + warm p50/p95.
  3. Resources  — docker-stats sampled (peak memory, mean CPU) during the latency phase.

It also records a cross-engine correctness check (every query's wrapped row count must
match) and, best-effort, the NetCDF->Parquet conversion cost that competitors pay but
Beacon avoids (the "no-ETL" advantage).

Run AFTER `docker compose -p beaconbench up -d` and `generate.py`:

    python run_benchmark.py --scale small
    python run_benchmark.py --engines beacon,postgres --warm 5
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

import clients
import metrics
import queries
import report as report_mod

PROJECT = os.environ.get("COMPOSE_PROJECT_NAME", "beaconbench")
DATA_DIR = Path(__file__).resolve().parents[1] / "data"


def cname(service: str) -> str:
    return f"{PROJECT}-{service}-1"


def build_clients(selected: set[str], hosts: dict) -> list[clients.Client]:
    out: list[clients.Client] = []
    if "beacon" in selected:
        out.append(clients.BeaconClient(
            "beacon", cname("beacon"),
            f"http://{hosts['beacon_host']}:{hosts['beacon_port']}",
            "read_parquet(['parquet/*.parquet'])",
        ))
    if "beacon-netcdf" in selected:
        out.append(clients.BeaconClient(
            "beacon-netcdf", cname("beacon"),
            f"http://{hosts['beacon_host']}:{hosts['beacon_port']}",
            "read_netcdf(['netcdf/*.nc'])",
        ))
    if "postgres" in selected:
        out.append(clients.PostgresClient(
            cname("postgres"),
            f"host={hosts['pg_host']} port={hosts['pg_port']} "
            f"user={hosts['pg_user']} password={hosts['pg_pw']} dbname={hosts['pg_db']}",
        ))
    if "trino" in selected:
        out.append(clients.TrinoClient(cname("trino"), hosts["trino_host"], hosts["trino_port"]))
    if "presto" in selected:
        out.append(clients.PrestoClient(cname("presto"), hosts["presto_host"], hosts["presto_port"]))
    if "duckdb" in selected:
        out.append(clients.DuckDBClient(
            cname("duckdb"),
            f"http://{hosts['duckdb_host']}:{hosts['duckdb_port']}",
            "/benchdata/parquet/*.parquet",  # path inside the duckdb container (native volume)
        ))
    return out


def run_engine(client: clients.Client, warm: int) -> dict:
    res: dict = {"engine": client.name, "table": client.table, "queries": {}}
    version = client.engine_version()
    if version:
        res["engine_version"] = version

    # ---- Ingestion -------------------------------------------------------------
    parquet_dir = str(DATA_DIR / "parquet")
    try:
        res["ingest_seconds"] = round(client.ingest(parquet_dir), 3)
        res["ingest_note"] = "queries files in place (no load)" if res["ingest_seconds"] == 0 else ""
    except Exception as e:  # ingestion failure is fatal for this engine
        res["error"] = f"ingest failed: {e}"
        return res

    # ---- Latency + resources ---------------------------------------------------
    with metrics.ResourceSampler(client.container) as sampler:
        for q in queries.QUERIES:
            sql = queries.render(q.sql, client.table)
            qres: dict = {"title": q.title}
            try:
                # Cold run (first touch).
                t0 = time.perf_counter()
                rows = client.count_rows(sql, wrap=q.wrap)
                qres["cold_ms"] = round((time.perf_counter() - t0) * 1000, 2)
                qres["result_rows"] = rows
                # Warm runs.
                lat = metrics.LatencyStats()
                for _ in range(warm):
                    t0 = time.perf_counter()
                    client.count_rows(sql, wrap=q.wrap)
                    lat.add(time.perf_counter() - t0)
                qres["warm"] = lat.summary()
            except Exception as e:
                qres["error"] = str(e)[:300]
            res["queries"][q.key] = qres
        res["resources"] = sampler.summary()
    return res


def measure_etl_conversion() -> dict:
    """Best-effort: time converting the generated NetCDF -> Parquet (the ETL Beacon skips).

    Requires xarray + pyarrow (data-gen deps). Skipped gracefully if unavailable or if
    no NetCDF was generated.
    """
    nc_dir = DATA_DIR / "netcdf"
    files = sorted(nc_dir.glob("*.nc")) if nc_dir.exists() else []
    if not files:
        return {"skipped": "no NetCDF files generated"}
    try:
        import tempfile

        import pyarrow as pa
        import pyarrow.parquet as pq
        import xarray as xr
    except Exception as e:
        return {"skipped": f"xarray/pyarrow unavailable: {e}"}

    t0 = time.perf_counter()
    rows = 0
    with tempfile.TemporaryDirectory() as tmp:
        for i, path in enumerate(files):
            ds = xr.open_dataset(path)
            table = pa.table({name: ds[name].values for name in ds.data_vars})
            pq.write_table(table, Path(tmp) / f"conv_{i:04d}.parquet", compression="snappy")
            rows += ds.sizes.get("obs", 0)
            ds.close()
    return {"seconds": round(time.perf_counter() - t0, 3), "rows": int(rows), "files": len(files)}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--engines", default="all",
                    help="comma list of: beacon,beacon-netcdf,postgres,trino,presto (or 'all')")
    ap.add_argument("--warm", type=int, default=3, help="warm repetitions per query")
    ap.add_argument("--scale", default=os.environ.get("BENCH_SCALE", "small"), help="label only")
    ap.add_argument("--no-etl", action="store_true", help="skip the NetCDF->Parquet conversion measurement")
    ap.add_argument("--out", type=Path, default=Path(__file__).resolve().parents[1] / "results")
    # Connection params (defaults match docker-compose + .env.example).
    ap.add_argument("--beacon-host", default="localhost")
    ap.add_argument("--beacon-port", type=int, default=int(os.environ.get("BEACON_HOST_PORT", 5001)))
    ap.add_argument("--pg-host", default="localhost")
    ap.add_argument("--pg-port", type=int, default=int(os.environ.get("POSTGRES_HOST_PORT", 5432)))
    ap.add_argument("--pg-user", default=os.environ.get("POSTGRES_USER", "bench"))
    ap.add_argument("--pg-pw", default=os.environ.get("POSTGRES_PASSWORD", "bench"))
    ap.add_argument("--pg-db", default=os.environ.get("POSTGRES_DB", "bench"))
    ap.add_argument("--trino-host", default="localhost")
    ap.add_argument("--trino-port", type=int, default=int(os.environ.get("TRINO_HOST_PORT", 8080)))
    ap.add_argument("--presto-host", default="localhost")
    ap.add_argument("--presto-port", type=int, default=int(os.environ.get("PRESTO_HOST_PORT", 8090)))
    ap.add_argument("--duckdb-host", default="localhost")
    ap.add_argument("--duckdb-port", type=int, default=int(os.environ.get("DUCKDB_HOST_PORT", 8500)))
    args = ap.parse_args()

    # `all` = the four engines reading the same Parquet (fair comparison + correctness).
    # beacon-netcdf is opt-in (--engines ...,beacon-netcdf): it reads the native NetCDF,
    # but the published beacon image's NetCDF reader can miscount rows, so it is kept out
    # of the default correctness-checked suite.
    all_engines = ["beacon", "postgres", "trino", "presto", "duckdb"]
    selected = set(all_engines) if args.engines == "all" else {e.strip() for e in args.engines.split(",")}
    if "beacon-netcdf" in selected and not (DATA_DIR / "netcdf").exists():
        print("note: skipping beacon-netcdf (no data/netcdf directory)")
        selected.discard("beacon-netcdf")

    hosts = {
        "beacon_host": args.beacon_host, "beacon_port": args.beacon_port,
        "pg_host": args.pg_host, "pg_port": args.pg_port, "pg_user": args.pg_user,
        "pg_pw": args.pg_pw, "pg_db": args.pg_db,
        "trino_host": args.trino_host, "trino_port": args.trino_port,
        "presto_host": args.presto_host, "presto_port": args.presto_port,
        "duckdb_host": args.duckdb_host, "duckdb_port": args.duckdb_port,
    }

    print(f"Engines: {sorted(selected)}  | warm={args.warm}  | scale={args.scale}")
    engine_results = []
    for client in build_clients(selected, hosts):
        print(f"\n=== {client.name} ===")
        r = run_engine(client, args.warm)
        client.close()
        if "error" in r:
            print(f"  ERROR: {r['error']}")
        else:
            ver = f" (v{r['engine_version']})" if r.get("engine_version") else ""
            print(f"  {client.name}{ver}  ingest: {r['ingest_seconds']}s  resources: {r.get('resources')}")
            for k, q in r["queries"].items():
                if "error" in q:
                    print(f"  {k:18s} ERROR: {q['error']}")
                else:
                    print(f"  {k:18s} cold={q['cold_ms']:>9}ms  warm_p50={q['warm'].get('p50_ms')}ms  rows={q['result_rows']}")
        engine_results.append(r)

    etl = {} if args.no_etl else measure_etl_conversion()
    if etl:
        print(f"\nNetCDF->Parquet conversion (ETL Beacon avoids): {etl}")

    payload = {
        "scale": args.scale,
        "warm": args.warm,
        "queries": {q.key: q.title for q in queries.QUERIES},
        "engines": engine_results,
        "etl_conversion": etl,
    }
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "results.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    report_mod.write_report(payload, args.out / "report.md")
    print(f"\nWrote {args.out / 'results.json'} and {args.out / 'report.md'}")

    # Cross-engine correctness check on wrapped row counts.
    report_mod.print_correctness(payload)


if __name__ == "__main__":
    main()
