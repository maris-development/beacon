"""Thin per-engine client wrappers used by the benchmark harness.

Every client exposes the same surface:

    .name                      display name
    .container                 docker container name (for resource sampling)
    .table                     the FROM target substituted into queries.py SQL
    .ingest(parquet_dir)       make the data query-ready; returns seconds (ETL cost)
    .count_rows(inner_sql)     execute the (wrapped) query, return the result row count

Latency is measured by the orchestrator around ``count_rows``. Wrapping each query as
``SELECT count(*) FROM (<q>) sub`` forces full execution on every engine while returning
a single integer, so the comparison isn't skewed by result-serialization differences —
and that integer doubles as the cross-engine correctness check.
"""

from __future__ import annotations

import csv
import io
import time

# Shared Hive DDL columns (Trino & Presto), matching data-gen/generate.py parquet schema.
HIVE_COLUMNS = [
    ("time", "bigint"),
    ("latitude", "double"),
    ("longitude", "double"),
    ("depth", "real"),
    ("platform", "varchar"),
    ("platform_id", "integer"),
    ("temperature", "real"),
    ("salinity", "real"),
    ("oxygen", "real"),
    ("pressure", "real"),
    ("chlorophyll", "real"),
    ("nitrate", "real"),
    ("ph", "real"),
]

PG_COLUMNS = [c for c, _ in HIVE_COLUMNS]


def wrap_count(inner_sql: str) -> str:
    return f"SELECT count(*) AS n FROM ({inner_sql}) AS _sub"


class Client:
    name: str = "?"
    container: str = ""
    table: str = ""

    def ingest(self, parquet_dir: str) -> float:
        raise NotImplementedError

    def count_rows(self, inner_sql: str, wrap: bool = True) -> int:
        """Run the query and return an integer: the wrapped row count (wrap=True) or
        the query's own scalar first cell (wrap=False)."""
        raise NotImplementedError

    def engine_version(self) -> str | None:
        """Best-effort engine version string for the report (None if unknown)."""
        return None

    def close(self) -> None:
        pass


# --------------------------------------------------------------------------- Beacon
class BeaconClient(Client):
    """Beacon over HTTP. `table` is a read_* table function so SQL is uniform."""

    def __init__(self, name: str, container: str, base_url: str, table: str):
        import requests  # local import so engines can be installed independently

        self.name = name
        self.container = container
        self.base_url = base_url.rstrip("/")
        self.table = table
        self._session = requests.Session()

    def _query_csv(self, sql: str) -> list[list[str]]:
        resp = self._session.post(
            f"{self.base_url}/api/query",
            json={"sql": sql, "output": {"format": "csv"}},
            timeout=600,
        )
        if resp.status_code != 200:
            raise RuntimeError(f"beacon query failed [{resp.status_code}]: {resp.text[:300]}")
        return list(csv.reader(io.StringIO(resp.text)))

    def ingest(self, parquet_dir: str) -> float:
        # Beacon queries files in place — no load step. We report 0 and capture the
        # native-vs-ETL story separately (NetCDF needs no conversion for Beacon).
        return 0.0

    def count_rows(self, inner_sql: str, wrap: bool = True) -> int:
        rows = self._query_csv(wrap_count(inner_sql) if wrap else inner_sql)
        # rows[0] is the header ("n"), rows[1] the value.
        return int(float(rows[1][0])) if len(rows) > 1 and rows[1] else 0

    def engine_version(self) -> str | None:
        # Beacon exposes its version via the beacon_version() SQL function.
        # The benchmark targets the 1.7.x line (tested on the 1.7.0 image; 1.7.1 is the
        # current source release and shares the same query engine).
        try:
            rows = self._query_csv("SELECT beacon_version() AS v")
            return rows[1][0] if len(rows) > 1 and rows[1] else None
        except Exception:
            return None


# --------------------------------------------------------------------------- Postgres
class PostgresClient(Client):
    name = "postgres"
    table = "obs"

    def __init__(self, container: str, dsn: str):
        import psycopg

        self.container = container
        self._conn = psycopg.connect(dsn, autocommit=True)

    def ingest(self, parquet_dir: str) -> float:
        import glob
        import os

        import pyarrow.parquet as pq

        files = sorted(glob.glob(os.path.join(parquet_dir, "*.parquet")))
        if not files:
            raise RuntimeError(f"no parquet files in {parquet_dir}")
        cols = ", ".join(PG_COLUMNS)
        t0 = time.perf_counter()
        with self._conn.cursor() as cur:
            cur.execute("TRUNCATE obs")
            with cur.copy(f"COPY obs ({cols}) FROM STDIN") as copy:
                for path in files:
                    pf = pq.ParquetFile(path)
                    for batch in pf.iter_batches(batch_size=50_000, columns=PG_COLUMNS):
                        pydict = batch.to_pydict()
                        for row in zip(*(pydict[c] for c in PG_COLUMNS)):
                            copy.write_row(row)
        return time.perf_counter() - t0

    def count_rows(self, inner_sql: str, wrap: bool = True) -> int:
        with self._conn.cursor() as cur:
            cur.execute(wrap_count(inner_sql) if wrap else inner_sql)
            return int(cur.fetchone()[0])

    def close(self) -> None:
        self._conn.close()


# --------------------------------------------------------- Trino / Presto (Hive) base
class _HiveDBAPIClient(Client):
    """Shared logic for Trino & Presto: both speak a DBAPI and read the same Hive table."""

    table = "hive.bench.obs"

    def __init__(self, container: str, conn):
        self.container = container
        self._conn = conn

    def _exec(self, sql: str) -> None:
        cur = self._conn.cursor()
        cur.execute(sql)
        cur.fetchall()

    def _scalar(self, sql: str) -> int:
        cur = self._conn.cursor()
        cur.execute(sql)
        return int(cur.fetchone()[0])

    def ingest(self, parquet_dir: str) -> float:
        # Register an EXTERNAL table over the existing parquet directory (no copy).
        cols = ", ".join(f"{c} {t}" for c, t in HIVE_COLUMNS)
        t0 = time.perf_counter()
        # No schema location: HMS uses its own writable warehouse dir. The Parquet lives
        # read-only at /benchdata in every Hive-stack container.
        self._exec("CREATE SCHEMA IF NOT EXISTS hive.bench")
        try:
            self._exec("DROP TABLE IF EXISTS hive.bench.obs")
        except Exception:
            pass
        self._exec(
            f"CREATE TABLE hive.bench.obs ({cols}) "
            "WITH (external_location = 'file:///benchdata/parquet', format = 'PARQUET')"
        )
        # Touch the table so the metastore/stats are warm and the cost is realistic.
        self._scalar("SELECT count(*) FROM hive.bench.obs")
        return time.perf_counter() - t0

    def count_rows(self, inner_sql: str, wrap: bool = True) -> int:
        return self._scalar(wrap_count(inner_sql) if wrap else inner_sql)

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass


class DuckDBClient(Client):
    """Containerized DuckDB queried over its HTTP server (engines/duckdb/server.py).
    Reads the same /benchdata volume under the same CPU/memory caps as the other engines —
    an equal comparison, not an in-process host run."""

    name = "duckdb"

    def __init__(self, container: str, base_url: str, parquet_glob: str):
        import requests

        self.container = container
        self.base_url = base_url.rstrip("/")
        # read_parquet over a glob inside the container = query the files in place, like Beacon.
        self.table = f"read_parquet('{parquet_glob}')"
        self._session = requests.Session()

    def ingest(self, parquet_dir: str) -> float:
        return 0.0  # queries files in place

    def count_rows(self, inner_sql: str, wrap: bool = True) -> int:
        sql = wrap_count(inner_sql) if wrap else inner_sql
        resp = self._session.post(f"{self.base_url}/query", json={"sql": sql}, timeout=600)
        if resp.status_code != 200:
            raise RuntimeError(f"duckdb query failed [{resp.status_code}]: {resp.text[:300]}")
        return int(resp.json()["value"])


class TrinoClient(_HiveDBAPIClient):
    name = "trino"

    def __init__(self, container: str, host: str, port: int):
        import trino

        conn = trino.dbapi.connect(host=host, port=port, user="bench", catalog="hive", schema="bench")
        super().__init__(container, conn)


class PrestoClient(_HiveDBAPIClient):
    name = "presto"

    def __init__(self, container: str, host: str, port: int):
        import prestodb

        conn = prestodb.dbapi.connect(host=host, port=port, user="bench", catalog="hive", schema="bench")
        super().__init__(container, conn)
