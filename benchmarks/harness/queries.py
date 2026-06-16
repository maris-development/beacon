"""Canonical benchmark query suite.

Each query is defined once as ANSI-ish SQL with a ``{table}`` placeholder. The five
engines accept nearly identical SQL, so per-engine differences are limited to how the
data source is named (``{table}``) and a couple of dialect quirks handled in clients.py:

    Postgres        -> table ``obs``
    Trino / Presto  -> ``hive.bench.obs``
    DuckDB          -> ``read_parquet('/benchdata/parquet/*.parquet')``
    Beacon (fair)   -> ``read_parquet(['parquet/*.parquet'])``
    Beacon (netcdf) -> ``read_netcdf(['netcdf/*.nc'])``     (native, no ETL)

Queries are intentionally analytical full-scan / filter / aggregate shapes — the
workloads these engines are built for and where Beacon's columnar pushdown shows.
``expect_rows`` marks queries whose row count must match across engines (the fairness
correctness check); aggregations return a fixed shape so they are checked too.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Query:
    key: str
    title: str
    sql: str  # contains a single {table} placeholder
    # When True the query is wrapped as `SELECT count(*) FROM (sql) sub` to measure
    # execution without result-transfer skew. When False the query is run as-is and its
    # scalar first cell is the recorded value (used by count_all to report the row total).
    wrap: bool = True


QUERIES: list[Query] = [
    Query(
        key="count_all",
        # count(temperature) touches a column, forcing a real scan on every engine
        # (a bare count(*) can be short-circuited from metadata) and returning the row total.
        title="Full-scan count (column-touching)",
        sql="SELECT count(temperature) AS n FROM {table}",
        wrap=False,
    ),
    Query(
        key="filter_temp",
        title="Projection + single range filter (temperature)",
        sql=(
            "SELECT time, latitude, longitude, temperature FROM {table} "
            "WHERE temperature BETWEEN 10 AND 12"
        ),
    ),
    Query(
        key="filter_multi",
        title="Multi-column BETWEEN filter (temperature + latitude)",
        sql=(
            "SELECT time, latitude, longitude, temperature, salinity FROM {table} "
            "WHERE temperature BETWEEN 5 AND 15 AND latitude BETWEEN -10 AND 10"
        ),
    ),
    Query(
        key="agg_by_platform",
        title="GROUP BY platform with AVG / COUNT",
        sql=(
            "SELECT platform, avg(temperature) AS avg_temp, avg(salinity) AS avg_sal, "
            "count(*) AS n FROM {table} GROUP BY platform ORDER BY n DESC"
        ),
    ),
    Query(
        key="spatial_box",
        title="Spatial bounding-box filter (lon/lat range)",
        sql=(
            "SELECT time, longitude, latitude, temperature FROM {table} "
            "WHERE longitude BETWEEN -10 AND 10 AND latitude BETWEEN 30 AND 60"
        ),
    ),
    Query(
        key="time_window",
        title="Time-range filter (~30-day window)",
        # time is epoch seconds; this picks a ~30-day window mid-dataset. With --sort
        # time/time-geo this prunes most row-groups; with random data it scans everything.
        sql=(
            "SELECT time, latitude, longitude, temperature FROM {table} "
            "WHERE time BETWEEN 1656625600 AND 1659217600"
        ),
    ),
    Query(
        key="topn_recent",
        title="ORDER BY time DESC LIMIT 1000 (top-N)",
        sql="SELECT time, platform, temperature, salinity FROM {table} ORDER BY time DESC LIMIT 1000",
    ),
    Query(
        key="distinct_platform",
        title="SELECT DISTINCT platform",
        sql="SELECT DISTINCT platform FROM {table} ORDER BY platform",
    ),
]


def render(sql: str, table: str) -> str:
    return sql.format(table=table)
