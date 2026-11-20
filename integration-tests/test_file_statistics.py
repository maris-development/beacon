"""File statistics of external tables, end to end over HTTP.

An external table with ``'use_statistics_pruning' 'true'`` keeps one statistics
table in ``beacon.db``. The background refresh fills it, a query prunes on it,
``table_statistics('<table>')`` shows it, ``ANALYZE FILES`` starts a refresh and
returns at once, and ``DROP TABLE`` deletes it.

The module runs its own containers with their own data, because one test restarts
the container and another one turns the statistics off.
"""

from __future__ import annotations

import re
import time
from pathlib import Path

import pytest

from beacon_client import BeaconHTTPClient, QueryError
from conftest import (
    ADMIN_PASSWORD,
    ADMIN_USERNAME,
    HTTP_PORT,
    _resolve_host_port,
    _run,
    _wait_healthy,
    run_beacon_container,
)

CONTAINER = "beacon-it-filestats"
CONTAINER_OFF = "beacon-it-filestats-off"
REFRESH_TIMEOUT_S = 120


def _write_parquet(path: Path, low: float, high: float) -> None:
    """Writes a Parquet file whose ``temperature`` goes from ``low`` to ``high``."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "temperature": pa.array([low, high], pa.float64()),
            "depth": pa.array([1, 2], pa.int64()),
        }
    )
    pq.write_table(table, path)


@pytest.fixture(scope="module")
def stats_datasets(tmp_path_factory) -> Path:
    """The datasets of this module, staged before the container starts."""
    root = tmp_path_factory.mktemp("filestats-datasets")
    _write_parquet(root / "obs" / "cold.parquet", 0.0, 5.0)
    _write_parquet(root / "obs" / "mild.parquet", 20.0, 25.0)
    _write_parquet(root / "obs" / "hot.parquet", 90.0, 100.0)
    _write_parquet(root / "grow" / "a.parquet", 1.0, 2.0)
    _write_parquet(root / "crawled" / "a.parquet", 1.0, 2.0)
    _write_parquet(root / "crawled" / "b.parquet", 50.0, 60.0)
    return root


@pytest.fixture(scope="module")
def stats_client(request, beacon_image, docker_network, stats_datasets, tmp_path_factory):
    base_url = run_beacon_container(
        request,
        name=CONTAINER,
        image=beacon_image,
        network=docker_network,
        datasets_dir=stats_datasets,
        tables_dir=tmp_path_factory.mktemp("filestats-tables"),
        extra_env={
            "BEACON_FILE_STATS_MAX_REFRESHES": "1",
            # Variables of the old engine: a server with an old configuration still starts.
            "BEACON_FILE_STATS_INTERVAL_SECS": "60",
            "BEACON_FILE_STATS_ON_STARTUP": "true",
        },
    )
    return BeaconHTTPClient(base_url, ADMIN_USERNAME, ADMIN_PASSWORD)


def _create(client: BeaconHTTPClient, table: str, location: str, prune: bool = True) -> None:
    options = " OPTIONS ('use_statistics_pruning' 'true')" if prune else ""
    client.execute(f"DROP TABLE IF EXISTS {table}")
    client.execute(
        f"CREATE EXTERNAL TABLE {table} STORED AS PARQUET LOCATION '{location}'{options}"
    )


def _statistics_rows(client: BeaconHTTPClient, table: str) -> int:
    return client.count(f"SELECT _path FROM table_statistics('{table}')", admin=True)


def _wait_for_rows(client: BeaconHTTPClient, table: str, rows: int) -> None:
    """Waits until the background refresh wrote ``rows`` statistics rows for ``table``."""
    deadline = time.time() + REFRESH_TIMEOUT_S
    last: object = None
    while time.time() < deadline:
        try:
            last = _statistics_rows(client, table)
            if last == rows:
                return
        except QueryError as error:
            last = error
        time.sleep(0.5)
    pytest.fail(f"{table}: expected {rows} statistics rows, last result: {last!r}")


def _metric(client: BeaconHTTPClient, sql: str, name: str) -> int:
    """The sum of the metric ``name`` in the ``EXPLAIN ANALYZE`` output of ``sql``."""
    rows = client.sql_rows(f"EXPLAIN ANALYZE {sql}", admin=True)
    text = "\n".join(" ".join(row) for row in rows)
    return sum(int(value) for value in re.findall(rf"{name}=(\d+)", text))


def _error(client: BeaconHTTPClient, sql: str, admin: bool = True) -> QueryError:
    with pytest.raises(QueryError) as raised:
        client.sql_rows(sql, admin=admin)
    return raised.value


def test_the_statistics_fill_in_the_background_after_create(stats_client):
    _create(stats_client, "fs_obs", "obs/")
    _wait_for_rows(stats_client, "fs_obs", 3)


def test_a_query_prunes_on_the_statistics(stats_client):
    _create(stats_client, "fs_prune", "obs/")
    _wait_for_rows(stats_client, "fs_prune", 3)

    sql = "SELECT * FROM fs_prune WHERE temperature > 80"
    assert _metric(stats_client, sql, "statistics_files_considered") == 3
    assert _metric(stats_client, sql, "statistics_files_pruned") == 2
    # Pruning changes the time, not the answer: hot.parquet holds both rows.
    assert stats_client.count(sql, admin=True) == 2
    assert stats_client.count("SELECT * FROM fs_prune WHERE temperature < 100", admin=True) == 5


def test_table_statistics_shows_the_range_of_each_file(stats_client):
    _create(stats_client, "fs_ranges", "obs/")
    _wait_for_rows(stats_client, "fs_ranges", 3)

    rows = stats_client.sql_rows(
        "SELECT _path, _row_count, _failed, temperature['min'], temperature['max'] "
        "FROM table_statistics('fs_ranges') ORDER BY _path",
        admin=True,
    )
    # Numbers, not text: a Parquet writer stores a zero minimum as -0.0.
    ranges = [(path, int(count), failed, float(low), float(high))
              for path, count, failed, low, high in rows[1:]]
    assert ranges == [
        ("obs/cold.parquet", 2, "False", 0.0, 5.0),
        ("obs/hot.parquet", 2, "False", 90.0, 100.0),
        ("obs/mild.parquet", 2, "False", 20.0, 25.0),
    ]


def test_table_statistics_is_super_user_only(stats_client):
    _create(stats_client, "fs_private", "obs/")
    error = _error(stats_client, "SELECT * FROM table_statistics('fs_private')", admin=False)
    assert "super-user" in error.body


def test_analyze_files_returns_at_once_and_reads_a_new_file(stats_client, stats_datasets):
    _create(stats_client, "fs_grow", "grow/")
    _wait_for_rows(stats_client, "fs_grow", 1)
    _write_parquet(stats_datasets / "grow" / "b.parquet", 3.0, 4.0)

    started = time.time()
    rows = stats_client.sql_rows("ANALYZE FILES fs_grow", admin=True)
    elapsed = time.time() - started

    assert rows[0] == ["table", "status"]
    assert len(rows) == 2 and rows[1][0] == "fs_grow"
    assert rows[1][1] in {"started", "queued"}
    assert elapsed < 10, f"ANALYZE FILES must not wait for the refresh: {elapsed:.1f} s"
    # The refresh interval is 900 seconds, so only the request reads the new file.
    _wait_for_rows(stats_client, "fs_grow", 2)


def test_analyze_files_covers_each_table_with_statistics(stats_client):
    _create(stats_client, "fs_each", "obs/")
    _create(stats_client, "fs_plain", "obs/", prune=False)

    tables = [row[0] for row in stats_client.sql_rows("ANALYZE FILES", admin=True)[1:]]
    assert "fs_each" in tables
    assert "fs_plain" not in tables


def test_analyze_files_reports_a_table_without_statistics(stats_client):
    _create(stats_client, "fs_none", "obs/", prune=False)
    error = _error(stats_client, "ANALYZE FILES fs_none")
    assert error.status_code == 400
    assert "use_statistics_pruning" in error.body

    error = _error(stats_client, "ANALYZE FILES fs_missing")
    assert error.status_code == 400
    assert "fs_missing" in error.body


def test_analyze_files_is_super_user_only(stats_client):
    _create(stats_client, "fs_gate", "obs/")
    error = _error(stats_client, "ANALYZE FILES fs_gate", admin=False)
    assert error.status_code >= 400


def test_a_crawler_passes_the_option_to_its_tables(stats_client):
    stats_client.execute(
        "CREATE CRAWLER fs_crawler ON 'crawled/' WITH ('use_statistics_pruning' 'true')"
    )
    stats_client.execute("RUN CRAWLER fs_crawler")
    _wait_for_rows(stats_client, "crawled", 2)
    sql = "SELECT * FROM crawled WHERE temperature > 40"
    assert _metric(stats_client, sql, "statistics_files_pruned") == 1


def test_or_replace_without_the_option_deletes_the_statistics(stats_client):
    _create(stats_client, "fs_replace", "obs/")
    _wait_for_rows(stats_client, "fs_replace", 3)

    stats_client.execute(
        "CREATE OR REPLACE EXTERNAL TABLE fs_replace STORED AS PARQUET LOCATION 'obs/'"
    )

    error = _error(stats_client, "SELECT * FROM table_statistics('fs_replace')")
    assert "no statistics" in error.body
    tables = [row[0] for row in stats_client.sql_rows("ANALYZE FILES", admin=True)[1:]]
    assert "fs_replace" not in tables


def test_drop_table_deletes_the_statistics(stats_client):
    _create(stats_client, "fs_drop", "obs/")
    _wait_for_rows(stats_client, "fs_drop", 3)

    stats_client.execute("DROP TABLE fs_drop")

    error = _error(stats_client, "SELECT * FROM table_statistics('fs_drop')")
    assert "no statistics" in error.body


def test_the_statistics_survive_a_restart(stats_client):
    _create(stats_client, "fs_restart", "obs/")
    _wait_for_rows(stats_client, "fs_restart", 3)

    _run(["docker", "restart", CONTAINER])
    stats_client.base_url = f"http://127.0.0.1:{_resolve_host_port(CONTAINER, HTTP_PORT)}"
    _wait_healthy(stats_client.base_url, CONTAINER)

    # The rows are in beacon.db, so the first query after the restart prunes.
    assert _statistics_rows(stats_client, "fs_restart") == 3
    sql = "SELECT * FROM fs_restart WHERE temperature > 80"
    assert _metric(stats_client, sql, "statistics_files_pruned") == 2


def test_analyze_files_names_the_setting_when_statistics_are_off(
    request, beacon_image, docker_network, stats_datasets, tmp_path_factory
):
    base_url = run_beacon_container(
        request,
        name=CONTAINER_OFF,
        image=beacon_image,
        network=docker_network,
        datasets_dir=stats_datasets,
        tables_dir=tmp_path_factory.mktemp("filestats-off-tables"),
        extra_env={"BEACON_FILE_STATS_ENABLE": "false"},
    )
    client = BeaconHTTPClient(base_url, ADMIN_USERNAME, ADMIN_PASSWORD)

    error = _error(client, "ANALYZE FILES")
    assert error.status_code == 400
    assert "BEACON_FILE_STATS_ENABLE" in error.body

    # The option does no harm: the table works, and no query prunes.
    _create(client, "fs_off", "obs/")
    sql = "SELECT * FROM fs_off WHERE temperature > 80"
    assert client.count(sql, admin=True) == 2
    assert _metric(client, sql, "statistics_files_pruned") == 0
