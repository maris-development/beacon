"""Materialized views: a Parquet-backed snapshot of a query, manually refreshed.

Covers create / query / staleness / REFRESH / drop, plus the admin gate. The
staleness test builds the view over a *managed* table so we can mutate the source
and prove the view only updates on an explicit REFRESH.
"""

from __future__ import annotations

import pytest

from beacon_client import QueryError

BASE = "mv_base"
VIEW = "mv_counts"


def _drop(client, name):
    try:
        client.execute(f"DROP TABLE IF EXISTS {name}", admin=True)
    except QueryError:
        pass


@pytest.fixture
def base_and_view(client):
    _drop(client, VIEW)
    _drop(client, BASE)
    client.execute(f"CREATE TABLE {BASE} (id BIGINT)", admin=True)
    client.execute(f"INSERT INTO {BASE} VALUES (1), (2)", admin=True)
    client.execute(
        f"CREATE MATERIALIZED VIEW {VIEW} AS SELECT count(*) AS n FROM {BASE}",
        admin=True,
    )
    yield
    _drop(client, VIEW)
    _drop(client, BASE)


def test_view_is_listed_and_queryable(client, base_and_view):
    assert VIEW in client.tables().json()
    show = client.sql_rows("SHOW TABLES", admin=True)
    assert VIEW in {cell for row in show[1:] for cell in row}
    # Reads do not require admin.
    assert int(client.scalar(f"SELECT n FROM {VIEW}")) == 2


def test_view_is_a_snapshot_until_refreshed(client, base_and_view):
    # Mutate the source after the view was materialized.
    client.execute(f"INSERT INTO {BASE} VALUES (3)", admin=True)
    # The view still reflects the snapshot (2), not the live source (3).
    assert int(client.scalar(f"SELECT n FROM {VIEW}")) == 2

    client.execute(f"REFRESH {VIEW}", admin=True)
    assert int(client.scalar(f"SELECT n FROM {VIEW}")) == 3


def test_view_over_parquet_aggregate(client, sample_data):
    name = "mv_platforms"
    _drop(client, name)
    try:
        client.execute(
            f"CREATE MATERIALIZED VIEW {name} AS "
            "SELECT platform, count(*) AS n FROM read_parquet(['obs/*.parquet']) "
            "GROUP BY platform",
            admin=True,
        )
        rows = client.sql_rows(f"SELECT platform, n FROM {name} ORDER BY platform")
        assert len(rows) - 1 == len(sample_data["platforms"])
        assert sum(int(r[1]) for r in rows[1:]) == sample_data["total"]
    finally:
        _drop(client, name)


def test_create_view_requires_admin(client):
    status = client.status(
        "CREATE MATERIALIZED VIEW mv_nope AS SELECT 1 AS a", admin=False
    )
    assert status == 400
    assert "mv_nope" not in client.tables().json()
