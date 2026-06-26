"""A full ETL pipeline driven entirely through Beacon's HTTP query API.

This stitches the individual features together into one realistic workflow on
Lance-backed managed tables (the default engine):

  1. Extract  — read the raw Parquet observations and the CSV station lookup.
  2. Load     — ``CREATE TABLE AS SELECT`` a staging table, enriching each
                observation with its station description via a cross-format join.
  3. Transform — evolve the schema (``ALTER TABLE ADD COLUMN``), derive a
                 category with ``UPDATE``, and cleanse out-of-scope rows with
                 ``DELETE``.
  4. Publish  — ``CREATE TABLE AS SELECT`` an aggregated data mart.
  5. Verify   — assert every stage's row counts against expectations computed
                directly from the generator, then tear the tables down.

Expectations are derived from ``sample_data`` in Python so the assertions track
the data rather than hard-coded magic numbers.
"""

from __future__ import annotations

import json

import pytest

from beacon_client import QueryError

STAGING = "etl_staging"
SUMMARY = "etl_summary"
DROPPED_PLATFORM = "FLOAT"  # cleansed out during the transform stage


def _drop(client, name):
    try:
        client.execute(f"DROP TABLE IF EXISTS {name}", admin=True)
    except QueryError:
        pass


@pytest.fixture
def etl_tables(client):
    for name in (SUMMARY, STAGING):
        _drop(client, name)
    yield
    for name in (SUMMARY, STAGING):
        _drop(client, name)


def _is_lance(client, name) -> bool:
    resp = client.admin_get(f"/api/admin/table-config?table_name={name}")
    assert resp.status_code == 200, resp.text
    return '"lance"' in json.dumps(resp.json())


def test_full_etl_pipeline(client, sample_data, etl_tables):
    rows = sample_data["rows"]
    total = sample_data["total"]
    warm_count = sample_data["warm_count"]
    remaining_rows = [r for r in rows if r["platform"] != DROPPED_PLATFORM]
    remaining = len(remaining_rows)
    expected_groups = len(
        {
            (r["platform"], "warm" if r["temperature"] > 20 else "cold")
            for r in remaining_rows
        }
    )
    # Guards: the fixtures must make every stage meaningful.
    assert 0 < warm_count < total
    assert 0 < remaining < total

    # --- 1+2. Extract & Load: stage the join of Parquet obs + CSV stations -----
    client.execute(
        f"CREATE TABLE {STAGING} AS "
        "SELECT o.time AS time, o.platform AS platform, "
        "       o.temperature AS temperature, o.depth AS depth, "
        "       s.description AS station "
        "FROM read_parquet(['obs/*.parquet']) o "
        "JOIN read_csv(['stations.csv']) s ON o.platform = s.platform",
        admin=True,
    )
    assert client.count(f"SELECT * FROM {STAGING}") == total
    assert _is_lance(client, STAGING)
    # The enrichment join populated a station description for every row.
    assert client.count(f"SELECT * FROM {STAGING} WHERE station IS NULL") == 0

    # --- 3. Transform: schema evolution + derived column + cleanse ------------
    client.execute(f"ALTER TABLE {STAGING} ADD COLUMN temp_category VARCHAR", admin=True)
    client.execute(
        f"UPDATE {STAGING} SET temp_category = 'warm' WHERE temperature > 20",
        admin=True,
    )
    client.execute(
        f"UPDATE {STAGING} SET temp_category = 'cold' WHERE temperature <= 20",
        admin=True,
    )
    # Every row got a category, and 'warm' matches the known warm subset.
    assert client.count(f"SELECT * FROM {STAGING} WHERE temp_category IS NULL") == 0
    assert (
        client.count(f"SELECT * FROM {STAGING} WHERE temp_category = 'warm'")
        == warm_count
    )

    # Cleanse: drop an out-of-scope platform.
    client.execute(
        f"DELETE FROM {STAGING} WHERE platform = '{DROPPED_PLATFORM}'", admin=True
    )
    assert client.count(f"SELECT * FROM {STAGING}") == remaining
    assert (
        client.count(f"SELECT * FROM {STAGING} WHERE platform = '{DROPPED_PLATFORM}'")
        == 0
    )

    # --- 4. Publish: aggregate data mart -------------------------------------
    client.execute(
        f"CREATE TABLE {SUMMARY} AS "
        "SELECT platform, temp_category, count(*) AS n, avg(temperature) AS avg_temp "
        f"FROM {STAGING} GROUP BY platform, temp_category",
        admin=True,
    )
    assert _is_lance(client, SUMMARY)

    summary = client.sql_rows(
        f"SELECT platform, temp_category, n, avg_temp FROM {SUMMARY} "
        "ORDER BY platform, temp_category"
    )
    groups = summary[1:]

    # --- 5. Verify the published mart against the source-derived expectations --
    assert len(groups) == expected_groups
    assert sum(int(r[2]) for r in groups) == remaining
    assert DROPPED_PLATFORM not in {r[0] for r in groups}
    # Every 'warm' bucket must average above the 20-degree threshold.
    warm_avgs = [float(r[3]) for r in groups if r[1] == "warm"]
    assert warm_avgs and all(avg > 20 for avg in warm_avgs)


def test_etl_writes_require_admin(client, sample_data, etl_tables):
    """The whole pipeline is gated: an anonymous CTAS load is rejected (400)."""
    status = client.status(
        f"CREATE TABLE {STAGING} AS SELECT * FROM read_parquet(['obs/*.parquet'])",
        admin=False,
    )
    assert status == 400
    assert STAGING not in client.tables().json()
