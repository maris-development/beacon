"""Broader SQL surface: window functions, CTEs, HAVING, aggregates, CASE,
string functions, and the numeric BETWEEN filter via the JSON DSL.

These guard that Beacon exposes the full DataFusion SQL dialect (not just simple
projections) over its readers.
"""

from __future__ import annotations

OBS = "read_parquet(['obs/*.parquet'])"


def test_window_row_number_top_per_platform(client, sample_data):
    """One row per platform has row_number() == 1 over a partitioned ranking."""
    n = client.count(
        "SELECT platform FROM ("
        "  SELECT platform, row_number() OVER "
        "    (PARTITION BY platform ORDER BY temperature DESC, time) AS rn "
        f"  FROM {OBS}"
        ") WHERE rn = 1"
    )
    assert n == len(sample_data["platforms"])


def test_cte(client, sample_data):
    n = client.count(
        f"WITH warm AS (SELECT * FROM {OBS} WHERE temperature > 20) "
        "SELECT * FROM warm"
    )
    assert n == sample_data["warm_count"]


def test_group_by_having(client, sample_data):
    many = client.sql_rows(
        f"SELECT platform, count(*) AS c FROM {OBS} GROUP BY platform HAVING count(*) > 100"
    )
    assert len(many) - 1 == len(sample_data["platforms"])
    none = client.sql_rows(
        f"SELECT platform, count(*) AS c FROM {OBS} GROUP BY platform HAVING count(*) > 100000"
    )
    assert len(none) - 1 == 0


def test_aggregate_functions(client, sample_data):
    rows = client.sql_rows(
        "SELECT min(temperature) AS lo, max(temperature) AS hi, "
        f"avg(temperature) AS mean, stddev(temperature) AS sd FROM {OBS}"
    )
    lo, hi, mean, sd = rows[1]
    temps = [r["temperature"] for r in sample_data["rows"]]
    assert float(lo) == min(temps)
    assert float(hi) == max(temps)
    assert min(temps) < float(mean) < max(temps)
    assert float(sd) > 0


def test_count_distinct(client, sample_data):
    n = int(client.scalar(f"SELECT count(DISTINCT platform) AS n FROM {OBS}"))
    assert n == len(sample_data["platforms"])


def test_case_expression(client, sample_data):
    n = client.count(
        f"SELECT * FROM {OBS} "
        "WHERE (CASE WHEN temperature > 20 THEN 1 ELSE 0 END) = 1"
    )
    assert n == sample_data["warm_count"]


def test_string_functions(client):
    rows = client.sql_rows(
        f"SELECT DISTINCT upper(platform) AS up, length(platform) AS len FROM {OBS} "
        "ORDER BY up"
    )
    ups = [r[0] for r in rows[1:]]
    assert "SHIP" in ups
    assert all(int(r[1]) == len(r[0]) for r in rows[1:])


def test_dsl_between_numeric(client, sample_data):
    rows = client.query_json_rows(
        {
            "from": {"parquet": {"paths": ["obs/*.parquet"]}},
            "select": ["temperature"],
            "filter": {"column": "temperature", "gt_eq": 10, "lt_eq": 20},
            "limit": 1_000_000,
        }
    )
    expected = sum(1 for r in sample_data["rows"] if 10 <= r["temperature"] <= 20)
    assert expected > 0
    assert len(rows) - 1 == expected
