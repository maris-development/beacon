"""Query the freshly generated Parquet files in place, over SQL and the JSON DSL."""

from __future__ import annotations


def test_sql_count_all_rows(client, sample_data):
    n = client.count("SELECT * FROM read_parquet(['obs/*.parquet'])")
    assert n == sample_data["total"]


def test_sql_where_filter(client, sample_data):
    n = client.count(
        "SELECT * FROM read_parquet(['obs/*.parquet']) WHERE temperature > 20"
    )
    assert n == sample_data["warm_count"]
    assert sample_data["warm_count"] > 0  # guard: the filter must be meaningful


def test_sql_group_by_aggregation(client, sample_data):
    rows = client.sql_rows(
        "SELECT platform, count(*) AS n, avg(temperature) AS avg_temp "
        "FROM read_parquet(['obs/*.parquet']) "
        "GROUP BY platform ORDER BY platform"
    )
    platforms = sorted(r[0] for r in rows[1:])
    assert platforms == sorted(sample_data["platforms"])
    total = sum(int(r[1]) for r in rows[1:])
    assert total == sample_data["total"]


def test_json_dsl_matches_sql(client, sample_data):
    """The JSON DSL parquet source + range filter must match the SQL count."""
    rows = client.query_json_rows(
        {
            "from": {"parquet": {"paths": ["obs/*.parquet"]}},
            "select": ["time", "temperature"],
            "filters": [{"column": "temperature", "min": 20.0001}],
            "limit": 1_000_000,
        }
    )
    # Subtract the header row; the JSON DSL returns the matching rows directly.
    assert len(rows) - 1 == sample_data["warm_count"]


def test_malformed_query_returns_400(client):
    assert client.status("SELECT FROM WHERE not valid sql") == 400
