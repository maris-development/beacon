"""CSV reading and cross-format joins.

The suite generates a ``stations.csv`` alongside the Parquet data but never reads
it. These tests cover the ``read_csv`` table function, schema inspection via
``read_schema``, the JSON DSL ``csv`` source, and a join that combines the CSV
lookup table with the Parquet observations — proving Beacon mixes file formats in
one query.
"""

from __future__ import annotations

STATIONS = "read_csv(['stations.csv'])"


def test_read_csv_row_count(client, sample_data):
    # The CSV has one row per platform.
    n = client.count(f"SELECT * FROM {STATIONS}")
    assert n == len(sample_data["platforms"])


def test_read_csv_columns(client):
    rows = client.sql_rows(f"SELECT platform, description FROM {STATIONS} ORDER BY platform")
    assert rows[0] == ["platform", "description"]
    platforms = [r[0] for r in rows[1:]]
    assert "SHIP" in platforms
    # The description column is derived from the platform name in the generator.
    descriptions = {r[0]: r[1] for r in rows[1:]}
    assert descriptions["SHIP"] == "ship platform"


def test_read_schema_reports_parquet_columns(client):
    rows = client.sql_rows("SELECT * FROM read_schema(['obs/*.parquet'], 'parquet')")
    flat = {cell for row in rows for cell in row}
    assert "temperature" in flat
    assert "salinity" in flat


def test_csv_parquet_join(client, sample_data):
    """Join the Parquet observations to the CSV station lookup by platform."""
    rows = client.sql_rows(
        "SELECT s.description AS station, count(*) AS n "
        "FROM read_parquet(['obs/*.parquet']) o "
        f"JOIN {STATIONS} s ON o.platform = s.platform "
        "GROUP BY s.description ORDER BY station"
    )
    groups = rows[1:]
    # Every platform present in the data resolves to a station description.
    assert len(groups) == len(sample_data["platforms"])
    assert sum(int(r[1]) for r in groups) == sample_data["total"]


def test_json_dsl_csv_source(client, sample_data):
    rows = client.query_json_rows(
        {
            "from": {"csv": {"paths": ["stations.csv"]}},
            "select": ["platform"],
            "limit": 1000,
        }
    )
    assert len(rows) - 1 == len(sample_data["platforms"])
