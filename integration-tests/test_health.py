"""Smoke tests: the container is up, the API answers, and it sees our datasets."""

from __future__ import annotations


def test_health_endpoint_ok(client):
    resp = client.get("/api/health")
    assert resp.status_code == 200
    assert resp.text.strip() == "Ok"


def test_swagger_reachable(client):
    resp = client.get("/swagger")
    assert resp.status_code == 200


def test_beacon_version_non_empty(client):
    version = client.scalar("SELECT beacon_version() AS v")
    assert version
    assert version.strip()


def test_datasets_endpoint_lists_generated_files(client):
    resp = client.datasets()
    assert resp.status_code == 200
    files = resp.json()
    assert any("part-0.parquet" in f for f in files)
    assert any("part-1.parquet" in f for f in files)


def test_list_datasets_table_function(client):
    """The list_datasets() SQL table function should report both parquet files."""
    rows = client.sql_rows(
        "SELECT file_name FROM list_datasets() WHERE file_format = 'parquet' ORDER BY file_name"
    )
    # rows[0] is the header.
    names = [r[0] for r in rows[1:]]
    assert any("part-0.parquet" in n for n in names)
    assert any("part-1.parquet" in n for n in names)
