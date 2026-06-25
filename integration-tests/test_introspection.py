"""Discovery / introspection endpoints: version, datasets, schemas, functions.

These are the read-only metadata surfaces a client (e.g. the web admin UI) uses to
explore a Beacon instance before querying it. They need no auth.
"""

from __future__ import annotations

import pytest

from beacon_client import QueryError


def test_info_reports_version(client):
    resp = client.get("/api/info")
    assert resp.status_code == 200
    body = resp.json()
    assert body["beacon_version"]
    assert body["beacon_version"].strip()


def test_total_datasets_counts_generated_files(client):
    resp = client.get("/api/total-datasets")
    assert resp.status_code == 200
    # Two parquet parts plus the stations CSV were generated.
    assert resp.json() >= 3


def test_list_datasets_describes_parquet_files(client):
    resp = client.get("/api/list-datasets")
    assert resp.status_code == 200
    entries = resp.json()
    parquet = [e for e in entries if e["format"] == "parquet"]
    assert len(parquet) >= 2
    paths = [e["file_path"] for e in parquet]
    assert any("part-0.parquet" in p for p in paths)
    assert any("part-1.parquet" in p for p in paths)
    # Every entry exposes the inspection-capability flags.
    for e in entries:
        assert set(e) >= {"file_path", "format", "can_inspect", "can_partial_explore"}


def test_dataset_schema_lists_columns(client):
    # Resolve a concrete parquet path from the discovery endpoint, then inspect it.
    entries = client.get("/api/list-datasets").json()
    parquet_path = next(e["file_path"] for e in entries if e["format"] == "parquet")

    resp = client.get(f"/api/dataset-schema?file={parquet_path}")
    assert resp.status_code == 200
    schema = resp.json()
    names = {f["name"] for f in schema["fields"]}
    assert {"time", "latitude", "longitude", "temperature", "salinity"} <= names
    # Each field carries an Arrow data-type string and a nullability flag.
    by_name = {f["name"]: f for f in schema["fields"]}
    assert by_name["temperature"]["data_type"]  # e.g. "Float32"
    assert isinstance(by_name["temperature"]["nullable"], bool)


def test_functions_endpoint_is_documented(client):
    resp = client.get("/api/functions")
    assert resp.status_code == 200
    funcs = resp.json()
    assert funcs, "expected at least one scalar/aggregate function"
    sample = funcs[0]
    assert set(sample) >= {"function_name", "description", "return_type", "params"}


def test_table_functions_endpoint_returns_list(client):
    resp = client.get("/api/table-functions")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


def test_table_functions_include_readers(client):
    names = {f["function_name"] for f in client.get("/api/table-functions").json()}
    assert "read_parquet" in names and "read_csv" in names, names


def test_default_table_endpoint_responds(client):
    # The runtime always answers, even if no default table is configured.
    assert client.get("/api/default-table").status_code == 200


@pytest.fixture(scope="module")
def schema_table(client):
    """An external table so the table-schema endpoints have something to report."""
    name = "intro_obs"
    try:
        client.execute(f"DROP TABLE IF EXISTS {name}", admin=True)
    except QueryError:
        pass
    client.execute(f"CREATE EXTERNAL TABLE {name} STORED AS PARQUET LOCATION 'obs/'", admin=True)
    yield name
    try:
        client.execute(f"DROP TABLE IF EXISTS {name}", admin=True)
    except QueryError:
        pass


def test_table_schema_endpoint(client, schema_table):
    resp = client.get(f"/api/table-schema?table_name={schema_table}")
    assert resp.status_code == 200
    names = {f["name"] for f in resp.json()["fields"]}
    assert "temperature" in names


def test_tables_with_schema_includes_external_table(client, schema_table):
    resp = client.get("/api/tables-with-schema")
    assert resp.status_code == 200
    by_name = {t["table_name"]: t for t in resp.json()}
    assert schema_table in by_name
    cols = {c["name"] for c in by_name[schema_table]["columns"]}
    assert "temperature" in cols
