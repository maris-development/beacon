"""External tables: create one over the generated Parquet (admin), then query it.

Also asserts the read-only gate still holds for the HTTP transport:
- no credentials  -> anonymous, DDL rejected by the query planner (400)
- bad credentials -> rejected by auth (401)
"""

from __future__ import annotations

import pytest

from beacon_client import QueryError

CREATE_DDL = "CREATE EXTERNAL TABLE obs_ext STORED AS PARQUET LOCATION 'obs/'"


@pytest.fixture(scope="module")
def external_table(client):
    """Create the external table once for this module (admin-authenticated)."""
    # Drop first so reruns against a reused image stay idempotent.
    try:
        client.execute("DROP TABLE IF EXISTS obs_ext", admin=True)
    except QueryError:
        pass
    client.execute(CREATE_DDL, admin=True)
    yield "obs_ext"
    try:
        client.execute("DROP TABLE IF EXISTS obs_ext", admin=True)
    except QueryError:
        pass


def test_external_table_count_matches_in_place(client, external_table, sample_data):
    via_table = client.count(f"SELECT * FROM {external_table}")
    in_place = client.count("SELECT * FROM read_parquet(['obs/*.parquet'])")
    assert via_table == in_place == sample_data["total"]


def test_external_table_listed_in_tables_endpoint(client, external_table):
    resp = client.tables()
    assert resp.status_code == 200
    assert external_table in resp.json()


def test_external_table_listed_in_show_tables(client, external_table):
    rows = client.sql_rows("SHOW TABLES", admin=True)
    flat = {cell for row in rows[1:] for cell in row}
    assert external_table in flat


def test_create_ddl_without_credentials_is_rejected(client):
    """Anonymous (no auth) -> read-only: the planner rejects DDL with a 400."""
    status = client.status(
        "CREATE EXTERNAL TABLE should_not_exist STORED AS PARQUET LOCATION 'obs/'",
        admin=False,
    )
    assert status == 400


def test_invalid_credentials_are_rejected(beacon_container):
    """Present-but-wrong credentials must error (401), not silently downgrade."""
    import requests

    resp = requests.post(
        f"{beacon_container}/api/query",
        json={"sql": "SELECT 1", "output": {"format": "csv"}},
        auth=("admin", "wrong-password"),
        timeout=30,
    )
    assert resp.status_code == 401
