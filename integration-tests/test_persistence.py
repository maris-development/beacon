"""Durability: catalog state survives a container restart.

A managed Lance table, an external table, and a crawler definition are all
persisted under the mounted data dirs and must reload when Beacon restarts. This
restarts the *shared* container once and re-points the session client at the new
host port (the mapped port can change across a restart).
"""

from __future__ import annotations

import pytest

from beacon_client import QueryError
from conftest import CONTAINER_NAME, HTTP_PORT, _resolve_host_port, _run, _wait_healthy

LANCE_TBL = "persist_lance"
EXT_TBL = "persist_ext"
CRAWLER = "persist_crawler"


@pytest.fixture
def seeded(client):
    for name in (LANCE_TBL, EXT_TBL):
        try:
            client.execute(f"DROP TABLE IF EXISTS {name}", admin=True)
        except QueryError:
            pass
    client.admin_delete(f"/api/admin/crawlers/{CRAWLER}")

    client.execute(f"CREATE TABLE {LANCE_TBL} (id BIGINT, name VARCHAR)", admin=True)
    client.execute(f"INSERT INTO {LANCE_TBL} VALUES (1, 'a'), (2, 'b'), (3, 'c')", admin=True)
    client.execute(f"CREATE EXTERNAL TABLE {EXT_TBL} STORED AS PARQUET LOCATION 'obs/'", admin=True)
    resp = client.admin_post(
        "/api/admin/crawlers",
        {"name": CRAWLER, "target_prefix": "obs/", "format_filter": ["parquet"]},
    )
    assert resp.status_code == 200, resp.text

    yield
    for name in (LANCE_TBL, EXT_TBL):
        try:
            client.execute(f"DROP TABLE IF EXISTS {name}", admin=True)
        except QueryError:
            pass
    client.admin_delete(f"/api/admin/crawlers/{CRAWLER}")


def test_catalog_survives_restart(client, seeded, sample_data):
    # Capture pre-restart truth.
    assert client.count(f"SELECT * FROM {LANCE_TBL}") == 3

    _run(["docker", "restart", CONTAINER_NAME])
    base_url = f"http://127.0.0.1:{_resolve_host_port(CONTAINER_NAME, HTTP_PORT)}"
    _wait_healthy(base_url, CONTAINER_NAME)
    client.base_url = base_url

    # Managed Lance table: data reloaded from the Lance dataset.
    assert client.count(f"SELECT * FROM {LANCE_TBL}") == 3
    assert client.scalar(f"SELECT name FROM {LANCE_TBL} WHERE id = 2") == "b"

    # External table: definition reloaded from table.json, still queryable.
    assert client.count(f"SELECT * FROM {EXT_TBL}") == sample_data["total"]

    # Crawler definition reloaded.
    listed = client.admin_get("/api/admin/crawlers")
    assert listed.status_code == 200
    assert CRAWLER in {c["name"] for c in listed.json()}
