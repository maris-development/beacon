"""A Beacon started with BEACON_ENABLE_SQL=false.

The SQL gate is intentionally narrow: only ``InnerQuery::Sql`` is rejected, while
the structured JSON DSL keeps working. A dedicated container verifies both halves
of that contract.
"""

from __future__ import annotations

import pytest

from beacon_client import BeaconHTTPClient
from conftest import ADMIN_PASSWORD, ADMIN_USERNAME, run_beacon_container


@pytest.fixture(scope="module")
def nosql_client(request, beacon_image, docker_network, datasets_dir, tmp_path_factory):
    tb = tmp_path_factory.mktemp("nosql-tables")
    base_url = run_beacon_container(
        request,
        name="beacon-it-nosql",
        image=beacon_image,
        network=docker_network,
        datasets_dir=datasets_dir,  # shared obs data (read-only use)
        tables_dir=tb,
        extra_env={"BEACON_ENABLE_SQL": "false"},
    )
    return BeaconHTTPClient(base_url, ADMIN_USERNAME, ADMIN_PASSWORD)


def test_health_ok_with_sql_disabled(nosql_client):
    assert nosql_client.get("/api/health").status_code == 200


def test_sql_query_is_rejected(nosql_client):
    # Even with admin auth, SQL is disabled at the transport.
    resp = nosql_client.raw({"sql": "SELECT 1", "output": {"format": "csv"}}, admin=True)
    assert resp.status_code == 400


def test_json_dsl_still_works(nosql_client, sample_data):
    rows = nosql_client.query_json_rows(
        {
            "from": {"parquet": {"paths": ["obs/*.parquet"]}},
            "select": ["temperature"],
            "filter": {"column": "temperature", "gt": 20},
            "limit": 1_000_000,
        }
    )
    assert len(rows) - 1 == sample_data["warm_count"]
