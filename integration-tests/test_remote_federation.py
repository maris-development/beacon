"""Beacon-to-Beacon federation via ``STORED AS REMOTE``.

A second Beacon container (started with anonymous Flight SQL allowed) hosts a
managed table. The main Beacon registers it as a remote table and queries it —
the scan is shipped to the remote over Flight SQL, with filters pushed down.
The two containers share the docker network, so the main one reaches the remote
by container name on the internal Flight port.
"""

from __future__ import annotations

import pytest

from beacon_client import BeaconHTTPClient, QueryError
from conftest import ADMIN_PASSWORD, ADMIN_USERNAME, FLIGHT_PORT, run_beacon_container

REMOTE_HOST = "beacon-it-remote"
SRC_TABLE = "remote_src"
SEED_ROWS = 3
ROWS_ID_GT_1 = 2


@pytest.fixture(scope="module")
def remote_target(request, beacon_image, docker_network, tmp_path_factory) -> str:
    ds = tmp_path_factory.mktemp("remote-datasets")
    tb = tmp_path_factory.mktemp("remote-tables")
    base_url = run_beacon_container(
        request,
        name=REMOTE_HOST,
        image=beacon_image,
        network=docker_network,
        datasets_dir=ds,
        tables_dir=tb,
        extra_env={"BEACON_FLIGHT_SQL_ALLOW_ANONYMOUS": "true"},
    )
    remote = BeaconHTTPClient(base_url, ADMIN_USERNAME, ADMIN_PASSWORD)
    remote.execute(f"CREATE TABLE {SRC_TABLE} (id BIGINT, val DOUBLE)", admin=True)
    remote.execute(
        f"INSERT INTO {SRC_TABLE} VALUES (1, 10.0), (2, 20.0), (3, 30.0)", admin=True
    )
    return SRC_TABLE


@pytest.fixture
def remote_table(client, remote_target):
    name = "remote_obs"
    try:
        client.execute("DROP TABLE IF EXISTS remote_obs", admin=True)
    except QueryError:
        pass
    # The remote reaches the target by container name on the shared network,
    # using the internal Flight SQL port.
    client.execute(
        f"CREATE EXTERNAL TABLE {name} STORED AS REMOTE "
        f"LOCATION 'beacon://{REMOTE_HOST}:{FLIGHT_PORT}/{remote_target}'",
        admin=True,
    )
    yield name
    try:
        client.execute("DROP TABLE IF EXISTS remote_obs", admin=True)
    except QueryError:
        pass


def test_remote_table_count(client, remote_table):
    assert client.count(f"SELECT * FROM {remote_table}") == SEED_ROWS


def test_remote_filter_pushdown(client, remote_table):
    n = int(client.scalar(f"SELECT count(*) AS n FROM {remote_table} WHERE id > 1"))
    assert n == ROWS_ID_GT_1


def test_remote_table_listed(client, remote_table):
    assert remote_table in client.tables().json()


def test_remote_join_with_local_parquet(client, remote_table, sample_data):
    """Combine the federated remote table with a local file source in one query."""
    total = client.scalar(
        f"SELECT (SELECT count(*) FROM {remote_table}) "
        "+ (SELECT count(*) FROM read_parquet(['obs/*.parquet'])) AS total"
    )
    assert int(float(total)) == SEED_ROWS + sample_data["total"]
