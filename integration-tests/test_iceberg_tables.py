"""Managed Apache Iceberg tables — the alternative managed engine.

The HTTP transport is stateless, so ``SET beacon.table_engine`` would not persist
across requests; instead this suite runs a *dedicated* Beacon container started
with ``BEACON_DEFAULT_TABLE_ENGINE=iceberg`` so every ``CREATE TABLE`` is
Iceberg-backed. It mirrors the Lance lifecycle (create / insert / update / delete
/ alter / CTAS) and asserts engine identity via the table-config typetag, which
diverges from Lance (Iceberg uses copy-on-write schema evolution).
"""

from __future__ import annotations

import json
import shutil

import pytest

from beacon_client import BeaconHTTPClient, QueryError
from conftest import ADMIN_PASSWORD, ADMIN_USERNAME, run_beacon_container


@pytest.fixture(scope="module")
def iceberg_client(request, beacon_image, docker_network, datasets_dir, tmp_path_factory):
    # Isolated data dirs: Iceberg writes its warehouse under the datasets store,
    # so give this container its own copy of the obs data.
    ds = tmp_path_factory.mktemp("iceberg-datasets")
    shutil.copytree(datasets_dir / "obs", ds / "obs")
    tb = tmp_path_factory.mktemp("iceberg-tables")
    base_url = run_beacon_container(
        request,
        name="beacon-it-iceberg",
        image=beacon_image,
        network=docker_network,
        datasets_dir=ds,
        tables_dir=tb,
        extra_env={"BEACON_DEFAULT_TABLE_ENGINE": "iceberg"},
    )
    return BeaconHTTPClient(base_url, ADMIN_USERNAME, ADMIN_PASSWORD)


def _drop(c, name):
    try:
        c.execute(f"DROP TABLE IF EXISTS {name}", admin=True)
    except QueryError:
        pass


def _table_type_json(c, name) -> str:
    resp = c.admin_get(f"/api/admin/table-config?table_name={name}")
    assert resp.status_code == 200, resp.text
    return json.dumps(resp.json())


def _columns(c, name) -> set[str]:
    resp = c.get(f"/api/table-schema?table_name={name}")
    assert resp.status_code == 200, resp.text
    return {f["name"] for f in resp.json()["fields"]}


@pytest.fixture
def ice_table(iceberg_client):
    name = "ice_tbl"
    _drop(iceberg_client, name)
    yield name
    _drop(iceberg_client, name)


def test_default_engine_is_iceberg(iceberg_client, ice_table):
    iceberg_client.execute(f"CREATE TABLE {ice_table} (id BIGINT, name VARCHAR)", admin=True)
    assert '"iceberg"' in _table_type_json(iceberg_client, ice_table)


def test_iceberg_insert_update_delete(iceberg_client, ice_table):
    c = iceberg_client
    c.execute(f"CREATE TABLE {ice_table} (id BIGINT, name VARCHAR)", admin=True)
    c.execute(f"INSERT INTO {ice_table} VALUES (1, 'a'), (2, 'b'), (3, 'c')", admin=True)
    assert c.count(f"SELECT * FROM {ice_table}") == 3

    c.execute(f"UPDATE {ice_table} SET name = 'Z' WHERE id = 2", admin=True)
    assert c.scalar(f"SELECT name FROM {ice_table} WHERE id = 2") == "Z"
    assert c.scalar(f"SELECT name FROM {ice_table} WHERE id = 1") == "a"

    c.execute(f"DELETE FROM {ice_table} WHERE id = 1", admin=True)
    assert c.count(f"SELECT * FROM {ice_table}") == 2


def test_iceberg_alter_add_drop_column(iceberg_client, ice_table):
    c = iceberg_client
    c.execute(f"CREATE TABLE {ice_table} (id BIGINT)", admin=True)
    c.execute(f"INSERT INTO {ice_table} VALUES (1), (2)", admin=True)

    c.execute(f"ALTER TABLE {ice_table} ADD COLUMN score DOUBLE", admin=True)
    assert "score" in _columns(c, ice_table)
    assert c.count(f"SELECT * FROM {ice_table} WHERE score IS NULL") == 2

    c.execute(f"ALTER TABLE {ice_table} DROP COLUMN score", admin=True)
    assert "score" not in _columns(c, ice_table)


def test_iceberg_ctas(iceberg_client):
    c = iceberg_client
    name = "ice_ctas"
    _drop(c, name)
    try:
        c.execute(
            f"CREATE TABLE {name} AS "
            "SELECT platform, temperature FROM read_parquet(['obs/*.parquet']) "
            "WHERE temperature > 20",
            admin=True,
        )
        assert c.count(f"SELECT * FROM {name}") > 0
        assert '"iceberg"' in _table_type_json(c, name)
    finally:
        _drop(c, name)


def test_iceberg_write_requires_admin(iceberg_client, ice_table):
    iceberg_client.execute(f"CREATE TABLE {ice_table} (id BIGINT)", admin=True)
    assert iceberg_client.status(f"INSERT INTO {ice_table} VALUES (9)", admin=False) == 400
