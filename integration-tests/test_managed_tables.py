"""Managed (local) tables: the full Iceberg-backed write lifecycle over HTTP.

CREATE -> INSERT -> SELECT -> UPDATE -> DELETE -> DROP, all requiring admin auth.
"""

from __future__ import annotations

import pytest

from beacon_client import QueryError

TABLE = "itest_measurements"


@pytest.fixture(scope="module")
def managed_table(client):
    try:
        client.execute(f"DROP TABLE IF EXISTS {TABLE}", admin=True)
    except QueryError:
        pass
    client.execute(
        f"CREATE TABLE {TABLE} (id BIGINT, name VARCHAR, value DOUBLE)", admin=True
    )
    yield TABLE
    try:
        client.execute(f"DROP TABLE IF EXISTS {TABLE}", admin=True)
    except QueryError:
        pass


def test_insert_and_select(client, managed_table):
    client.execute(
        f"INSERT INTO {managed_table} VALUES "
        "(1, 'argo', 12.5), (2, 'glider', 9.0), (3, 'argo', 7.0)",
        admin=True,
    )
    assert client.count(f"SELECT * FROM {managed_table}") == 3
    # Reads do not need credentials.
    assert client.count(f"SELECT * FROM {managed_table} WHERE name = 'argo'") == 2


def test_update(client, managed_table):
    client.execute(
        f"UPDATE {managed_table} SET value = 100.0 WHERE name = 'glider'", admin=True
    )
    val = client.scalar(f"SELECT value FROM {managed_table} WHERE name = 'glider'")
    assert float(val) == 100.0


def test_delete(client, managed_table):
    client.execute(f"DELETE FROM {managed_table} WHERE name = 'argo'", admin=True)
    assert client.count(f"SELECT * FROM {managed_table}") == 1


def test_insert_requires_admin(client, managed_table):
    """Writes without credentials stay read-only and are rejected (400)."""
    status = client.status(
        f"INSERT INTO {managed_table} VALUES (99, 'anon', 0.0)", admin=False
    )
    assert status == 400


def test_drop_table(client):
    name = "itest_drop_me"
    client.execute(f"CREATE TABLE IF NOT EXISTS {name} (id BIGINT)", admin=True)
    assert name in client.tables().json()
    client.execute(f"DROP TABLE {name}", admin=True)
    assert name not in client.tables().json()
