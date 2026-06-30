"""Lance-backed managed tables — the default ``CREATE TABLE`` engine.

``test_managed_tables.py`` covers the basic CRUD shape; this module drills into the
Lance engine specifically: that a plain ``CREATE TABLE`` is Lance-backed, native
schema evolution (``ALTER TABLE`` add / rename / drop column, which Lance applies
atomically without a rebuild), ``CREATE TABLE AS SELECT``, ``INSERT ... SELECT``,
and the row-mutation semantics of ``UPDATE``/``DELETE``.

All writes require admin basic-auth; reads do not. Table names deliberately avoid
the substring "lance" so the engine-identity assertion (which greps the
table-config JSON for ``"lance"``) is meaningful.
"""

from __future__ import annotations

import json

import pytest

from beacon_client import QueryError


def _drop(client, name):
    try:
        client.execute(f"DROP TABLE IF EXISTS {name}", admin=True)
    except QueryError:
        pass


def _table_config(client, name) -> dict:
    resp = client.admin_get(f"/api/admin/table-config?table_name={name}")
    assert resp.status_code == 200, resp.text
    return resp.json()


def _schema_columns(client, name) -> set[str]:
    resp = client.get(f"/api/table-schema?table_name={name}")
    assert resp.status_code == 200, resp.text
    return {f["name"] for f in resp.json()["fields"]}


@pytest.fixture
def table(client):
    """A throwaway table name, dropped before and after each test."""
    name = "mt_lifecycle"
    _drop(client, name)
    yield name
    _drop(client, name)


def test_plain_create_table_is_lance_backed(client, table):
    client.execute(f"CREATE TABLE {table} (id BIGINT, name VARCHAR)", admin=True)
    config = _table_config(client, table)
    # The Lance table definition serializes with a "lance" typetag.
    assert '"lance"' in json.dumps(config), config


def test_update_where_leaves_other_rows(client, table):
    client.execute(f"CREATE TABLE {table} (id BIGINT, name VARCHAR)", admin=True)
    client.execute(
        f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b'), (3, 'c')", admin=True
    )

    client.execute(f"UPDATE {table} SET name = 'Z' WHERE id = 2", admin=True)
    assert client.scalar(f"SELECT name FROM {table} WHERE id = 2") == "Z"
    # The non-matching rows are untouched and the row count is unchanged.
    assert client.scalar(f"SELECT name FROM {table} WHERE id = 1") == "a"
    assert client.count(f"SELECT * FROM {table}") == 3


def test_update_all_rows(client, table):
    client.execute(f"CREATE TABLE {table} (id BIGINT, name VARCHAR)", admin=True)
    client.execute(
        f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b'), (3, 'c')", admin=True
    )
    client.execute(f"UPDATE {table} SET name = 'all'", admin=True)
    assert int(client.scalar(f"SELECT count(DISTINCT name) AS n FROM {table}")) == 1


def test_delete_where_and_delete_all(client, table):
    client.execute(f"CREATE TABLE {table} (id BIGINT, name VARCHAR)", admin=True)
    client.execute(
        f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b'), (3, 'c')", admin=True
    )
    client.execute(f"DELETE FROM {table} WHERE id = 1", admin=True)
    assert client.count(f"SELECT * FROM {table}") == 2
    assert int(client.scalar(f"SELECT min(id) AS m FROM {table}")) == 2

    client.execute(f"DELETE FROM {table}", admin=True)
    assert client.count(f"SELECT * FROM {table}") == 0


def test_alter_add_rename_drop_column_preserves_data(client, table):
    """Lance schema evolution: add a column, populate it, rename it (data must
    survive the rename), then drop it."""
    client.execute(f"CREATE TABLE {table} (id BIGINT, name VARCHAR)", admin=True)
    client.execute(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b')", admin=True)

    # ADD COLUMN: new column is all-null for existing rows.
    client.execute(f"ALTER TABLE {table} ADD COLUMN score DOUBLE", admin=True)
    assert "score" in _schema_columns(client, table)
    assert client.count(f"SELECT * FROM {table} WHERE score IS NULL") == 2

    client.execute(f"UPDATE {table} SET score = 9.5 WHERE id = 1", admin=True)

    # RENAME COLUMN: the populated value must carry over to the new name.
    client.execute(f"ALTER TABLE {table} RENAME COLUMN score TO rating", admin=True)
    cols = _schema_columns(client, table)
    assert "rating" in cols and "score" not in cols
    assert float(client.scalar(f"SELECT rating FROM {table} WHERE id = 1")) == 9.5

    # DROP COLUMN.
    client.execute(f"ALTER TABLE {table} DROP COLUMN rating", admin=True)
    assert "rating" not in _schema_columns(client, table)


def test_alter_requires_admin(client, table):
    client.execute(f"CREATE TABLE {table} (id BIGINT)", admin=True)
    status = client.status(f"ALTER TABLE {table} ADD COLUMN x INT", admin=False)
    assert status == 400


def test_ctas_creates_lance_table(client, sample_data):
    name = "mt_ctas_warm"
    _drop(client, name)
    try:
        client.execute(
            f"CREATE TABLE {name} AS "
            "SELECT platform, temperature FROM read_parquet(['obs/*.parquet']) "
            "WHERE temperature > 20",
            admin=True,
        )
        assert client.count(f"SELECT * FROM {name}") == sample_data["warm_count"]
        assert {"platform", "temperature"} <= _schema_columns(client, name)
        assert '"lance"' in json.dumps(_table_config(client, name))
    finally:
        _drop(client, name)


def test_insert_select_from_parquet(client, sample_data):
    name = "mt_load"
    _drop(client, name)
    try:
        client.execute(
            f"CREATE TABLE {name} (platform VARCHAR, temperature DOUBLE)", admin=True
        )
        client.execute(
            f"INSERT INTO {name} "
            "SELECT platform, temperature FROM read_parquet(['obs/*.parquet']) "
            "WHERE temperature > 30",
            admin=True,
        )
        expected = sum(1 for r in sample_data["rows"] if r["temperature"] > 30)
        assert expected > 0
        assert client.count(f"SELECT * FROM {name}") == expected
    finally:
        _drop(client, name)
