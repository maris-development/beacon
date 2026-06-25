"""Scalar indexes on Lance-backed managed tables.

Indexes are transparent optimizations: ``CREATE INDEX`` / ``SHOW INDEXES`` /
``DROP INDEX`` must not change query results. Lance is the only engine that
supports them (external/Iceberg tables are rejected).
"""

from __future__ import annotations

import pytest

from beacon_client import QueryError

TABLE = "idx_tbl"


def _drop(client, name):
    try:
        client.execute(f"DROP TABLE IF EXISTS {name}", admin=True)
    except QueryError:
        pass


@pytest.fixture
def indexed_table(client):
    _drop(client, TABLE)
    client.execute(f"CREATE TABLE {TABLE} (id BIGINT, status VARCHAR, note VARCHAR)", admin=True)
    client.execute(
        f"INSERT INTO {TABLE} VALUES "
        "(1, 'open', 'alpha'), (2, 'closed', 'beta'), (3, 'open', 'gamma')",
        admin=True,
    )
    yield TABLE
    _drop(client, TABLE)


def _index_names(client, table):
    # SHOW INDEXES requires super-user (admin) in the current build.
    rows = client.sql_rows(f"SHOW INDEXES ON {table}", admin=True)
    return {r[0] for r in rows[1:]}


def test_create_show_drop_btree_index(client, indexed_table):
    before = client.sql_rows(f"SELECT id, status FROM {indexed_table} ORDER BY id")

    client.execute(
        f"CREATE INDEX {indexed_table}_id_idx ON {indexed_table} (id) USING btree",
        admin=True,
    )
    assert f"{indexed_table}_id_idx" in _index_names(client, indexed_table)

    # Results are identical with the index present (transparent optimization).
    after = client.sql_rows(f"SELECT id, status FROM {indexed_table} ORDER BY id")
    assert before == after
    assert client.count(f"SELECT * FROM {indexed_table} WHERE id = 2") == 1

    client.execute(f"DROP INDEX {indexed_table}_id_idx ON {indexed_table}", admin=True)
    assert f"{indexed_table}_id_idx" not in _index_names(client, indexed_table)


def test_bitmap_index(client, indexed_table):
    client.execute(
        f"CREATE INDEX {indexed_table}_status_idx ON {indexed_table} (status) USING bitmap",
        admin=True,
    )
    assert f"{indexed_table}_status_idx" in _index_names(client, indexed_table)
    # The bitmap-indexed column still filters correctly.
    assert client.count(f"SELECT * FROM {indexed_table} WHERE status = 'open'") == 2


def test_inverted_index(client, indexed_table):
    client.execute(
        f"CREATE INDEX {indexed_table}_note_idx ON {indexed_table} (note) USING inverted",
        admin=True,
    )
    assert f"{indexed_table}_note_idx" in _index_names(client, indexed_table)


def test_create_index_requires_admin(client, indexed_table):
    status = client.status(
        f"CREATE INDEX {indexed_table}_anon ON {indexed_table} (id)", admin=False
    )
    assert status == 400


def test_index_on_external_table_is_rejected(client):
    """Only Lance tables support indexes; an external table must be refused."""
    name = "idx_ext"
    try:
        client.execute("DROP TABLE IF EXISTS idx_ext", admin=True)
    except QueryError:
        pass
    client.execute(f"CREATE EXTERNAL TABLE {name} STORED AS PARQUET LOCATION 'obs/'", admin=True)
    try:
        assert client.status(f"CREATE INDEX {name}_idx ON {name} (temperature)", admin=True) == 400
    finally:
        _drop(client, name)
