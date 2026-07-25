"""Registering in-memory Python data as queryable tables.

Registration copies a frame into a **session-only** table — queryable by bare name, but never
written into `beacon.db`. The tests cover the accepted input shapes, replace/unregister, and
the not-persisted property (the reason this uses a temporary table rather than the managed
create path).
"""

from __future__ import annotations

import pytest

import beacondb

pa = pytest.importorskip("pyarrow")


@pytest.fixture
def con():
    with beacondb.connect(":memory:") as connection:
        yield connection


def test_register_pyarrow_table(con):
    con.register("t", pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]}))
    assert con.sql("SELECT sum(a) AS s, count(*) AS n FROM t").fetchall() == [(6, 3)]
    assert con.table("t").columns == ["a", "b"]


def test_register_pandas_dataframe(con):
    pd = pytest.importorskip("pandas")
    con.register("df", pd.DataFrame({"x": [10, 20, 30], "g": ["a", "b", "a"]}))
    rows = con.sql("SELECT g, sum(x) AS s FROM df GROUP BY g ORDER BY g").fetchall()
    assert rows == [("a", 40), ("b", 20)]


def test_register_polars_dataframe(con):
    pl = pytest.importorskip("polars")
    con.register("pf", pl.DataFrame({"c": [7, 8, 9]}))
    assert con.sql("SELECT sum(c) AS s FROM pf").fetchall() == [(24,)]


def test_register_a_beacondb_relation(con):
    # A relation exposes __arrow_c_stream__, so it registers like any Arrow object — its result
    # is materialized into the new table.
    rel = con.sql("SELECT 99 AS z UNION ALL SELECT 100")
    con.register("snap", rel)
    assert con.sql("SELECT * FROM snap ORDER BY z").fetchall() == [(99,), (100,)]


def test_register_returns_the_connection_for_chaining(con):
    pd = pytest.importorskip("pandas")
    rows = con.register("t2", pd.DataFrame({"v": [1, 2]})).sql("SELECT count(*) FROM t2").fetchall()
    assert rows == [(2,)]


def test_register_replaces_an_existing_name(con):
    con.register("t", pa.table({"a": [1, 2, 3]}))
    con.register("t", pa.table({"a": [100]}))
    assert con.sql("SELECT * FROM t").fetchall() == [(100,)]


def test_unregister_removes_the_table(con):
    con.register("t", pa.table({"a": [1]}))
    con.unregister("t")
    with pytest.raises(beacondb.ProgrammingError):
        con.sql("SELECT * FROM t").fetchall()


def test_register_rejects_non_tabular_input(con):
    with pytest.raises(beacondb.DataError, match="could not read"):
        con.register("bad", 12345)


def test_a_registered_table_is_not_persisted(tmp_path):
    path = str(tmp_path / "beacon.db")

    con = beacondb.connect(path)
    con.register("ephemeral", pa.table({"a": [1, 2, 3]}))
    assert con.sql("SELECT count(*) FROM ephemeral").fetchall() == [(3,)]
    con.close()  # last handle -> the database (and its lock) is released

    # Reopen: a session-only table must not have been written into the file.
    reopened = beacondb.connect(path)
    with pytest.raises(beacondb.ProgrammingError):
        reopened.sql("SELECT * FROM ephemeral").fetchall()
    reopened.close()


def test_persist_true_writes_a_table_that_survives_reopen(tmp_path):
    path = str(tmp_path / "beacon.db")

    con = beacondb.connect(path)
    con.register("kept", pa.table({"x": [10, 20, 30]}), persist=True)
    assert con.sql("SELECT sum(x) FROM kept").fetchall() == [(60,)]
    con.close()

    # Reopen: a persisted table is written into beacon.db, so it is still there.
    reopened = beacondb.connect(path)
    assert reopened.sql("SELECT sum(x) FROM kept").fetchall() == [(60,)]
    reopened.close()


def test_persist_and_session_registrations_coexist(tmp_path):
    path = str(tmp_path / "beacon.db")
    con = beacondb.connect(path)
    con.register("mem", pa.table({"a": [1]}))
    con.register("disk", pa.table({"a": [2]}), persist=True)
    assert con.sql("SELECT * FROM mem").fetchall() == [(1,)]
    assert con.sql("SELECT * FROM disk").fetchall() == [(2,)]
    con.close()

    reopened = beacondb.connect(path)
    assert reopened.sql("SELECT * FROM disk").fetchall() == [(2,)]  # survives
    with pytest.raises(beacondb.ProgrammingError):
        reopened.sql("SELECT * FROM mem").fetchall()  # gone
    reopened.close()


def test_persist_over_an_existing_name_is_refused(con):
    con.register("t", pa.table({"a": [1]}), persist=True)
    # Refuse rather than silently overwrite persisted data — drop it first to replace.
    with pytest.raises(beacondb.Error):
        con.register("t", pa.table({"a": [2]}), persist=True)
