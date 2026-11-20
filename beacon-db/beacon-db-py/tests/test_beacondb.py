"""End-to-end tests for the beacondb bindings.

These run against the real engine — no mocks — because the things worth pinning here are
exactly the ones a mock would paper over: that the container file persists, and that engine
errors land on the right PEP 249 class.
"""

from __future__ import annotations

import datetime
import decimal

import pytest

import beacondb


@pytest.fixture
def mem():
    """An in-memory database."""
    with beacondb.connect(":memory:") as con:
        yield con


# ----------------------------------------------------------------------------------------
# Module surface
# ----------------------------------------------------------------------------------------


def test_dbapi_module_attributes():
    assert beacondb.apilevel == "2.0"
    assert beacondb.threadsafety == 2
    assert beacondb.paramstyle == "qmark"
    assert beacondb.__version__


def test_exception_hierarchy_matches_pep_249():
    assert issubclass(beacondb.InterfaceError, beacondb.Error)
    assert issubclass(beacondb.DatabaseError, beacondb.Error)
    for cls in (
        beacondb.DataError,
        beacondb.OperationalError,
        beacondb.IntegrityError,
        beacondb.InternalError,
        beacondb.ProgrammingError,
        beacondb.NotSupportedError,
    ):
        assert issubclass(cls, beacondb.DatabaseError)
    # A refused operation is catchable as a plain ProgrammingError.
    assert issubclass(beacondb.NotPermittedError, beacondb.ProgrammingError)


def test_module_level_helpers_use_a_default_connection():
    assert beacondb.sql("SELECT 1 AS a").fetchall() == [(1,)]
    assert beacondb.execute("SELECT 2 AS a").fetchall() == [(2,)]


# ----------------------------------------------------------------------------------------
# Querying
# ----------------------------------------------------------------------------------------


def test_select_returns_rows_and_metadata(mem):
    result = mem.sql("SELECT 1 AS a, 'hello' AS b, 3.5 AS c")
    assert result.columns == ["a", "b", "c"]
    assert result.types == ["Int64", "Utf8", "Float64"]
    assert result.fetchall() == [(1, "hello", 3.5)]


def test_execute_chains_and_reports_description(mem):
    rows = mem.execute("SELECT * FROM (VALUES (1,'x'),(2,'y')) AS t(i, s)").fetchall()
    assert rows == [(1, "x"), (2, "y")]
    assert [column[0] for column in mem.description] == ["i", "s"]
    assert mem.rowcount == 2


def test_rowcount_is_minus_one_before_any_statement():
    with beacondb.connect(":memory:") as con:
        assert con.rowcount == -1
        assert con.description is None


def test_fetch_methods_share_one_cursor_position(mem):
    mem.execute("SELECT * FROM (VALUES (1),(2),(3),(4)) AS t(i)")
    assert mem.fetchone() == (1,)
    assert mem.fetchmany(2) == [(2,), (3,)]
    assert mem.fetchall() == [(4,)]
    assert mem.fetchone() is None, "the cursor is exhausted"


def test_cursor_gets_its_own_result_slot(mem):
    mem.execute("SELECT 1 AS a")
    cursor = mem.cursor()
    cursor.execute("SELECT 2 AS a")
    assert cursor.fetchall() == [(2,)]
    assert mem.fetchall() == [(1,)], "the parent's result is untouched"


def test_value_conversion_covers_the_common_types(mem):
    row = mem.sql(
        """
        SELECT DATE '2026-07-24'                  AS d,
               TIMESTAMP '2026-07-24 12:34:56'    AS ts,
               CAST(NULL AS INT)                  AS n,
               CAST('12.34' AS DECIMAL(10,2))     AS dec,
               true                               AS flag
        """
    ).fetchall()[0]
    assert row[0] == datetime.date(2026, 7, 24)
    assert row[1] == datetime.datetime(2026, 7, 24, 12, 34, 56)
    assert row[2] is None
    assert row[3] == decimal.Decimal("12.34")
    assert row[4] is True


# ----------------------------------------------------------------------------------------
# Arrow interop
# ----------------------------------------------------------------------------------------


def test_arrow_c_stream_is_consumable_without_our_help(mem):
    pa = pytest.importorskip("pyarrow")
    # The consumer drives the PyCapsule protocol itself; beacondb never imports pyarrow here.
    table = pa.table(mem.sql("SELECT 1 AS x, 'y' AS z"))
    assert table.num_rows == 1
    assert table.schema.names == ["x", "z"]


def test_arrow_and_df_helpers(mem):
    pytest.importorskip("pyarrow")
    assert mem.sql("SELECT 42 AS answer").arrow().to_pydict() == {"answer": [42]}
    pytest.importorskip("pandas")
    frame = mem.sql("SELECT 1 AS a, 'z' AS b").df()
    assert list(frame.columns) == ["a", "b"]
    assert len(frame) == 1


# ----------------------------------------------------------------------------------------
# The container file
# ----------------------------------------------------------------------------------------


def test_file_database_persists_across_reopen(tmp_path):
    path = str(tmp_path / "beacon.db")

    con = beacondb.connect(path)
    con.execute("CREATE TABLE kept AS SELECT 1 AS id, 'persisted' AS label")
    assert con.sql("SELECT * FROM kept").fetchall() == [(1, "persisted")]
    con.close()

    reopened = beacondb.connect(path)
    assert reopened.sql("SELECT * FROM kept").fetchall() == [(1, "persisted")]
    reopened.close()


def test_second_connect_in_process_shares_the_locked_handle(tmp_path):
    path = str(tmp_path / "beacon.db")
    first = beacondb.connect(path)
    first.execute("CREATE TABLE t AS SELECT 1 AS a")

    # The file is exclusively locked, so this must attach to the open database rather than
    # fail — otherwise a notebook that calls connect() twice would be stuck.
    second = beacondb.connect(path)
    assert second.sql("SELECT * FROM t").fetchall() == [(1,)]

    first.close()
    second.close()


# ----------------------------------------------------------------------------------------
# No users
# ----------------------------------------------------------------------------------------


@pytest.mark.parametrize("keyword", ["auth", "username", "password", "token"])
def test_connect_takes_no_user_arguments(keyword):
    # The embedded database has no users, so connect() has no user arguments.
    with pytest.raises(TypeError):
        beacondb.connect(":memory:", **{keyword: "x"})


# ----------------------------------------------------------------------------------------
# Error mapping and honest refusals
# ----------------------------------------------------------------------------------------


def test_unknown_table_is_a_programming_error(mem):
    with pytest.raises(beacondb.ProgrammingError):
        mem.sql("SELECT * FROM does_not_exist").fetchall()


def test_read_only_opens_and_refuses_writes(mem):
    # read_only=True now opens a genuine read-only handle (writes refused), not a NotSupportedError.
    ro = beacondb.connect(":memory:", read_only=True)
    assert ro.sql("SELECT 1 AS a").fetchall() == [(1,)]
    with pytest.raises(beacondb.Error, match="read-only"):
        ro.execute("CREATE TABLE t (a INT)")
    ro.close()


def test_closed_connection_raises_interface_error():
    con = beacondb.connect(":memory:")
    con.close()
    with pytest.raises(beacondb.InterfaceError):
        con.sql("SELECT 1")


def test_context_manager_closes_and_propagates():
    with beacondb.connect(":memory:") as con:
        assert con.sql("SELECT 1").fetchall() == [(1,)]
    with pytest.raises(beacondb.InterfaceError):
        con.sql("SELECT 1")

    with pytest.raises(ValueError):
        with beacondb.connect(":memory:") as con:
            raise ValueError("body errors must not be swallowed")
