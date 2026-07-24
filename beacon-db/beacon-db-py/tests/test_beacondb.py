"""End-to-end tests for the beacondb bindings.

These run against the real engine — no mocks — because the things worth pinning here are
exactly the ones a mock would paper over: that auth modes resolve to the right identity,
that the container file persists, and that engine errors land on the right PEP 249 class.
"""

from __future__ import annotations

import datetime
import decimal

import pytest

import beacondb


@pytest.fixture
def mem():
    """An in-memory database with auth off — the default mode."""
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
    # An authorization denial is catchable as a plain ProgrammingError.
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
# Auth modes
# ----------------------------------------------------------------------------------------


def test_auth_disabled_is_the_default_and_permits_everything(tmp_path):
    con = beacondb.connect(str(tmp_path / "beacon.db"))
    assert con.whoami() == {
        "username": "local",
        "roles": [],
        "is_super_user": True,
        "auth": False,
    }
    assert con.auth_enabled is False

    con.execute("CREATE TABLE t AS SELECT 1 AS a")
    con.execute("CREATE USER analyst WITH PASSWORD 'sekrit'")
    # The auth directory is super-user-only in every mode, so reading it proves the identity
    # really is privileged rather than merely unenforced.
    assert con.sql("SELECT * FROM beacon.system.users").fetchall()
    con.close()


def test_credentials_with_auth_disabled_are_refused_not_ignored(tmp_path):
    with pytest.raises(beacondb.ProgrammingError, match="auth is disabled"):
        beacondb.connect(str(tmp_path / "beacon.db"), username="a", password="b")


def test_auth_enabled_defaults_to_anonymous_and_blocks_privileged_work(tmp_path):
    path = str(tmp_path / "beacon.db")
    # Seed a user while auth is off, then reopen with auth on.
    setup = beacondb.connect(path)
    setup.execute("CREATE TABLE t AS SELECT 1 AS a")
    setup.execute("CREATE ROLE readers")
    setup.execute("CREATE USER analyst WITH PASSWORD 'sekrit'")
    setup.execute("GRANT SELECT ON TABLE t TO ROLE readers")
    setup.execute("GRANT ROLE readers TO USER analyst")
    setup.close()

    anon = beacondb.connect(
        path, auth=True, admin_username="admin", admin_password="hunter2"
    )
    assert anon.whoami()["username"] == "anonymous"
    assert anon.whoami()["is_super_user"] is False
    assert anon.sql("SELECT 1 AS a").fetchall() == [(1,)]

    with pytest.raises(beacondb.NotPermittedError):
        anon.execute("CREATE TABLE u (a INT)")
    with pytest.raises(beacondb.NotPermittedError):
        anon.sql("SELECT * FROM beacon.system.users").fetchall()

    analyst = anon.connect_as(username="analyst", password="sekrit")
    assert analyst.whoami()["roles"] == ["readers"]
    assert analyst.sql("SELECT * FROM t").fetchall() == [(1,)]

    admin = anon.connect_as(username="admin", password="hunter2")
    assert admin.whoami()["is_super_user"] is True
    admin.execute("CREATE TABLE u AS SELECT 2 AS b")
    assert admin.sql("SELECT * FROM u").fetchall() == [(2,)]

    with pytest.raises(beacondb.OperationalError):
        anon.connect_as(username="analyst", password="wrong")

    for con in (anon, analyst, admin):
        con.close()


def test_mode_cannot_change_while_a_database_is_open(tmp_path):
    path = str(tmp_path / "beacon.db")
    con = beacondb.connect(path)
    with pytest.raises(beacondb.ProgrammingError, match="already open"):
        beacondb.connect(path, auth=True)
    con.close()


def test_connect_as_is_meaningless_without_auth(mem):
    with pytest.raises(beacondb.ProgrammingError, match="auth disabled"):
        mem.connect_as(username="a", password="b")
    with pytest.raises(beacondb.ProgrammingError, match="auth disabled"):
        mem.as_anonymous()


# ----------------------------------------------------------------------------------------
# Error mapping and honest refusals
# ----------------------------------------------------------------------------------------


def test_unknown_table_is_a_programming_error(mem):
    with pytest.raises(beacondb.ProgrammingError):
        mem.sql("SELECT * FROM does_not_exist").fetchall()


def test_unsupported_features_refuse_rather_than_pretend(mem):
    with pytest.raises(beacondb.NotSupportedError):
        beacondb.connect(":memory:", read_only=True)


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
