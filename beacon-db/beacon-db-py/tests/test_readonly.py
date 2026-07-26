"""Read-only connections, and the privilege gate on write-convenience methods.

`connect(read_only=True)` opens a database that refuses every write — DDL/DML and beacon's
side-effecting statements (`ATTACH`, `CREATE SECRET`, …) — while reads (`SELECT`, `SHOW …`) work.
The file is still opened with an exclusive lock (a per-connection writability guarantee, not yet
multi-process concurrency). Separately, the `attach`/`detach` convenience methods bypass the engine
query path, so they carry the same read-only / super-user gate the SQL path applies.
"""

from __future__ import annotations

import pytest

import beacondb

pa = pytest.importorskip("pyarrow")


@pytest.fixture
def db(tmp_path):
    """A small file-backed database with one managed table, then closed."""
    path = str(tmp_path / "beacon.db")
    con = beacondb.connect(path)
    con.execute("CREATE TABLE t (a BIGINT)")
    con.execute("INSERT INTO t VALUES (1), (2)")
    con.close()
    return path


def test_read_only_allows_reads(db):
    ro = beacondb.connect(db, read_only=True)
    assert ro.sql("SELECT count(*) FROM t").fetchall() == [(2,)]
    assert isinstance(ro.sql("SHOW SECRETS").fetchall(), list)  # SHOW is a read
    ro.close()


@pytest.mark.parametrize(
    "write",
    [
        "INSERT INTO t VALUES (3)",
        "CREATE TABLE u (x INT)",
        "DROP TABLE t",
        "UPDATE t SET a = 9",
        "CREATE SECRET s (TYPE S3, KEY_ID 'x')",
    ],
)
def test_read_only_refuses_writes(db, write):
    ro = beacondb.connect(db, read_only=True)
    with pytest.raises(beacondb.Error, match="read-only"):
        ro.execute(write)
    # nothing changed
    assert ro.sql("SELECT count(*) FROM t").fetchall() == [(2,)]
    ro.close()


def test_read_only_refuses_append_and_attach(db):
    ro = beacondb.connect(db, read_only=True)
    with pytest.raises(beacondb.Error):
        ro.append("t", pa.table({"a": [3]}))
    # attach bypasses the query path but is gated too — refused before any network call
    with pytest.raises(beacondb.Error, match="read-only"):
        ro.attach("r", "beacon://127.0.0.1:1")
    ro.close()


# ----------------------------------------------------------------------------------------
# attach/detach honor the auth super-user gate (they bypass the SQL validation path)
# ----------------------------------------------------------------------------------------


def test_anonymous_session_cannot_attach_or_detach():
    con = beacondb.connect(
        ":memory:", auth=True, admin_username="admin", admin_password="pw"
    )
    assert con.whoami()["is_super_user"] is False
    with pytest.raises(beacondb.NotPermittedError):
        con.attach("r", "beacon://127.0.0.1:1")
    with pytest.raises(beacondb.NotPermittedError):
        con.detach("r")


def test_auth_off_allows_attach_path():
    # With auth off the local session is a super-user, so the gate passes (the attach then fails
    # only because the endpoint is unreachable — a DatabaseError, not a permission error).
    con = beacondb.connect(":memory:")
    with pytest.raises(beacondb.Error) as excinfo:
        con.attach("r", "beacon://127.0.0.1:1")
    assert not isinstance(excinfo.value, beacondb.NotPermittedError)
