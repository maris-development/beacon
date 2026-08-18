"""Icechunk, end to end, in one file.

Writes its own Icechunk repository with `icechunk` and `zarr` — one commit, two commits, and a
branch — opens an embedded Beacon over it, queries each version, and creates an external table.

    pytest formats/test_icechunk.py -v

`read_icechunk(location [, branch [, snapshot [, dimensions]]])`. A bare SQL `NULL` is accepted
for the branch, so reading a snapshot needs no cast — unlike `read_csv`'s delimiter, which does.
"""

from __future__ import annotations

from pathlib import Path

import pytest

beacondb = pytest.importorskip("beacondb", reason="build it with maturin")
icechunk = pytest.importorskip("icechunk", reason="pip install icechunk")
zarr = pytest.importorskip("zarr", reason="pip install zarr")
np = pytest.importorskip("numpy")

ROWS = 6
#: The first commit writes 1..6, the second overwrites with 101..106.
FIRST_VALUES = [float(i) + 1.0 for i in range(ROWS)]
SECOND_VALUES = [float(i) + 101.0 for i in range(ROWS)]
#: A branch is created from the first commit and then gets its own values.
BRANCH_VALUES = [float(i) + 201.0 for i in range(ROWS)]


@pytest.fixture(scope="module")
def written(tmp_path_factory):
    """Write the repository, and hand back its path plus the snapshot ids."""
    root = tmp_path_factory.mktemp("icechunk")
    repo = icechunk.Repository.create(icechunk.local_filesystem_storage(str(root / "repo")))

    # The first commit.
    session = repo.writable_session("main")
    group = zarr.open_group(store=session.store, mode="w", zarr_format=3)
    array = group.create_array(
        "temperature", shape=(ROWS,), chunks=(3,), dtype="float64", dimension_names=["obs"]
    )
    array[:] = np.array(FIRST_VALUES)
    first = session.commit("first")

    # A branch from that commit, with its own values, so the two do not agree by accident.
    repo.create_branch("dev", snapshot_id=first)
    session = repo.writable_session("dev")
    zarr.open_group(store=session.store, mode="a")["temperature"][:] = np.array(BRANCH_VALUES)
    session.commit("on the branch")

    # The second commit on main.
    session = repo.writable_session("main")
    zarr.open_group(store=session.store, mode="a")["temperature"][:] = np.array(SECOND_VALUES)
    second = session.commit("second")

    return {"root": root, "first": first, "second": second}


@pytest.fixture
def datasets(written) -> Path:
    return written["root"]


@pytest.fixture
def con(datasets, tmp_path):
    with beacondb.connect(str(tmp_path / "beacon.db"), datasets=str(datasets)) as connection:
        yield connection


# --- reading ------------------------------------------------------------------


def test_the_repository_reads(con):
    rows = con.sql(
        "SELECT temperature FROM read_icechunk('repo') ORDER BY temperature"
    ).fetchall()
    assert [r[0] for r in rows] == SECOND_VALUES, "a bare location reads the tip of main"


def test_the_schema_is_reported(con):
    relation = con.sql("SELECT * FROM read_icechunk('repo')")
    assert relation.columns == ["temperature"]
    assert relation.types == ["Float64"]


def test_count_star(con):
    assert con.sql("SELECT count(*) AS n FROM read_icechunk('repo')").fetchall() == [(ROWS,)]


def test_a_filter_and_an_aggregate(con):
    got = con.sql(
        "SELECT count(*) n, min(temperature) lo, max(temperature) hi FROM read_icechunk('repo') "
        "WHERE temperature >= 103.0"
    ).fetchall()[0]
    assert got == (4, 103.0, max(SECOND_VALUES))


def test_an_older_commit_can_be_read(con, written):
    """The third argument is a snapshot id, so a query can travel back.

    The second argument is the branch, and a bare `NULL` is refused there, so it is cast.
    """
    rows = con.sql(
        "SELECT temperature FROM read_icechunk('repo', CAST(NULL AS VARCHAR), "
        f"'{written['first']}') ORDER BY temperature"
    ).fetchall()
    assert [r[0] for r in rows] == FIRST_VALUES


def test_the_two_commits_differ(con, written):
    """A commit selects a version of the data, so the two must not agree."""
    query = (
        "SELECT min(temperature) lo FROM read_icechunk('repo', CAST(NULL AS VARCHAR), '{}')"
    )
    first = con.sql(query.format(written["first"])).fetchall()
    second = con.sql(query.format(written["second"])).fetchall()
    assert first == [(min(FIRST_VALUES),)]
    assert second == [(min(SECOND_VALUES),)]
    assert first != second


def test_a_branch_can_be_read(con):
    """The second argument is a branch name, and it has its own values."""
    rows = con.sql(
        "SELECT temperature FROM read_icechunk('repo', 'dev') ORDER BY temperature"
    ).fetchall()
    assert [r[0] for r in rows] == BRANCH_VALUES


def test_a_branch_and_main_are_independent(con):
    """The branch was created from the first commit and then moved on its own."""
    main = con.sql("SELECT min(temperature) lo FROM read_icechunk('repo')").fetchall()
    dev = con.sql("SELECT min(temperature) lo FROM read_icechunk('repo', 'dev')").fetchall()
    assert main == [(min(SECOND_VALUES),)]
    assert dev == [(min(BRANCH_VALUES),)]


def test_an_unknown_branch_is_refused(con):
    with pytest.raises(Exception) as refusal:
        con.sql("SELECT * FROM read_icechunk('repo', 'no-such-branch')").fetchall()
    assert "branch" in str(refusal.value).lower() or "no-such-branch" in str(refusal.value)


def test_a_bare_null_branch_is_accepted(con, written):
    """`read_icechunk('repo', NULL, '<snapshot>')` needs no cast.

    Worth pinning because `read_csv`'s delimiter argument *does* need one: a bare SQL `NULL` is
    untyped and that reader matches only a typed null. These two readers differ, so a caller who
    learns the cast from one should not have to apply it here.
    """
    rows = con.sql(
        f"SELECT min(temperature) lo FROM read_icechunk('repo', NULL, '{written['first']}')"
    ).fetchall()
    assert rows == [(min(FIRST_VALUES),)]


# --- external tables and a restart --------------------------------------------


def test_an_external_table_reads(con):
    con.execute("CREATE EXTERNAL TABLE repo_tbl STORED AS ICECHUNK LOCATION 'repo'")
    assert con.sql("SELECT count(*) AS n FROM repo_tbl").fetchall() == [(ROWS,)]
    assert "repo_tbl" in con.list_tables()


def test_an_external_table_survives_a_restart(datasets, tmp_path):
    """The table points at the repository, so it reads the tip of main after a restart."""
    path = str(tmp_path / "restart.db")

    with beacondb.connect(path, datasets=str(datasets)) as con:
        con.execute("CREATE EXTERNAL TABLE repo_tbl STORED AS ICECHUNK LOCATION 'repo'")
        before = con.sql("SELECT count(*) AS n FROM repo_tbl").fetchall()

    with beacondb.connect(path, datasets=str(datasets)) as con:
        assert con.sql("SELECT count(*) AS n FROM repo_tbl").fetchall() == before
        assert "repo_tbl" in con.list_tables()
        rows = con.sql("SELECT temperature FROM repo_tbl ORDER BY temperature").fetchall()
        assert [r[0] for r in rows] == SECOND_VALUES, "the table reads the tip of main"


def test_an_external_table_sees_a_later_commit(written, tmp_path):
    """A commit made after the definition has to show up: the table is not a snapshot."""
    path = str(tmp_path / "commit.db")
    with beacondb.connect(path, datasets=str(written["root"])) as con:
        con.execute("CREATE EXTERNAL TABLE repo_tbl STORED AS ICECHUNK LOCATION 'repo'")
        assert con.sql("SELECT min(temperature) AS lo FROM repo_tbl").fetchall() == [
            (min(SECOND_VALUES),)
        ]

    repo = icechunk.Repository.open(
        icechunk.local_filesystem_storage(str(written["root"] / "repo"))
    )
    session = repo.writable_session("main")
    zarr.open_group(store=session.store, mode="a")["temperature"][:] = np.full(ROWS, 500.0)
    session.commit("third")

    with beacondb.connect(path, datasets=str(written["root"])) as con:
        assert con.sql("SELECT min(temperature) AS lo FROM repo_tbl").fetchall() == [(500.0,)], (
            "the table must re-open the repository rather than hold the commit it was created at"
        )
