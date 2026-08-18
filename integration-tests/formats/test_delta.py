"""Delta Lake, end to end, in one file.

Writes its own Delta tables with `deltalake` — one version, several versions, partitioned, and
one that gains a column — opens an embedded Beacon over them, queries them, travels to an older
version, creates an external table, reopens the database, and checks the table survived.

    pytest formats/test_delta.py -v

`read_delta(location)` takes one location, not a glob, because a Delta table is a directory with
a `_delta_log`. An optional second argument is the version to read.
"""

from __future__ import annotations

from pathlib import Path

import pytest

beacondb = pytest.importorskip("beacondb", reason="build it with maturin")
deltalake = pytest.importorskip("deltalake", reason="pip install deltalake")
pa = pytest.importorskip("pyarrow")

from deltalake import write_deltalake  # noqa: E402

FIRST_ROWS = 3
APPENDED_ROWS = 2


def _table(ids, values, platforms=None) -> "pa.Table":
    columns = {
        "id": pa.array(ids, pa.int64()),
        "value": pa.array(values, pa.float64()),
    }
    if platforms is not None:
        columns["platform"] = pa.array(platforms, pa.string())
    return pa.table(columns)


@pytest.fixture(scope="module")
def datasets(tmp_path_factory) -> Path:
    """Write every Delta table this module queries."""
    root = tmp_path_factory.mktemp("delta")

    # One version.
    write_deltalake(str(root / "single"), _table([1, 2, 3], [1.5, 2.5, 3.5]))

    # Three versions: an initial write and two appends.
    many = str(root / "many")
    write_deltalake(many, _table([1, 2, 3], [1.5, 2.5, 3.5]))
    write_deltalake(many, _table([4, 5], [4.5, 5.5]), mode="append")
    write_deltalake(many, _table([6], [6.5]), mode="append")

    # Partitioned by a column, so the layout puts rows in per-value directories.
    write_deltalake(
        str(root / "partitioned"),
        _table([1, 2, 3, 4], [1.5, 2.5, 3.5, 4.5], ["SHIP", "BUOY", "SHIP", "BUOY"]),
        partition_by=["platform"],
    )

    # A schema change: version 0 has two columns, version 1 has three.
    evolving = str(root / "evolving")
    write_deltalake(evolving, _table([1, 2], [1.5, 2.5]))
    write_deltalake(
        evolving,
        _table([3], [3.5], ["SHIP"]),
        mode="append",
        schema_mode="merge",
    )
    return root


@pytest.fixture
def con(datasets, tmp_path):
    with beacondb.connect(str(tmp_path / "beacon.db"), datasets=str(datasets)) as connection:
        yield connection


# --- reading ------------------------------------------------------------------


def test_a_table_reads(con):
    rows = con.sql("SELECT id, value FROM read_delta('single') ORDER BY id").fetchall()
    assert rows == [(1, 1.5), (2, 2.5), (3, 3.5)]


def test_the_schema_is_reported(con):
    relation = con.sql("SELECT * FROM read_delta('single')")
    assert relation.columns == ["id", "value"]
    assert relation.types == ["Int64", "Float64"]


def test_count_star(con):
    assert con.sql("SELECT count(*) AS n FROM read_delta('single')").fetchall() == [(FIRST_ROWS,)]


def test_a_filter_and_an_aggregate(con):
    got = con.sql(
        "SELECT count(*) n, sum(value) total FROM read_delta('many') WHERE id >= 4"
    ).fetchall()[0]
    assert got == (3, 4.5 + 5.5 + 6.5)


def test_the_latest_version_holds_every_append(con):
    """Three writes: 3 rows, then 2 more, then 1. The tip holds all six."""
    assert con.sql("SELECT count(*) AS n FROM read_delta('many')").fetchall() == [(6,)]


def test_an_older_version_can_be_read(con):
    """The second argument is the version, so a query can travel back.

    Version 0 is the initial write, 1 adds two rows, 2 adds one more.
    """
    assert con.sql("SELECT count(*) AS n FROM read_delta('many', 0)").fetchall() == [(3,)]
    assert con.sql("SELECT count(*) AS n FROM read_delta('many', 1)").fetchall() == [(5,)]
    assert con.sql("SELECT count(*) AS n FROM read_delta('many', 2)").fetchall() == [(6,)]


def test_an_older_version_holds_the_older_rows(con):
    """Not just the count: version 0 must not contain the appended ids."""
    ids = [r[0] for r in con.sql("SELECT id FROM read_delta('many', 0) ORDER BY id").fetchall()]
    assert ids == [1, 2, 3]


def test_a_partitioned_table_reads_every_partition(con):
    """A partition column comes back dictionary-encoded, so this reads it with `.arrow()`."""
    table = con.sql(
        "SELECT platform, count(*) n FROM read_delta('partitioned') GROUP BY platform ORDER BY platform"
    ).arrow()
    assert table.column("platform").to_pylist() == ["BUOY", "SHIP"]
    assert table.column("n").to_pylist() == [2, 2]


def test_a_partition_column_filters(con):
    """A predicate on the partition column is what the layout exists to make cheap."""
    rows = con.sql(
        "SELECT id FROM read_delta('partitioned') WHERE platform = 'SHIP' ORDER BY id"
    ).fetchall()
    assert rows == [(1,), (3,)]


def test_a_schema_change_lands_without_a_restart(con):
    """Version 1 adds a column, and the rows written before it read null for it."""
    relation = con.sql("SELECT * FROM read_delta('evolving')")
    assert "platform" in relation.columns

    rows = con.sql("SELECT id, platform FROM read_delta('evolving') ORDER BY id").fetchall()
    assert rows == [(1, None), (2, None), (3, "SHIP")]


# --- external tables and a restart --------------------------------------------


def test_an_external_table_reads(con):
    con.execute("CREATE EXTERNAL TABLE obs STORED AS DELTA LOCATION 'single'")
    assert con.sql("SELECT count(*) AS n FROM obs").fetchall() == [(FIRST_ROWS,)]
    assert "obs" in con.list_tables()


def test_an_external_table_survives_a_restart(datasets, tmp_path):
    path = str(tmp_path / "restart.db")

    with beacondb.connect(path, datasets=str(datasets)) as con:
        con.execute("CREATE EXTERNAL TABLE obs STORED AS DELTA LOCATION 'single'")
        con.execute("CREATE EXTERNAL TABLE parts STORED AS DELTA LOCATION 'partitioned'")

    with beacondb.connect(path, datasets=str(datasets)) as con:
        assert con.sql("SELECT count(*) AS n FROM obs").fetchall() == [(FIRST_ROWS,)]
        assert con.sql("SELECT count(*) AS n FROM parts").fetchall() == [(4,)]
        assert {"obs", "parts"} <= set(con.list_tables())
        assert con.sql("SELECT id, value FROM obs ORDER BY id LIMIT 1").fetchall() == [(1, 1.5)]
        # The partition column has to survive as a column, not only as a directory name.
        assert "platform" in con.sql("SELECT * FROM parts").columns
        platforms = con.sql("SELECT platform FROM parts").arrow().column("platform").to_pylist()
        assert sorted(platforms) == ["BUOY", "BUOY", "SHIP", "SHIP"]


def test_an_external_table_sees_a_later_append(datasets, tmp_path):
    """A table points at a location, so a write after the definition has to show up.

    This is what makes an external table useful over a live Delta table: the definition is not a
    snapshot.
    """
    path = str(tmp_path / "append.db")
    location = str(datasets / "growing")
    write_deltalake(location, _table([1, 2], [1.5, 2.5]))

    with beacondb.connect(path, datasets=str(datasets)) as con:
        con.execute("CREATE EXTERNAL TABLE growing STORED AS DELTA LOCATION 'growing'")
        assert con.sql("SELECT count(*) AS n FROM growing").fetchall() == [(2,)]

    write_deltalake(location, _table([3], [3.5]), mode="append")

    with beacondb.connect(path, datasets=str(datasets)) as con:
        assert con.sql("SELECT count(*) AS n FROM growing").fetchall() == [(3,)], (
            "the table must re-open the log rather than hold the version it was created at"
        )
