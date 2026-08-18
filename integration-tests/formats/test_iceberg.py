"""Apache Iceberg, end to end, in one file.

Writes its own Iceberg tables with `pyiceberg` over a local SQLite catalog — one snapshot,
several, partitioned, and one that gains a column — opens an embedded Beacon over them, queries
them, travels to an older snapshot, creates an external table, reopens the database, and checks
the table survived.

    pytest formats/test_iceberg.py -v

Beacon reads the table directory, so a location is `<warehouse>/<namespace>/<table>`. An optional
second argument is the snapshot id to read. Beacon never writes Iceberg; `pyiceberg` is the
writer here.
"""

from __future__ import annotations

from pathlib import Path

import pytest

beacondb = pytest.importorskip("beacondb", reason="build it with maturin")
pyiceberg = pytest.importorskip("pyiceberg", reason="pip install pyiceberg")
pytest.importorskip("sqlalchemy", reason="pyiceberg's SQL catalog needs sqlalchemy")
pa = pytest.importorskip("pyarrow")

from pyiceberg.catalog.sql import SqlCatalog  # noqa: E402

NAMESPACE = "ns"


def _table(ids, values, platforms=None) -> "pa.Table":
    columns = {"id": pa.array(ids, pa.int64()), "value": pa.array(values, pa.float64())}
    if platforms is not None:
        columns["platform"] = pa.array(platforms, pa.string())
    return pa.table(columns)


@pytest.fixture(scope="module")
def written(tmp_path_factory):
    """Write every Iceberg table, and hand back the warehouse plus the snapshot ids."""
    root = tmp_path_factory.mktemp("iceberg")
    warehouse = root / "warehouse"
    warehouse.mkdir()
    catalog = SqlCatalog(
        "tests",
        **{"uri": f"sqlite:///{warehouse}/catalog.db", "warehouse": f"file://{warehouse}"},
    )
    catalog.create_namespace(NAMESPACE)

    # One snapshot.
    single = catalog.create_table(f"{NAMESPACE}.single", schema=_table([1], [1.5]).schema)
    single.append(_table([1, 2, 3], [1.5, 2.5, 3.5]))

    # Three snapshots, so a query can travel back to each.
    many = catalog.create_table(f"{NAMESPACE}.many", schema=_table([1], [1.5]).schema)
    many.append(_table([1, 2, 3], [1.5, 2.5, 3.5]))
    first = many.current_snapshot().snapshot_id
    many.append(_table([4, 5], [4.5, 5.5]))
    second = many.current_snapshot().snapshot_id
    many.append(_table([6], [6.5]))

    # Partitioned by a column.
    partitioned = catalog.create_table(
        f"{NAMESPACE}.partitioned",
        schema=_table([1], [1.5], ["SHIP"]).schema,
    )
    partitioned.append(_table([1, 2, 3, 4], [1.5, 2.5, 3.5, 4.5], ["SHIP", "BUOY", "SHIP", "BUOY"]))

    # A schema change: a column is added after the first snapshot.
    evolving = catalog.create_table(f"{NAMESPACE}.evolving", schema=_table([1], [1.5]).schema)
    evolving.append(_table([1, 2], [1.5, 2.5]))
    with evolving.update_schema() as update:
        update.add_column("platform", pyiceberg.types.StringType())
    evolving.append(_table([3], [3.5], ["SHIP"]))

    return {"root": root, "first": first, "second": second}


@pytest.fixture
def datasets(written) -> Path:
    return written["root"]


@pytest.fixture
def con(datasets, tmp_path):
    with beacondb.connect(str(tmp_path / "beacon.db"), datasets=str(datasets)) as connection:
        yield connection


def _location(name: str) -> str:
    return f"warehouse/{NAMESPACE}/{name}"


# --- reading ------------------------------------------------------------------


def test_a_table_reads(con):
    rows = con.sql(
        f"SELECT id, value FROM read_iceberg('{_location('single')}') ORDER BY id"
    ).fetchall()
    assert rows == [(1, 1.5), (2, 2.5), (3, 3.5)]


def test_the_schema_is_reported(con):
    relation = con.sql(f"SELECT * FROM read_iceberg('{_location('single')}')")
    assert relation.columns == ["id", "value"]
    assert relation.types == ["Int64", "Float64"]


def test_count_star(con):
    n = con.sql(f"SELECT count(*) AS n FROM read_iceberg('{_location('single')}')").fetchall()
    assert n == [(3,)]


def test_a_filter_and_an_aggregate(con):
    got = con.sql(
        f"SELECT count(*) n, sum(value) total FROM read_iceberg('{_location('many')}') WHERE id >= 4"
    ).fetchall()[0]
    assert got == (3, 4.5 + 5.5 + 6.5)


def test_the_latest_snapshot_holds_every_append(con):
    n = con.sql(f"SELECT count(*) AS n FROM read_iceberg('{_location('many')}')").fetchall()
    assert n == [(6,)]


def test_an_older_snapshot_can_be_read(con, written):
    """The second argument is a snapshot id, so a query can travel back."""
    location = _location("many")
    first = con.sql(
        f"SELECT count(*) AS n FROM read_iceberg('{location}', {written['first']})"
    ).fetchall()
    second = con.sql(
        f"SELECT count(*) AS n FROM read_iceberg('{location}', {written['second']})"
    ).fetchall()
    assert first == [(3,)]
    assert second == [(5,)]


def test_an_older_snapshot_holds_the_older_rows(con, written):
    ids = con.sql(
        f"SELECT id FROM read_iceberg('{_location('many')}', {written['first']}) ORDER BY id"
    ).fetchall()
    assert [r[0] for r in ids] == [1, 2, 3]


def test_a_partitioned_table_reads_every_partition(con):
    table = con.sql(
        f"SELECT platform, count(*) n FROM read_iceberg('{_location('partitioned')}') "
        "GROUP BY platform ORDER BY platform"
    ).arrow()
    assert table.column("platform").to_pylist() == ["BUOY", "SHIP"]
    assert table.column("n").to_pylist() == [2, 2]


def test_a_partition_column_filters(con):
    rows = con.sql(
        f"SELECT id FROM read_iceberg('{_location('partitioned')}') "
        "WHERE platform = 'SHIP' ORDER BY id"
    ).fetchall()
    assert rows == [(1,), (3,)]


def test_a_schema_change_lands_without_a_restart(con):
    """A column added after the first snapshot appears, and older rows read null for it."""
    relation = con.sql(f"SELECT * FROM read_iceberg('{_location('evolving')}')")
    assert "platform" in relation.columns

    rows = con.sql(
        f"SELECT id, platform FROM read_iceberg('{_location('evolving')}') ORDER BY id"
    ).fetchall()
    assert rows == [(1, None), (2, None), (3, "SHIP")]


# --- external tables and a restart --------------------------------------------


def test_an_external_table_reads(con):
    con.execute(f"CREATE EXTERNAL TABLE obs STORED AS ICEBERG LOCATION '{_location('single')}'")
    assert con.sql("SELECT count(*) AS n FROM obs").fetchall() == [(3,)]
    assert "obs" in con.list_tables()


def test_an_external_table_survives_a_restart(datasets, tmp_path):
    path = str(tmp_path / "restart.db")

    with beacondb.connect(path, datasets=str(datasets)) as con:
        con.execute(f"CREATE EXTERNAL TABLE obs STORED AS ICEBERG LOCATION '{_location('single')}'")
        con.execute(
            f"CREATE EXTERNAL TABLE parts STORED AS ICEBERG LOCATION '{_location('partitioned')}'"
        )

    with beacondb.connect(path, datasets=str(datasets)) as con:
        assert con.sql("SELECT count(*) AS n FROM obs").fetchall() == [(3,)]
        assert con.sql("SELECT count(*) AS n FROM parts").fetchall() == [(4,)]
        assert {"obs", "parts"} <= set(con.list_tables())
        assert con.sql("SELECT id, value FROM obs ORDER BY id LIMIT 1").fetchall() == [(1, 1.5)]


def test_an_external_table_sees_a_later_snapshot(written, tmp_path):
    """A table points at a location, so a snapshot written after the definition has to show up."""
    catalog = SqlCatalog(
        "tests",
        **{
            "uri": f"sqlite:///{written['root']}/warehouse/catalog.db",
            "warehouse": f"file://{written['root']}/warehouse",
        },
    )
    table = catalog.create_table(f"{NAMESPACE}.growing", schema=_table([1], [1.5]).schema)
    table.append(_table([1, 2], [1.5, 2.5]))

    path = str(tmp_path / "grow.db")
    with beacondb.connect(path, datasets=str(written["root"])) as con:
        con.execute(
            f"CREATE EXTERNAL TABLE growing STORED AS ICEBERG LOCATION '{_location('growing')}'"
        )
        assert con.sql("SELECT count(*) AS n FROM growing").fetchall() == [(2,)]

    table.append(_table([3], [3.5]))

    with beacondb.connect(path, datasets=str(written["root"])) as con:
        assert con.sql("SELECT count(*) AS n FROM growing").fetchall() == [(3,)], (
            "the table must re-read the metadata rather than hold the snapshot it was created at"
        )
