"""Parquet, end to end, in one file.

Writes its own Parquet files, opens an embedded Beacon over them, queries them, creates an
external table, reopens the database, and checks the table survived.

    pytest formats/test_parquet.py -v
"""

from __future__ import annotations

from pathlib import Path

import pytest

beacondb = pytest.importorskip("beacondb", reason="build it with maturin")
pa = pytest.importorskip("pyarrow", reason="pip install pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

ROWS = 60
PLATFORMS = ["SHIP", "BUOY", "GLIDER", "FLOAT"]


def _table(rows: int = ROWS, offset: int = 0) -> "pa.Table":
    return pa.table(
        {
            "id": pa.array(range(offset, offset + rows), pa.int32()),
            "value": pa.array([i * 1.5 for i in range(offset, offset + rows)], pa.float64()),
            "platform": pa.array([PLATFORMS[i % 4] for i in range(offset, offset + rows)]),
        }
    )


@pytest.fixture(scope="module")
def datasets(tmp_path_factory) -> Path:
    """Write every Parquet file this module queries."""
    root = tmp_path_factory.mktemp("parquet")
    base = _table()

    pq.write_table(base, root / "one_row_group.parquet", row_group_size=ROWS)
    pq.write_table(base, root / "many_row_groups.parquet", row_group_size=10)
    pq.write_table(base, root / "zstd.parquet", compression="zstd")
    pq.write_table(base, root / "snappy.parquet", compression="snappy")
    pq.write_table(base, root / "no_statistics.parquet", write_statistics=False)

    # A null every third row.
    pq.write_table(
        base.set_column(
            1, "value", pa.array([None if i % 3 == 0 else i * 1.5 for i in range(ROWS)], pa.float64())
        ),
        root / "nulls.parquet",
    )

    # A struct column, so a nested projection can be checked.
    pq.write_table(
        pa.table(
            {
                "id": pa.array(range(ROWS), pa.int32()),
                "position": pa.array(
                    [{"lat": -60.0 + i, "lon": i * 2.0} for i in range(ROWS)],
                    pa.struct([("lat", pa.float64()), ("lon", pa.float64())]),
                ),
            }
        ),
        root / "struct.parquet",
    )

    # Two files for a glob, and a directory holding them.
    parts = root / "parts"
    parts.mkdir()
    pq.write_table(_table(20, 0), parts / "part-0.parquet")
    pq.write_table(_table(20, 100), parts / "part-1.parquet")
    return root


@pytest.fixture
def con(datasets, tmp_path):
    with beacondb.connect(str(tmp_path / "beacon.db"), datasets=str(datasets)) as connection:
        yield connection


# --- reading ------------------------------------------------------------------


def test_a_file_reads(con):
    rows = con.sql(
        "SELECT id, value, platform FROM read_parquet('one_row_group.parquet') ORDER BY id LIMIT 2"
    ).fetchall()
    assert rows == [(0, 0.0, "SHIP"), (1, 1.5, "BUOY")]


def test_the_schema_is_reported(con):
    relation = con.sql("SELECT * FROM read_parquet('one_row_group.parquet')")
    assert relation.columns == ["id", "value", "platform"]
    assert relation.types == ["Int32", "Float64", "Utf8"]


def test_count_star(con):
    assert con.sql("SELECT count(*) AS n FROM read_parquet('one_row_group.parquet')").fetchall() == [(ROWS,)]


def test_a_filter(con):
    n = con.sql(
        "SELECT count(*) AS n FROM read_parquet('one_row_group.parquet') WHERE value >= 45.0"
    ).fetchall()[0][0]
    assert n == ROWS - 30


def test_aggregates(con):
    got = con.sql(
        "SELECT min(id) lo, max(id) hi, sum(id) total FROM read_parquet('one_row_group.parquet')"
    ).fetchall()[0]
    assert got == (0, ROWS - 1, sum(range(ROWS)))


def test_group_by(con):
    rows = con.sql(
        "SELECT platform, count(*) n FROM read_parquet('one_row_group.parquet') "
        "GROUP BY platform ORDER BY platform"
    ).fetchall()
    assert rows == [("BUOY", 15), ("FLOAT", 15), ("GLIDER", 15), ("SHIP", 15)]


def test_many_row_groups_return_the_same_rows_as_one(con):
    """A partition split must not repeat or drop a row."""
    order = "ORDER BY id"
    one = con.sql(f"SELECT id, value FROM read_parquet('one_row_group.parquet') {order}").fetchall()
    many = con.sql(f"SELECT id, value FROM read_parquet('many_row_groups.parquet') {order}").fetchall()
    assert one == many
    assert len(many) == ROWS


def test_the_compression_codec_is_invisible(con):
    order = "ORDER BY id"
    zstd = con.sql(f"SELECT id, value FROM read_parquet('zstd.parquet') {order}").fetchall()
    snappy = con.sql(f"SELECT id, value FROM read_parquet('snappy.parquet') {order}").fetchall()
    assert zstd == snappy


def test_a_file_without_statistics_returns_the_same_rows(con):
    """Statistics drive row-group pruning, and pruning must not change an answer."""
    predicate = "WHERE value >= 45.0 ORDER BY id"
    with_stats = con.sql(f"SELECT id FROM read_parquet('one_row_group.parquet') {predicate}").fetchall()
    without = con.sql(f"SELECT id FROM read_parquet('no_statistics.parquet') {predicate}").fetchall()
    assert with_stats == without


def test_a_null_is_a_null(con):
    total, present = con.sql(
        "SELECT count(*), count(value) FROM read_parquet('nulls.parquet')"
    ).fetchall()[0]
    assert total == ROWS
    assert present == ROWS - 20, "every third row is null"

    zeros = con.sql(
        "SELECT count(*) AS n FROM read_parquet('nulls.parquet') WHERE value = 0.0"
    ).fetchall()[0][0]
    assert zeros == 0, "a null must not read as 0.0"


def test_a_struct_column_reads(con):
    """A struct needs `.arrow()`: `fetchall()` has no Python row value for one, and says so."""
    table = con.sql("SELECT position FROM read_parquet('struct.parquet') ORDER BY id LIMIT 1").arrow()
    assert table.column("position").to_pylist() == [{"lat": -60.0, "lon": 0.0}]


# --- many files ---------------------------------------------------------------


def test_a_glob_reads_every_file(con):
    assert con.sql("SELECT count(*) AS n FROM read_parquet('parts/*.parquet')").fetchall() == [(40,)]


def test_a_directory_reads_every_file(con):
    assert con.sql("SELECT count(*) AS n FROM read_parquet('parts/')").fetchall() == [(40,)]


def test_a_glob_column_order_is_stable(con):
    orders = {tuple(con.sql("SELECT * FROM read_parquet('parts/*.parquet')").columns) for _ in range(5)}
    assert len(orders) == 1, f"the column order changed between runs: {orders}"


# --- external tables and a restart --------------------------------------------


def test_an_external_table_reads(con):
    con.execute("CREATE EXTERNAL TABLE obs STORED AS PARQUET LOCATION 'one_row_group.parquet'")
    assert con.sql("SELECT count(*) AS n FROM obs").fetchall() == [(ROWS,)]
    assert "obs" in con.list_tables()


def test_an_external_table_survives_a_restart(datasets, tmp_path):
    path = str(tmp_path / "restart.db")

    with beacondb.connect(path, datasets=str(datasets)) as con:
        con.execute("CREATE EXTERNAL TABLE obs STORED AS PARQUET LOCATION 'one_row_group.parquet'")
        con.execute("CREATE EXTERNAL TABLE parts STORED AS PARQUET LOCATION 'parts/*.parquet'")

    with beacondb.connect(path, datasets=str(datasets)) as con:
        assert con.sql("SELECT count(*) AS n FROM obs").fetchall() == [(ROWS,)]
        assert con.sql("SELECT count(*) AS n FROM parts").fetchall() == [(40,)]
        assert {"obs", "parts"} <= set(con.list_tables())
        rows = con.sql("SELECT id, value FROM obs ORDER BY id LIMIT 2").fetchall()
        assert rows == [(0, 0.0), (1, 1.5)]


def test_a_query_joins_two_external_tables_after_a_restart(datasets, tmp_path):
    """Two tables, one join, across a restart: the definitions have to be usable together."""
    path = str(tmp_path / "join.db")
    with beacondb.connect(path, datasets=str(datasets)) as con:
        con.execute("CREATE EXTERNAL TABLE a STORED AS PARQUET LOCATION 'one_row_group.parquet'")
        con.execute("CREATE EXTERNAL TABLE b STORED AS PARQUET LOCATION 'zstd.parquet'")

    with beacondb.connect(path, datasets=str(datasets)) as con:
        n = con.sql("SELECT count(*) AS n FROM a JOIN b ON a.id = b.id").fetchall()[0][0]
        assert n == ROWS
