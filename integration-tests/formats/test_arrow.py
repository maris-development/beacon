"""Arrow IPC, end to end, in one file.

Writes its own Arrow IPC files, opens an embedded Beacon over them, queries them, creates an
external table, reopens the database, and checks the table survived.

    pytest formats/test_arrow.py -v

Arrow IPC is the one format whose on-disk types are the Arrow types exactly, so whatever comes
back different was changed by Beacon.
"""

from __future__ import annotations

from pathlib import Path

import pytest

beacondb = pytest.importorskip("beacondb", reason="build it with maturin")
pa = pytest.importorskip("pyarrow", reason="pip install pyarrow")

ROWS = 24
BATCH = 6
PLATFORMS = ["SHIP", "BUOY", "GLIDER", "FLOAT"]


def _write(path: Path, table: "pa.Table", *, batch_rows: int) -> None:
    with pa.ipc.new_file(path, table.schema) as writer:
        for batch in table.to_batches(max_chunksize=batch_rows):
            writer.write_batch(batch)


@pytest.fixture(scope="module")
def datasets(tmp_path_factory) -> Path:
    """Write every Arrow IPC file this module queries."""
    root = tmp_path_factory.mktemp("arrow")
    base = pa.table(
        {
            "id": pa.array(range(ROWS), pa.int32()),
            "value": pa.array([i * 1.5 for i in range(ROWS)], pa.float64()),
            "platform": pa.array([PLATFORMS[i % 4] for i in range(ROWS)]),
        }
    )

    _write(root / "one_batch.arrow", base, batch_rows=ROWS)
    _write(root / "many_batches.arrow", base, batch_rows=BATCH)

    # A dictionary column, which IPC stores as a dictionary batch of its own.
    _write(
        root / "dictionary.arrow",
        pa.table(
            {
                "id": pa.array(range(ROWS), pa.int32()),
                "platform": pa.array([PLATFORMS[i % 4] for i in range(ROWS)]).dictionary_encode(),
            }
        ),
        batch_rows=ROWS,
    )

    _write(
        root / "nulls.arrow",
        pa.table(
            {
                "id": pa.array(range(ROWS), pa.int32()),
                "value": pa.array(
                    [None if i % 3 == 0 else i * 1.5 for i in range(ROWS)], pa.float64()
                ),
            }
        ),
        batch_rows=ROWS,
    )

    # Every Arrow type this format carries exactly, one column each.
    _write(
        root / "types.arrow",
        pa.table(
            {
                "i8": pa.array([1, 2], pa.int8()),
                "u64": pa.array([1, 2], pa.uint64()),
                "f32": pa.array([1.5, 2.5], pa.float32()),
                "flag": pa.array([True, False], pa.bool_()),
                "text": pa.array(["a", "b"], pa.string()),
                "big_text": pa.array(["a", "b"], pa.large_string()),
                "when": pa.array([0, 1], pa.timestamp("ms")),
            }
        ),
        batch_rows=2,
    )

    parts = root / "parts"
    parts.mkdir()
    for index in range(2):
        _write(
            parts / f"part-{index}.arrow",
            pa.table({"id": pa.array(range(index * 100, index * 100 + 5), pa.int32())}),
            batch_rows=5,
        )
    return root


@pytest.fixture
def con(datasets, tmp_path):
    with beacondb.connect(str(tmp_path / "beacon.db"), datasets=str(datasets)) as connection:
        yield connection


# --- reading ------------------------------------------------------------------


def test_a_file_reads(con):
    rows = con.sql(
        "SELECT id, value, platform FROM read_arrow('one_batch.arrow') ORDER BY id LIMIT 2"
    ).fetchall()
    assert rows == [(0, 0.0, "SHIP"), (1, 1.5, "BUOY")]


def test_the_schema_is_reported(con):
    relation = con.sql("SELECT * FROM read_arrow('one_batch.arrow')")
    assert relation.columns == ["id", "value", "platform"]
    assert relation.types == ["Int32", "Float64", "Utf8"]


def test_count_star(con):
    assert con.sql("SELECT count(*) AS n FROM read_arrow('one_batch.arrow')").fetchall() == [(ROWS,)]


def test_a_filter_and_an_aggregate(con):
    got = con.sql(
        "SELECT count(*) n, sum(id) total FROM read_arrow('one_batch.arrow') WHERE id >= 12"
    ).fetchall()[0]
    assert got == (12, sum(range(12, ROWS)))


def test_every_batch_is_read(con):
    """Four batches of six. A reader that stops after the first returns 6 of 24 rows."""
    assert con.sql("SELECT count(*) AS n FROM read_arrow('many_batches.arrow')").fetchall() == [(ROWS,)]


def test_the_batch_count_is_invisible(con):
    order = "ORDER BY id"
    one = con.sql(f"SELECT id, value FROM read_arrow('one_batch.arrow') {order}").fetchall()
    many = con.sql(f"SELECT id, value FROM read_arrow('many_batches.arrow') {order}").fetchall()
    assert one == many


def test_a_dictionary_column_keeps_its_values(con):
    """A dictionary column needs `.arrow()`: `fetchall()` has no Python row value for one."""
    table = con.sql(
        "SELECT platform FROM read_arrow('dictionary.arrow') ORDER BY id LIMIT 4"
    ).arrow()
    assert "dictionary" in str(table.schema.field("platform").type)
    assert table.column("platform").to_pylist() == ["SHIP", "BUOY", "GLIDER", "FLOAT"]


def test_a_null_is_a_null(con):
    total, present = con.sql(
        "SELECT count(*), count(value) FROM read_arrow('nulls.arrow')"
    ).fetchall()[0]
    assert total == ROWS
    assert present == ROWS - 8, "every third row is null"


def test_the_types_survive_the_round_trip(con):
    """The types the writer wrote are the types the reader reports."""
    relation = con.sql("SELECT * FROM read_arrow('types.arrow')")
    assert dict(zip(relation.columns, relation.types)) == {
        "i8": "Int8",
        "u64": "UInt64",
        "f32": "Float32",
        "flag": "Boolean",
        "text": "Utf8",
        "big_text": "LargeUtf8",
        "when": "Timestamp(ms)",
    }


# --- many files ---------------------------------------------------------------


def test_a_glob_reads_every_file(con):
    assert con.sql("SELECT count(*) AS n FROM read_arrow('parts/*.arrow')").fetchall() == [(10,)]


# --- external tables and a restart --------------------------------------------


def test_an_external_table_reads(con):
    con.execute("CREATE EXTERNAL TABLE obs STORED AS ARROW LOCATION 'one_batch.arrow'")
    assert con.sql("SELECT count(*) AS n FROM obs").fetchall() == [(ROWS,)]
    assert "obs" in con.list_tables()


def test_an_external_table_survives_a_restart(datasets, tmp_path):
    path = str(tmp_path / "restart.db")

    with beacondb.connect(path, datasets=str(datasets)) as con:
        con.execute("CREATE EXTERNAL TABLE obs STORED AS ARROW LOCATION 'one_batch.arrow'")
        con.execute("CREATE EXTERNAL TABLE batched STORED AS ARROW LOCATION 'many_batches.arrow'")

    with beacondb.connect(path, datasets=str(datasets)) as con:
        assert con.sql("SELECT count(*) AS n FROM obs").fetchall() == [(ROWS,)]
        assert con.sql("SELECT count(*) AS n FROM batched").fetchall() == [(ROWS,)]
        assert {"obs", "batched"} <= set(con.list_tables())
        assert con.sql("SELECT id, value FROM obs ORDER BY id LIMIT 1").fetchall() == [(0, 0.0)]
