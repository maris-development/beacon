"""Zarr, end to end, in one file.

Writes its own Zarr stores, opens an embedded Beacon over them, queries them, creates an
external table, reopens the database, and checks the table survived.

    pytest formats/test_zarr.py -v

Beacon's Zarr reader is v3 only: it finds a store by its `zarr.json`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

beacondb = pytest.importorskip("beacondb", reason="build it with maturin")
zarr = pytest.importorskip("zarr", reason="pip install zarr")
np = pytest.importorskip("numpy")

pytestmark = pytest.mark.filterwarnings(
    "ignore:Consolidated metadata is currently not part in the Zarr format 3 specification"
)

ROWS = 24
TIME, LAT, LON = 2, 3, 4
GRID_ROWS = TIME * LAT * LON  # 24
FILL = -999.0
FILL_CHUNK = 8


def _flat(root: Path, name: str, *, zarr_format: int = 3, consolidate: bool = False,
          compressor: str | None = None) -> None:
    """A group of two rank-1 arrays over one dimension."""
    group = zarr.open_group(store=str(root / name), mode="w", zarr_format=zarr_format)
    extra = {}
    if compressor == "zstd":
        from zarr.codecs import ZstdCodec

        extra["compressors"] = [ZstdCodec(level=5)]
    # v3 states its axes in `dimension_names`; v2 has no such field.
    names = {} if zarr_format == 2 else {"dimension_names": ["obs"]}
    for array_name, values in (
        ("temperature", np.arange(ROWS, dtype="float64") * 0.5 + 5.0),
        ("depth", np.arange(ROWS, dtype="float64") * 10.0),
    ):
        array = group.create_array(
            array_name, shape=(ROWS,), chunks=(8,), dtype="float64", **extra, **names
        )
        array[:] = values
    if consolidate:
        zarr.consolidate_metadata(str(root / name))


@pytest.fixture(scope="module")
def datasets(tmp_path_factory) -> Path:
    """Write every Zarr store this module queries."""
    root = tmp_path_factory.mktemp("zarr")

    _flat(root, "flat.zarr")
    _flat(root, "v2.zarr", zarr_format=2)
    _flat(root, "consolidated.zarr", consolidate=True)
    _flat(root, "zstd.zarr", compressor="zstd")

    # A rank-3 grid and its coordinate arrays.
    group = zarr.open_group(store=str(root / "grid.zarr"), mode="w", zarr_format=3)
    temperature = group.create_array(
        "temperature", shape=(TIME, LAT, LON), chunks=(1, 2, 2), dtype="float32",
        dimension_names=["time", "latitude", "longitude"],
    )
    temperature[:] = np.arange(GRID_ROWS, dtype="float32").reshape(TIME, LAT, LON)
    for axis, size, start, step in (
        ("time", TIME, 0.0, 1.0),
        ("latitude", LAT, -30.0, 10.0),
        ("longitude", LON, 0.0, 15.0),
    ):
        array = group.create_array(
            axis, shape=(size,), chunks=(size,), dtype="float32", dimension_names=[axis]
        )
        array[:] = np.arange(size, dtype="float32") * step + start

    # An array whose chunks are only partly written: the rest reads as `fill_value`.
    group = zarr.open_group(store=str(root / "partial.zarr"), mode="w", zarr_format=3)
    partial = group.create_array(
        "temperature", shape=(ROWS,), chunks=(FILL_CHUNK,), dtype="float64",
        fill_value=FILL, dimension_names=["obs"],
    )
    partial[:FILL_CHUNK] = np.arange(FILL_CHUNK, dtype="float64") + 1.0
    return root


@pytest.fixture
def con(datasets, tmp_path):
    with beacondb.connect(str(tmp_path / "beacon.db"), datasets=str(datasets)) as connection:
        yield connection


# --- reading ------------------------------------------------------------------


def test_a_store_reads(con):
    rows = con.sql(
        "SELECT depth, temperature FROM read_zarr('flat.zarr') ORDER BY depth LIMIT 2"
    ).fetchall()
    assert rows == [(0.0, 5.0), (10.0, 5.5)]


def test_the_schema_is_reported(con):
    relation = con.sql("SELECT * FROM read_zarr('flat.zarr')")
    assert relation.columns == ["depth", "temperature"]
    assert relation.types == ["Float64", "Float64"]


def test_count_star(con):
    assert con.sql("SELECT count(*) AS n FROM read_zarr('flat.zarr')").fetchall() == [(ROWS,)]


def test_a_filter_and_an_aggregate(con):
    got = con.sql(
        "SELECT count(*) n, min(depth) lo, max(depth) hi FROM read_zarr('flat.zarr') "
        "WHERE depth >= 100.0"
    ).fetchall()[0]
    assert got == (ROWS - 10, 100.0, (ROWS - 1) * 10.0)


def test_consolidated_metadata_does_not_change_the_answer(con):
    """A consolidated copy changes how a reader plans, not what it answers."""
    order = "ORDER BY depth"
    plain = con.sql(f"SELECT depth, temperature FROM read_zarr('flat.zarr') {order}").fetchall()
    consolidated = con.sql(f"SELECT depth, temperature FROM read_zarr('consolidated.zarr') {order}").fetchall()
    assert plain == consolidated


def test_the_zstd_codec_is_invisible(con):
    order = "ORDER BY depth"
    plain = con.sql(f"SELECT depth, temperature FROM read_zarr('flat.zarr') {order}").fetchall()
    zstd = con.sql(f"SELECT depth, temperature FROM read_zarr('zstd.zarr') {order}").fetchall()
    assert plain == zstd


def test_two_reads_return_the_same_rows(con):
    """Chunks are read in parallel and land in completion order, so compare with ORDER BY."""
    query = "SELECT depth, temperature FROM read_zarr('flat.zarr') ORDER BY depth"
    assert con.sql(query).fetchall() == con.sql(query).fetchall()


# --- the grid -----------------------------------------------------------------


def test_a_grid_pairs_every_cell_with_its_own_coordinates(con):
    """2 x 3 x 4 dimensions, so 24 rows and 24 distinct coordinate triples.

    A broadcast that pairs the wrong coordinate with a value returns exactly 24 rows with the
    grid scrambled, so counting distinct triples is what catches it.
    """
    rows = con.sql(
        "SELECT time, latitude, longitude, temperature FROM read_zarr('grid.zarr')"
    ).fetchall()
    assert len(rows) == GRID_ROWS
    assert len({(t, la, lo) for t, la, lo, _ in rows}) == GRID_ROWS


def test_projecting_a_grid_coordinate_narrows_the_grid(con):
    assert len(con.sql("SELECT latitude FROM read_zarr('grid.zarr')").fetchall()) == LAT
    assert len(con.sql("SELECT temperature FROM read_zarr('grid.zarr')").fetchall()) == GRID_ROWS


# --- an unwritten chunk -------------------------------------------------------


def test_an_unwritten_chunk_reads_as_the_fill_value(con):
    """A Zarr `fill_value` is a default, not an absent value: it reads as the number.

    Different from a netCDF `_FillValue`, which marks a cell absent and reads as a null. An
    array of zeros with `fill_value: 0` would come back entirely null if these were alike.
    """
    total, present = con.sql(
        "SELECT count(*), count(temperature) FROM read_zarr('partial.zarr')"
    ).fetchall()[0]
    assert total == ROWS
    assert present == ROWS, "an unwritten chunk still produces rows, holding the fill value"

    fills = con.sql(
        f"SELECT count(*) AS n FROM read_zarr('partial.zarr') WHERE temperature = {FILL}"
    ).fetchall()[0][0]
    assert fills == ROWS - FILL_CHUNK


# --- what is not supported ----------------------------------------------------


def test_a_v2_store_is_refused_with_a_clear_message(con):
    """The reader finds a store by its `zarr.json`, which is v3's metadata file.

    A v2 store keeps `.zgroup` and `.zarray` instead. Refusing it is the right answer for a
    version the reader cannot read; the wrong answer would be to read it partially.
    """
    with pytest.raises(Exception) as refusal:
        con.sql("SELECT * FROM read_zarr('v2.zarr')").fetchall()
    message = str(refusal.value)
    assert "zarr" in message.lower()
    assert "v3" in message or "zarr.json" in message, f"got: {message}"


# --- external tables and a restart --------------------------------------------


def test_an_external_table_reads(con):
    con.execute("CREATE EXTERNAL TABLE obs STORED AS ZARR LOCATION 'flat.zarr'")
    assert con.sql("SELECT count(*) AS n FROM obs").fetchall() == [(ROWS,)]
    assert "obs" in con.list_tables()


def test_an_external_table_survives_a_restart(datasets, tmp_path):
    path = str(tmp_path / "restart.db")

    with beacondb.connect(path, datasets=str(datasets)) as con:
        con.execute("CREATE EXTERNAL TABLE obs STORED AS ZARR LOCATION 'flat.zarr'")
        con.execute("CREATE EXTERNAL TABLE grid STORED AS ZARR LOCATION 'grid.zarr'")

    with beacondb.connect(path, datasets=str(datasets)) as con:
        assert con.sql("SELECT count(*) AS n FROM obs").fetchall() == [(ROWS,)]
        assert con.sql("SELECT count(*) AS n FROM grid").fetchall() == [(GRID_ROWS,)]
        assert {"obs", "grid"} <= set(con.list_tables())
        rows = con.sql("SELECT depth, temperature FROM obs ORDER BY depth LIMIT 1").fetchall()
        assert rows == [(0.0, 5.0)]
