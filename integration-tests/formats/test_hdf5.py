"""HDF5, end to end, in one file.

Writes its own HDF5 files with `h5py` — plain HDF5, carrying no netCDF convention — opens an
embedded Beacon over them, queries them, creates an external table, reopens the database, and
checks the table survived.

    pytest formats/test_hdf5.py -v
"""

from __future__ import annotations

from pathlib import Path

import pytest

beacondb = pytest.importorskip("beacondb", reason="build it with maturin")
h5py = pytest.importorskip("h5py", reason="pip install h5py")
np = pytest.importorskip("numpy")

ROWS = 20
STATIONS = 3


@pytest.fixture(scope="module")
def datasets(tmp_path_factory) -> Path:
    """Write every HDF5 file this module queries."""
    root = tmp_path_factory.mktemp("hdf5")
    temperature = np.arange(ROWS, dtype="float64") * 0.5 + 4.0
    depth = np.arange(ROWS, dtype="float64") * 10.0

    with h5py.File(root / "plain.h5", "w") as f:
        f.create_dataset("temperature", data=temperature)
        f.create_dataset("depth", data=depth)

    # Chunked and gzip-compressed: the layout a large dataset ships in.
    with h5py.File(root / "compressed.h5", "w") as f:
        f.create_dataset("temperature", data=temperature, chunks=(5,), compression="gzip")
        f.create_dataset("depth", data=depth, chunks=(5,), compression="gzip")

    # A dimension scale, which is the HDF5 mechanism netCDF-4 is built on.
    with h5py.File(root / "scales.h5", "w") as f:
        axis = f.create_dataset("depth", data=depth)
        values = f.create_dataset("temperature", data=temperature)
        axis.make_scale("depth")
        values.dims[0].attach_scale(axis)

    # Datasets two group levels deep. netcdf-c reports only the root group, so this is the
    # pure-Rust reader's alone.
    with h5py.File(root / "groups.h5", "w") as f:
        f.create_dataset("station_id", data=np.arange(STATIONS, dtype="i4"))
        inner = f.create_group("observations")
        inner.create_dataset("salinity", data=np.arange(STATIONS, dtype="f8") + 33.0)

    # A compound dataset: rows of structs, flattened to one column per member.
    compound = np.zeros(STATIONS, dtype=[("station", "i4"), ("label", "S8"), ("reading", "f8")])
    for index in range(STATIONS):
        compound[index] = (index + 1, [b"alpha", b"bravo", b"charlie"][index], 1.5 * (index + 1))
    with h5py.File(root / "compound.h5", "w") as f:
        f.create_dataset("measurements", data=compound)

    # A variable-length string, HDF5's own text type.
    with h5py.File(root / "strings.h5", "w") as f:
        f.create_dataset("station_id", data=np.arange(STATIONS, dtype="i4"))
        f.create_dataset(
            "station_name",
            data=np.array(["ALPHA", "BRAVO", "CHARLIE"], dtype=h5py.string_dtype()),
        )

    parts = root / "parts"
    parts.mkdir()
    for index in range(2):
        with h5py.File(parts / f"part-{index}.h5", "w") as f:
            f.create_dataset("depth", data=np.arange(5, dtype="f8") + index * 100)
    return root


@pytest.fixture
def con(datasets, tmp_path):
    with beacondb.connect(str(tmp_path / "beacon.db"), datasets=str(datasets)) as connection:
        yield connection


# --- reading ------------------------------------------------------------------


def test_a_file_reads(con):
    rows = con.sql("SELECT depth, temperature FROM read_hdf5('plain.h5') ORDER BY depth LIMIT 2").fetchall()
    assert rows == [(0.0, 4.0), (10.0, 4.5)]


def test_the_schema_is_reported(con):
    relation = con.sql("SELECT * FROM read_hdf5('plain.h5')")
    assert relation.columns == ["depth", "temperature"]
    assert relation.types == ["Float64", "Float64"]


def test_count_star(con):
    assert con.sql("SELECT count(*) AS n FROM read_hdf5('plain.h5')").fetchall() == [(ROWS,)]


def test_a_filter_and_an_aggregate(con):
    got = con.sql(
        "SELECT count(*) n, max(temperature) hi FROM read_hdf5('plain.h5') WHERE depth >= 100"
    ).fetchall()[0]
    assert got == (ROWS - 10, 4.0 + (ROWS - 1) * 0.5)


def test_chunking_and_compression_are_invisible(con):
    order = "ORDER BY depth"
    plain = con.sql(f"SELECT depth, temperature FROM read_hdf5('plain.h5') {order}").fetchall()
    packed = con.sql(f"SELECT depth, temperature FROM read_hdf5('compressed.h5') {order}").fetchall()
    assert plain == packed


def test_a_dimension_scale_does_not_change_the_table(con):
    order = "ORDER BY depth"
    plain = con.sql(f"SELECT depth, temperature FROM read_hdf5('plain.h5') {order}").fetchall()
    scaled = con.sql(f"SELECT depth, temperature FROM read_hdf5('scales.h5') {order}").fetchall()
    assert plain == scaled


# --- what only the pure-Rust reader does --------------------------------------


def test_a_nested_group_is_read(con):
    """netcdf-c reports only the root group, so a group variable would be missing there.

    The column keeps its group path: `observations/salinity`.
    """
    relation = con.sql("SELECT * FROM read_hdf5('groups.h5')")
    assert "observations/salinity" in relation.columns

    rows = con.sql(
        'SELECT station_id, "observations/salinity" FROM read_hdf5(\'groups.h5\') ORDER BY station_id'
    ).fetchall()
    assert rows == [(0, 33.0), (1, 34.0), (2, 35.0)]


def test_a_compound_dataset_becomes_one_column_per_member(con):
    rows = con.sql(
        'SELECT "measurements/station", "measurements/label", "measurements/reading" '
        "FROM read_hdf5('compound.h5') ORDER BY \"measurements/station\""
    ).fetchall()
    assert rows == [(1, "alpha", 1.5), (2, "bravo", 3.0), (3, "charlie", 4.5)]


def test_a_variable_length_string_reads_whole(con):
    rows = con.sql(
        "SELECT station_name FROM read_hdf5('strings.h5') ORDER BY station_id"
    ).fetchall()
    assert rows == [("ALPHA",), ("BRAVO",), ("CHARLIE",)], "a string must not be split per character"


# --- many files ---------------------------------------------------------------


def test_a_glob_reads_every_file(con):
    assert con.sql("SELECT count(*) AS n FROM read_hdf5('parts/*.h5')").fetchall() == [(10,)]


# --- external tables and a restart --------------------------------------------


def test_an_external_table_reads(con):
    con.execute("CREATE EXTERNAL TABLE obs STORED AS HDF5 LOCATION 'plain.h5'")
    assert con.sql("SELECT count(*) AS n FROM obs").fetchall() == [(ROWS,)]
    assert "obs" in con.list_tables()


def test_the_h5_alias_also_works(con):
    con.execute("CREATE EXTERNAL TABLE obs_h5 STORED AS H5 LOCATION 'plain.h5'")
    assert con.sql("SELECT count(*) AS n FROM obs_h5").fetchall() == [(ROWS,)]


def test_an_external_table_survives_a_restart(datasets, tmp_path):
    path = str(tmp_path / "restart.db")

    with beacondb.connect(path, datasets=str(datasets)) as con:
        con.execute("CREATE EXTERNAL TABLE obs STORED AS HDF5 LOCATION 'plain.h5'")
        con.execute("CREATE EXTERNAL TABLE grouped STORED AS HDF5 LOCATION 'groups.h5'")

    with beacondb.connect(path, datasets=str(datasets)) as con:
        assert con.sql("SELECT count(*) AS n FROM obs").fetchall() == [(ROWS,)]
        assert {"obs", "grouped"} <= set(con.list_tables())
        rows = con.sql("SELECT depth, temperature FROM obs ORDER BY depth LIMIT 1").fetchall()
        assert rows == [(0.0, 4.0)]
        # The group column has to survive the restart too, not just the table name.
        assert "observations/salinity" in con.sql("SELECT * FROM grouped").columns
