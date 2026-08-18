"""netCDF, end to end, in one file.

Writes its own netCDF files, opens an embedded Beacon over them, queries them, creates an
external table, reopens the database, and checks the table survived.

    pytest formats/test_netcdf.py -v

Needs `netCDF4` and the `beacondb` extension. Both are skipped cleanly if absent.
"""

from __future__ import annotations

from pathlib import Path

import pytest

beacondb = pytest.importorskip("beacondb", reason="build it with maturin")
pytest.importorskip("netCDF4", reason="pip install netCDF4")

import numpy as np  # noqa: E402
from netCDF4 import Dataset  # noqa: E402

#: netCDF4 1.7.4 sets `.shape` on a numpy array, which numpy 2.5 deprecated. It comes from
#: inside the library on every write and there is no writer call that avoids it.
pytestmark = pytest.mark.filterwarnings(
    "ignore:Setting the shape on a NumPy array has been deprecated"
)

ROWS = 20
TIME, LAT, LON = 3, 4, 5
GRID_ROWS = TIME * LAT * LON  # 60
FILL = -999.0


@pytest.fixture(scope="module")
def datasets(tmp_path_factory) -> Path:
    """Write every netCDF file this module queries."""
    root = tmp_path_factory.mktemp("netcdf")

    # A flat table: one observation dimension.
    with Dataset(root / "point.nc", "w", format="NETCDF4") as ds:
        ds.createDimension("obs", ROWS)
        ds.createVariable("temperature", "f4", ("obs",))[:] = np.arange(ROWS) * 0.5 + 5.0
        ds.createVariable("depth", "f4", ("obs",))[:] = np.arange(ROWS) * 10.0

    # The same data as netCDF-3, so the two on-disk formats can be compared.
    with Dataset(root / "classic.nc", "w", format="NETCDF3_CLASSIC") as ds:
        ds.createDimension("obs", ROWS)
        ds.createVariable("temperature", "f4", ("obs",))[:] = np.arange(ROWS) * 0.5 + 5.0
        ds.createVariable("depth", "f4", ("obs",))[:] = np.arange(ROWS) * 10.0

    # A grid over time, latitude and longitude.
    with Dataset(root / "grid.nc", "w", format="NETCDF4") as ds:
        ds.createDimension("time", TIME)
        ds.createDimension("latitude", LAT)
        ds.createDimension("longitude", LON)
        ds.createVariable("time", "i4", ("time",))[:] = np.arange(TIME)
        ds.createVariable("latitude", "f4", ("latitude",))[:] = np.arange(LAT) * 10.0 - 60.0
        ds.createVariable("longitude", "f4", ("longitude",))[:] = np.arange(LON) * 30.0 - 180.0
        ds.createVariable("temperature", "f4", ("time", "latitude", "longitude"))[:] = (
            np.arange(GRID_ROWS, dtype="f4").reshape(TIME, LAT, LON)
        )

    # A fill value, which must read as a null rather than as -999.
    with Dataset(root / "fill.nc", "w", format="NETCDF4") as ds:
        ds.createDimension("obs", ROWS)
        temp = ds.createVariable("temperature", "f4", ("obs",), fill_value=FILL)
        temp.set_auto_maskandscale(False)  # write the sentinel, not a masked array
        values = np.arange(ROWS, dtype="f4") * 0.5 + 5.0
        values[::5] = FILL  # rows 0, 5, 10, 15
        temp[:] = values

    # A packed variable: the value is raw * scale_factor + add_offset.
    with Dataset(root / "packed.nc", "w", format="NETCDF4") as ds:
        ds.createDimension("obs", ROWS)
        temp = ds.createVariable("temperature", "i2", ("obs",))
        temp.scale_factor = 0.01
        temp.add_offset = 10.0
        temp.set_auto_maskandscale(False)
        temp[:] = np.arange(ROWS, dtype="i2") * 100

    # A CF time axis, so the epoch can be checked.
    with Dataset(root / "cftime.nc", "w", format="NETCDF4") as ds:
        ds.createDimension("time", ROWS)
        time = ds.createVariable("time", "i4", ("time",))
        time.units = "days since 2020-01-01 00:00:00"
        time.calendar = "standard"
        time.set_auto_maskandscale(False)
        time[:] = np.arange(ROWS)
        ds.createVariable("temperature", "f4", ("time",))[:] = np.arange(ROWS) * 0.25 + 9.0

    # Two files with the same variables, for a glob.
    for index, start in enumerate((0.0, 100.0)):
        with Dataset(root / f"part-{index}.nc", "w", format="NETCDF4") as ds:
            ds.createDimension("obs", 5)
            ds.createVariable("depth", "f4", ("obs",))[:] = np.arange(5) + start
    return root


@pytest.fixture
def con(datasets, tmp_path):
    """An embedded Beacon over those files."""
    with beacondb.connect(str(tmp_path / "beacon.db"), datasets=str(datasets)) as connection:
        yield connection


# --- reading ------------------------------------------------------------------


def test_a_flat_file_reads(con):
    rows = con.sql("SELECT depth, temperature FROM read_netcdf('point.nc') ORDER BY depth").fetchall()
    assert len(rows) == ROWS
    assert rows[0] == (0.0, 5.0)
    assert rows[1] == (10.0, 5.5)


def test_the_schema_is_reported(con):
    relation = con.sql("SELECT * FROM read_netcdf('point.nc')")
    assert relation.columns == ["depth", "temperature"]
    assert relation.types == ["Float32", "Float32"]


def test_count_star(con):
    assert con.sql("SELECT count(*) AS n FROM read_netcdf('point.nc')").fetchall() == [(ROWS,)]


def test_a_filter(con):
    rows = con.sql("SELECT depth FROM read_netcdf('point.nc') WHERE depth >= 100").fetchall()
    assert len(rows) == ROWS - 10


def test_aggregates(con):
    got = con.sql(
        "SELECT min(temperature) lo, max(temperature) hi, count(*) n "
        "FROM read_netcdf('point.nc')"
    ).fetchall()[0]
    assert got == (5.0, 5.0 + (ROWS - 1) * 0.5, ROWS)


def test_order_by_with_limit(con):
    rows = con.sql(
        "SELECT depth FROM read_netcdf('point.nc') ORDER BY depth DESC LIMIT 3"
    ).fetchall()
    assert rows == [(190.0,), (180.0,), (170.0,)]


def test_netcdf3_and_netcdf4_read_alike(con):
    """The same data in the two on-disk formats must give the same rows."""
    order = "ORDER BY depth"
    four = con.sql(f"SELECT depth, temperature FROM read_netcdf('point.nc') {order}").fetchall()
    three = con.sql(f"SELECT depth, temperature FROM read_netcdf('classic.nc') {order}").fetchall()
    assert four == three


# --- the grid -----------------------------------------------------------------


def test_a_grid_broadcasts_to_one_row_per_cell(con):
    """3 x 4 x 5 dimensions, so 60 rows, each carrying its own coordinates."""
    assert con.sql("SELECT count(*) AS n FROM read_netcdf('grid.nc')").fetchall() == [(GRID_ROWS,)]

    rows = con.sql(
        "SELECT time, latitude, longitude, temperature FROM read_netcdf('grid.nc') "
        "ORDER BY temperature"
    ).fetchall()
    assert len(rows) == GRID_ROWS
    # Every cell has its own coordinate triple.
    assert len({(t, la, lo) for t, la, lo, _ in rows}) == GRID_ROWS
    assert rows[0] == (0, -60.0, -180.0, 0.0)


def test_projecting_a_grid_coordinate_narrows_the_grid(con):
    """An nd projection re-infers the grid, so a coordinate alone returns its own length."""
    assert len(con.sql("SELECT latitude FROM read_netcdf('grid.nc')").fetchall()) == LAT
    assert len(con.sql("SELECT temperature FROM read_netcdf('grid.nc')").fetchall()) == GRID_ROWS


# --- CF conventions -----------------------------------------------------------


def test_a_fill_value_reads_as_null(con):
    total, present = con.sql(
        "SELECT count(*), count(temperature) FROM read_netcdf('fill.nc')"
    ).fetchall()[0]
    assert total == ROWS
    assert present == ROWS - 4, "rows 0, 5, 10 and 15 hold the fill value"

    leaked = con.sql(
        f"SELECT count(*) AS n FROM read_netcdf('fill.nc') WHERE temperature = {FILL}"
    ).fetchall()[0][0]
    assert leaked == 0, "the sentinel must not appear as a number"


def test_a_packed_variable_is_unpacked(con):
    """raw * scale_factor + add_offset, in that order: 0, 100, 200 -> 10.0, 11.0, 12.0."""
    rows = con.sql(
        "SELECT temperature FROM read_netcdf('packed.nc') ORDER BY temperature LIMIT 3"
    ).fetchall()
    assert rows == [(10.0,), (11.0,), (12.0,)]


def test_cf_time_decodes_against_its_epoch(con):
    rows = con.sql(
        "SELECT time FROM read_netcdf('cftime.nc') ORDER BY time LIMIT 2"
    ).fetchall()
    assert [str(r[0])[:10] for r in rows] == ["2020-01-01", "2020-01-02"]


# --- many files ---------------------------------------------------------------


def test_a_glob_reads_every_file(con):
    total = con.sql("SELECT count(*) AS n FROM read_netcdf('part-*.nc')").fetchall()[0][0]
    assert total == 10, "two files of five rows"


def test_a_glob_column_order_is_stable(con):
    """Five runs of one query over a file set: the column order must not change (#377)."""
    orders = {
        tuple(con.sql("SELECT * FROM read_netcdf('part-*.nc')").columns) for _ in range(5)
    }
    assert len(orders) == 1, f"the column order changed between runs: {orders}"


# --- external tables and a restart --------------------------------------------


def test_an_external_table_reads(con):
    con.execute("CREATE EXTERNAL TABLE obs STORED AS NC LOCATION 'point.nc'")
    assert con.sql("SELECT count(*) AS n FROM obs").fetchall() == [(ROWS,)]
    assert "obs" in con.list_tables()


def test_an_external_table_survives_a_restart(datasets, tmp_path):
    """Create the table, close the database, open it again: the definition is still there."""
    path = str(tmp_path / "restart.db")

    with beacondb.connect(path, datasets=str(datasets)) as con:
        con.execute("CREATE EXTERNAL TABLE obs STORED AS NC LOCATION 'point.nc'")
        con.execute("CREATE EXTERNAL TABLE grid STORED AS NC LOCATION 'grid.nc'")
        before = con.sql("SELECT count(*) AS n FROM obs").fetchall()

    # Reopened: a different connection to the same file.
    with beacondb.connect(path, datasets=str(datasets)) as con:
        assert con.sql("SELECT count(*) AS n FROM obs").fetchall() == before
        assert con.sql("SELECT count(*) AS n FROM grid").fetchall() == [(GRID_ROWS,)]
        assert {"obs", "grid"} <= set(con.list_tables())
        rows = con.sql("SELECT depth, temperature FROM obs ORDER BY depth LIMIT 2").fetchall()
        assert rows == [(0.0, 5.0), (10.0, 5.5)]


def test_an_external_table_over_a_glob_survives_a_restart(datasets, tmp_path):
    path = str(tmp_path / "glob.db")
    with beacondb.connect(path, datasets=str(datasets)) as con:
        con.execute("CREATE EXTERNAL TABLE parts STORED AS NC LOCATION 'part-*.nc'")
        assert con.sql("SELECT count(*) AS n FROM parts").fetchall() == [(10,)]

    with beacondb.connect(path, datasets=str(datasets)) as con:
        assert con.sql("SELECT count(*) AS n FROM parts").fetchall() == [(10,)]
