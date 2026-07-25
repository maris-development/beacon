"""Tests for the catalog-driven readers and the file sinks.

The readers are not hand-written methods — `con.read_parquet(...)` resolves through
`__getattr__` against `beacon.system.table_functions`, so these tests also pin that a real
catalog function is reachable and a bogus name is not. The sinks go through the engine's
output-format path (the same one the HTTP API uses), so a round-trip read of what a sink wrote
is the sharpest end-to-end check.
"""

from __future__ import annotations

import pytest

import beacondb


@pytest.fixture
def con():
    with beacondb.connect(":memory:") as connection:
        yield connection


@pytest.fixture
def parquet_file(con, tmp_path):
    """A small Parquet fixture written by the engine itself."""
    path = str(tmp_path / "data.parquet")
    con.sql(
        "SELECT * FROM (VALUES (1,'a',1.5),(2,'b',2.5),(3,'a',3.5)) AS t(id, g, x)"
    ).to_parquet(path)
    return path


# ----------------------------------------------------------------------------------------
# Catalog-driven readers
# ----------------------------------------------------------------------------------------


def test_table_functions_are_discovered_from_the_catalog(con):
    names = con.table_functions()
    assert "read_parquet" in names
    assert "read_netcdf" in names
    assert "read_hdf5" in names
    assert "read_csv" in names


def test_read_hdf5(con, tmp_path):
    # netCDF-4 files are HDF5, and netCDF-c also opens plain HDF5 files, so `read_hdf5` is the
    # same reader under an HDF5-friendly name. Prove it on a genuine (non-netCDF) HDF5 file.
    h5py = pytest.importorskip("h5py")
    import numpy as np

    path = str(tmp_path / "plain.h5")
    with h5py.File(path, "w") as f:
        f.create_dataset("temperature", data=np.array([10.0, 20.0, 30.0]))
        f.create_dataset("depth", data=np.array([0.0, 50.0, 100.0]))

    rel = con.read_hdf5(path)
    assert rel.columns == ["depth", "temperature"]
    assert rel.fetchall() == [(0.0, 10.0), (50.0, 20.0), (100.0, 30.0)]
    # composes like any reader, and the _schema counterpart exists
    assert con.read_hdf5(path).filter("depth <= 50").order("depth desc").fetchall() == [
        (50.0, 20.0),
        (0.0, 10.0),
    ]
    assert [row[0] for row in con.read_hdf5_schema(path).fetchall()] == ["depth", "temperature"]


def test_hdf5_external_table(tmp_path):
    # `CREATE EXTERNAL TABLE ... STORED AS {H5,HDF5}` resolves to the netCDF reader, and the
    # `.h5`/`.hdf5` extensions are recognized (so a glob works too).
    h5py = pytest.importorskip("h5py")
    import numpy as np

    for name in ("a.h5", "b.h5"):
        with h5py.File(tmp_path / name, "w") as f:
            f.create_dataset("temperature", data=np.array([10.0, 20.0, 30.0]))
            f.create_dataset("depth", data=np.array([0.0, 50.0, 100.0]))

    con = beacondb.connect(":memory:", datasets=str(tmp_path))
    con.execute("CREATE EXTERNAL TABLE one STORED AS H5 LOCATION 'a.h5'")
    assert con.sql("SELECT count(*) FROM one").fetchall() == [(3,)]

    con.execute("CREATE EXTERNAL TABLE both STORED AS HDF5 LOCATION '*.h5'")
    assert con.sql("SELECT count(*) FROM both").fetchall() == [(6,)]
    con.close()


def test_reader_attributes_exist_only_for_real_table_functions(con):
    assert hasattr(con, "read_parquet")
    assert hasattr(con, "read_netcdf")
    assert not hasattr(con, "read_nonsense")
    # A dunder probe must not be resolved against the catalog.
    assert not hasattr(con, "__wrapped__")


def test_read_parquet_returns_a_lazy_relation(con, parquet_file):
    rel = con.read_parquet(parquet_file)
    assert isinstance(rel, beacondb.Relation)
    # Reading is lazy: the reader call is in the SQL, nothing has run.
    assert "read_parquet(" in rel.sql
    assert rel.columns == ["id", "g", "x"]
    assert rel.fetchall() == [(1, "a", 1.5), (2, "b", 2.5), (3, "a", 3.5)]


def test_a_reader_composes_like_any_relation(con, parquet_file):
    rel = con.read_parquet(parquet_file).filter("g = 'a'").order("x desc").limit(1)
    # order + limit fold onto the reader's select — a correct top-1.
    assert rel.sql.endswith("ORDER BY x desc LIMIT 1")
    assert rel.fetchall() == [(3, "a", 3.5)]


def test_generic_read_escape_hatch(con, parquet_file):
    assert con.read("read_parquet", parquet_file).count().fetchall() == [(3,)]


def test_schema_reader_is_free_via_the_catalog(con, parquet_file):
    # `read_parquet_schema` is just another table function, so it works with no extra code.
    schema = con.read_parquet_schema(parquet_file).fetchall()
    assert [row[0] for row in schema] == ["id", "g", "x"]


def test_bad_reader_argument_is_refused_not_stringified(con):
    with pytest.raises(beacondb.ProgrammingError, match="table function"):
        con.read_parquet({"not": "a path"})


def test_reader_argument_quoting_is_injection_safe(con, tmp_path):
    # A path containing a quote must be escaped, not break out of the literal.
    weird = str(tmp_path / "o'brien.parquet")
    con.sql("SELECT 1 AS a").to_parquet(weird)
    assert con.read_parquet(weird).fetchall() == [(1,)]


# ----------------------------------------------------------------------------------------
# Reader keyword options
# ----------------------------------------------------------------------------------------


def test_reader_columns_keyword_projects(con, parquet_file):
    # `columns=` is universal: it projects the named columns rather than being passed to the
    # reader, and the result composes further like any relation.
    rel = con.read_parquet(parquet_file, columns=["id", "x"])
    assert rel.columns == ["id", "x"]
    assert rel.fetchall() == [(1, 1.5), (2, 2.5), (3, 3.5)]
    assert rel.filter("id = 2").fetchall() == [(2, 2.5)]
    # a single column name (not a list) is accepted too
    assert con.read_parquet(parquet_file, columns="g").columns == ["g"]


def test_reader_keyword_option_fills_a_named_positional_slot(con, tmp_path):
    # read_csv's declared parameters are [glob_paths, delimiter, infer_records]; passing
    # `delimiter=";"` by name fills the second slot without positional counting.
    path = tmp_path / "semi.csv"
    path.write_text("a;b\n1;z\n2;y\n")
    # The default delimiter is a comma, so the whole row is one column until the option is set.
    assert con.read_csv(str(path)).columns == ["a;b"]
    rel = con.read_csv(str(path), delimiter=";")
    assert rel.columns == ["a", "b"]
    assert rel.fetchall() == [(1, "z"), (2, "y")]


def test_reader_unknown_keyword_option_is_refused(con, parquet_file):
    with pytest.raises(beacondb.ProgrammingError, match="no option"):
        con.read_parquet(parquet_file, bogus=1)


def test_reader_keyword_conflicting_with_positional_is_refused(con, tmp_path):
    path = tmp_path / "semi.csv"
    path.write_text("a;b\n1;z\n")
    # The delimiter is given both positionally (slot 1) and by keyword — a mistake, not a merge.
    with pytest.raises(beacondb.ProgrammingError, match="more than once"):
        con.read_csv(str(path), ";", delimiter=";")


# ----------------------------------------------------------------------------------------
# File sinks
# ----------------------------------------------------------------------------------------


def test_parquet_round_trip(con, tmp_path):
    path = str(tmp_path / "rt.parquet")
    con.sql("SELECT 7 AS n, 'x' AS s").to_parquet(path)
    assert con.read_parquet(path).fetchall() == [(7, "x")]


def test_csv_round_trip(con, tmp_path):
    path = str(tmp_path / "rt.csv")
    con.sql("SELECT 10 AS a, 'z' AS b").to_csv(path)
    assert con.read_csv(path).fetchall() == [(10, "z")]


def test_arrow_ipc_round_trip(con, tmp_path):
    path = str(tmp_path / "rt.arrow")
    con.sql("SELECT 1 AS a UNION ALL SELECT 2").to_arrow_ipc(path)
    assert sorted(con.read_arrow(path).fetchall()) == [(1,), (2,)]


def test_netcdf_sink_writes_a_real_file(con, tmp_path):
    path = tmp_path / "out.nc"
    con.sql("SELECT 1.0 AS temperature, 2.0 AS depth").to_netcdf(str(path))
    assert path.exists() and path.stat().st_size > 0
    # It is a genuine NetCDF/HDF5 file (magic bytes), not an empty placeholder.
    assert path.read_bytes()[:4] in (b"\x89HDF", b"CDF\x01", b"CDF\x02")


def test_hdf5_sink_writes_a_real_hdf5_file(con, tmp_path):
    # A NetCDF-4 file *is* HDF5, so to_hdf5 is the flat NetCDF writer under an HDF5 name.
    path = tmp_path / "out.h5"
    con.sql("SELECT 1.0 AS temperature, 2.0 AS depth").to_hdf5(str(path))
    assert path.exists() and path.read_bytes()[:4] == b"\x89HDF"

    # It is genuine HDF5: h5py opens it and finds the columns as datasets.
    h5py = pytest.importorskip("h5py")
    with h5py.File(str(path), "r") as f:
        assert "temperature" in f and "depth" in f


def test_hdf5_and_netcdf_sinks_are_the_same_writer(con, tmp_path):
    # to_hdf5 and to_netcdf produce identical output — HDF5 is the NetCDF-4 container.
    query = "SELECT 1.0 AS a, 2.0 AS b"
    h5, nc = tmp_path / "x.h5", tmp_path / "x.nc"
    con.sql(query).to_hdf5(str(h5))
    con.sql(query).to_netcdf(str(nc))
    assert h5.read_bytes() == nc.read_bytes()


def test_nd_netcdf_requires_dimensions(con, tmp_path):
    with pytest.raises(beacondb.ProgrammingError, match="dimension"):
        con.sql("SELECT 1.0 AS v").to_nd_netcdf(str(tmp_path / "x.nc"), [])


def test_nd_netcdf_sink(con, tmp_path):
    path = tmp_path / "grid.nc"
    con.sql(
        "SELECT * FROM (VALUES (0.0, 1.0), (1.0, 2.0)) AS t(depth, value)"
    ).to_nd_netcdf(str(path), ["depth"])
    assert path.exists() and path.stat().st_size > 0


def test_geoparquet_sink_round_trips_with_a_geometry_column(con, tmp_path):
    path = str(tmp_path / "points.parquet")
    con.sql("SELECT 4.5 AS lon, 52.0 AS lat, 'nl' AS name").to_geoparquet(
        path, longitude="lon", latitude="lat"
    )
    back = con.read_geoparquet(path)
    # The geometry column is added on write and reads back as a native GeoArrow point struct.
    assert back.columns == ["lon", "lat", "name", "geometry"]
    assert back.types[-1].startswith("Struct")
    # The non-geometry columns survive unchanged.
    assert con.sql(f"SELECT lon, lat, name FROM read_geoparquet('{path}')").fetchall() == [
        (4.5, 52.0, "nl")
    ]


def test_geoparquet_sink_auto_detects_lon_lat(con, tmp_path):
    path = str(tmp_path / "auto.parquet")
    con.sql("SELECT 1.0 AS longitude, 2.0 AS latitude, 99 AS v").to_geoparquet(path)
    assert "geometry" in con.read_geoparquet(path).columns


def test_geoparquet_sink_handles_multiple_rows(con, tmp_path):
    path = str(tmp_path / "many.parquet")
    con.sql(
        "SELECT * FROM (VALUES (1.0,2.0),(3.0,4.0),(5.0,6.0)) AS t(lon, lat)"
    ).to_geoparquet(path, longitude="lon", latitude="lat")
    assert con.read_geoparquet(path).count().fetchall() == [(3,)]


def test_sink_to_url_scheme_is_refused(con):
    with pytest.raises(beacondb.NotSupportedError, match="s3"):
        con.sql("SELECT 1").to_parquet("s3://bucket/x.parquet")


# ----------------------------------------------------------------------------------------
# ODV sink
# ----------------------------------------------------------------------------------------


@pytest.fixture
def ocean_rel(con):
    """A tiny oceanographic relation: position + depth + time + a flagged data column."""
    return con.sql(
        """
        SELECT * FROM (VALUES
          (1.0, 51.0, 0.0,  TIMESTAMP '2020-01-01 00:00:00', 'A', 10.0, 1),
          (1.0, 51.0, 10.0, TIMESTAMP '2020-01-01 00:00:00', 'A', 11.0, 1),
          (2.0, 52.0, 0.0,  TIMESTAMP '2020-01-02 00:00:00', 'B', 12.0, 1)
        ) AS t(lon, lat, depth, time, cruise, temperature, temperature_qf)
        """
    )


def test_to_odv_writes_a_valid_archive(ocean_rel, tmp_path):
    import zipfile

    path = str(tmp_path / "out.zip")
    ocean_rel.to_odv(
        path, longitude="lon", latitude="lat", depth="depth", time="time", key="cruise"
    )
    assert zipfile.is_zipfile(path)
    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        # ODV splits by feature type; profiles carry our depth series.
        assert "profile.txt" in names
        profile = archive.read("profile.txt").decode("utf-8", "replace")
    # a SEADATANET-schema ODV spreadsheet with our data column and both cruises
    assert "SEADATANET" in profile
    assert "temperature" in profile.lower()
    assert sum(1 for line in profile.splitlines() if line.startswith(("A", "B"))) == 3


def test_to_odv_qf_schema_override_is_applied(ocean_rel, tmp_path):
    import zipfile

    path = str(tmp_path / "argo.zip")
    ocean_rel.to_odv(
        path,
        longitude="lon",
        latitude="lat",
        depth="depth",
        time="time",
        key="cruise",
        qf_schema="ARGO",
    )
    with zipfile.ZipFile(path) as archive:
        profile = archive.read("profile.txt").decode("utf-8", "replace")
    assert 'qf_schema="ARGO"' in profile


def test_to_odv_missing_mapped_column_errors_clearly(ocean_rel, tmp_path):
    # Without a mapping, ODV looks for its default column names (e.g. "Cruise"), which this data
    # does not have — a clear schema error, not a silent empty file.
    path = str(tmp_path / "bad.zip")
    with pytest.raises(beacondb.ProgrammingError, match="Cruise"):
        ocean_rel.to_odv(path)
