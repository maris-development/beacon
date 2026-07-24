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
    assert "read_csv" in names


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
