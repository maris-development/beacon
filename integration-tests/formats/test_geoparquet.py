"""GeoParquet, end to end, in one file.

Writes its own GeoParquet files with `geopandas` — points as WKB and as GeoArrow, a line, a
polygon, and a covering bounding box — opens an embedded Beacon over them, queries them, creates
an external table, reopens the database, and checks the table survived.

    pytest formats/test_geoparquet.py -v
"""

from __future__ import annotations

from pathlib import Path

import pytest

beacondb = pytest.importorskip("beacondb", reason="build it with maturin")
gpd = pytest.importorskip("geopandas", reason="pip install geopandas")
shapely = pytest.importorskip("shapely")

from shapely.geometry import LineString, Point, Polygon  # noqa: E402

#: (longitude, latitude) per station, spread out so a bounding box picks a real subset.
POINTS = [
    (-170.0, -80.0), (-150.0, -60.0), (-130.0, -40.0),
    (-30.0, -20.0), (-10.0, 10.0), (10.0, 20.0),
    (30.0, 40.0), (50.0, 50.0), (70.0, 60.0),
    (150.0, 70.0), (160.0, 75.0), (170.0, 80.0),
]
ROWS = len(POINTS)
BOX = (0.0, 0.0, 100.0, 100.0)
#: The stations inside that box: 5, 6, 7, 8.
IN_BOX = [i for i, (lon, lat) in enumerate(POINTS) if 0.0 <= lon <= 100.0 and 0.0 <= lat <= 100.0]


@pytest.fixture(scope="module")
def datasets(tmp_path_factory) -> Path:
    """Write every GeoParquet file this module queries."""
    root = tmp_path_factory.mktemp("geoparquet")
    points = gpd.GeoDataFrame(
        {
            "station": list(range(ROWS)),
            "reading": [i * 1.5 for i in range(ROWS)],
            "geometry": [Point(lon, lat) for lon, lat in POINTS],
        },
        crs="EPSG:4326",
    )

    # GeoParquet 1.0 stores a geometry as a WKB blob; 1.1 adds native GeoArrow.
    points.to_parquet(root / "wkb.parquet", geometry_encoding="WKB")
    points.to_parquet(root / "geoarrow.parquet", geometry_encoding="geoarrow")

    # A covering bounding box, so a reader can skip a row group, and the same file without one.
    points.to_parquet(root / "bbox.parquet", geometry_encoding="WKB",
                      write_covering_bbox=True, row_group_size=4)
    points.to_parquet(root / "no_bbox.parquet", geometry_encoding="WKB", row_group_size=4)

    # The same points with no coordinate reference system, which is the control for the
    # spatial-predicate behaviour below.
    gpd.GeoDataFrame(
        {"station": list(range(ROWS)), "geometry": [Point(lon, lat) for lon, lat in POINTS]}
    ).to_parquet(root / "no_crs.parquet", geometry_encoding="WKB")

    gpd.GeoDataFrame(
        {
            "route": list(range(4)),
            "geometry": [LineString([(i * 10.0, 0.0), (i * 10.0 + 5.0, 0.0)]) for i in range(4)],
        },
        crs="EPSG:4326",
    ).to_parquet(root / "line.parquet", geometry_encoding="WKB")

    gpd.GeoDataFrame(
        {
            "zone": list(range(4)),
            "geometry": [
                Polygon([(i * 20.0, 0.0), (i * 20.0 + 10.0, 0.0),
                         (i * 20.0 + 10.0, 10.0), (i * 20.0, 10.0), (i * 20.0, 0.0)])
                for i in range(4)
            ],
        },
        crs="EPSG:4326",
    ).to_parquet(root / "polygon.parquet", geometry_encoding="WKB")
    return root


@pytest.fixture
def con(datasets, tmp_path):
    with beacondb.connect(str(tmp_path / "beacon.db"), datasets=str(datasets)) as connection:
        yield connection


# --- reading ------------------------------------------------------------------


def test_a_file_reads(con):
    rows = con.sql(
        "SELECT station, reading FROM read_geoparquet('wkb.parquet') ORDER BY station LIMIT 2"
    ).fetchall()
    assert rows == [(0, 0.0), (1, 1.5)]


def test_count_star(con):
    assert con.sql("SELECT count(*) AS n FROM read_geoparquet('wkb.parquet')").fetchall() == [(ROWS,)]


def test_a_geometry_decodes_to_a_struct_of_coordinates(con):
    """Beacon decodes WKB into native GeoArrow, so the column is `struct<x, y>`."""
    table = con.sql(
        "SELECT geometry FROM read_geoparquet('wkb.parquet') ORDER BY station LIMIT 2"
    ).arrow()
    assert table.column("geometry").to_pylist() == [
        {"x": POINTS[0][0], "y": POINTS[0][1]},
        {"x": POINTS[1][0], "y": POINTS[1][1]},
    ]


def test_the_geoarrow_extension_name_survives(con):
    """The extension keys are what mark a column as a geometry; strip them and every spatial
    function refuses it."""
    for name in ("wkb.parquet", "geoarrow.parquet"):
        table = con.sql(f"SELECT * FROM read_geoparquet('{name}')").arrow()
        metadata = table.schema.field("geometry").metadata or {}
        assert metadata.get(b"ARROW:extension:name") == b"geoarrow.point", name


def test_wkb_and_geoarrow_read_alike(con):
    """The two GeoParquet encodings of one geometry must give one answer."""
    query = (
        "SELECT station, ST_X(geometry) x, ST_Y(geometry) y FROM read_geoparquet('{}') "
        "ORDER BY station"
    )
    assert con.sql(query.format("wkb.parquet")).fetchall() == con.sql(
        query.format("geoarrow.parquet")
    ).fetchall()


def test_st_x_and_st_y_return_the_coordinates(con):
    rows = con.sql(
        "SELECT ST_X(geometry) x, ST_Y(geometry) y FROM read_geoparquet('wkb.parquet') "
        "ORDER BY station"
    ).fetchall()
    assert rows == POINTS


def test_a_numeric_bounding_box_selects_the_right_stations(con):
    rows = con.sql(
        f"SELECT station FROM read_geoparquet('wkb.parquet') "
        f"WHERE ST_X(geometry) BETWEEN {BOX[0]} AND {BOX[2]} "
        f"AND ST_Y(geometry) BETWEEN {BOX[1]} AND {BOX[3]} ORDER BY station"
    ).fetchall()
    assert [r[0] for r in rows] == IN_BOX


def test_a_line_and_a_polygon_read(con):
    line = con.sql("SELECT geometry FROM read_geoparquet('line.parquet') ORDER BY route LIMIT 1").arrow()
    assert line.column("geometry").to_pylist() == [[{"x": 0.0, "y": 0.0}, {"x": 5.0, "y": 0.0}]]
    assert (line.schema.field("geometry").metadata or {}).get(b"ARROW:extension:name") == b"geoarrow.linestring"

    polygon = con.sql("SELECT geometry FROM read_geoparquet('polygon.parquet') ORDER BY zone LIMIT 1").arrow()
    ring = polygon.column("geometry").to_pylist()[0][0]
    assert len(ring) == 5, "the closing vertex must be kept"
    assert ring[0] == ring[-1] == {"x": 0.0, "y": 0.0}


def test_a_bbox_column_does_not_change_the_rows(con):
    """A covering box prunes row groups, and pruning must never change an answer."""
    predicate = (
        f"WHERE ST_X(geometry) BETWEEN {BOX[0]} AND {BOX[2]} "
        f"AND ST_Y(geometry) BETWEEN {BOX[1]} AND {BOX[3]} ORDER BY station"
    )
    with_box = con.sql(f"SELECT station FROM read_geoparquet('bbox.parquet') {predicate}").fetchall()
    without = con.sql(f"SELECT station FROM read_geoparquet('no_bbox.parquet') {predicate}").fetchall()
    assert with_box == without
    assert [r[0] for r in with_box] == IN_BOX


def test_the_bbox_column_holds_each_row_envelope(con):
    """The covering box is a real column, and the reader returns it: a point's box is the point."""
    table = con.sql("SELECT bbox FROM read_geoparquet('bbox.parquet') ORDER BY station LIMIT 1").arrow()
    lon, lat = POINTS[0]
    assert table.column("bbox").to_pylist() == [{"xmin": lon, "ymin": lat, "xmax": lon, "ymax": lat}]


# --- the coordinate reference system ------------------------------------------


def test_a_spatial_predicate_needs_a_crs_less_column(con):
    """A geometry predicate against a literal works only where the column has no CRS.

    The column carries a full PROJJSON coordinate reference system, a WKT literal carries none,
    and `ST_Intersects` refuses to mix them. `ST_GeomFromText(wkt, 4326)`, `ST_SetSRID(...)` and
    `ST_MakeEnvelope(..., 4326)` all fail to attach one, so the predicate cannot be written
    against a real GeoParquet file. `ST_X`/`ST_Y` work, which is the idiom available today.
    """
    box = f"POLYGON(({BOX[0]} {BOX[1]},{BOX[2]} {BOX[1]},{BOX[2]} {BOX[3]},{BOX[0]} {BOX[3]},{BOX[0]} {BOX[1]}))"

    # With no CRS on the column, the predicate answers correctly.
    rows = con.sql(
        f"SELECT station FROM read_geoparquet('no_crs.parquet') "
        f"WHERE ST_Intersects(geometry, ST_GeomFromText('{box}')) ORDER BY station"
    ).fetchall()
    assert [r[0] for r in rows] == IN_BOX

    # With a CRS, it is refused, and the message says why.
    with pytest.raises(Exception) as refusal:
        con.sql(
            f"SELECT station FROM read_geoparquet('wkb.parquet') "
            f"WHERE ST_Intersects(geometry, ST_GeomFromText('{box}'))"
        ).fetchall()
    assert "coordinate reference system" in str(refusal.value)


# --- external tables and a restart --------------------------------------------


def test_an_external_table_reads(con):
    con.execute("CREATE EXTERNAL TABLE stations STORED AS GEOPARQUET LOCATION 'wkb.parquet'")
    assert con.sql("SELECT count(*) AS n FROM stations").fetchall() == [(ROWS,)]
    assert "stations" in con.list_tables()


def test_an_external_table_survives_a_restart(datasets, tmp_path):
    path = str(tmp_path / "restart.db")

    with beacondb.connect(path, datasets=str(datasets)) as con:
        con.execute("CREATE EXTERNAL TABLE stations STORED AS GEOPARQUET LOCATION 'wkb.parquet'")
        con.execute("CREATE EXTERNAL TABLE zones STORED AS GEOPARQUET LOCATION 'polygon.parquet'")

    with beacondb.connect(path, datasets=str(datasets)) as con:
        assert con.sql("SELECT count(*) AS n FROM stations").fetchall() == [(ROWS,)]
        assert {"stations", "zones"} <= set(con.list_tables())
        # The geometry has to be usable through the table, not just present.
        rows = con.sql("SELECT ST_X(geometry) x FROM stations ORDER BY station LIMIT 1").fetchall()
        assert rows == [(POINTS[0][0],)]
        # And the extension metadata has to survive the restart.
        table = con.sql("SELECT * FROM stations").arrow()
        assert (table.schema.field("geometry").metadata or {}).get(b"ARROW:extension:name") == b"geoarrow.point"
