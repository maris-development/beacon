"""Spatial queries: GeoParquet I/O, the GeoJSON DSL filter, and the spatial functions.

The PostGIS-named function set comes from ``datafusion-spatial``: 117 scalar, 3
aggregate and 2 window functions. The second half of this file covers it. Those
tests build the geometry with ``ST_Point(longitude, latitude)``, because that is
the shape a netCDF, Zarr, CSV or Parquet table has. One test reads a GeoParquet
geometry column instead, and it is ``xfail`` today: the GeoParquet scan reports
the wrong position for every column after the first one.

Our generated observations carry ``longitude``/``latitude`` columns, so we can
exercise the spatial surface without hand-crafting GeoArrow geometry:

* the JSON-DSL GeoJSON filter (point-in-polygon over lon/lat columns), and
* GeoParquet OUTPUT round-tripped back through ``read_geoparquet`` (Beacon builds
  a geometry column from lon/lat on write and decodes it on read).

The polygon edges are placed on half-integers so no generated point (integer
lon/lat) lands on a boundary — keeping the expected count unambiguous.
"""

from __future__ import annotations

import io
import math

import pytest

# Axis-aligned box with half-integer edges; data lon/lat are integers.
LON_MIN, LON_MAX = 0.5, 50.5
LAT_MIN, LAT_MAX = -0.5, 40.5
WKT_BOX = (
    f"POLYGON (({LON_MIN} {LAT_MIN}, {LON_MAX} {LAT_MIN}, "
    f"{LON_MAX} {LAT_MAX}, {LON_MIN} {LAT_MAX}, {LON_MIN} {LAT_MIN}))"
)
GEOJSON_BOX = {
    "type": "Polygon",
    "coordinates": [
        [
            [LON_MIN, LAT_MIN],
            [LON_MAX, LAT_MIN],
            [LON_MAX, LAT_MAX],
            [LON_MIN, LAT_MAX],
            [LON_MIN, LAT_MIN],
        ]
    ],
}


def _expected_in_box(rows) -> int:
    return sum(
        1
        for r in rows
        if LON_MIN < r["longitude"] < LON_MAX and LAT_MIN < r["latitude"] < LAT_MAX
    )


def test_st_within_from_geojson(client, sample_data):
    """ST_GeomFromGeoJSON reads the request geometry, with no WKT step between."""
    import json

    geojson = json.dumps(GEOJSON_BOX)
    n = client.count(
        "SELECT * FROM read_parquet(['obs/*.parquet']) "
        f"WHERE ST_Within(ST_Point(longitude, latitude), ST_GeomFromGeoJSON('{geojson}'))"
    )
    expected = _expected_in_box(sample_data["rows"])
    assert expected > 0
    assert n == expected


def test_geojson_dsl_filter(client, sample_data):
    """The JSON DSL filter plans ST_Within(ST_Point(lon, lat), ST_GeomFromGeoJSON(...))."""
    rows = client.query_json_rows(
        {
            "from": {"parquet": {"paths": ["obs/*.parquet"]}},
            "select": ["longitude", "latitude"],
            "filter": {
                "longitude_column": "longitude",
                "latitude_column": "latitude",
                "geometry": GEOJSON_BOX,
            },
            "limit": 1_000_000,
        }
    )
    assert len(rows) - 1 == _expected_in_box(sample_data["rows"])


def test_geoparquet_output_and_readback(client, datasets_dir):
    """GeoParquet output carries a geometry column and reads back via read_geoparquet."""
    import pyarrow.parquet as pq

    src = (
        "SELECT longitude AS lon, latitude AS lat, temperature "
        "FROM read_parquet(['obs/*.parquet']) ORDER BY time LIMIT 100"
    )
    fmt = {"geoparquet": {"longitude_column": "lon", "latitude_column": "lat"}}

    resp = client.raw({"sql": src, "output": {"format": fmt}})
    assert resp.status_code == 200, resp.text
    assert resp.headers["Content-Type"] == "application/vnd.apache.arrow.geo+parquet"
    data = resp.content
    assert data[:4] == b"PAR1" and data[-4:] == b"PAR1"  # parquet magic

    table = pq.read_table(io.BytesIO(data))
    assert table.num_rows == 100
    assert "geometry" in table.column_names

    # Round-trip: write the produced file back into the mounted datasets dir and
    # read it through Beacon's GeoParquet reader.
    dst = datasets_dir / "geo" / "out.geoparquet"
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_bytes(data)
    assert client.count("SELECT * FROM read_geoparquet(['geo/out.geoparquet'])") == 100




# --------------------------------------------------------------------------
# The PostGIS-named function set (datafusion-spatial)
# --------------------------------------------------------------------------

#: A geometry column built from the two coordinate columns of the sample data.
#: A netCDF, Zarr, CSV or plain Parquet table holds coordinates, not geometry,
#: so this is the shape most Beacon queries have.
POINT = "ST_Point(longitude, latitude)"
OBS = "read_parquet(['obs/*.parquet'])"


def _distance_to_origin(row) -> float:
    return math.hypot(row["longitude"], row["latitude"])


def test_st_geomfromtext_and_st_astext(client):
    """Input and output: WKT in, WKT out."""
    text = client.scalar("SELECT ST_AsText(ST_GeomFromText('POINT(1 2)')) AS t")
    assert text.upper().startswith("POINT")
    assert "1" in text and "2" in text


def test_st_asgeojson(client):
    """ST_AsGeoJSON writes the GeoJSON form of a geometry."""
    import json

    parsed = json.loads(client.scalar("SELECT ST_AsGeoJSON(ST_GeomFromText('POINT(3 4)')) AS g"))
    assert parsed["type"] == "Point"
    assert parsed["coordinates"] == [3.0, 4.0]


def test_st_distance_of_two_literals(client):
    """Measurement is planar, so a 3-4-5 triangle gives 5."""
    assert float(client.scalar(
        "SELECT ST_Distance(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(3 4)')) AS d"
    )) == 5.0


def test_st_area_and_st_buffer(client):
    """A unit square covers 1. A buffer of radius 1 covers about pi."""
    area = float(client.scalar(
        "SELECT ST_Area(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))')) AS a"
    ))
    assert abs(area - 1.0) < 1e-9
    # The buffer is a polygon with a limited corner count, so it stays under the exact area.
    buffered = float(client.scalar(
        "SELECT ST_Area(ST_Buffer(ST_GeomFromText('POINT(0 0)'), 1.0)) AS a"
    ))
    assert 3.0 < buffered <= math.pi


def test_st_transform_reprojects(client):
    """ST_Transform links PROJ, and a standard build ships it.

    WGS 84 (4326) to Web Mercator (3857). The origin stays at the origin, and one
    degree of longitude becomes about 111 km.
    """
    text = client.scalar(
        "SELECT ST_AsText(ST_Transform(ST_SetSRID(ST_GeomFromText('POINT(1 0)'), 4326), 3857)) AS p"
    )
    assert text.upper().startswith("POINT")
    x = float(text.upper().replace("POINT", "").strip(" ()").split()[0])
    assert abs(x - 111_319.49) < 1.0


def test_st_centroid_of_a_square(client):
    """A processing function returns a geometry, so the next function reads it."""
    text = client.scalar(
        "SELECT ST_AsText(ST_Centroid(ST_GeomFromText('POLYGON((0 0,2 0,2 2,0 2,0 0))'))) AS c"
    )
    assert text.upper().startswith("POINT")
    assert "1" in text


def test_st_x_and_st_y_read_back_the_ordinates(client, sample_data):
    """ST_Point adopts the two coordinate buffers. The accessors return them."""
    rows = client.sql_rows(
        f"SELECT ST_X({POINT}) AS x, ST_Y({POINT}) AS y FROM {OBS} "
        "ORDER BY longitude, latitude LIMIT 5"
    )
    got = [(float(x), float(y)) for x, y in rows[1:]]
    want = sorted((r["longitude"], r["latitude"]) for r in sample_data["rows"])[:5]
    assert got == want


def test_st_within_a_polygon(client, sample_data):
    """A predicate over a geometry column agrees with the lon/lat filter."""
    n = client.count(
        f"SELECT * FROM {OBS} WHERE ST_Within({POINT}, ST_GeomFromText('{WKT_BOX}'))"
    )
    assert n == _expected_in_box(sample_data["rows"])


def test_st_intersects_a_polygon(client, sample_data):
    """ST_Intersects answers the same question for a point argument."""
    n = client.count(
        f"SELECT * FROM {OBS} WHERE ST_Intersects({POINT}, ST_GeomFromText('{WKT_BOX}'))"
    )
    assert n == _expected_in_box(sample_data["rows"])


def test_st_distance_over_a_table(client, sample_data):
    """ST_Distance runs per row against a constant geometry."""
    n = client.count(
        f"SELECT * FROM {OBS} "
        "WHERE ST_Distance(ST_Point(longitude, latitude), ST_GeomFromText('POINT(0 0)')) < 100"
    )
    expected = sum(1 for r in sample_data["rows"] if _distance_to_origin(r) < 100)
    assert expected > 0
    assert n == expected


def test_st_dwithin_takes_a_constant_radius(client, sample_data):
    """ST_DWithin needs a constant radius. It grows the box that drives the prefilter."""
    n = client.count(
        f"SELECT * FROM {OBS} WHERE ST_DWithin({POINT}, ST_GeomFromText('POINT(0 0)'), 50.0)"
    )
    expected = sum(1 for r in sample_data["rows"] if _distance_to_origin(r) <= 50.0)
    assert n == expected


def test_st_extent_aggregate(client, sample_data):
    """ST_Extent is an aggregate. Its box answers the four ordinate accessors."""
    rows = client.sql_rows(
        f"SELECT ST_XMin(ST_Extent({POINT})) AS x0, ST_XMax(ST_Extent({POINT})) AS x1, "
        f"ST_YMin(ST_Extent({POINT})) AS y0, ST_YMax(ST_Extent({POINT})) AS y1 FROM {OBS}"
    )
    x0, x1, y0, y1 = (float(v) for v in rows[1])
    data = sample_data["rows"]
    assert x0 == min(r["longitude"] for r in data)
    assert x1 == max(r["longitude"] for r in data)
    assert y0 == min(r["latitude"] for r in data)
    assert y1 == max(r["latitude"] for r in data)


def test_st_collect_aggregate(client, sample_data):
    """ST_Collect gathers every row into one collection."""
    kind = client.scalar(f"SELECT ST_GeometryType(ST_Collect({POINT})) AS t FROM {OBS}")
    assert kind == "ST_GeometryCollection"
    points = client.scalar(f"SELECT ST_NPoints(ST_Collect({POINT})) AS n FROM {OBS}")
    assert int(points) == sample_data["total"]


def test_st_memunion_aggregate(client):
    """The one-argument PostGIS ST_Union is ST_MemUnion here. One name cannot serve both."""
    area = float(client.scalar(
        f"SELECT ST_Area(ST_MemUnion(ST_Buffer({POINT}, 0.5))) AS a FROM {OBS}"
    ))
    assert area > 0.0


def test_one_argument_st_union_is_an_error(client):
    """The scalar registry answers first, so the aggregate cannot share the name."""
    assert client.status(f"SELECT ST_Union({POINT}) FROM {OBS}") >= 400


def test_st_clusterkmeans_window(client, sample_data):
    """A cluster function is a window function, as in PostGIS. It needs OVER ()."""
    rows = client.sql_rows(f"SELECT ST_ClusterKMeans({POINT}, 3) OVER () AS cluster FROM {OBS}")
    assignments = [r[0] for r in rows[1:]]
    assert len(assignments) == sample_data["total"]
    assert len(set(assignments)) == 3


def test_st_clusterdbscan_window(client, sample_data):
    """DBSCAN groups by density. Noise returns null, so it returns a row per input row."""
    rows = client.sql_rows(
        f"SELECT ST_ClusterDBSCAN({POINT}, 10.0, 3) OVER () AS cluster FROM {OBS}"
    )
    assert len(rows) - 1 == sample_data["total"]


def test_spatial_constructors_are_listed(client):
    """The function catalog holds the spatial functions that take plain arguments.

    ``SHOW FUNCTIONS`` reads ``information_schema.parameters``, and that view holds one
    row per argument. A function that takes a geometry uses ``Signature::any``, which
    states no example argument types, so it gets no row there and drops out of the
    join. A function that takes numbers or text states its types, and appears. See
    ``test_geometry_functions_are_absent_from_the_catalog``.
    """
    resp = client.get("/api/functions")
    assert resp.status_code == 200
    names = {f["function_name"].lower() for f in resp.json()}
    for name in ["st_point", "st_makepoint", "st_geomfromtext", "st_geomfromgeojson"]:
        assert name in names, f"{name} is absent from the function catalog"
    # Beacon carries no geospatial UDF of its own. `st_within_point` and `st_geojson_as_wkt`
    # were removed; the PostGIS-named set states every spatial test now.
    assert "st_within_point" not in names
    assert "st_geojson_as_wkt" not in names


@pytest.mark.xfail(
    reason="robinskil/datafusion-spatial#1. A function that takes a geometry uses "
           "Signature::any, which states no example argument types. It therefore gets no "
           "information_schema.parameters row, and SHOW FUNCTIONS reads that view. Every one "
           "of these functions runs; none of them is discoverable from SQL.",
    strict=False,
)
def test_geometry_functions_are_absent_from_the_catalog(client):
    """One name from each group: scalar, aggregate and window."""
    names = {f["function_name"].lower() for f in client.get("/api/functions").json()}
    for name in [
        "st_distance",
        "st_intersects",
        "st_buffer",
        "st_extent",
        "st_collect",
        "st_clusterkmeans",
        # PROJ is on by default, so a standard build ships the one function that links it.
        "st_transform",
    ]:
        assert name in names, f"{name} is absent from the function catalog"


def test_spatial_functions_over_a_geoparquet_geometry_column(client, datasets_dir):
    """The spatial functions read a GeoParquet geometry column directly."""
    src = (
        "SELECT longitude AS lon, latitude AS lat, temperature "
        "FROM read_parquet(['obs/*.parquet']) ORDER BY time LIMIT 200"
    )
    fmt = {"geoparquet": {"longitude_column": "lon", "latitude_column": "lat"}}
    resp = client.raw({"sql": src, "output": {"format": fmt}})
    assert resp.status_code == 200, resp.text
    dst = datasets_dir / "geo" / "points.geoparquet"
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_bytes(resp.content)

    table = "read_geoparquet(['geo/points.geoparquet'])"
    assert client.count(f"SELECT * FROM {table}") == 200
    n = client.count(
        f"SELECT * FROM {table} WHERE ST_Distance(geometry, ST_GeomFromText('POINT(0 0)')) < 100"
    )
    assert n > 0

    # The scalar, aggregate and window groups each read the geometry column.
    assert client.scalar(
        f"SELECT ST_AsText(ST_Extent(geometry)) AS extent FROM {table}"
    ).startswith("POLYGON")
    assert client.scalar(
        f"SELECT ST_GeometryType(ST_Collect(geometry)) AS t FROM {table}"
    )
    assert (
        client.count(f"SELECT ST_ClusterKMeans(geometry, 3) OVER () AS k FROM {table}")
        == 200
    )


def test_a_geoparquet_column_after_the_first_reads_correctly(client, datasets_dir):
    """Issue #378: the scan kept a column's old position after a projection.

    The writer appends `geometry` to the columns it was given, so the file holds
    `lon, lat, temperature, geometry`. This reads every position past the first:
    a plain column on its own, an alias over it, and a filter on a later column.
    Each one is a query the old scan answered with a 400, a dropped connection or
    a column of NULLs.
    """
    if not (datasets_dir / "geo" / "points.geoparquet").exists():
        pytest.skip("the GeoParquet fixture is written by the spatial function test")

    table = "read_geoparquet(['geo/points.geoparquet'])"

    # `temperature` is the third column, and the alias must carry its values.
    plain = client.sql_rows(f"SELECT temperature FROM {table} ORDER BY temperature")
    aliased = client.sql_rows(f"SELECT temperature AS t FROM {table} ORDER BY t")
    assert plain[0] == ["temperature"] and aliased[0] == ["t"]
    assert plain[1:] == aliased[1:] and len(plain) == 201

    # `lat` is the second column. Filtering on it must agree with the count the
    # same predicate gives over the plain-Parquet source the file was built from.
    lat_rows = client.count(f"SELECT * FROM {table} WHERE lat > 0")
    assert lat_rows == client.count(
        "SELECT * FROM ("
        "SELECT latitude AS lat FROM read_parquet(['obs/*.parquet']) "
        "ORDER BY time LIMIT 200"
        ") WHERE lat > 0"
    )
