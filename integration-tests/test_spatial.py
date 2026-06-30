"""Spatial queries: GeoParquet I/O, the GeoJSON DSL filter, and geo UDFs.

Our generated observations carry ``longitude``/``latitude`` columns, so we can
exercise the spatial surface without hand-crafting GeoArrow geometry:

* ``st_geojson_as_wkt`` and ``st_within_point`` scalar functions,
* the JSON-DSL GeoJSON filter (point-in-polygon over lon/lat columns), and
* GeoParquet OUTPUT round-tripped back through ``read_geoparquet`` (Beacon builds
  a geometry column from lon/lat on write and decodes it on read).

The polygon edges are placed on half-integers so no generated point (integer
lon/lat) lands on a boundary — keeping the expected count unambiguous.
"""

from __future__ import annotations

import io

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


def test_st_geojson_as_wkt(client):
    wkt = client.scalar(
        "SELECT st_geojson_as_wkt('{\"type\":\"Point\",\"coordinates\":[1.0,2.0]}') AS w"
    )
    assert wkt.upper().startswith("POINT")
    assert "1" in wkt and "2" in wkt


def test_st_within_point_filter(client, sample_data):
    n = client.count(
        "SELECT * FROM read_parquet(['obs/*.parquet']) "
        f"WHERE st_within_point('{WKT_BOX}', longitude, latitude)"
    )
    expected = _expected_in_box(sample_data["rows"])
    assert expected > 0
    assert n == expected


def test_st_within_point_with_geojson_wkt(client, sample_data):
    """st_within_point composed with st_geojson_as_wkt yields the same result."""
    import json

    geojson = json.dumps(GEOJSON_BOX)
    n = client.count(
        "SELECT * FROM read_parquet(['obs/*.parquet']) "
        f"WHERE st_within_point(st_geojson_as_wkt('{geojson}'), longitude, latitude)"
    )
    assert n == _expected_in_box(sample_data["rows"])


def test_geojson_dsl_filter(client, sample_data):
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
