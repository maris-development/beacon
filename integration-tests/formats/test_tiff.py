"""TIFF, end to end, in one file.

Writes its own GeoTIFFs with `rasterio` — one band, many bands, tiled and stripped — opens an
embedded Beacon over them, queries them, creates an external table, reopens the database, and
checks the table survived.

    pytest formats/test_tiff.py -v

A raster reads as one row per pixel: `band.0` holds the value and `geo.lat`/`geo.lon` hold that
pixel's coordinates, beside the `image.*` and `geo.*` columns from the file's tags.
"""

from __future__ import annotations

from pathlib import Path

import pytest

beacondb = pytest.importorskip("beacondb", reason="build it with maturin")
rasterio = pytest.importorskip("rasterio", reason="pip install rasterio")
np = pytest.importorskip("numpy")

from rasterio.transform import from_origin  # noqa: E402

WIDTH, HEIGHT = 4, 3
PIXELS = WIDTH * HEIGHT  # 12
BANDS = 3


def _write(path: Path, bands: int, **options) -> None:
    """A raster whose pixel values are 0, 1, 2, ... per band."""
    with rasterio.open(
        path, "w", driver="GTiff", height=HEIGHT, width=WIDTH, count=bands,
        dtype="float32", crs="EPSG:4326", transform=from_origin(0, HEIGHT, 1, 1), **options
    ) as dst:
        for band in range(1, bands + 1):
            values = np.arange(PIXELS, dtype="float32").reshape(HEIGHT, WIDTH)
            dst.write(values + (band - 1) * 100.0, band)


@pytest.fixture(scope="module")
def datasets(tmp_path_factory) -> Path:
    """Write every TIFF this module queries."""
    root = tmp_path_factory.mktemp("tiff")
    _write(root / "one_band.tif", 1)
    _write(root / "many_bands.tif", BANDS)
    # Tiled and stripped are the two layouts raster data ships in. A tile must be at least
    # 16x16, so the tiled file is larger than the others.
    with rasterio.open(
        root / "tiled.tif", "w", driver="GTiff", height=32, width=32, count=1,
        dtype="float32", crs="EPSG:4326", transform=from_origin(0, 32, 1, 1),
        tiled=True, blockxsize=16, blockysize=16,
    ) as dst:
        dst.write(np.arange(32 * 32, dtype="float32").reshape(32, 32), 1)
    with rasterio.open(
        root / "stripped.tif", "w", driver="GTiff", height=32, width=32, count=1,
        dtype="float32", crs="EPSG:4326", transform=from_origin(0, 32, 1, 1),
        tiled=False,
    ) as dst:
        dst.write(np.arange(32 * 32, dtype="float32").reshape(32, 32), 1)
    return root


@pytest.fixture
def con(datasets, tmp_path):
    with beacondb.connect(str(tmp_path / "beacon.db"), datasets=str(datasets)) as connection:
        yield connection


# --- reading ------------------------------------------------------------------


def test_a_raster_reads_one_row_per_pixel(con):
    n = con.sql("SELECT count(*) AS n FROM read_tiff('one_band.tif')").fetchall()
    assert n == [(PIXELS,)], f"{WIDTH} x {HEIGHT} pixels"


def test_a_pixel_carries_its_value_and_its_coordinates(con):
    rows = con.sql(
        'SELECT "band.0", "geo.lat", "geo.lon" FROM read_tiff(\'one_band.tif\') LIMIT 4'
    ).fetchall()
    # The transform puts the origin at (0, 3) with a 1-degree pixel, so the first row of pixels
    # sits at latitude 3 and walks east.
    assert rows == [(0.0, 3.0, 0.0), (1.0, 3.0, 1.0), (2.0, 3.0, 2.0), (3.0, 3.0, 3.0)]


def test_the_image_tags_are_columns(con):
    relation = con.sql("SELECT * FROM read_tiff('one_band.tif')")
    for column in ("band.0", "geo.lat", "geo.lon", "image.width", "image.height", "geo.epsg"):
        assert column in relation.columns, column

    rows = con.sql(
        'SELECT "image.width", "image.height", "geo.epsg" FROM read_tiff(\'one_band.tif\') LIMIT 1'
    ).fetchall()
    assert rows == [(WIDTH, HEIGHT, 4326)]


def test_a_filter_and_an_aggregate(con):
    got = con.sql(
        'SELECT count(*) n, min("band.0") lo, max("band.0") hi FROM read_tiff(\'one_band.tif\') '
        'WHERE "band.0" >= 6.0'
    ).fetchall()[0]
    assert got == (PIXELS - 6, 6.0, float(PIXELS - 1))


def test_many_bands_become_many_columns(con):
    """Each band is a column of its own, over the same pixel grid."""
    relation = con.sql("SELECT * FROM read_tiff('many_bands.tif')")
    for band in range(BANDS):
        assert f"band.{band}" in relation.columns

    rows = con.sql(
        'SELECT "band.0", "band.1", "band.2" FROM read_tiff(\'many_bands.tif\') LIMIT 1'
    ).fetchall()
    assert rows == [(0.0, 100.0, 200.0)], "band n is offset by n * 100"
    assert len(relation.fetchall()) == PIXELS, "the bands share one pixel grid"


def test_tiled_and_stripped_read_alike(con):
    """A tile layout and a strip layout are storage decisions, not data."""
    query = 'SELECT "band.0" FROM read_tiff(\'{}\') ORDER BY "band.0"'
    tiled = con.sql(query.format("tiled.tif")).fetchall()
    stripped = con.sql(query.format("stripped.tif")).fetchall()
    assert tiled == stripped
    assert len(tiled) == 32 * 32


def test_a_glob_reads_every_file(con):
    """The two 32x32 files hold 1024 pixels each."""
    n = con.sql("SELECT count(*) AS n FROM read_tiff('*ed.tif')").fetchall()
    assert n == [(2 * 32 * 32,)], "tiled.tif and stripped.tif"


# --- external tables and a restart --------------------------------------------


def test_an_external_table_reads(con):
    con.execute("CREATE EXTERNAL TABLE raster STORED AS TIFF LOCATION 'one_band.tif'")
    assert con.sql("SELECT count(*) AS n FROM raster").fetchall() == [(PIXELS,)]
    assert "raster" in con.list_tables()


def test_an_external_table_survives_a_restart(datasets, tmp_path):
    path = str(tmp_path / "restart.db")

    with beacondb.connect(path, datasets=str(datasets)) as con:
        con.execute("CREATE EXTERNAL TABLE raster STORED AS TIFF LOCATION 'one_band.tif'")
        con.execute("CREATE EXTERNAL TABLE multi STORED AS TIFF LOCATION 'many_bands.tif'")

    with beacondb.connect(path, datasets=str(datasets)) as con:
        assert con.sql("SELECT count(*) AS n FROM raster").fetchall() == [(PIXELS,)]
        assert {"raster", "multi"} <= set(con.list_tables())
        rows = con.sql('SELECT "band.0", "geo.lat" FROM raster LIMIT 1').fetchall()
        assert rows == [(0.0, 3.0)]
        # Every band has to survive, not just the first.
        assert "band.2" in con.sql("SELECT * FROM multi").columns
