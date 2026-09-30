"""TIFF, end to end, in one file.

Writes its own GeoTIFFs with `rasterio` — one band, many bands, tiled and stripped — opens an
embedded Beacon over them, queries them, creates an external table, reopens the database, and
checks the table survived. A second part reads many layouts (compression, predictor, byte order,
BigTIFF, planar bands, nodata, sparse blocks, COG) and compares every pixel with GDAL. A third part
reads collections: globs, mixed types, partitions, JSON queries and broken files.

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

import rasterio.shutil  # noqa: E402
from rasterio.transform import from_origin  # noqa: E402
from rasterio.windows import Window  # noqa: E402

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
    # The transform puts the top-left corner at (0, 3) with a 1-degree pixel. The coordinates are
    # pixel centers, the same as CF, so the first row of pixels sits at latitude 2.5 and walks east.
    assert rows == [(0.0, 2.5, 0.5), (1.0, 2.5, 1.5), (2.0, 2.5, 2.5), (3.0, 2.5, 3.5)]


def test_both_raster_types_give_pixel_centers(tmp_path):
    """A PixelIsArea and a PixelIsPoint copy of one grid get the same coordinates (issue #525)."""
    root = tmp_path / "raster_types"
    root.mkdir()
    for name, area_or_point in (("area.tif", "Area"), ("point.tif", "Point")):
        _write(root / name, 1)
        with rasterio.open(root / name, "r+") as dst:
            dst.update_tags(AREA_OR_POINT=area_or_point)

    with beacondb.connect(str(tmp_path / "types.db"), datasets=str(root)) as con:
        for name in ("area.tif", "point.tif"):
            rows = con.sql(
                f'SELECT "geo.lon", "geo.lat" FROM read_tiff(\'{name}\') ORDER BY "band.0" LIMIT 1'
            ).fetchall()
            assert rows == [(0.5, 2.5)], name


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
        assert rows == [(0.0, 2.5)]
        # Every band has to survive, not just the first.
        assert "band.2" in con.sql("SELECT * FROM multi").columns


# --- layouts, compared with GDAL -------------------------------------------------------------
#
# Each file below is read by Beacon and by GDAL (rasterio). Beacon's rows are put back on the
# pixel grid through geo.lon/geo.lat, and every value and every NULL must match GDAL's masked read.

LAYOUTS = {
    "deflate_predictor2_tiled_int16": dict(dtype="int16", tiled=True, compress="deflate",
                                           predictor=2),
    "lzw_float_predictor_stripped_f32": dict(dtype="float32", compress="lzw", predictor=3,
                                             blockysize=5),
    "zstd_tiled_uint32": dict(dtype="uint32", tiled=True, compress="zstd"),
    "packbits_stripped_uint8": dict(dtype="uint8", compress="packbits", blockysize=4),
    "big_endian_tiled_f64": dict(dtype="float64", tiled=True, ENDIANNESS="BIG",
                                 compress="deflate", predictor=3),
    "big_endian_stripped_int32": dict(dtype="int32", ENDIANNESS="BIG", blockysize=7),
    "bigtiff_tiled_uint16": dict(dtype="uint16", tiled=True, BIGTIFF="YES"),
    "planar_three_bands_int16": dict(dtype="int16", count=3, interleave="band", tiled=True),
    "pixel_three_bands_uint8": dict(dtype="uint8", count=3, interleave="pixel",
                                    compress="deflate", blockysize=6),
    "nodata_int16": dict(dtype="int16", nodata=-9999),
    "nodata_nan_f32": dict(dtype="float32", nodata=float("nan"), tiled=True),
    "sparse_tiled_int16": dict(dtype="int16", nodata=-1, tiled=True, sparse=True),
    "sparse_stripped_uint16": dict(dtype="uint16", blockysize=16, sparse=True),
    # The fixture copies this one with the COG driver: deflate, 16 x 16 tiles, overviews.
    "cog_with_overviews_f32": dict(driver="COG", dtype="float32"),
}
LAYOUT_HEIGHT, LAYOUT_WIDTH = 37, 50  # Not a multiple of the 16 x 16 tile: edge tiles are cut.


def _write_layout(path: Path, spec: dict) -> None:
    spec = dict(spec)
    count = spec.pop("count", 1)
    sparse = spec.pop("sparse", False)
    nodata = spec.get("nodata")
    if spec.get("tiled"):
        spec.update(blockxsize=16, blockysize=16)
    if sparse:
        spec["SPARSE_OK"] = "TRUE"
    rng = np.random.default_rng(len(path.name))
    with rasterio.open(
        path, "w", driver=spec.pop("driver", "GTiff"), height=LAYOUT_HEIGHT, width=LAYOUT_WIDTH,
        count=count, crs="EPSG:4326", transform=from_origin(10, 60, 0.25, 0.25), **spec
    ) as dst:
        for band in range(1, count + 1):
            if np.dtype(spec["dtype"]).kind == "f":
                values = rng.standard_normal((LAYOUT_HEIGHT, LAYOUT_WIDTH)) * 1000
            else:
                values = rng.integers(0, 250, (LAYOUT_HEIGHT, LAYOUT_WIDTH))
            values = values.astype(spec["dtype"])
            if nodata is not None:
                values[::4, ::3] = nodata
            if sparse:
                # Write two tiles only. GDAL leaves the other blocks out of the file.
                dst.write(values[:16, :16], band, window=Window(0, 0, 16, 16))
                dst.write(values[16:32, 32:48], band, window=Window(32, 16, 16, 16))
            else:
                dst.write(values, band)


@pytest.fixture(scope="module")
def layouts(tmp_path_factory) -> Path:
    root = tmp_path_factory.mktemp("tiff_layouts")
    for name, spec in LAYOUTS.items():
        if spec.get("driver") == "COG":
            # The COG driver copies a finished file, so write a plain one first.
            _write_layout(root / f"{name}.src.tif", dict(dtype=spec["dtype"]))
            with rasterio.open(root / f"{name}.src.tif") as src:
                rasterio.shutil.copy(src, root / f"{name}.tif", driver="COG",
                                     COMPRESS="DEFLATE", BLOCKSIZE=16, OVERVIEWS="AUTO")
            (root / f"{name}.src.tif").unlink()
        else:
            _write_layout(root / f"{name}.tif", spec)
    return root


def _assert_matches_gdal(con, root: Path, name: str) -> None:
    table = con.sql(f"SELECT * FROM read_tiff('{name}')").arrow()
    with rasterio.open(root / name) as src:
        want = src.read(masked=True)
        inverse = ~src.transform
        height, width = src.height, src.width
    assert table.num_rows == height * width, name

    lon = table.column("geo.lon").to_numpy()
    lat = table.column("geo.lat").to_numpy()
    col_f, row_f = inverse * (lon, lat)
    # A pixel center sits at .5 in pixel space.
    assert np.allclose(col_f % 1, 0.5) and np.allclose(row_f % 1, 0.5), f"{name}: not centers"
    order = np.argsort(np.floor(row_f).astype(int) * width + np.floor(col_f).astype(int))

    for band in range(want.shape[0]):
        column = table.column(f"band.{band}")
        got_null = column.is_null().to_numpy(zero_copy_only=False)[order]
        got = column.fill_null(0).to_numpy()[order]
        want_null = np.ma.getmaskarray(want[band]).ravel()
        assert (got_null == want_null).all(), f"{name} band.{band}: NULLs differ"
        np.testing.assert_array_equal(
            got[~got_null], want[band].compressed(), err_msg=f"{name} band.{band}")


@pytest.mark.parametrize("layout", sorted(LAYOUTS))
def test_a_layout_reads_like_gdal(layouts, tmp_path, layout):
    with beacondb.connect(str(tmp_path / "layouts.db"), datasets=str(layouts)) as con:
        _assert_matches_gdal(con, layouts, f"{layout}.tif")


def test_a_sparse_file_counts_every_pixel(layouts, tmp_path):
    """Issue #523: a sparse block reads as nodata. GDAL leaves 10 of the 12 tiles out of the file."""
    with beacondb.connect(str(tmp_path / "sparse.db"), datasets=str(layouts)) as con:
        got = con.sql(
            'SELECT count(*), count("band.0") FROM read_tiff(\'sparse_tiled_int16.tif\')'
        ).fetchall()
    # Two 16 x 16 tiles hold data. Their nodata cells are NULL too.
    with rasterio.open(layouts / "sparse_tiled_int16.tif") as src:
        present = int(src.read(1, masked=True).count())
    assert got == [(LAYOUT_HEIGHT * LAYOUT_WIDTH, present)]


# --- many files --------------------------------------------------------------------------------


def _write_tile(path: Path, x0: float, dtype: str = "int16", count: int = 1, **options) -> None:
    """A 6 x 5 raster at longitude x0, so the files of one glob do not overlap."""
    path.parent.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(int(x0))
    with rasterio.open(
        path, "w", driver="GTiff", height=5, width=6, count=count, dtype=dtype,
        crs="EPSG:4326", transform=from_origin(x0, 50, 1, 1), **options
    ) as dst:
        for band in range(1, count + 1):
            dst.write(rng.integers(0, 100, (5, 6)).astype(dtype), band)


def _gdal_rows(path: Path) -> set[tuple]:
    """(lon, lat, band values...) of every pixel, with NULL for a masked value."""
    with rasterio.open(path) as src:
        data = src.read(masked=True)
        rows, cols = np.mgrid[0:src.height, 0:src.width]
        lon, lat = src.transform * (cols.ravel() + 0.5, rows.ravel() + 0.5)
        values = [[None if v is np.ma.masked else float(v) for v in band.ravel()] for band in data]
    return {(float(x), float(y), *(band[i] for band in values)) for i, (x, y) in enumerate(zip(lon, lat))}


def _beacon_rows(con, query: str, bands: int) -> set[tuple]:
    table = con.sql(query).arrow().to_pylist()
    return {
        (row["geo.lon"], row["geo.lat"],
         *(None if row[f"band.{b}"] is None else float(row[f"band.{b}"]) for b in range(bands)))
        for row in table
    }


@pytest.fixture(scope="module")
def collection(tmp_path_factory) -> Path:
    root = tmp_path_factory.mktemp("tiff_collection")
    for i in range(4):
        _write_tile(root / "mosaic" / f"m{i}.tif", x0=i * 6, nodata=-1,
                    tiled=i % 2 == 0, blockxsize=16, blockysize=16)
    _write_tile(root / "deep" / "a" / "b" / "one.tif", x0=100)
    _write_tile(root / "deep" / "two.tif", x0=106)
    _write_tile(root / "mixed" / "int16.tif", x0=200, dtype="int16")
    _write_tile(root / "mixed" / "float32_three_bands.tif", x0=206, dtype="float32", count=3)
    _write_tile(root / "mixed" / "big_endian.tif", x0=212, dtype="uint8", ENDIANNESS="BIG")
    for region, x0 in (("north", 300), ("south", 306)):
        for year in (2023, 2024):
            _write_tile(root / "hive" / f"region={region}" / f"year={year}" / "r.tif",
                        x0=x0 + (year - 2023) * 100)
    _write_tile(root / "broken" / "good.tif", x0=500)
    (root / "broken" / "truncated.tif").write_bytes(
        (root / "broken" / "good.tif").read_bytes()[:200])
    return root


@pytest.fixture
def many(collection, tmp_path):
    with beacondb.connect(str(tmp_path / "many.db"), datasets=str(collection)) as connection:
        yield connection


def test_a_glob_reads_every_pixel_of_every_file(many, collection):
    want = set().union(*(_gdal_rows(p) for p in sorted((collection / "mosaic").glob("*.tif"))))
    got = _beacon_rows(many, "SELECT * FROM read_tiff('mosaic/*.tif')", 1)
    assert got == want
    assert len(got) == 4 * 30


def test_a_list_of_paths_and_a_recursive_glob(many, collection):
    listed = _beacon_rows(many, "SELECT * FROM read_tiff(['mosaic/m0.tif', 'mosaic/m3.tif'])", 1)
    assert listed == _gdal_rows(collection / "mosaic" / "m0.tif") | _gdal_rows(
        collection / "mosaic" / "m3.tif")
    deep = _beacon_rows(many, "SELECT * FROM read_tiff('deep/**/*.tif')", 1)
    assert deep == _gdal_rows(collection / "deep" / "a" / "b" / "one.tif") | _gdal_rows(
        collection / "deep" / "two.tif")


def test_files_of_other_types_and_band_counts_share_one_table(many, collection):
    relation = many.sql("SELECT * FROM read_tiff('mixed/*.tif')")
    types = dict(zip(relation.columns, relation.types))
    assert {"band.0", "band.1", "band.2"} <= set(types)
    got = _beacon_rows(many, "SELECT * FROM read_tiff('mixed/*.tif')", 3)
    want = set()
    for path in sorted((collection / "mixed").glob("*.tif")):
        # A file with fewer bands gets NULL in the bands it lacks.
        want |= {row + (None,) * (5 - len(row)) for row in _gdal_rows(path)}
    assert got == want


def test_a_filter_and_an_aggregate_over_a_glob(many, collection):
    rows = set().union(*(_gdal_rows(p) for p in (collection / "mosaic").glob("*.tif")))
    kept = [r for r in rows if 5 < r[0] < 19 and r[1] > 47 and r[2] is not None]
    got = many.sql(
        'SELECT count(*), sum("band.0") FROM read_tiff(\'mosaic/*.tif\') '
        'WHERE "geo.lon" > 5 AND "geo.lon" < 19 AND "geo.lat" > 47 AND "band.0" IS NOT NULL'
    ).fetchall()
    assert got == [(len(kept), sum(r[2] for r in kept))]


def test_a_broken_file_is_named_in_the_error(many):
    with pytest.raises(Exception, match="truncated.tif"):
        many.sql("SELECT count(*) FROM read_tiff('broken/*.tif')").fetchall()


def test_an_external_table_over_a_directory_replace_and_drop(many):
    many.execute("CREATE EXTERNAL TABLE mosaic STORED AS TIFF LOCATION 'mosaic/'")
    assert many.sql("SELECT count(*) FROM mosaic").fetchall() == [(4 * 30,)]
    many.execute("CREATE OR REPLACE EXTERNAL TABLE mosaic STORED AS TIFF LOCATION 'deep/**/*.tif'")
    assert many.sql("SELECT count(*) FROM mosaic").fetchall() == [(2 * 30,)]
    many.execute("DROP TABLE mosaic")
    assert "mosaic" not in many.list_tables()


def test_a_partitioned_table_takes_its_values_from_the_path(many):
    many.execute(
        "CREATE EXTERNAL TABLE hive STORED AS TIFF LOCATION 'hive/' PARTITIONED BY (region, year)")
    # A partition column is dictionary encoded, so the rows are read through Arrow.
    def grouped(aggregate: str) -> list[tuple]:
        rows = many.sql(
            f"SELECT region, year, {aggregate} AS v FROM hive GROUP BY region, year "
            "ORDER BY region, year"
        ).arrow().to_pylist()
        return [(r["region"], str(r["year"]), r["v"]) for r in rows]

    assert grouped("count(*)") == [
        ("north", "2023", 30), ("north", "2024", 30), ("south", "2023", 30), ("south", "2024", 30)]
    # Each partition holds its own file: the smallest longitude tells which one.
    assert grouped('min("geo.lon")') == [
        ("north", "2023", 300.5), ("north", "2024", 400.5),
        ("south", "2023", 306.5), ("south", "2024", 406.5)]
    one = many.sql("SELECT count(*) FROM hive WHERE region = 'south' AND year = '2024'").fetchall()
    assert one == [(30,)]


def test_a_json_query_reads_a_glob_with_a_filter(many):
    result = many.json_query({
        "from": {"tiff": {"paths": ["mosaic/*.tif"]}},
        "select": ["geo.lon", "geo.lat", "band.0"],
        "filters": [{"column": "geo.lon", "min": 6, "max": 12}],
    })
    lons = result.arrow().column("geo.lon").to_pylist()
    assert len(lons) == 6 * 5 and all(6 <= x <= 12 for x in lons)


def test_a_coordinate_alone_narrows_the_grid(many):
    """The documented rule: the grid comes from the columns a query uses.

    `geo.lon` spans only the x axis, so a filter on it alone counts columns of pixels. A band
    column, or geo.lon with geo.lat, spans the image and keeps one row per pixel.
    """
    base = "FROM read_tiff('mosaic/m0.tif') WHERE \"geo.lon\" > -1000"
    assert many.sql(f"SELECT count(*) {base}").fetchall() == [(6,)]
    assert many.sql(f'SELECT count(*), max("band.0") {base}').fetchall()[0][0] == 30
    assert many.sql(f'SELECT count(*) {base} AND "geo.lat" > -1000').fetchall() == [(30,)]
