"""Generate the small GeoTIFF fixtures for the block reader tests.

Run from this folder: `python gen_block_fixtures.py`. Needs rasterio and numpy.
Each pixel value follows a formula, so the Rust tests can check every value.
"""

import numpy as np
import rasterio
from rasterio.transform import from_origin
from rasterio.windows import Window

BASE = dict(driver="GTiff", crs="EPSG:28992", transform=from_origin(0, 64, 1, 1))


def pattern(height, width, dtype, band=0):
    rows, cols = np.mgrid[0:height, 0:width]
    return (rows * 100 + cols + band * 10000).astype(dtype)


def sparse_tiled(path, nodata, **extra):
    # 64x64 int16, 16x16 tiles. Only tiles (0,0) and (2,1) get data; GDAL skips the rest.
    profile = dict(BASE, width=64, height=64, count=1, dtype="int16", tiled=True,
                   blockxsize=16, blockysize=16, compress="none", **extra)
    if nodata is not None:
        profile["nodata"] = nodata
    with rasterio.open(path, "w", **profile, SPARSE_OK="TRUE") as d:
        d.write(np.full((16, 16), 7, "int16"), 1, window=Window(0, 0, 16, 16))
        d.write(np.full((16, 16), 5, "int16"), 1, window=Window(32, 16, 16, 16))


def sparse_stripped(path):
    # 64x64 int16, 8 rows per strip. Only strip 1 (rows 8..16) gets data.
    profile = dict(BASE, width=64, height=64, count=1, dtype="int16", tiled=False,
                   blockysize=8, compress="none", nodata=-1)
    with rasterio.open(path, "w", **profile, SPARSE_OK="TRUE") as d:
        d.write(np.full((8, 64), 3, "int16"), 1, window=Window(0, 8, 64, 8))


def planar_partly_sparse(path):
    # 2 bands, band-interleaved, 16x16 tiles. Band 1 is full, band 2 has one tile only.
    profile = dict(BASE, width=32, height=32, count=2, dtype="uint8", tiled=True,
                   blockxsize=16, blockysize=16, compress="deflate", interleave="band",
                   nodata=255)
    with rasterio.open(path, "w", **profile, SPARSE_OK="TRUE") as d:
        d.write((pattern(32, 32, "int64") % 200).astype("uint8"), 1)
        d.write(np.full((16, 16), 9, "uint8"), 2, window=Window(16, 0, 16, 16))


def big_endian_predictor_tiled(path):
    # Big-endian, horizontal predictor, deflate. 50x37 is not a multiple of the tile size.
    profile = dict(BASE, width=50, height=37, count=1, dtype="uint16", tiled=True,
                   blockxsize=16, blockysize=16, compress="deflate", predictor=2)
    with rasterio.open(path, "w", **profile, ENDIANNESS="BIG") as d:
        d.write(pattern(37, 50, "uint16"), 1)


def float_predictor_stripped(path):
    # 3 pixel-interleaved float32 bands, LZW, floating-point predictor.
    # 37 rows at 8 rows per strip, so the last strip holds 5 rows.
    profile = dict(BASE, width=20, height=37, count=3, dtype="float32", tiled=False,
                   blockysize=8, compress="lzw", predictor=3, interleave="pixel")
    with rasterio.open(path, "w", **profile) as d:
        for band in range(3):
            d.write(pattern(37, 20, "float32", band) / 4, band + 1)


def int_predictor_stripped(path):
    # Stripped int32 with the horizontal predictor and deflate.
    profile = dict(BASE, width=33, height=21, count=1, dtype="int32", tiled=False,
                   blockysize=4, compress="deflate", predictor=2)
    with rasterio.open(path, "w", **profile) as d:
        d.write(pattern(21, 33, "int32") - 1000, 1)


def packbits_tiled(path):
    # PackBits, the run-length code of baseline TIFF. Same grid as the predictor test.
    profile = dict(BASE, width=50, height=37, count=1, dtype="uint16", tiled=True,
                   blockxsize=16, blockysize=16, compress="packbits")
    with rasterio.open(path, "w", **profile) as d:
        d.write(pattern(37, 50, "uint16"), 1)


def raster_type(path, area_or_point):
    # The 4x3 grid of issue #525. Pixel centers are lon 0.5..3.5 and lat 2.5..0.5.
    profile = dict(driver="GTiff", width=4, height=3, count=1, dtype="float32",
                   crs="EPSG:4326", transform=from_origin(0, 3, 1, 1))
    with rasterio.open(path, "w", **profile) as d:
        d.write(np.arange(12, dtype="float32").reshape(3, 4), 1)
        d.update_tags(AREA_OR_POINT=area_or_point)


if __name__ == "__main__":
    packbits_tiled("packbits_tiled_u16.tif")
    raster_type("pixel_is_area.tif", "Area")
    raster_type("pixel_is_point.tif", "Point")
    sparse_tiled("sparse_tiled_i16.tif", nodata=-32767)
    sparse_tiled("sparse_tiled_bigtiff_i16.tif", nodata=-32767, BIGTIFF="YES")
    sparse_tiled("sparse_tiled_no_nodata_i16.tif", nodata=None)
    sparse_stripped("sparse_stripped_i16.tif")
    planar_partly_sparse("planar_partly_sparse_u8.tif")
    big_endian_predictor_tiled("big_endian_predictor_tiled_u16.tif")
    float_predictor_stripped("float_predictor_stripped_f32.tif")
    int_predictor_stripped("int_predictor_stripped_i32.tif")
