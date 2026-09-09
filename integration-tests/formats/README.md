# One file per format

Each file here writes its own data, opens an embedded Beacon over it, queries it, creates an
external table, reopens the database, and checks the table survived. Nothing is shared between
them: open one file and everything it does is in it.

```bash
pytest formats/                      # every format
pytest formats/test_netcdf.py -v     # one
```

| File | Writer | Covers |
| --- | --- | --- |
| `test_netcdf.py` | `netCDF4` | flat and netCDF-3, a 3-dim grid, `_FillValue`, `scale_factor`, CF time, a glob |
| `test_parquet.py` | `pyarrow` | row groups, zstd and snappy, statistics on and off, nulls, a struct column, a directory |
| `test_csv.py` | text | the delimiter argument, quoting, nulls, type inference, gzip, a glob |
| `test_zarr.py` | `zarr` | v3, consolidated metadata, the zstd codec, a rank-3 grid, an unwritten chunk |
| `test_atlas.py` | `atlas-python` | many datasets in one file, attributes, dataset pruning, a glob, `OPTIONS` |
| `test_hdf5.py` | `h5py` | plain HDF5, dimension scales, a nested group, a compound dataset, strings |
| `test_arrow.py` | `pyarrow` | one batch and many, a dictionary column, nulls, every Arrow type |
| `test_geoparquet.py` | `geopandas` | WKB and GeoArrow, a line, a polygon, a covering bbox, `ST_X`/`ST_Y` |
| `test_odv.py` | text | the header declarations, station metadata, absent values |
| `test_tiff.py` | `rasterio` | one band and many, tiled and stripped, per-pixel coordinates |
| `test_delta.py` | `deltalake` | versions and time travel, partitioning, a schema change |
| `test_iceberg.py` | `pyiceberg` | snapshots and time travel, partitioning, a schema change |
| `test_icechunk.py` | `icechunk` | one commit, two commits, a branch |

A file skips itself when its writer or the `beacondb` extension is absent, so a partial install
runs what it can.

`__init__.py` makes this a package. Without it, `formats/test_delta.py` and the HTTP suite's
`test_delta.py` are two modules called `test_delta` and pytest refuses to import the second.

## Behaviour these tests record rather than wish away

Each of these is asserted as it behaves today, with the reason in the test's own docstring, so a
fix turns the test red and it gets rewritten as an equality.

| Format | What |
| --- | --- |
| CSV | `read_csv` takes no header flag, so a headerless file loses its first row |
| CSV | `read_csv` does not decompress: `.csv.gz` is refused rather than misread |
| Zarr | the reader is v3 only, and refuses a v2 store |
| ODV | `count(*)` fails; `count(<column>)` works |
| ODV | a station's metadata is not carried down its rows |
| ODV | the attribute column order changes between runs |
| ODV | there is no `STORED AS ODV`, so no external table |
| CSV | `read_csv`'s delimiter refuses a bare `NULL` and needs `CAST(NULL AS VARCHAR)`; `read_icechunk`'s branch accepts one |
| GeoParquet | a geometry predicate cannot be written against a CRS-tagged column |

## Building `beacondb`

The extension is not published, so build it from the repository:

```bash
maturin develop --manifest-path ../beacon-db/beacon-db-py/Cargo.toml
```
