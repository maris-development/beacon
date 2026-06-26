"""Multi-dimensional scientific formats: NetCDF and Zarr.

Beacon's nd-array engine is one of its defining features. These tests use the
committed example fixtures (a gridded SST NetCDF and the equivalent Zarr store),
copied into the mounted datasets dir, and exercise:

* ``read_netcdf`` / ``read_zarr`` table functions (including the explicit
  dimension-projection argument),
* ``SELECT *`` broadcast-compatible dimension auto-selection, and
* the flat ``netcdf`` and multi-dimensional ``ndnetcdf`` OUTPUT formats, by
  round-tripping Beacon's own writer back through ``read_netcdf`` (the datasets
  dir is a host-mounted volume, so a file written on the host is visible to the
  container).
"""

from __future__ import annotations

import shutil

import pytest

from conftest import REPO_ROOT

NETCDF_SRC = REPO_ROOT / "beacon-file-formats/beacon-arrow-netcdf/test_files/gridded-example.nc"
ZARR_SRC = REPO_ROOT / "test-datasets/gridded-example.zarr"

# Variables known to exist in the gridded SST example (see `ncdump -h`).
EXPECTED_VARS = {"analysed_sst", "lat", "lon", "time"}


@pytest.fixture(scope="module")
def netcdf_file(datasets_dir) -> str:
    if not NETCDF_SRC.exists():
        pytest.skip(f"missing NetCDF fixture: {NETCDF_SRC}")
    dst_dir = datasets_dir / "nc"
    dst_dir.mkdir(exist_ok=True)
    dst = dst_dir / "gridded-example.nc"
    if not dst.exists():
        shutil.copy(NETCDF_SRC, dst)
    return "nc/gridded-example.nc"


@pytest.fixture(scope="module")
def zarr_store(datasets_dir) -> str:
    if not ZARR_SRC.exists():
        pytest.skip(f"missing Zarr fixture: {ZARR_SRC}")
    dst = datasets_dir / "zarr" / "gridded-example.zarr"
    if not dst.exists():
        dst.parent.mkdir(exist_ok=True)
        shutil.copytree(ZARR_SRC, dst)
    return "zarr/gridded-example.zarr"


def _header(client, sql) -> list[str]:
    return client.sql_rows(f"{sql} LIMIT 1")[0]


# --- NetCDF reading -----------------------------------------------------------
def test_read_netcdf_select_star(client, netcdf_file):
    header = _header(client, f"SELECT * FROM read_netcdf(['{netcdf_file}'])")
    # SELECT * auto-selects a broadcast-compatible dimension set; the SST grid
    # variables and their coordinates come back as columns.
    assert EXPECTED_VARS <= set(header), header
    # The grid is large (time=1, lat=1208, lon=1920), so the flattened scan
    # returns well over a thousand rows.
    n = client.count(f"SELECT * FROM read_netcdf(['{netcdf_file}'])")
    assert n > 1000


def test_read_netcdf_projects_named_variable(client, netcdf_file):
    n = client.count(
        f"SELECT analysed_sst FROM read_netcdf(['{netcdf_file}']) "
        "WHERE analysed_sst IS NOT NULL"
    )
    assert n > 0


def test_read_netcdf_explicit_dimensions(client, netcdf_file):
    """The 2-arg form projects onto an explicit dimension set."""
    rows = client.sql_rows(
        f"SELECT * FROM read_netcdf(['{netcdf_file}'], ['lat', 'lon']) LIMIT 5"
    )
    assert len(rows) - 1 >= 1
    # lat/lon coordinates fit the chosen dimensions and remain present.
    assert {"lat", "lon"} <= set(rows[0]), rows[0]


# --- Zarr reading -------------------------------------------------------------
def test_read_zarr_select_star(client, zarr_store):
    header = _header(client, f"SELECT * FROM read_zarr(['{zarr_store}'])")
    assert EXPECTED_VARS <= set(header), header
    assert client.count(f"SELECT * FROM read_zarr(['{zarr_store}'])") > 1000


# --- NetCDF output round-trips ------------------------------------------------
def _write_to_datasets(datasets_dir, rel_path: str, data: bytes) -> str:
    dst = datasets_dir / rel_path
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_bytes(data)
    return rel_path


def test_flat_netcdf_output_roundtrip(client, datasets_dir):
    """Flat (record-oriented) NetCDF output is readable again by read_netcdf."""
    src = "SELECT temperature, salinity, depth FROM read_parquet(['obs/*.parquet']) LIMIT 100"
    data = client.query_bytes(src, "netcdf")
    # NetCDF files start with the classic "CDF" magic or the HDF5 signature.
    assert data[:3] == b"CDF" or data[:4] == b"\x89HDF", data[:8]

    rel = _write_to_datasets(datasets_dir, "roundtrip/flat.nc", data)
    back = client.sql_rows(f"SELECT * FROM read_netcdf(['{rel}'])")
    assert len(back) - 1 == 100
    assert "temperature" in back[0]


def test_nd_netcdf_output_roundtrip(client, datasets_dir):
    """ND-NetCDF output keyed on the unique `time` column round-trips."""
    src = (
        "SELECT time, temperature, salinity FROM read_parquet(['obs/*.parquet']) "
        "ORDER BY time LIMIT 50"
    )
    data = client.query_bytes(src, {"ndnetcdf": {"dimension_columns": ["time"]}})
    assert data[:3] == b"CDF" or data[:4] == b"\x89HDF", data[:8]

    rel = _write_to_datasets(datasets_dir, "roundtrip/nd.nc", data)
    n = client.count(f"SELECT * FROM read_netcdf(['{rel}'])")
    # 50 unique time values -> 50 cells along the single dimension.
    assert n == 50
    assert "temperature" in client.sql_rows(f"SELECT * FROM read_netcdf(['{rel}']) LIMIT 1")[0]
