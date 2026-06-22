"""Tests for output-format inference and spec building."""

from __future__ import annotations

import pytest

from beacon_cli.formats import (
    ExportFormatError,
    build_output_format,
    infer_format_name,
)


def test_infer_format_from_extension():
    assert infer_format_name("o.csv", None) == "csv"
    assert infer_format_name("o.parquet", None) == "parquet"
    assert infer_format_name("o.arrow", None) == "ipc"
    assert infer_format_name("o.nc", None) == "netcdf"
    assert infer_format_name("o.geoparquet", None) == "geoparquet"


def test_infer_format_override_and_alias():
    assert infer_format_name("o.bin", "arrow") == "ipc"
    assert infer_format_name("o.bin", "nc") == "netcdf"


def test_infer_format_unknown_raises():
    with pytest.raises(ExportFormatError):
        infer_format_name("o.unknownext", None)


def test_build_output_format_simple():
    assert build_output_format("csv") == "csv"
    assert build_output_format("parquet") == "parquet"
    assert build_output_format("arrow") == "ipc"


def test_build_output_format_structured():
    assert build_output_format("geoparquet", lon="lon", lat="lat") == {
        "geoparquet": {"longitude_column": "lon", "latitude_column": "lat"}
    }
    assert build_output_format("nd_netcdf", dimension_columns=["t", "x"]) == {
        "nd_netcdf": {"dimension_columns": ["t", "x"]}
    }


def test_nd_netcdf_requires_dimensions():
    with pytest.raises(ExportFormatError):
        build_output_format("nd_netcdf")
