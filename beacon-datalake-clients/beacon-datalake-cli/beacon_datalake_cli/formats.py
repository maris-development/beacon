"""Output-format selection for the ``export`` command and ``\\export`` REPL verb.

Maps a user-facing format name (or output-file extension) to the JSON value the
server expects under ``output.format``. The supported formats mirror the
``OutputFormat`` enum in ``beacon-core/src/query/output.rs``.
"""

from __future__ import annotations

import os
from typing import Any

# Simple (string) formats keyed by canonical name.
SIMPLE_FORMATS = {"csv", "ipc", "parquet", "netcdf", "odv"}

# File-extension -> canonical format name.
EXT_TO_FORMAT = {
    ".csv": "csv",
    ".parquet": "parquet",
    ".arrow": "ipc",
    ".ipc": "ipc",
    ".nc": "netcdf",
    ".geoparquet": "geoparquet",
    ".odv": "odv",
}

# Aliases accepted on the --format flag.
FORMAT_ALIASES = {"arrow": "ipc", "nc": "netcdf"}


class ExportFormatError(ValueError):
    """Raised when a format cannot be determined or is misconfigured."""


def infer_format_name(path: str, override: str | None) -> str:
    """Resolve the canonical format name from ``--format`` or the file extension."""
    if override:
        name = override.lower()
        return FORMAT_ALIASES.get(name, name)
    ext = os.path.splitext(path)[1].lower()
    if ext in EXT_TO_FORMAT:
        return EXT_TO_FORMAT[ext]
    raise ExportFormatError(
        f"cannot infer output format from '{path}'; pass --format "
        f"(csv, parquet, ipc/arrow, netcdf, nd_netcdf, geoparquet, odv)"
    )


def build_output_format(
    name: str,
    *,
    dimension_columns: list[str] | None = None,
    lon: str | None = None,
    lat: str | None = None,
) -> Any:
    """Build the JSON value for ``output.format`` from a format name + options."""
    name = FORMAT_ALIASES.get(name.lower(), name.lower())

    if name in SIMPLE_FORMATS:
        return name
    if name == "geoparquet":
        return {"geoparquet": {"longitude_column": lon, "latitude_column": lat}}
    if name == "nd_netcdf":
        if not dimension_columns:
            raise ExportFormatError("nd_netcdf requires --dimension-columns")
        return {"nd_netcdf": {"dimension_columns": dimension_columns}}
    raise ExportFormatError(
        f"unknown output format '{name}'; expected one of: "
        f"csv, parquet, ipc/arrow, netcdf, nd_netcdf, geoparquet, odv"
    )
