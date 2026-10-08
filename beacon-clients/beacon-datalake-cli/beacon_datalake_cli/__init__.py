"""beacon-datalake-cli — a terminal client for a Beacon server.

Run SQL via one-shot subcommands or an interactive REPL, explore
tables/datasets/schemas, render results as tables, and export to
CSV / Parquet / Arrow IPC / NetCDF (and the other server formats).
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("beacon-datalake-cli")
except PackageNotFoundError:  # A source tree on sys.path with no install has no metadata.
    __version__ = "0+unknown"
