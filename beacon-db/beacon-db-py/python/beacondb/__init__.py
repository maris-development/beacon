"""beacondb — an embeddable, DuckDB-class database for scientific data.

One file holds the catalog and the data beacon manages; everything else — S3 objects,
netCDF/Zarr/Parquet files on disk, remote SQL databases — is referenced from it.

    import beacondb

    con = beacondb.connect("beacon.db")
    con.sql("SELECT 1 AS a").fetchall()

Auth is off by default: opening a file locally needs no credentials and permits everything,
the same contract as SQLite and DuckDB. Pass ``auth=True`` to switch on beacon's RBAC, where a
session is anonymous and read-only until credentials are supplied::

    con = beacondb.connect("beacon.db", auth=True, username="analyst", password=...)
    con.whoami()
"""

from __future__ import annotations

from ._beacondb import (
    Connection,
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotPermittedError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Relation,
    Result,
    Warning,
    connect,
    engine_version,
)

__all__ = [
    "Connection",
    "Relation",
    "Result",
    "connect",
    "sql",
    "query",
    "execute",
    # PEP 249 module attributes
    "apilevel",
    "threadsafety",
    "paramstyle",
    # PEP 249 exception tree
    "Warning",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
    "NotPermittedError",
]

__version__ = engine_version()

#: PEP 249 compliance level.
apilevel = "2.0"
#: Threads may share the module and connections; a connection's result slot is its own.
threadsafety = 2
#: Parameter style: ``?`` placeholders bound positionally (``$1`` is also accepted).
paramstyle = "qmark"

_default_connection: Connection | None = None


def _default() -> Connection:
    """The lazily-created in-memory connection backing the module-level helpers."""
    global _default_connection
    if _default_connection is None:
        _default_connection = connect(":memory:")
    return _default_connection


def sql(query: str) -> Relation:
    """A lazy relation over ``query`` on the default in-memory connection."""
    return _default().sql(query)


def query(query_text: str) -> Relation:
    """Alias of :func:`sql`, matching DuckDB."""
    return _default().sql(query_text)


def execute(query: str, parameters=None) -> Connection:
    """Runs ``query`` on the default in-memory connection, DB-API style."""
    return _default().execute(query, parameters)
