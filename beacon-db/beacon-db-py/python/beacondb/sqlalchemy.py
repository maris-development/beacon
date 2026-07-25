"""A SQLAlchemy dialect for beacondb, built on the PEP 249 (DB-API) surface.

Registered as the ``beacondb`` dialect, so::

    from sqlalchemy import create_engine
    engine = create_engine("beacondb:///beacon.db")          # a file
    engine = create_engine("beacondb://")                     # in-memory
    engine = create_engine("beacondb:///beacon.db?auth=true&username=u&password=p")

    import pandas as pd
    pd.read_sql("SELECT * FROM obs", engine)

The dialect is deliberately thin: beacon speaks DataFusion SQL (ANSI/Postgres-flavoured), so
SQLAlchemy's generic SQL compiler is used as-is. Schema reflection is answered from beacon's
``information_schema``. The engine is autocommit (no multi-statement transactions), which the
DB-API's no-op ``commit``/``rollback`` model transparently.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import types as sqltypes
from sqlalchemy.engine import default

# DataFusion's information_schema reports Arrow type names (e.g. "Int64", "Utf8",
# "Timestamp(Microsecond, None)"). Match on a lowercased prefix so parameterized types resolve.
_TYPE_PREFIXES: list[tuple[str, type[sqltypes.TypeEngine]]] = [
    ("boolean", sqltypes.BOOLEAN),
    ("int8", sqltypes.SMALLINT),
    ("int16", sqltypes.SMALLINT),
    ("int32", sqltypes.INTEGER),
    ("int64", sqltypes.BIGINT),
    ("uint8", sqltypes.SMALLINT),
    ("uint16", sqltypes.INTEGER),
    ("uint32", sqltypes.BIGINT),
    ("uint64", sqltypes.BIGINT),
    ("float16", sqltypes.FLOAT),
    ("float32", sqltypes.FLOAT),
    ("float64", sqltypes.FLOAT),
    ("decimal", sqltypes.NUMERIC),
    ("utf8view", sqltypes.VARCHAR),
    ("largeutf8", sqltypes.VARCHAR),
    ("utf8", sqltypes.VARCHAR),
    ("timestamp", sqltypes.TIMESTAMP),
    ("date32", sqltypes.DATE),
    ("date64", sqltypes.DATE),
    ("time", sqltypes.TIME),
    ("duration", sqltypes.Interval),
    ("largebinary", sqltypes.LargeBinary),
    ("binaryview", sqltypes.LargeBinary),
    ("binary", sqltypes.LargeBinary),
]


def _resolve_type(data_type: str) -> sqltypes.TypeEngine:
    """Maps a beacon/DataFusion column type string to a SQLAlchemy type."""
    key = (data_type or "").strip().lower()
    for prefix, sqltype in _TYPE_PREFIXES:
        if key.startswith(prefix):
            return sqltype()
    # Unknown/nested (List, Struct, Map, geometry, …) — SQLAlchemy tolerates NullType and still
    # round-trips values via the DB-API.
    return sqltypes.NullType()


def _as_bool(value: Any) -> bool:
    return str(value).strip().lower() in ("1", "true", "yes", "on")


class BeacondbDialect(default.DefaultDialect):
    name = "beacondb"
    driver = "beacondb"

    # beacondb binds `?` placeholders positionally.
    paramstyle = "qmark"
    # Compiled-statement caching is safe: compilation is pure and depends only on the statement.
    supports_statement_cache = True

    # DataFusion has a native BOOLEAN; there are no sequences, and rowcount is best-effort.
    supports_native_boolean = True
    supports_sequences = False
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    # No server-side transaction/savepoint machinery (autocommit).
    supports_savepoints = False

    @classmethod
    def import_dbapi(cls):
        import beacondb

        return beacondb

    # SQLAlchemy < 2.0 spelling.
    @classmethod
    def dbapi(cls):
        return cls.import_dbapi()

    def create_connect_args(self, url):
        # The URL's "database" is beacondb's database path; empty means in-memory.
        kwargs: dict[str, Any] = {"database": url.database or ":memory:"}

        query = dict(url.query)
        for key in ("auth", "anonymous", "crawlers", "read_only"):
            if key in query:
                kwargs[key] = _as_bool(query.pop(key))
        for key in ("batch_size", "memory_limit", "cpu_limit"):
            if key in query:
                kwargs[key] = int(query.pop(key))
        for key in (
            "username",
            "password",
            "token",
            "admin_username",
            "admin_password",
            "datasets",
        ):
            if key in query:
                kwargs[key] = query.pop(key)

        # Credentials may also ride on the URL's userinfo (beacondb://user:pass@/db).
        if url.username and "username" not in kwargs:
            kwargs["username"] = url.username
        if url.password and "password" not in kwargs:
            kwargs["password"] = url.password

        return ([], kwargs)

    def _get_default_schema_name(self, connection):
        return "public"

    def do_ping(self, dbapi_connection) -> bool:
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("SELECT 1")
            cursor.fetchall()
        finally:
            cursor.close()
        return True

    # ---- reflection, answered from information_schema ---------------------------------------

    def get_schema_names(self, connection, **kw):
        rows = connection.exec_driver_sql(
            "SELECT DISTINCT table_schema FROM information_schema.tables ORDER BY table_schema"
        ).fetchall()
        return [row[0] for row in rows]

    def get_table_names(self, connection, schema=None, **kw):
        schema = schema or self.default_schema_name
        rows = connection.exec_driver_sql(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = ? AND table_type <> 'VIEW' ORDER BY table_name",
            (schema,),
        ).fetchall()
        return [row[0] for row in rows]

    def get_view_names(self, connection, schema=None, **kw):
        schema = schema or self.default_schema_name
        rows = connection.exec_driver_sql(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = ? AND table_type = 'VIEW' ORDER BY table_name",
            (schema,),
        ).fetchall()
        return [row[0] for row in rows]

    def has_table(self, connection, table_name, schema=None, **kw):
        schema = schema or self.default_schema_name
        rows = connection.exec_driver_sql(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ? LIMIT 1",
            (schema, table_name),
        ).fetchall()
        return len(rows) > 0

    def get_columns(self, connection, table_name, schema=None, **kw):
        schema = schema or self.default_schema_name
        rows = connection.exec_driver_sql(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
            (schema, table_name),
        ).fetchall()
        return [
            {
                "name": name,
                "type": _resolve_type(data_type),
                "nullable": str(is_nullable).strip().upper() != "NO",
                "default": None,
            }
            for (name, data_type, is_nullable) in rows
        ]

    # beacon has no primary keys, foreign keys, or indexes to reflect.
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        return {"constrained_columns": [], "name": None}

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        return []


dialect = BeacondbDialect
