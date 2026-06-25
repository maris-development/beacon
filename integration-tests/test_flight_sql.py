"""Arrow Flight SQL transport.

The container already publishes the Flight SQL port (32011); this exercises it
over ADBC. Beacon's Flight authorizer accepts an ``Authorization: Basic`` header
on the handshake and issues a bearer token the driver reuses, so admin
credentials map to a super-user exactly like the HTTP path. Anonymous access is
off by default, so an unauthenticated client is rejected.

Skipped unless ``adbc-driver-flightsql`` is installed (see
``requirements-optional.txt``).
"""

from __future__ import annotations

import base64

import pytest

from conftest import (
    ADMIN_PASSWORD,
    ADMIN_USERNAME,
    CONTAINER_NAME,
    FLIGHT_PORT,
    _resolve_host_port,
)

pytest.importorskip(
    "adbc_driver_flightsql",
    reason="adbc-driver-flightsql not installed (pip install -r requirements-optional.txt)",
)

import adbc_driver_flightsql.dbapi as flight_dbapi  # noqa: E402
from adbc_driver_flightsql import DatabaseOptions  # noqa: E402


@pytest.fixture(scope="module")
def flight_uri(beacon_container) -> str:
    port = _resolve_host_port(CONTAINER_NAME, FLIGHT_PORT)
    return f"grpc://127.0.0.1:{port}"


def _connect(uri: str, auth: bool = True):
    db_kwargs = {}
    if auth:
        token = base64.b64encode(f"{ADMIN_USERNAME}:{ADMIN_PASSWORD}".encode()).decode()
        db_kwargs[DatabaseOptions.AUTHORIZATION_HEADER.value] = f"Basic {token}"
    return flight_dbapi.connect(uri, db_kwargs=db_kwargs)


def test_flight_select_constant(flight_uri):
    conn = _connect(flight_uri)
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 AS one")
        assert cur.fetchone()[0] == 1
    finally:
        conn.close()


def test_flight_query_parquet(flight_uri, sample_data):
    """Flight SQL hits the same runtime and datasets as the HTTP transport."""
    conn = _connect(flight_uri)
    try:
        cur = conn.cursor()
        cur.execute("SELECT count(*) AS n FROM read_parquet(['obs/*.parquet'])")
        assert cur.fetchone()[0] == sample_data["total"]
    finally:
        conn.close()


def test_flight_returns_arrow(flight_uri, sample_data):
    conn = _connect(flight_uri)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT platform, temperature FROM read_parquet(['obs/*.parquet']) LIMIT 10"
        )
        table = cur.fetch_arrow_table()
        assert table.num_rows == 10
        assert set(table.column_names) == {"platform", "temperature"}
    finally:
        conn.close()


def test_flight_anonymous_rejected(flight_uri):
    """Anonymous Flight access is disabled by default."""
    with pytest.raises(Exception):
        conn = _connect(flight_uri, auth=False)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchall()
