"""External SQL-database tables: stand up a PostgreSQL sidecar, register one of
its tables in Beacon via ``CREATE EXTERNAL TABLE ... STORED AS POSTGRES``, and
verify querying, cross-source joins, credential redaction, and restart survival.

Requires the Docker CLI (the whole suite is skipped without it). The Postgres
container shares ``docker_network`` with the Beacon container so Beacon can reach
it by name.
"""

from __future__ import annotations

import subprocess
import time

import pytest

from beacon_client import QueryError
from conftest import CONTAINER_NAME, _run, _resolve_host_port, _wait_healthy, HTTP_PORT

PG_CONTAINER = "beacon-it-postgres"
PG_HOST_ALIAS = "beacon-it-postgres"  # resolvable on docker_network
PG_DB = "shop"
PG_USER = "beacon"
PG_PASSWORD = "beaconpw"

# Seed data: kept tiny and deterministic so assertions are obvious.
SEED_SQL = """
CREATE TABLE companies (id INT PRIMARY KEY, name TEXT, revenue DOUBLE PRECISION);
INSERT INTO companies (id, name, revenue) VALUES
  (1, 'Acme',   100.0),
  (2, 'Globex',  50.0),
  (3, 'Initech', 25.0);
"""
SEED_ROWS = 3
HIGH_REVENUE_ROWS = 2  # revenue > 40


@pytest.fixture(scope="module")
def postgres_container(docker_network):
    """Run PostgreSQL on the shared network and seed a `companies` table."""
    _run(["docker", "rm", "-f", PG_CONTAINER])
    started = _run(
        [
            "docker", "run", "-d",
            "--name", PG_CONTAINER,
            "--network", docker_network,
            "--network-alias", PG_HOST_ALIAS,
            "-e", f"POSTGRES_DB={PG_DB}",
            "-e", f"POSTGRES_USER={PG_USER}",
            "-e", f"POSTGRES_PASSWORD={PG_PASSWORD}",
            "postgres:16-alpine",
        ]
    )
    if started.returncode != 0:
        pytest.fail(f"docker run postgres failed:\n{started.stdout}\n{started.stderr}")

    # Wait until Postgres accepts connections, then seed.
    deadline = time.time() + 60
    while time.time() < deadline:
        ready = _run(["docker", "exec", PG_CONTAINER, "pg_isready", "-U", PG_USER])
        if ready.returncode == 0:
            break
        time.sleep(1)
    else:
        logs = _run(["docker", "logs", PG_CONTAINER])
        _run(["docker", "rm", "-f", PG_CONTAINER])
        pytest.fail(f"postgres did not become ready:\n{logs.stdout}\n{logs.stderr}")

    seed = subprocess.run(
        ["docker", "exec", "-i", PG_CONTAINER, "psql", "-U", PG_USER, "-d", PG_DB],
        input=SEED_SQL,
        capture_output=True,
        text=True,
    )
    if seed.returncode != 0:
        _run(["docker", "rm", "-f", PG_CONTAINER])
        pytest.fail(f"seeding postgres failed:\n{seed.stdout}\n{seed.stderr}")

    yield {"host": PG_HOST_ALIAS, "db": PG_DB, "user": PG_USER, "password": PG_PASSWORD}
    _run(["docker", "rm", "-f", PG_CONTAINER])


@pytest.fixture(scope="module")
def pg_table(client, postgres_container):
    """Register the Postgres `companies` table in Beacon (admin DDL)."""
    pg = postgres_container
    ddl = (
        "CREATE EXTERNAL TABLE pg_companies STORED AS POSTGRES "
        "LOCATION 'public.companies' OPTIONS ("
        f"'host' '{pg['host']}', 'port' '5432', 'user' '{pg['user']}', "
        f"'password' '{pg['password']}', 'database' '{pg['db']}', 'sslmode' 'disable')"
    )
    try:
        client.execute("DROP TABLE IF EXISTS pg_companies", admin=True)
    except QueryError:
        pass
    client.execute(ddl, admin=True)
    yield "pg_companies"
    try:
        client.execute("DROP TABLE IF EXISTS pg_companies", admin=True)
    except QueryError:
        pass


def test_postgres_table_row_count(client, pg_table):
    assert client.count(f"SELECT * FROM {pg_table}") == SEED_ROWS


def test_postgres_filter_pushdown_result(client, pg_table):
    """A filtered count returns the correct subset (filter pushed to Postgres)."""
    n = client.scalar(f"SELECT count(*) FROM {pg_table} WHERE revenue > 40")
    assert int(n) == HIGH_REVENUE_ROWS


def test_postgres_join_with_parquet(client, pg_table, external_table_obs, sample_data):
    """Cross-source query: a Postgres table and a local Parquet table referenced
    together, proving Beacon can combine a federated DB source with file data."""
    total = client.scalar(
        f"SELECT (SELECT count(*) FROM {pg_table}) "
        f"+ (SELECT count(*) FROM {external_table_obs}) AS total"
    )
    assert int(float(total)) == SEED_ROWS + sample_data["total"]


def test_postgres_table_listed(client, pg_table):
    resp = client.tables()
    assert resp.status_code == 200
    assert pg_table in resp.json()


def test_postgres_credentials_redacted_in_config(client, pg_table):
    """The unauthenticated table-config endpoint must not expose the password."""
    resp = client.get(f"/api/table-config?table_name={pg_table}")
    assert resp.status_code == 200
    body = resp.text
    assert PG_PASSWORD not in body
    config = resp.json()
    assert config.get("secret") == "***"


def test_postgres_table_survives_restart(client, pg_table, beacon_container):
    """The encrypted definition reloads from table.json after a beacon restart."""
    _run(["docker", "restart", CONTAINER_NAME])
    host_port = _resolve_host_port(CONTAINER_NAME, HTTP_PORT)
    base_url = f"http://127.0.0.1:{host_port}"
    _wait_healthy(base_url, CONTAINER_NAME)
    # Re-point the client at the (possibly new) mapped port and re-query.
    client.base_url = base_url
    assert client.count(f"SELECT * FROM {pg_table}") == SEED_ROWS


@pytest.fixture(scope="module")
def external_table_obs(client):
    """A local Parquet external table, for the cross-source join test."""
    try:
        client.execute("DROP TABLE IF EXISTS obs_join", admin=True)
    except QueryError:
        pass
    client.execute("CREATE EXTERNAL TABLE obs_join STORED AS PARQUET LOCATION 'obs/'", admin=True)
    yield "obs_join"
    try:
        client.execute("DROP TABLE IF EXISTS obs_join", admin=True)
    except QueryError:
        pass
