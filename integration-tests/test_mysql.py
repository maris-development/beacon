"""External MySQL tables via ``STORED AS MYSQL`` (the MySQL sibling of
``test_sql_databases.py``).

Stands up a MySQL sidecar on the shared docker network, seeds a table, registers
it in Beacon, and checks querying, filter pushdown (federated plan), and that the
password is redacted in the admin table-config. MySQL is a default feature of
``beacon-sql-databases``, so it is compiled into the image.

The server is launched with ``mysql_native_password`` so the driver authenticates
over plaintext TCP without TLS.
"""

from __future__ import annotations

import subprocess
import time

import pytest

from beacon_client import QueryError
from conftest import _run

MY_CONTAINER = "beacon-it-mysql"
MY_HOST = "beacon-it-mysql"  # resolvable on docker_network
MY_DB = "shop"
MY_ROOT_PASSWORD = "beaconmysqlpw"

SEED_SQL = f"""
-- Use mysql_native_password so the driver authenticates over plaintext TCP.
ALTER USER 'root'@'%' IDENTIFIED WITH mysql_native_password BY '{MY_ROOT_PASSWORD}';
FLUSH PRIVILEGES;
CREATE TABLE companies (id INT PRIMARY KEY, name VARCHAR(64), revenue DOUBLE);
INSERT INTO companies (id, name, revenue) VALUES
  (1, 'Acme',   100.0),
  (2, 'Globex',  50.0),
  (3, 'Initech', 25.0);
"""
SEED_ROWS = 3
HIGH_REVENUE_ROWS = 2  # revenue > 40


@pytest.fixture(scope="module")
def mysql_container(docker_network):
    _run(["docker", "rm", "-f", MY_CONTAINER])
    started = _run(
        [
            "docker", "run", "-d",
            "--name", MY_CONTAINER,
            "--network", docker_network,
            "--network-alias", MY_HOST,
            "-e", f"MYSQL_ROOT_PASSWORD={MY_ROOT_PASSWORD}",
            "-e", f"MYSQL_DATABASE={MY_DB}",
            "mysql:8.0",
            "--default-authentication-plugin=mysql_native_password",
        ]
    )
    if started.returncode != 0:
        pytest.fail(f"docker run mysql failed:\n{started.stdout}\n{started.stderr}")

    # Wait until MySQL accepts connections, then seed.
    deadline = time.time() + 120
    ready = False
    while time.time() < deadline:
        ping = _run(
            ["docker", "exec", MY_CONTAINER, "mysqladmin", "ping",
             "-uroot", f"-p{MY_ROOT_PASSWORD}", "--silent"]
        )
        if ping.returncode == 0 and "mysqld is alive" in (ping.stdout + ping.stderr).lower():
            ready = True
            break
        time.sleep(2)
    if not ready:
        logs = _run(["docker", "logs", MY_CONTAINER])
        _run(["docker", "rm", "-f", MY_CONTAINER])
        pytest.fail(f"mysql did not become ready:\n{logs.stdout}\n{logs.stderr}")

    seed = subprocess.run(
        ["docker", "exec", "-i", MY_CONTAINER, "mysql", "-uroot",
         f"-p{MY_ROOT_PASSWORD}", MY_DB],
        input=SEED_SQL,
        capture_output=True,
        text=True,
    )
    if seed.returncode != 0:
        _run(["docker", "rm", "-f", MY_CONTAINER])
        pytest.fail(f"seeding mysql failed:\n{seed.stdout}\n{seed.stderr}")

    yield {"host": MY_HOST, "db": MY_DB, "user": "root", "password": MY_ROOT_PASSWORD}
    _run(["docker", "rm", "-f", MY_CONTAINER])


@pytest.fixture(scope="module")
def my_table(client, mysql_container):
    my = mysql_container
    # `sslmode=disabled` is required: mysql_async defaults to requiring TLS and
    # rejects MySQL 8's self-signed server cert ("validity period ... exceeds the
    # maximum allowed"). The Postgres sibling test passes `sslmode=disable` too.
    ddl = (
        "CREATE EXTERNAL TABLE my_companies STORED AS MYSQL "
        "LOCATION 'companies' OPTIONS ("
        f"'host' '{my['host']}', 'port' '3306', 'user' '{my['user']}', "
        f"'password' '{my['password']}', 'database' '{my['db']}', 'sslmode' 'disabled')"
    )
    try:
        client.execute("DROP TABLE IF EXISTS my_companies", admin=True)
    except QueryError:
        pass
    client.execute(ddl, admin=True)
    yield "my_companies"
    try:
        client.execute("DROP TABLE IF EXISTS my_companies", admin=True)
    except QueryError:
        pass


# Like Postgres, executing a federated MySQL scan returns no rows (the federation
# unparser sends the local alias to the remote); planning/EXPLAIN and the
# admin-only config endpoint work. Left unfixed per request (federation is shared
# with the Postgres path).
MY_EXEC_XFAIL = pytest.mark.xfail(
    reason="federated MySQL execution returns no rows (shared federation-unparse issue "
    "with Postgres)",
    strict=False,
)


@MY_EXEC_XFAIL
def test_mysql_table_row_count(client, my_table):
    assert client.count(f"SELECT * FROM {my_table}") == SEED_ROWS


@MY_EXEC_XFAIL
def test_mysql_filter_result(client, my_table):
    n = client.scalar(f"SELECT count(*) FROM {my_table} WHERE revenue > 40")
    assert int(n) == HIGH_REVENUE_ROWS


def test_mysql_query_is_federated(client, my_table):
    rows = client.sql_rows(
        f"EXPLAIN SELECT count(*) FROM {my_table} WHERE revenue > 40", admin=True
    )
    plan = "\n".join(cell for row in rows for cell in row)
    assert "Federated" in plan, f"expected a federated plan, got:\n{plan}"


def test_mysql_credentials_redacted(client, my_table):
    resp = client.admin_get(f"/api/admin/table-config?table_name={my_table}")
    assert resp.status_code == 200
    assert MY_ROOT_PASSWORD not in resp.text
    assert resp.json().get("secret") == "***"
