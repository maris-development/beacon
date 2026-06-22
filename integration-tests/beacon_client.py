"""Thin HTTP client for the Beacon API used by the integration tests.

Beacon exposes a single query endpoint (``POST /api/query``) that accepts either a
SQL string or a JSON DSL body. The HTTP transport is read-only unless valid admin
basic-auth is supplied, which elevates the request to super-user and allows DDL/DML
(``CREATE EXTERNAL TABLE``, managed-table ``CREATE``/``INSERT``/...). This mirrors the
pattern in ``benchmarks/harness/clients.py``.

Two response modes matter:

* **Reads** (``SELECT``, JSON DSL queries) request ``output: {format: csv}`` so the
  result is easy to parse.
* **Writes** (DDL/DML) must NOT set an ``output`` format: an output format makes the
  runtime wrap the plan in a ``COPY ... TO`` file, and a DDL node nested inside a COPY
  cannot be physically planned. Sending no ``output`` runs the statement directly and
  returns an (empty) Arrow IPC stream, of which we only need the status code.
"""

from __future__ import annotations

import csv
import io

import requests


class QueryError(RuntimeError):
    """Raised when a query returns a non-2xx status, carrying the status code."""

    def __init__(self, status_code: int, body: str):
        self.status_code = status_code
        self.body = body
        super().__init__(f"beacon query failed [{status_code}]: {body[:300]}")


class BeaconHTTPClient:
    def __init__(self, base_url: str, username: str, password: str, timeout: float = 600.0):
        self.base_url = base_url.rstrip("/")
        self._auth = (username, password)
        self._timeout = timeout
        self._session = requests.Session()

    # -- low-level --------------------------------------------------------------
    def _post(self, body: dict, admin: bool) -> requests.Response:
        return self._session.post(
            f"{self.base_url}/api/query",
            json=body,
            auth=self._auth if admin else None,
            timeout=self._timeout,
        )

    # -- writes (DDL/DML): no output format -------------------------------------
    def execute(self, sql: str, admin: bool = True) -> None:
        """Run a side-effecting statement (DDL/DML) and raise on failure."""
        resp = self._post({"sql": sql}, admin)
        if resp.status_code != 200:
            raise QueryError(resp.status_code, resp.text)

    # -- reads: CSV output ------------------------------------------------------
    def sql_rows(self, sql: str, admin: bool = False) -> list[list[str]]:
        """Run a read query and return all CSV rows (row 0 is the header)."""
        resp = self._post({"sql": sql, "output": {"format": "csv"}}, admin)
        if resp.status_code != 200:
            raise QueryError(resp.status_code, resp.text)
        return list(csv.reader(io.StringIO(resp.text)))

    def scalar(self, sql: str, admin: bool = False):
        """Run a read query and return the single first data cell (e.g. a count)."""
        rows = self.sql_rows(sql, admin)
        if len(rows) < 2 or not rows[1]:
            raise QueryError(200, f"expected a scalar result, got: {rows!r}")
        return rows[1][0]

    def count(self, inner_sql: str, admin: bool = False) -> int:
        """Wrap a query as ``SELECT count(*)`` and return the integer count."""
        return int(float(self.scalar(f"SELECT count(*) AS n FROM ({inner_sql}) AS _sub", admin)))

    def query_json_rows(self, body: dict, admin: bool = False) -> list[list[str]]:
        resp = self._post({**body, "output": {"format": "csv"}}, admin)
        if resp.status_code != 200:
            raise QueryError(resp.status_code, resp.text)
        return list(csv.reader(io.StringIO(resp.text)))

    # -- status-only (negative tests) -------------------------------------------
    def status(self, sql: str, admin: bool = False) -> int:
        """Run a statement (no output format) and return only the HTTP status."""
        return self._post({"sql": sql}, admin).status_code

    # -- discovery / health -----------------------------------------------------
    def get(self, path: str) -> requests.Response:
        return self._session.get(f"{self.base_url}{path}", timeout=self._timeout)

    def datasets(self) -> requests.Response:
        return self.get("/api/datasets")

    def tables(self) -> requests.Response:
        return self.get("/api/tables")

    # -- admin endpoints (/api/admin/*) -----------------------------------------
    # These require admin basic-auth; pass ``admin=False`` to assert the auth gate.
    def admin_get(self, path: str, admin: bool = True) -> requests.Response:
        return self._session.get(
            f"{self.base_url}{path}",
            auth=self._auth if admin else None,
            timeout=self._timeout,
        )

    def admin_post(self, path: str, body: dict | None = None, admin: bool = True) -> requests.Response:
        return self._session.post(
            f"{self.base_url}{path}",
            json=body,
            auth=self._auth if admin else None,
            timeout=self._timeout,
        )

    def admin_delete(self, path: str, admin: bool = True) -> requests.Response:
        return self._session.delete(
            f"{self.base_url}{path}",
            auth=self._auth if admin else None,
            timeout=self._timeout,
        )
