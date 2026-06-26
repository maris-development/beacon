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

    # -- reads: default Arrow IPC stream, parsed with pyarrow -------------------
    @staticmethod
    def _cell(value) -> str:
        """Render an Arrow cell as a CSV-like string (None -> empty)."""
        return "" if value is None else str(value)

    def sql_rows(self, sql: str, admin: bool = False) -> list[list[str]]:
        """Run a read query and return all rows (row 0 is the header).

        Uses the default (no ``output`` block) Arrow IPC stream rather than a CSV
        ``output`` format. The CSV path wraps the plan in ``COPY ... TO`` which the
        engine cannot physically plan for federated sources (Postgres/MySQL/remote)
        or for ``EXPLAIN``; the streaming path has no such limitation. Cells are
        stringified so existing string/`int(float(...))` assertions keep working.
        """
        import pyarrow as pa

        resp = self._post({"sql": sql}, admin)
        if resp.status_code != 200:
            raise QueryError(resp.status_code, resp.text)
        content = resp.content
        if not content:
            # An empty result set streams as a zero-length body (no schema message).
            return [[]]
        try:
            table = pa.ipc.open_stream(pa.BufferReader(pa.py_buffer(content))).read_all()
        except pa.lib.ArrowInvalid:
            return [[]]
        rows: list[list[str]] = [list(table.column_names)]
        columns = []
        for col in table.columns:
            try:
                columns.append(col.to_pylist())
            except (pa.lib.ArrowInvalid, ValueError):
                # Nanosecond timestamps (e.g. CF time decoded from NetCDF) are not
                # safely convertible to python datetime; render them as strings.
                import pyarrow.compute as pc

                columns.append(pc.cast(col, pa.string()).to_pylist())
        for i in range(table.num_rows):
            rows.append([self._cell(col[i]) for col in columns])
        return rows

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

    # -- binary output formats (Arrow IPC / Parquet) parsed with pyarrow --------
    def raw(self, body: dict, admin: bool = False) -> requests.Response:
        """POST a query body verbatim and return the raw response.

        Useful for asserting response headers (``x-beacon-query-id``,
        ``Content-Type``) and decoding non-CSV output formats.
        """
        return self._post(body, admin)

    def arrow_table(self, sql: str, admin: bool = False):
        """Run a query with Arrow IPC output and return a ``pyarrow.Table``."""
        import pyarrow as pa

        resp = self._post({"sql": sql, "output": {"format": "arrow"}}, admin)
        if resp.status_code != 200:
            raise QueryError(resp.status_code, resp.text)
        buf = pa.py_buffer(resp.content)
        # The "arrow" output format writes the Arrow IPC *file* format; fall back
        # to the streaming reader in case a build emits the stream format instead.
        try:
            return pa.ipc.open_file(buf).read_all()
        except pa.lib.ArrowInvalid:
            return pa.ipc.open_stream(buf).read_all()

    def parquet_table(self, sql: str, admin: bool = False):
        """Run a query with Parquet output and return a ``pyarrow.Table``."""
        import pyarrow.parquet as pq

        resp = self._post({"sql": sql, "output": {"format": "parquet"}}, admin)
        if resp.status_code != 200:
            raise QueryError(resp.status_code, resp.text)
        return pq.read_table(io.BytesIO(resp.content))

    def query_bytes(self, sql: str, output_format, admin: bool = False) -> bytes:
        """Run a query and return the raw response body for ``output_format``.

        ``output_format`` is the value of ``output.format`` — a string (e.g.
        ``"netcdf"``) or an object (e.g. ``{"geoparquet": {...}}`` or
        ``{"ndnetcdf": ..., "dimension_columns": [...]}``). Used to round-trip
        Beacon's own writers back through its readers in the tests.
        """
        resp = self._post({"sql": sql, "output": {"format": output_format}}, admin)
        if resp.status_code != 200:
            raise QueryError(resp.status_code, resp.text)
        return resp.content

    def query_id(self, sql: str, admin: bool = False) -> str:
        """Run a query and return the ``x-beacon-query-id`` response header.

        Uses the streaming path (no ``output``) so the recorded metrics reflect the
        real scan rather than a ``COPY`` wrapper.
        """
        resp = self._post({"sql": sql}, admin)
        if resp.status_code != 200:
            raise QueryError(resp.status_code, resp.text)
        qid = resp.headers.get("x-beacon-query-id")
        if not qid:
            raise QueryError(200, "response did not carry an x-beacon-query-id header")
        return qid

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
