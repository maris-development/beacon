"""Thin HTTP client over the Beacon ``/api/*`` endpoints.

The request contract mirrors ``integration-tests/beacon_client.py``:

* ``POST /api/query`` accepts either ``{"sql": "..."}`` or a JSON DSL body.
* **Reads** that should land in a file add ``output: {format: ...}``; the server
  materializes the file and streams it back as an attachment.
* **Writes** (DDL/DML) must NOT set an ``output`` format — an output format wraps
  the plan in ``COPY ... TO`` and a DDL node cannot be physically planned inside
  a COPY. With no output format the statement runs directly and returns an
  (often empty) Arrow IPC stream.
* Basic auth is sent only when credentials are configured, which elevates the
  request to super-user (DDL/DML allowed).
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import httpx

from .arrow_io import QueryResult, collect_ipc_stream
from .config import ClientConfig
from .errors import (
    AuthenticationError,
    ConnectionFailedError,
    QueryError,
    StreamInterruptedError,
)

QUERY_ID_HEADER = "x-beacon-query-id"


@dataclass(frozen=True)
class Identity:
    """Who the server considers the caller, resolved from ``/api/admin/check``.

    ``kind`` is one of ``"super-user"`` (admin auth accepted), ``"user"`` (valid
    credentials but not a super-user), ``"anonymous"`` (no credentials), or
    ``"unknown"`` (server didn't report — e.g. an older build without the check).
    """

    kind: str
    username: str | None = None

    @property
    def is_super_user(self) -> bool:
        return self.kind == "super-user"

    @property
    def label(self) -> str:
        if self.kind == "super-user":
            return f"super-user ({self.username})"
        if self.kind == "user":
            return f"{self.username} (read-only)"
        if self.kind == "anonymous":
            return "anonymous (read-only)"
        return "unknown identity"


class BeaconClient:
    """Synchronous client for a single Beacon server."""

    def __init__(self, config: ClientConfig):
        self.config = config
        self._http = httpx.Client(base_url=config.url, timeout=config.timeout)

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> BeaconClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- low level --------------------------------------------------------------
    def _auth(self) -> tuple[str, str] | None:
        return self.config.auth

    def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        try:
            return self._http.request(method, path, **kwargs)
        except httpx.ConnectError as exc:
            raise ConnectionFailedError(self.config.url, str(exc)) from exc
        except httpx.TimeoutException as exc:
            raise ConnectionFailedError(self.config.url, f"request timed out: {exc}") from exc

    # -- query (in-terminal, no output format) ----------------------------------
    def query(self, sql_or_body: str | dict, row_limit: int | None = None) -> QueryResult:
        """Run a query and decode the Arrow IPC stream into a ``pyarrow.Table``.

        Accepts a raw SQL string or a JSON DSL body dict. Works for SELECTs and
        for DDL/DML (which yield an empty table).

        The result stream is consumed incrementally: when ``row_limit`` is a
        non-negative int, only enough record batches are read to exceed that many
        rows (the rest of the response is never downloaded) and the result is
        flagged ``truncated``. ``row_limit=None`` (or negative) reads everything.
        """
        body = self._build_body(sql_or_body, output=None)
        start = time.perf_counter()
        try:
            with self._http.stream("POST", "/api/query", json=body, auth=self._auth()) as resp:
                if resp.status_code != 200:
                    raise QueryError(resp.status_code, resp.read().decode("utf-8", "replace"))
                query_id = resp.headers.get(QUERY_ID_HEADER)
                table, truncated = collect_ipc_stream(resp.iter_bytes(), row_limit)
        except httpx.ConnectError as exc:
            raise ConnectionFailedError(self.config.url, str(exc)) from exc
        except httpx.TimeoutException as exc:
            raise ConnectionFailedError(self.config.url, f"request timed out: {exc}") from exc
        except (httpx.RemoteProtocolError, httpx.ReadError) as exc:
            raise StreamInterruptedError(self.config.url, str(exc)) from exc
        elapsed = time.perf_counter() - start
        return QueryResult(table, query_id, elapsed, truncated)

    # -- query to file (sets an output format) ----------------------------------
    def query_to_file(self, sql_or_body: str | dict, output_format: Any, path: str) -> int:
        """Run a SELECT, asking the server for ``output_format``; stream to ``path``.

        Returns the number of bytes written. ``output_format`` is the JSON value
        the server expects under ``output.format`` (a string like ``"csv"`` or a
        structured object like ``{"nd_netcdf": {"dimension_columns": [...]}}``).
        """
        body = self._build_body(sql_or_body, output={"format": output_format})
        written = 0
        try:
            with self._http.stream("POST", "/api/query", json=body, auth=self._auth()) as resp:
                if resp.status_code != 200:
                    raise QueryError(resp.status_code, resp.read().decode("utf-8", "replace"))
                with open(path, "wb") as fh:
                    for chunk in resp.iter_bytes():
                        fh.write(chunk)
                        written += len(chunk)
        except httpx.ConnectError as exc:
            raise ConnectionFailedError(self.config.url, str(exc)) from exc
        except httpx.TimeoutException as exc:
            raise ConnectionFailedError(self.config.url, f"request timed out: {exc}") from exc
        except (httpx.RemoteProtocolError, httpx.ReadError) as exc:
            raise StreamInterruptedError(self.config.url, str(exc)) from exc
        return written

    # -- metadata (GET, JSON) ---------------------------------------------------
    def get_json(self, path: str, params: dict | None = None) -> Any:
        resp = self._request("GET", path, params=params, auth=self._auth())
        if resp.status_code != 200:
            raise QueryError(resp.status_code, resp.text)
        return resp.json()

    def tables(self) -> list[str]:
        return self.get_json("/api/tables")

    def tables_with_schema(self) -> list[dict]:
        return self.get_json("/api/tables-with-schema")

    def table_schema(self, table_name: str) -> dict:
        return self.get_json("/api/table-schema", {"table_name": table_name})

    def table_config(self, table_name: str) -> dict:
        return self.get_json("/api/table-config", {"table_name": table_name})

    def tables_with_config(self) -> list[tuple[str, dict]]:
        """Pair every table with its config (kind/format/location/...).

        Tables without a config (e.g. the built-in ``default``) yield an empty
        dict rather than failing the whole listing.
        """
        entries: list[tuple[str, dict]] = []
        for name in self.tables():
            try:
                entries.append((name, self.table_config(name)))
            except QueryError:
                entries.append((name, {}))
        return entries

    def datasets(self, pattern: str | None = None, limit: int | None = None) -> list[dict]:
        params: dict[str, Any] = {}
        if pattern is not None:
            params["pattern"] = pattern
        if limit is not None:
            params["limit"] = limit
        return self.get_json("/api/list-datasets", params or None)

    def dataset_schema(self, file: str) -> dict:
        return self.get_json("/api/dataset-schema", {"file": file})

    def functions(self, table: bool = False) -> list[dict]:
        return self.get_json("/api/table-functions" if table else "/api/functions")

    def info(self) -> Any:
        return self.get_json("/api/info")

    def identity(self) -> Identity:
        """Resolve who the server considers this session, via ``/api/admin/check``.

        * No credentials configured -> ``anonymous`` (no request is made).
        * ``200`` -> ``super-user`` (admin auth accepted).
        * ``401`` -> credentials were supplied but rejected; raises
          :class:`AuthenticationError` so the caller can report that it could not
          connect as a super-user.
        * ``403`` -> valid credentials, but not a super-user (``user``).
        * anything else -> ``unknown`` (don't block the session on it).
        """
        auth = self._auth()
        if auth is None:
            return Identity(kind="anonymous")
        resp = self._request("GET", "/api/admin/check", auth=auth)
        if resp.status_code == 200:
            return Identity(kind="super-user", username=self.config.username)
        if resp.status_code == 401:
            raise AuthenticationError(self.config.url, self.config.username)
        if resp.status_code == 403:
            return Identity(kind="user", username=self.config.username)
        return Identity(kind="unknown", username=self.config.username)

    def metrics(self, query_id: str) -> dict:
        return self.get_json(f"/api/query/metrics/{query_id}")

    # -- helpers ----------------------------------------------------------------
    @staticmethod
    def _build_body(sql_or_body: str | dict, output: dict | None) -> dict:
        if isinstance(sql_or_body, str):
            body: dict[str, Any] = {"sql": sql_or_body}
        else:
            body = dict(sql_or_body)
        if output is not None:
            body["output"] = output
        return body
