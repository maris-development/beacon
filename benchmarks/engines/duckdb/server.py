"""Minimal HTTP query server around an in-container DuckDB.

DuckDB has no native client/server protocol, so to run it as a containerized engine on the
same volume (and under the same CPU/memory cgroup) as the others, we expose one long-lived
DuckDB connection over HTTP. POST /query {"sql": "..."} -> {"value": <first cell>}.
A single persistent connection keeps DuckDB's caches warm across the warm-run loop.
"""

import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer

import duckdb

con = duckdb.connect(database=":memory:")
con.execute(f"SET threads={os.environ.get('DUCKDB_THREADS', '4')}")
con.execute(f"SET memory_limit='{os.environ.get('DUCKDB_MEMORY', '8GB')}'")


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, payload):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # health check
        self._send(200, {"status": "ok"})

    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        try:
            sql = json.loads(self.rfile.read(n) or b"{}")["sql"]
            row = con.execute(sql).fetchone()
            self._send(200, {"value": row[0] if row else None})
        except Exception as e:  # surface query/parse errors to the client
            self._send(400, {"error": str(e)})

    def log_message(self, *_):  # silence per-request logging
        pass


if __name__ == "__main__":
    port = int(os.environ.get("DUCKDB_PORT", "8500"))
    print(f"DuckDB HTTP server listening on 0.0.0.0:{port}", flush=True)
    HTTPServer(("0.0.0.0", port), Handler).serve_forever()
