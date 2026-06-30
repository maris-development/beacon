"""Query planning & observability endpoints.

Beyond running a query, Beacon can validate it (``/api/parse-query``), explain the
plan it would run (``/api/explain-query``), execute-and-annotate it
(``/api/explain-analyze-query``), and report recorded metrics for a finished query
(``/api/query/metrics/{id}`` keyed by the ``x-beacon-query-id`` response header).
"""

from __future__ import annotations

SCAN = "SELECT temperature FROM read_parquet(['obs/*.parquet']) WHERE temperature > 20"


def test_parse_query_accepts_valid_body(client):
    # parse-query only checks the body deserializes; no auth, not executed.
    resp = client.admin_post("/api/parse-query", {"sql": "SELECT 1"}, admin=False)
    assert resp.status_code == 200


def test_parse_query_rejects_non_object_body(client):
    # A JSON array cannot deserialize into the query request map -> 4xx.
    resp = client.admin_post("/api/parse-query", [], admin=False)
    assert resp.status_code in (400, 422)


def test_explain_query_returns_json_plan(client):
    resp = client.admin_post("/api/explain-query", {"sql": SCAN}, admin=False)
    assert resp.status_code == 200
    assert resp.headers["Content-Type"].startswith("application/json")
    plan = resp.json()  # must be valid JSON
    assert plan  # non-empty plan description


def test_explain_query_rejects_invalid_query(client):
    resp = client.admin_post(
        "/api/explain-query", {"sql": "SELECT * FROM no_such_table_xyz"}, admin=False
    )
    assert resp.status_code == 400


def test_explain_analyze_executes_and_returns_json(client):
    # EXPLAIN ANALYZE runs the query, so it follows the same SQL/auth gating as
    # /api/query; admin credentials keep parity with the rest of the suite.
    resp = client.admin_post("/api/explain-analyze-query", {"sql": SCAN}, admin=True)
    assert resp.status_code == 200
    assert resp.headers["Content-Type"].startswith("application/json")
    assert resp.json()


def test_query_metrics_round_trip(client, sample_data):
    qid = client.query_id(SCAN)
    resp = client.get(f"/api/query/metrics/{qid}")
    assert resp.status_code == 200
    metrics = resp.json()
    assert metrics["query_id"] == qid
    # The recorded result row count matches the warm subset the scan produced.
    # (input_rows / file_paths / logical plans are not populated for this scan in
    # the current build, so we assert only the fields that are.)
    assert metrics["result_num_rows"] == sample_data["warm_count"]


def test_query_metrics_bad_uuid_is_400(client):
    assert client.get("/api/query/metrics/not-a-uuid").status_code == 400


def test_query_metrics_unknown_uuid_is_404(client):
    unknown = "00000000-0000-0000-0000-000000000000"
    assert client.get(f"/api/query/metrics/{unknown}").status_code == 404


def test_sql_explain_statement(client):
    """The SQL ``EXPLAIN`` statement returns a textual plan."""
    rows = client.sql_rows(f"EXPLAIN {SCAN}", admin=True)
    plan = "\n".join(cell for row in rows for cell in row)
    assert "temperature" in plan.lower()
