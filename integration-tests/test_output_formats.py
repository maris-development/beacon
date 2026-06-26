"""Exercise Beacon's query output formats.

The other suites only ever request CSV output. Beacon can also serialize a result
as an Arrow IPC file or a Parquet file (see ``OutputFormat`` in
``beacon-core/src/query/output.rs``). These tests decode those binary formats with
``pyarrow`` and assert all three agree with each other, and that the HTTP envelope
(content type, download filename, ``x-beacon-query-id`` header) is correct.
"""

from __future__ import annotations

# A deterministic, ordered projection so every format sees the exact same rows.
QUERY = (
    "SELECT platform, temperature, depth "
    "FROM read_parquet(['obs/*.parquet']) "
    "ORDER BY time LIMIT 100"
)


def test_arrow_and_parquet_agree_with_csv(client, sample_data):
    csv_rows = client.sql_rows(QUERY)
    arrow_tbl = client.arrow_table(QUERY)
    parquet_tbl = client.parquet_table(QUERY)

    # Row counts line up (CSV row 0 is the header).
    assert len(csv_rows) - 1 == arrow_tbl.num_rows == parquet_tbl.num_rows == 100

    # Same projection -> same columns, in order.
    assert csv_rows[0] == ["platform", "temperature", "depth"]
    assert arrow_tbl.column_names == ["platform", "temperature", "depth"]
    assert parquet_tbl.column_names == ["platform", "temperature", "depth"]

    # The numeric column matches cell-for-cell between the two binary encodings.
    assert arrow_tbl.column("temperature").to_pylist() == (
        parquet_tbl.column("temperature").to_pylist()
    )
    # ...and matches the CSV rendering once parsed back to floats.
    csv_temps = [float(r[1]) for r in csv_rows[1:]]
    assert csv_temps == [float(v) for v in arrow_tbl.column("temperature").to_pylist()]


def test_full_scan_arrow_row_count(client, sample_data):
    """An unbounded Arrow result returns every generated row."""
    tbl = client.arrow_table("SELECT * FROM read_parquet(['obs/*.parquet'])")
    assert tbl.num_rows == sample_data["total"]


def test_parquet_output_headers(client):
    resp = client.raw({"sql": QUERY, "output": {"format": "parquet"}})
    assert resp.status_code == 200
    assert resp.headers["Content-Type"] == "application/vnd.apache.parquet"
    assert "output.parquet" in resp.headers.get("Content-Disposition", "")
    assert resp.headers.get("x-beacon-query-id")


def test_arrow_output_headers(client):
    resp = client.raw({"sql": QUERY, "output": {"format": "arrow"}})
    assert resp.status_code == 200
    assert resp.headers["Content-Type"] == "application/vnd.apache.arrow.file"
    assert "output.arrow" in resp.headers.get("Content-Disposition", "")


def test_default_output_is_arrow_stream(client):
    """With no ``output`` block the runtime streams zstd-compressed Arrow IPC."""
    resp = client.raw({"sql": "SELECT 1 AS one"})
    assert resp.status_code == 200
    assert resp.headers["Content-Type"].startswith("application/vnd.apache.arrow.stream")
    assert resp.headers.get("x-beacon-query-id")
