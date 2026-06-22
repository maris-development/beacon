"""Tests for BeaconClient using respx to mock the HTTP layer."""

from __future__ import annotations

import io

import httpx
import pyarrow as pa
import pytest
import respx

from beacon_cli.client import QUERY_ID_HEADER, BeaconClient
from beacon_cli.config import ClientConfig
from beacon_cli.errors import ConnectionFailedError, QueryError

BASE = "http://beacon.test"


def _client() -> BeaconClient:
    return BeaconClient(ClientConfig(url=BASE))


def _ipc_stream(table: pa.Table) -> bytes:
    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue()


def _multi_batch_ipc(num_batches: int, rows_per_batch: int) -> bytes:
    """An IPC stream with several record batches (to exercise incremental reads)."""
    schema = pa.schema([("a", pa.int64())])
    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, schema) as writer:
        for b in range(num_batches):
            start = b * rows_per_batch
            writer.write_batch(
                pa.record_batch({"a": list(range(start, start + rows_per_batch))}, schema=schema)
            )
    return sink.getvalue()


@respx.mock
def test_query_decodes_ipc_stream():
    table = pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    respx.post(f"{BASE}/api/query").mock(
        return_value=httpx.Response(
            200, content=_ipc_stream(table), headers={QUERY_ID_HEADER: "q-123"}
        )
    )
    with _client() as client:
        result = client.query("SELECT * FROM default")
    assert result.num_rows == 3
    assert result.query_id == "q-123"
    assert result.table.column_names == ["a", "b"]


@respx.mock
def test_query_row_limit_stops_early_and_flags_truncated():
    # 3 batches x 2 rows = 6 rows total; a budget of 1 reads only the first batch.
    respx.post(f"{BASE}/api/query").mock(
        return_value=httpx.Response(200, content=_multi_batch_ipc(3, 2))
    )
    with _client() as client:
        result = client.query("SELECT * FROM default", row_limit=1)
    assert result.truncated is True
    assert result.num_rows == 2  # only the first batch was decoded


@respx.mock
def test_query_no_limit_reads_all():
    respx.post(f"{BASE}/api/query").mock(
        return_value=httpx.Response(200, content=_multi_batch_ipc(3, 2))
    )
    with _client() as client:
        result = client.query("SELECT * FROM default", row_limit=None)
    assert result.truncated is False
    assert result.num_rows == 6


@respx.mock
def test_query_limit_at_or_above_total_not_truncated():
    respx.post(f"{BASE}/api/query").mock(
        return_value=httpx.Response(200, content=_multi_batch_ipc(3, 2))
    )
    with _client() as client:
        result = client.query("SELECT * FROM default", row_limit=10)
    assert result.truncated is False
    assert result.num_rows == 6


@respx.mock
def test_query_empty_body_is_empty_table():
    # DDL/DML returns an empty stream; should become a 0-row, 0-col table.
    respx.post(f"{BASE}/api/query").mock(return_value=httpx.Response(200, content=b""))
    with _client() as client:
        result = client.query("CREATE EXTERNAL TABLE t ...")
    assert result.num_rows == 0
    assert result.table.num_columns == 0


@respx.mock
def test_query_error_surfaces_server_message():
    respx.post(f"{BASE}/api/query").mock(
        return_value=httpx.Response(400, text='"unknown column foo"')
    )
    with _client() as client:
        with pytest.raises(QueryError) as exc:
            client.query("SELECT foo")
    assert exc.value.status_code == 400
    assert "unknown column foo" in str(exc.value)


@respx.mock
def test_query_to_file_streams_bytes(tmp_path):
    respx.post(f"{BASE}/api/query").mock(
        return_value=httpx.Response(200, content=b"a,b\n1,2\n")
    )
    out = tmp_path / "out.csv"
    with _client() as client:
        written = client.query_to_file("SELECT * FROM default", "csv", str(out))
    assert written == len(b"a,b\n1,2\n")
    assert out.read_bytes() == b"a,b\n1,2\n"


@respx.mock
def test_build_body_passthrough_and_output():
    captured = {}

    def _capture(request):
        import json

        captured.update(json.loads(request.content))
        table = pa.table({"a": [1]})
        return httpx.Response(200, content=_ipc_stream(table))

    respx.post(f"{BASE}/api/query").mock(side_effect=_capture)
    with _client() as client:
        client.query({"select": [{"column": "a"}], "from": {"table": "default"}})
    assert "output" not in captured  # in-terminal query never sets output
    assert captured["select"] == [{"column": "a"}]


@respx.mock
def test_metadata_get_json():
    respx.get(f"{BASE}/api/tables").mock(return_value=httpx.Response(200, json=["default", "obs"]))
    with _client() as client:
        assert client.tables() == ["default", "obs"]


@respx.mock
def test_tables_with_config_pairs_and_tolerates_404():
    respx.get(f"{BASE}/api/tables").mock(
        return_value=httpx.Response(200, json=["wod", "default"])
    )

    def _config(request):
        if request.url.params.get("table_name") == "wod":
            return httpx.Response(200, json={"definition_type": "listing_table", "file_type": "NC"})
        return httpx.Response(404, text='"Table default not found"')

    respx.get(f"{BASE}/api/table-config").mock(side_effect=_config)
    with _client() as client:
        entries = client.tables_with_config()
    assert entries[0][0] == "wod" and entries[0][1]["file_type"] == "NC"
    assert entries[1] == ("default", {})  # 404 -> empty config, not an error


@respx.mock
def test_connection_error_is_friendly():
    respx.post(f"{BASE}/api/query").mock(side_effect=httpx.ConnectError("refused"))
    with _client() as client:
        with pytest.raises(ConnectionFailedError) as exc:
            client.query("SELECT 1")
    assert BASE in str(exc.value)
