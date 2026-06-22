"""Tests for Arrow IPC decoding and the QueryResult wrapper."""

from __future__ import annotations

import io

import pyarrow as pa

from beacon_cli.arrow_io import QueryResult, decode_ipc_stream


def _ipc_stream(table: pa.Table) -> bytes:
    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue()


def test_decode_roundtrip():
    table = pa.table({"a": [1, 2, 3]})
    decoded = decode_ipc_stream(_ipc_stream(table))
    assert decoded.num_rows == 3
    assert decoded.column_names == ["a"]


def test_decode_empty_is_empty_table():
    assert decode_ipc_stream(b"").num_columns == 0


def test_query_result_is_empty():
    assert QueryResult(pa.table({}), None, 0.0).is_empty
    assert not QueryResult(pa.table({"a": [1]}), "q", 0.1).is_empty
