"""Tests for Arrow IPC decoding and the QueryResult wrapper."""

from __future__ import annotations

import io
import time

import pyarrow as pa

from beacon_cli.arrow_io import QueryResult, collect_ipc_stream, decode_ipc_stream


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


class _SlowTail:
    """Yields the whole IPC body, then blocks before EOF — mimicking a server
    that holds the (keep-alive) connection open after the last data byte."""

    def __init__(self, data: bytes, delay: float):
        self._data = data
        self._delay = delay
        self._sent = False

    def __iter__(self):
        return self

    def __next__(self) -> bytes:
        if not self._sent:
            self._sent = True
            return self._data
        time.sleep(self._delay)
        raise StopIteration


def test_collect_stops_at_eos_without_waiting_for_connection_end():
    # The Arrow stream is fully available up front; decoding must not block on the
    # slow connection tail (regression: a BufferedReader used to over-read into it).
    data = _ipc_stream(pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]}))
    start = time.perf_counter()
    table, truncated = collect_ipc_stream(_SlowTail(data, delay=5.0), None)
    assert table.num_rows == 3
    assert not truncated
    assert time.perf_counter() - start < 1.0  # must not wait on the 5s tail
