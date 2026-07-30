"""Streaming a relation's result batch by batch.

`record_batch()` returns a genuine `pyarrow.RecordBatchReader` that pulls from the engine on
demand — a memory-bounded alternative to `.arrow()`/`.df()`, which collect everything. These
tests pin the reader's laziness, the optional `batch_size` re-chunking (which must preserve every
row, in order, across the concat/slice it does internally), and that each call runs the query
afresh.
"""

from __future__ import annotations

import pytest

import beacondb


@pytest.fixture
def con():
    with beacondb.connect(":memory:") as connection:
        yield connection


def test_record_batch_returns_a_pyarrow_reader(con):
    pa = pytest.importorskip("pyarrow")
    reader = con.sql("SELECT i FROM range(0, 10) t(i)").record_batch()
    assert isinstance(reader, pa.RecordBatchReader)
    # the schema is available before any batch is pulled
    assert reader.schema.names == ["i"]
    table = reader.read_all()
    assert table.num_rows == 10
    assert table.column("i").to_pylist() == list(range(10))


def test_batch_size_rechunks_without_losing_rows(con):
    pytest.importorskip("pyarrow")
    rel = con.sql("SELECT i, i * i AS sq FROM range(0, 2500) t(i) ORDER BY i")
    batches = list(rel.record_batch(1000))
    # every batch is at most the requested size, and the concat/slice keeps every row in order
    assert all(b.num_rows <= 1000 for b in batches)
    assert sum(b.num_rows for b in batches) == 2500
    i_values, sq_values = [], []
    for b in batches:
        i_values.extend(b.column("i").to_pylist())
        sq_values.extend(b.column("sq").to_pylist())
    assert i_values == list(range(2500))
    assert sq_values == [i * i for i in range(2500)]


def test_reader_is_lazy_batch_by_batch(con):
    pytest.importorskip("pyarrow")
    reader = con.sql("SELECT i FROM range(0, 3) t(i)").record_batch(1)
    # pulled one at a time; StopIteration at the end (not an error)
    assert reader.read_next_batch().num_rows == 1
    assert reader.read_next_batch().num_rows == 1
    assert reader.read_next_batch().num_rows == 1
    with pytest.raises(StopIteration):
        reader.read_next_batch()


def test_streaming_works_with_a_filtered_ordered_query(con):
    pytest.importorskip("pyarrow")
    rel = con.sql(
        "SELECT i FROM range(0, 100) t(i) WHERE i % 2 = 0 ORDER BY i desc LIMIT 3"
    )
    rows = [v for b in rel.record_batch() for v in b.column("i").to_pylist()]
    assert rows == [98, 96, 94]


def test_empty_result_streams_zero_batches_with_a_schema(con):
    pa = pytest.importorskip("pyarrow")
    reader = con.sql("SELECT 1 AS a WHERE 1 = 0").record_batch(100)
    assert isinstance(reader, pa.RecordBatchReader)
    assert reader.schema.names == ["a"]
    assert list(reader) == []


def test_each_call_runs_the_query_afresh(con):
    pytest.importorskip("pyarrow")
    rel = con.sql("SELECT count(*) AS n FROM range(0, 42) t(i)")
    first = rel.record_batch().read_all().column("n").to_pylist()
    second = rel.record_batch().read_all().column("n").to_pylist()
    assert first == second == [42]


def test_fetch_record_batch_and_fetch_arrow_reader_are_aliases(con):
    pa = pytest.importorskip("pyarrow")
    rel = con.sql("SELECT i FROM range(0, 5) t(i)")
    assert isinstance(rel.fetch_record_batch(), pa.RecordBatchReader)
    assert isinstance(rel.fetch_arrow_reader(2), pa.RecordBatchReader)
    assert rel.fetch_record_batch().read_all().num_rows == 5


def test_batch_size_must_be_positive(con):
    with pytest.raises(beacondb.ProgrammingError, match="positive"):
        con.sql("SELECT 1").record_batch(0)
