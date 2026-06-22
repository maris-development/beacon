"""Tests for result rendering helpers."""

from __future__ import annotations

import pyarrow as pa
from rich.console import Console

from beacon_cli.render import (
    table_to_json,
    table_to_records,
    table_to_rich,
    truncation_note,
)


def test_table_to_rich_columns_match():
    table = pa.table({"a": [1, 2], "b": ["x", "y"]})
    rt = table_to_rich(table, max_rows=10)
    assert rt.row_count == 2
    assert len(rt.columns) == 2


def test_table_to_rich_respects_max_rows():
    table = pa.table({"a": list(range(10))})
    rt = table_to_rich(table, max_rows=3)
    assert rt.row_count == 3
    assert truncation_note(10, 3) is not None
    assert truncation_note(3, 10) is None


def test_table_to_json_roundtrip():
    table = pa.table({"a": [1], "b": ["x"]})
    out = table_to_json(table)
    assert '"a": 1' in out and '"b": "x"' in out


def test_table_to_records_expanded_layout():
    # Each row becomes a field/value block; every column name appears once per row.
    table = pa.table({"alpha": [1, 2], "beta": ["x", "y"]})
    out = _render_to_text(table_to_records(table, max_rows=10))
    assert out.count("alpha") == 2  # one per record
    assert out.count("beta") == 2
    assert "record 1" in out and "record 2" in out


def _render_to_text(renderable) -> str:
    console = Console(width=120, no_color=True, record=True)
    console.print(renderable)
    return console.export_text()


def test_renders_nanosecond_timestamps_out_of_datetime_range():
    # Nanosecond timestamps that overflow Python datetime must not crash; they are
    # stringified for display instead.
    ts = pa.array([2**62, 2**62 + 1], type=pa.timestamp("ns"))
    table = pa.table({"t": ts, "v": [1, 2]})
    rt = table_to_rich(table, max_rows=10)  # must not raise
    assert rt.row_count == 2
    out = table_to_json(table)  # must not raise
    assert '"v": 1' in out
