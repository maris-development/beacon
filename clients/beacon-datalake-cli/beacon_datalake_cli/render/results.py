"""Rendering for query result sets (pyarrow tables -> rich / JSON)."""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
from rich import box
from rich.console import Group, RenderableType
from rich.table import Table as RichTable

DEFAULT_MAX_ROWS = 100


def table_to_rich(table: pa.Table, max_rows: int = DEFAULT_MAX_ROWS) -> RichTable:
    """Render a ``pyarrow.Table`` as a rich table, capped at ``max_rows`` rows."""
    rt = RichTable(show_header=True, header_style="bold cyan", show_lines=False)

    if table.num_columns == 0:
        rt.add_column("(no columns)")
        return rt

    for field in table.schema:
        rt.add_column(f"{field.name}\n[dim]{field.type}[/dim]", overflow="fold")

    shown = table.slice(0, max_rows) if max_rows >= 0 else table
    rows = _to_rows(shown)
    columns = [f.name for f in table.schema]
    for row in rows:
        rt.add_row(*[_fmt_cell(row.get(col)) for col in columns])

    return rt


def table_to_records(table: pa.Table, max_rows: int = DEFAULT_MAX_ROWS) -> RenderableType:
    """Render rows vertically, one record per block (``field | value``).

    This 'expanded' layout (à la psql ``\\x``) keeps very wide results readable:
    column count no longer competes for terminal width, so each value gets the
    full line.
    """
    if table.num_columns == 0:
        return RichTable()  # empty; nothing to expand

    shown = table.slice(0, max_rows) if max_rows >= 0 else table
    rows = _to_rows(shown)
    names = table.column_names

    blocks: list[RenderableType] = []
    for i, row in enumerate(rows, start=1):
        rec = RichTable(
            show_header=False,
            box=box.SIMPLE,
            title=f"[dim]record {i}[/dim]",
            title_justify="left",
            pad_edge=False,
        )
        rec.add_column("field", style="bold cyan", no_wrap=True)
        rec.add_column("value", overflow="fold")
        for name in names:
            rec.add_row(name, _fmt_cell(row.get(name)))
        blocks.append(rec)

    return Group(*blocks)


def truncation_note(num_rows: int, max_rows: int) -> str | None:
    if 0 <= max_rows < num_rows:
        return f"... {num_rows - max_rows} more row(s) not shown (use --max-rows)"
    return None


def footer(num_rows: int, elapsed: float, query_id: str | None, *, more: bool = False) -> str:
    """Summary line. With ``more=True``, ``num_rows`` is the number shown and the
    full result has additional rows that were not fetched."""
    count = f"first {num_rows} rows (more available)" if more else f"{num_rows} row(s)"
    parts = [count, f"{elapsed * 1000:.0f} ms"]
    if query_id:
        parts.append(f"query {query_id}")
    return "[dim](" + " | ".join(parts) + ")[/dim]"


def table_to_json(table: pa.Table, max_rows: int = -1) -> str:
    shown = table.slice(0, max_rows) if max_rows >= 0 else table
    return json.dumps(_to_rows(shown), default=str, indent=2)


def _to_rows(table: pa.Table) -> list[dict[str, Any]]:
    """``Table.to_pylist`` with temporal columns rendered as strings.

    pyarrow refuses to convert nanosecond timestamps (and similar high-precision
    temporal values) to Python ``datetime`` when they fall outside its range, so
    cast those columns to their string form first — which is what we want to show
    anyway.
    """
    return _stringify_temporal(table).to_pylist()


def _stringify_temporal(table: pa.Table) -> pa.Table:
    columns = []
    changed = False
    for field in table.schema:
        column = table.column(field.name)
        if _is_temporal(field.type):
            try:
                column = pc.cast(column, pa.string())
                changed = True
            except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
                pass
        columns.append(column)
    return pa.table(columns, names=table.column_names) if changed else table


def _is_temporal(dtype: pa.DataType) -> bool:
    return (
        pa.types.is_timestamp(dtype)
        or pa.types.is_time(dtype)
        or pa.types.is_duration(dtype)
    )


def _fmt_cell(value: Any) -> str:
    if value is None:
        return "[dim]NULL[/dim]"
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str)
    return str(value)
