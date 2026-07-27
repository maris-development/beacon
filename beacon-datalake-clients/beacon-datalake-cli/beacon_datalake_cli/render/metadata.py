"""Rendering for catalog metadata (schemas, tables, datasets, functions)."""

from __future__ import annotations

from typing import Any

from rich.table import Table as RichTable


def schema_to_rich(fields: list[dict[str, Any]], title: str | None = None) -> RichTable:
    """Render a list of schema fields (``{name, data_type, nullable}``)."""
    rt = RichTable(title=title, show_header=True, header_style="bold cyan")
    rt.add_column("column")
    rt.add_column("type")
    rt.add_column("nullable", justify="center")
    for field in fields:
        rt.add_row(
            str(field.get("name", "")),
            str(field.get("data_type", "")),
            "yes" if field.get("nullable") else "",
        )
    return rt


def tables_to_rich(names: list[str]) -> RichTable:
    rt = RichTable(show_header=True, header_style="bold cyan")
    rt.add_column("table")
    for name in names:
        rt.add_row(str(name))
    return rt


# Friendly labels for the server's internal ``definition_type`` discriminators
# (see beacon-datafusion-ext table definitions). Unknown values fall through
# unchanged so new definition kinds are never hidden.
_KIND_LABELS = {
    "listing_table": "external (files)",
    "remote_table": "external (remote)",
    "view_table": "view",
    "materialized_view": "materialized view",
    "logical": "logical",
}


def friendly_kind(definition_type: str) -> str:
    """Map a raw ``definition_type`` to a human label, else return it unchanged."""
    return _KIND_LABELS.get(definition_type, definition_type)


def tables_detail_to_rich(entries: list[tuple[str, dict[str, Any]]]) -> RichTable:
    """Render tables with their kind/format/location from ``/api/table-config``.

    ``entries`` is a list of ``(table_name, config)`` pairs; an empty config
    (e.g. the built-in ``default`` table) renders blank metadata cells.
    """
    rt = RichTable(show_header=True, header_style="bold cyan")
    rt.add_column("table")
    rt.add_column("kind")
    rt.add_column("format")
    rt.add_column("location", overflow="fold")
    rt.add_column("partitions")
    for name, cfg in entries:
        partitions = cfg.get("partition_cols") or []
        rt.add_row(
            str(name),
            friendly_kind(str(cfg.get("definition_type", ""))),
            str(cfg.get("file_type", "")),
            str(cfg.get("location", "")),
            ", ".join(str(p) for p in partitions),
        )
    return rt


def datasets_to_rich(datasets: list[dict[str, Any]]) -> RichTable:
    rt = RichTable(show_header=True, header_style="bold cyan")
    rt.add_column("path", overflow="fold")
    rt.add_column("format")
    rt.add_column("inspect", justify="center")
    for ds in datasets:
        rt.add_row(
            str(ds.get("file_path", "")),
            str(ds.get("format", "")),
            "yes" if ds.get("can_inspect") else "",
        )
    return rt


def functions_to_rich(functions: list[dict[str, Any]]) -> RichTable:
    rt = RichTable(show_header=True, header_style="bold cyan")
    rt.add_column("function")
    rt.add_column("returns")
    rt.add_column("description", overflow="fold")
    for fn in functions:
        params = ", ".join(p.get("name", "") for p in fn.get("params", []))
        name = fn.get("function_name", "")
        rt.add_row(
            f"{name}({params})",
            str(fn.get("return_type", "")),
            str(fn.get("description", "")),
        )
    return rt
