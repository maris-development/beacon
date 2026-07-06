"""Catalog/metadata commands: tables, schema, datasets, functions, info, metrics."""

from __future__ import annotations

import typer

from ..errors import BeaconCliError
from ..render import (
    datasets_to_rich,
    functions_to_rich,
    schema_to_rich,
    tables_detail_to_rich,
    tables_to_rich,
)
from ._shared import console, fail, get_client


def tables(
    ctx: typer.Context,
    schema: bool = typer.Option(False, "--schema", help="Include each table's columns."),
    detail: bool = typer.Option(
        False, "--detail", "-l", help="Show each table's kind, format, location and partitions."
    ),
) -> None:
    """List the tables registered in the catalog."""
    try:
        with get_client(ctx) as client:
            if schema:
                for entry in client.tables_with_schema():
                    name = entry.get("table_name", "")
                    console.print(schema_to_rich(entry.get("columns", []), title=name))
            elif detail:
                console.print(tables_detail_to_rich(client.tables_with_config()))
            else:
                console.print(tables_to_rich(client.tables()))
    except BeaconCliError as exc:
        fail(exc)


def schema(ctx: typer.Context, table: str = typer.Argument(..., help="Table name.")) -> None:
    """Show the schema of a table."""
    try:
        with get_client(ctx) as client:
            view = client.table_schema(table)
    except BeaconCliError as exc:
        fail(exc)
    console.print(schema_to_rich(view.get("fields", []), title=table))


def datasets(
    ctx: typer.Context,
    pattern: str | None = typer.Option(None, "--pattern", help="Glob pattern filter."),
    limit: int | None = typer.Option(None, "--limit", help="Maximum datasets to list."),
) -> None:
    """List available datasets."""
    try:
        with get_client(ctx) as client:
            rows = client.datasets(pattern, limit)
    except BeaconCliError as exc:
        fail(exc)
    console.print(datasets_to_rich(rows))


def dataset_schema(
    ctx: typer.Context, file: str = typer.Argument(..., help="Dataset file path.")
) -> None:
    """Show the schema of a dataset file."""
    try:
        with get_client(ctx) as client:
            view = client.dataset_schema(file)
    except BeaconCliError as exc:
        fail(exc)
    console.print(schema_to_rich(view.get("fields", []), title=file))


def functions(
    ctx: typer.Context,
    table: bool = typer.Option(False, "--table", help="List table-valued functions instead."),
) -> None:
    """List available scalar/aggregate (or table) functions."""
    try:
        with get_client(ctx) as client:
            rows = client.functions(table)
    except BeaconCliError as exc:
        fail(exc)
    console.print(functions_to_rich(rows))


def info(ctx: typer.Context) -> None:
    """Show Beacon server/system information."""
    try:
        with get_client(ctx) as client:
            console.print_json(data=client.info())
    except BeaconCliError as exc:
        fail(exc)


def metrics(
    ctx: typer.Context, query_id: str = typer.Argument(..., help="A prior query id.")
) -> None:
    """Show execution metrics for a previously run query."""
    try:
        with get_client(ctx) as client:
            console.print_json(data=client.metrics(query_id))
    except BeaconCliError as exc:
        fail(exc)


def register(app: typer.Typer) -> None:
    app.command()(tables)
    app.command()(schema)
    app.command()(datasets)
    app.command(name="dataset-schema")(dataset_schema)
    app.command()(functions)
    app.command()(info)
    app.command()(metrics)
