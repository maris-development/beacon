"""The ``query`` and ``export`` commands: run SQL, render or write results."""

from __future__ import annotations

import typer

from ..errors import BeaconCliError
from ..formats import build_output_format, infer_format_name
from ..render import (
    DEFAULT_MAX_ROWS,
    footer,
    table_to_json,
    table_to_records,
    table_to_rich,
    truncation_note,
)
from ._shared import console, err_console, fail, get_client, read_sql


def query(
    ctx: typer.Context,
    sql: str | None = typer.Argument(None, help="SQL statement (or use --file / stdin)."),
    file: str | None = typer.Option(None, "--file", "-f", help="Read SQL from a file."),
    max_rows: int = typer.Option(
        DEFAULT_MAX_ROWS, "--max-rows", help="Max rows to render (-1 = all)."
    ),
    fetch_all: bool = typer.Option(
        False, "--all", "-a", help="Fetch the whole result instead of stopping at the render limit."
    ),
    expand: bool = typer.Option(
        False, "--expand", "-x", help="Render rows vertically (field/value) — good for wide tables."
    ),
    as_json: bool = typer.Option(False, "--json", help="Print rows as JSON instead of a table."),
) -> None:
    """Run a query and render the results in the terminal.

    By default only enough rows to fill the render limit are streamed from the
    server; pass --all (or --max-rows -1) to fetch the entire result. Use
    --expand for wide tables, where the grid layout squeezes columns too small.
    """
    statement = read_sql(sql, file)
    # row budget: None => fetch everything; otherwise stop once we exceed max_rows.
    row_limit = None if (fetch_all or max_rows < 0) else max_rows
    try:
        with get_client(ctx) as client:
            result = client.query(statement, row_limit=row_limit)
    except BeaconCliError as exc:
        fail(exc)

    # In JSON mode the footer goes to stderr so stdout stays valid JSON.
    footer_console = err_console if as_json else console

    # A side-effecting statement (DDL/DML, CREATE/RUN CRAWLER, REFRESH, ...) comes
    # back with no schema at all; report success rather than an empty grid.
    if result.is_empty:
        if as_json:
            console.print_json(data=[])
        else:
            console.print("[green]OK[/green]")
        footer_console.print(footer(0, result.elapsed, result.query_id))
        return

    if as_json:
        console.print_json(table_to_json(result.table, max_rows))
    else:
        if expand:
            console.print(table_to_records(result.table, max_rows))
        else:
            console.print(table_to_rich(result.table, max_rows))
        if not result.truncated:
            note = truncation_note(result.num_rows, max_rows)
            if note:
                console.print(f"[dim]{note}[/dim]")

    if result.truncated:
        shown = result.num_rows if max_rows < 0 else min(max_rows, result.num_rows)
        footer_console.print(footer(shown, result.elapsed, result.query_id, more=True))
    else:
        footer_console.print(footer(result.num_rows, result.elapsed, result.query_id))


def export(
    ctx: typer.Context,
    sql: str | None = typer.Argument(None, help="SQL SELECT (or use --file / stdin)."),
    output: str = typer.Option(..., "--output", "-o", help="Destination file path."),
    fmt: str | None = typer.Option(
        None, "--format", help="Output format (inferred from -o extension if omitted)."
    ),
    file: str | None = typer.Option(None, "--file", "-f", help="Read SQL from a file."),
    dimension_columns: list[str] | None = typer.Option(
        None, "--dimension-columns", help="Dimension columns for nd_netcdf."
    ),
    lon: str | None = typer.Option(None, "--lon", help="Longitude column for geoparquet."),
    lat: str | None = typer.Option(None, "--lat", help="Latitude column for geoparquet."),
) -> None:
    """Run a query and write the results to a file (CSV/Parquet/Arrow/NetCDF/...)."""
    statement = read_sql(sql, file)
    try:
        name = infer_format_name(output, fmt)
        output_format = build_output_format(
            name, dimension_columns=dimension_columns, lon=lon, lat=lat
        )
        with get_client(ctx) as client:
            written = client.query_to_file(statement, output_format, output)
    except (BeaconCliError, ValueError, OSError) as exc:
        fail(exc)

    console.print(f"[green]wrote[/green] {output} [dim]({name}, {written} bytes)[/dim]")


def register(app: typer.Typer) -> None:
    app.command()(query)
    app.command()(export)
