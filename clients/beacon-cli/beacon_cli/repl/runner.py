"""Execute a SQL statement in the shell and render the outcome."""

from __future__ import annotations

from rich.console import Console

from ..client import BeaconClient
from ..errors import BeaconCliError
from ..render import footer, table_to_records, table_to_rich, truncation_note
from .state import ReplState


def run_sql(statement: str, client: BeaconClient, console: Console, state: ReplState) -> None:
    """Run ``statement`` (no output format), render rows or ``OK``, and record it.

    Only enough rows to fill the render limit are streamed; set ``\\limit -1`` to
    fetch the whole result.
    """
    state.last_statement = statement
    row_limit = None if state.max_rows < 0 else state.max_rows
    try:
        result = client.query(statement, row_limit=row_limit)
    except BeaconCliError as exc:
        console.print(f"[red]error:[/red] {exc}")
        return

    if result.is_empty:
        console.print("[green]OK[/green]")
    else:
        if state.expand:
            console.print(table_to_records(result.table, state.max_rows))
        else:
            console.print(table_to_rich(result.table, state.max_rows))
        if result.truncated:
            console.print("[dim]... more rows available (\\limit -1 to fetch all)[/dim]")
        else:
            note = truncation_note(result.num_rows, state.max_rows)
            if note:
                console.print(f"[dim]{note}[/dim]")
    if state.timing:
        if result.truncated:
            shown = result.num_rows if state.max_rows < 0 else min(state.max_rows, result.num_rows)
            console.print(footer(shown, result.elapsed, result.query_id, more=True))
        else:
            console.print(footer(result.num_rows, result.elapsed, result.query_id))
