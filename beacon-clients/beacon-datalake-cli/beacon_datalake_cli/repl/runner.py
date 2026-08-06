"""Execute a SQL statement in the shell and render the outcome."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import TypeVar

from rich.console import Console

from ..client import BeaconClient
from ..errors import BeaconCliError
from ..render import footer, table_to_records, table_to_rich, truncation_note
from .state import ReplState

T = TypeVar("T")


def run_with_spinner(console: Console, work: Callable[[], T]) -> T:
    """Run ``work()`` while showing a spinner with a live elapsed timer.

    Ctrl+C interrupts the (blocking) call — the request is torn down and the
    ``KeyboardInterrupt`` propagates to the caller so it can report a cancel.
    """
    start = time.perf_counter()
    stop = threading.Event()
    with console.status("[dim]running…  0.0s  (Ctrl+C to cancel)[/dim]", spinner="dots") as status:

        def tick() -> None:
            while not stop.wait(0.1):
                elapsed = time.perf_counter() - start
                status.update(f"[dim]running…  {elapsed:.1f}s  (Ctrl+C to cancel)[/dim]")

        updater = threading.Thread(target=tick, daemon=True)
        updater.start()
        try:
            return work()
        finally:
            stop.set()


def run_sql(statement: str, client: BeaconClient, console: Console, state: ReplState) -> None:
    """Run ``statement`` (no output format), render rows or ``OK``, and record it.

    Only enough rows to fill the render limit are streamed; set ``\\limit -1`` to
    fetch the whole result. A spinner shows progress; Ctrl+C cancels the query.
    """
    state.last_statement = statement
    row_limit = None if state.max_rows < 0 else state.max_rows
    # KeyboardInterrupt is left to propagate so the caller can report a cancel
    # (and, for a script, abort the remaining statements).
    try:
        result = run_with_spinner(console, lambda: client.query(statement, row_limit=row_limit))
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
