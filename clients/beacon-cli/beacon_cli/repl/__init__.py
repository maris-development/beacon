"""Interactive REPL shell for beacon-cli (prompt_toolkit + rich).

SQL statements are executed via the no-``output`` path (so both SELECTs and DDL
work) and rendered as tables. Lines beginning with ``\\`` are psql-style
meta-commands. The pieces are split across submodules:

* :mod:`reader`  — history, the prompt callable, multi-line statement reads
* :mod:`runner`  — execute one SQL statement and render the outcome
* :mod:`meta`    — backslash meta-command dispatch
* :mod:`state`   — per-session settings
* :mod:`help`    — the ``\\help`` text
"""

from __future__ import annotations

from rich.console import Console

from ..client import BeaconClient
from .meta import handle_meta
from .reader import make_prompt_fn, read_statement
from .runner import run_sql
from .state import ReplState

__all__ = ["run_repl"]


def run_repl(client: BeaconClient, console: Console) -> None:
    state = ReplState()
    prompt_fn = make_prompt_fn()

    console.print(
        f"[bold]beacon-cli[/bold] -> [cyan]{client.config.url}[/cyan]  "
        "(type [bold]\\help[/bold] for commands, [bold]\\q[/bold] to quit)"
    )

    while True:
        try:
            statement = read_statement(prompt_fn)
        except KeyboardInterrupt:
            continue  # Ctrl-C clears the current line
        except EOFError:
            break  # Ctrl-D exits

        if statement is None:
            continue
        statement = statement.strip()
        if not statement:
            continue

        if statement.startswith("\\") or statement.lower() in {"quit", "exit"}:
            if handle_meta(statement, client, console, state) == "quit":
                break
            continue

        run_sql(statement, client, console, state)

    console.print("[dim]exiting[/dim]")
