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

import threading

from rich.console import Console

from ..client import BeaconClient, Identity
from .completion import BeaconCompleter, TableColumns, TableNames, warm_cache
from .meta import handle_meta
from .reader import ansi_fg, make_prompt_session, read_statement
from .runner import run_sql
from .state import ReplState
from .toolbar import make_bottom_toolbar

__all__ = ["run_repl"]


def _identity_markup(identity: Identity) -> str:
    """Colour the session identity for the connect banner (palette hues)."""
    if identity.is_super_user:
        return f"[#c3e88d]super-user[/] [dim]({identity.username})[/dim]"
    if identity.kind == "user":
        return f"[#89ddff]{identity.username}[/] [dim](read-only)[/dim]"
    if identity.kind == "anonymous":
        return "[dim]anonymous (read-only)[/dim]"
    return "[dim]unknown identity[/dim]"


# Prompt tint by access level, drawn from the SQL palette so it matches the rest
# of the theme: super-user = string green, user = quoted-identifier cyan,
# anonymous = comment gray (an intuitive full → limited → none ladder).
_PROMPT_COLORS = {
    "super-user": ansi_fg("#c3e88d", bold=True),
    "user": ansi_fg("#89ddff", bold=True),
    "anonymous": ansi_fg("#6c7086"),
}


def _prompt(identity: Identity) -> str:
    """First-line prompt encoding the session identity, e.g. ``beacon (admin - super-user)> ``.

    Anonymous sessions have no username, so they collapse to ``beacon (anonymous)> ``.
    """
    if identity.kind in ("super-user", "user") and identity.username:
        return f"beacon ({identity.username} - {identity.kind})> "
    if identity.kind == "anonymous":
        return "beacon (anonymous)> "
    return "beacon> "


def run_repl(
    client: BeaconClient, console: Console, identity: Identity, vi_mode: bool = False
) -> None:
    state = ReplState()
    table_names = TableNames(client)
    table_columns = TableColumns(client)
    completer = BeaconCompleter(table_names.get, table_columns.get)
    # Warm the table + column caches in the background so completions are served
    # from memory, not a blocking round-trip on the first keystroke.
    threading.Thread(
        target=warm_cache, args=(table_names, table_columns), daemon=True
    ).start()
    toolbar = make_bottom_toolbar(client.config.url, identity, state)
    session = make_prompt_session(completer, vi_mode=vi_mode, bottom_toolbar=toolbar)
    state.session = session  # let \vi / \emacs flip the editing mode at runtime
    prompt = _prompt(identity)
    prompt_color = _PROMPT_COLORS.get(identity.kind)

    console.print(
        f"[bold]beacon-cli[/bold] -> [cyan]{client.config.url}[/cyan] "
        f"[dim]as[/dim] {_identity_markup(identity)}  "
        "(type [bold]\\help[/bold] for commands, [bold]\\q[/bold] to quit)"
    )

    while True:
        try:
            statement = read_statement(session, prompt, prompt_color)
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
            try:
                if handle_meta(statement, client, console, state) == "quit":
                    break
            except KeyboardInterrupt:
                console.print("[yellow]cancelled[/yellow]")
            continue

        try:
            run_sql(statement, client, console, state)
        except KeyboardInterrupt:
            console.print("[yellow]cancelled[/yellow]")

    console.print("[dim]exiting[/dim]")
