"""Dispatch and handlers for the shell's backslash meta-commands."""

from __future__ import annotations

from rich.console import Console

from ..client import BeaconClient
from ..errors import BeaconCliError
from ..formats import build_output_format, infer_format_name
from ..render import (
    datasets_to_rich,
    functions_to_rich,
    schema_to_rich,
    tables_detail_to_rich,
    tables_to_rich,
)
from .help import HELP_TEXT
from .runner import run_sql
from .state import ReplState


def handle_meta(line: str, client: BeaconClient, console: Console, state: ReplState) -> str | None:
    """Handle a meta-command line. Returns ``"quit"`` to leave the shell."""
    parts = line.strip().split()
    cmd = parts[0].lower()
    args = parts[1:]

    if cmd in {"\\q", "quit", "exit"}:
        return "quit"
    if cmd in {"\\help", "\\?"}:
        console.print(HELP_TEXT)
        return None

    try:
        _dispatch(cmd, args, client, console, state)
    except BeaconCliError as exc:
        console.print(f"[red]error:[/red] {exc}")
    return None


def _dispatch(
    cmd: str, args: list[str], client: BeaconClient, console: Console, state: ReplState
) -> None:
    if cmd == "\\dt":
        console.print(tables_to_rich(client.tables()))
    elif cmd == "\\dt+":
        console.print(tables_detail_to_rich(client.tables_with_config()))
    elif cmd == "\\d":
        if not args:
            console.print("[yellow]usage: \\d <table>[/yellow]")
        else:
            view = client.table_schema(args[0])
            console.print(schema_to_rich(view.get("fields", []), title=args[0]))
    elif cmd == "\\df":
        console.print(functions_to_rich(client.functions(table=False)))
    elif cmd == "\\dft":
        console.print(functions_to_rich(client.functions(table=True)))
    elif cmd == "\\datasets":
        pattern = args[0] if args else None
        console.print(datasets_to_rich(client.datasets(pattern)))
    elif cmd == "\\crawlers":
        run_sql("SHOW CRAWLERS", client, console, state)
    elif cmd == "\\run-crawler":
        if not args:
            console.print("[yellow]usage: \\run-crawler <name>[/yellow]")
        else:
            run_sql(f"RUN CRAWLER {args[0]}", client, console, state)
    elif cmd == "\\refresh":
        if not args:
            console.print("[yellow]usage: \\refresh <table>[/yellow]")
        else:
            run_sql(f"REFRESH TABLE {args[0]}", client, console, state)
    elif cmd == "\\info":
        console.print_json(data=client.info())
    elif cmd == "\\format":
        _set_format(args, console, state)
    elif cmd in {"\\export", "\\o"}:
        _export_last(args, client, console, state)
    elif cmd == "\\limit":
        _set_limit(args, console, state)
    elif cmd == "\\timing":
        state.timing = not state.timing
        console.print(f"timing [bold]{'on' if state.timing else 'off'}[/bold]")
    elif cmd == "\\x":
        state.expand = not state.expand
        mode = "expanded (field/value)" if state.expand else "table"
        console.print(f"display [bold]{mode}[/bold]")
    else:
        console.print(f"[yellow]unknown command: {cmd}[/yellow] (try \\help)")


def _set_format(args: list[str], console: Console, state: ReplState) -> None:
    if not args:
        current = state.export_format or "(from file extension)"
        console.print(f"export format is [bold]{current}[/bold]")
        return
    state.export_format = args[0].lower()
    console.print(f"export format -> [bold]{state.export_format}[/bold]")


def _set_limit(args: list[str], console: Console, state: ReplState) -> None:
    if not args:
        console.print(f"render limit is [bold]{state.max_rows}[/bold]")
        return
    try:
        state.max_rows = int(args[0])
        console.print(f"render limit -> [bold]{state.max_rows}[/bold]")
    except ValueError:
        console.print("[yellow]usage: \\limit <n>[/yellow]")


def _export_last(args: list[str], client: BeaconClient, console: Console, state: ReplState) -> None:
    if not args:
        console.print("[yellow]usage: \\export <file>[/yellow]")
        return
    if not state.last_statement:
        console.print("[yellow]no statement to export yet[/yellow]")
        return
    path = args[0]
    try:
        name = infer_format_name(path, state.export_format)
        output_format = build_output_format(name)
        written = client.query_to_file(state.last_statement, output_format, path)
    except (BeaconCliError, ValueError, OSError) as exc:
        console.print(f"[red]error:[/red] {exc}")
        return
    console.print(f"[green]wrote[/green] {path} [dim]({name}, {written} bytes)[/dim]")
