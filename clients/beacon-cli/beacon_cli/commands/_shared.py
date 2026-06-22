"""Shared helpers and consoles for the CLI command modules."""

from __future__ import annotations

import sys

import typer
from rich.console import Console

from ..client import BeaconClient
from ..config import ClientConfig

# Shared consoles; cli.main toggles ``no_color`` on these.
console = Console()
err_console = Console(stderr=True)


def get_client(ctx: typer.Context) -> BeaconClient:
    """Build a client from the connection config stashed on the Typer context."""
    config: ClientConfig = ctx.obj
    return BeaconClient(config)


def fail(exc: Exception) -> None:
    """Print a friendly one-line error and exit non-zero (no traceback)."""
    err_console.print(f"[red]error:[/red] {exc}")
    raise typer.Exit(code=1)


def read_sql(sql: str | None, file: str | None) -> str:
    """Resolve a SQL statement from an argument, a ``--file``, or stdin."""
    if file:
        with open(file, encoding="utf-8") as fh:
            return fh.read()
    if sql is not None:
        return sql
    if not sys.stdin.isatty():
        data = sys.stdin.read().strip()
        if data:
            return data
    raise typer.BadParameter("provide SQL as an argument, with --file, or via stdin")
