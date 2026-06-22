"""Typer application wiring for beacon-cli.

This module owns only the Typer ``app`` and the global connection callback;
the individual subcommands live in :mod:`beacon_cli.commands` and the
interactive shell in :mod:`beacon_cli.repl`.
"""

from __future__ import annotations

import typer

from . import __version__, commands
from .commands._shared import console, err_console, get_client
from .config import ClientConfig

app = typer.Typer(
    add_completion=True,
    no_args_is_help=False,
    invoke_without_command=True,
    help="Terminal client for the Beacon data lake.",
    rich_markup_mode="rich",
)


@app.callback()
def main(
    ctx: typer.Context,
    url: str | None = typer.Option(
        None, "--url", "-u", envvar="BEACON_URL", help="Beacon server URL."
    ),
    username: str | None = typer.Option(
        None, "--username", envvar="BEACON_ADMIN_USERNAME", help="Admin username (enables DDL/DML)."
    ),
    password: str | None = typer.Option(
        None, "--password", envvar="BEACON_ADMIN_PASSWORD", help="Admin password."
    ),
    timeout: float | None = typer.Option(None, "--timeout", help="Request timeout (seconds)."),
    no_color: bool = typer.Option(False, "--no-color", help="Disable coloured output."),
    version: bool = typer.Option(False, "--version", help="Show version and exit."),
) -> None:
    if version:
        console.print(f"beacon-cli {__version__}")
        raise typer.Exit()

    if no_color:
        console.no_color = True
        err_console.no_color = True

    config = ClientConfig.resolve(url=url, username=username, password=password, timeout=timeout)
    ctx.obj = config

    # No subcommand -> interactive shell.
    if ctx.invoked_subcommand is None:
        from .repl import run_repl

        with get_client(ctx) as client:
            run_repl(client, console)


# Register all subcommands onto the app.
commands.register(app)


if __name__ == "__main__":
    app()
