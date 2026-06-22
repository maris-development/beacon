"""CLI command modules, grouped by concern and attached via :func:`register`."""

from __future__ import annotations

import typer

from . import catalog, query


def register(app: typer.Typer) -> None:
    """Attach every subcommand to the Typer ``app``."""
    query.register(app)
    catalog.register(app)
