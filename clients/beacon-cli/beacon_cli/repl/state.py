"""Mutable per-session state for the interactive shell."""

from __future__ import annotations

from ..render import DEFAULT_MAX_ROWS


class ReplState:
    """Settings and history carried across statements within one shell session."""

    def __init__(self) -> None:
        self.max_rows = DEFAULT_MAX_ROWS
        self.timing = True
        self.expand = False  # vertical (field/value) rendering for wide tables
        self.export_format: str | None = None  # None -> infer from file extension
        self.last_statement: str | None = None
