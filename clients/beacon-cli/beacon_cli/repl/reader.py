"""Input handling for the shell: history, the prompt callable, and statement reads."""

from __future__ import annotations

import os
import sys
from collections.abc import Callable
from pathlib import Path

from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory


def history_path() -> Path:
    if os.name == "nt":
        base = Path(os.environ.get("APPDATA", Path.home())) / "beacon-cli"
    else:
        base = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")) / "beacon-cli"
    base.mkdir(parents=True, exist_ok=True)
    return base / "history"


def make_prompt_fn() -> Callable[[str], str]:
    """Return a ``prompt(text) -> line`` callable.

    Uses prompt_toolkit (history + line editing) when attached to a real
    terminal, and falls back to the builtin ``input`` for pipes / CI / any
    context where prompt_toolkit cannot create a terminal session.
    """
    if sys.stdin.isatty() and sys.stdout.isatty():
        try:
            session: PromptSession = PromptSession(history=FileHistory(str(history_path())))
            return session.prompt
        except Exception:
            pass
    return input


def read_statement(prompt_fn: Callable[[str], str]) -> str | None:
    """Read one statement, continuing across lines until ';' or a blank line.

    Meta-commands (starting with '\\') are returned immediately as a single line.
    """
    lines: list[str] = []
    prompt = "beacon> "
    while True:
        line = prompt_fn(prompt)
        if not lines and line.strip().startswith("\\"):
            return line
        if not lines and line.strip().lower() in {"quit", "exit"}:
            return line
        lines.append(line)
        stripped = line.rstrip()
        if stripped.endswith(";"):
            return "\n".join(lines).rstrip().rstrip(";")
        if stripped == "":
            return "\n".join(lines).strip()
        prompt = "   ...> "
