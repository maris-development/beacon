"""The shell's bottom status bar: connection, identity, and live session settings."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from prompt_toolkit.application import get_app
from prompt_toolkit.enums import EditingMode
from prompt_toolkit.key_binding.vi_state import InputMode

from ..client import Identity
from .state import ReplState


def _host(url: str) -> str:
    """The host[:port] part of a URL, without the scheme."""
    return url.split("://", 1)[-1]


def _identity_label(identity: Identity) -> str:
    if identity.kind == "super-user":
        return f"super-user:{identity.username}" if identity.username else "super-user"
    if identity.kind == "user":
        return f"user:{identity.username}"
    if identity.kind == "anonymous":
        return "anonymous"
    return "unknown"


def _editing_label(app: Any) -> str:
    """emacs, or vi with its live mode (NORMAL when navigating, else INSERT)."""
    if app.editing_mode == EditingMode.VI:
        mode = "NORMAL" if app.vi_state.input_mode == InputMode.NAVIGATION else "INSERT"
        return f"vi:{mode}"
    return "emacs"


def _toolbar_fields(host: str, who: str, state: ReplState, app: Any) -> list[str]:
    """The ordered text segments shown in the bar (pure, for testing)."""
    rows = "all" if state.max_rows < 0 else str(state.max_rows)
    return [
        host,
        who,
        f"rows:{rows}",
        f"fmt:{state.export_format or 'auto'}",
        f"timing:{'on' if state.timing else 'off'}",
        _editing_label(app),
    ]


def make_bottom_toolbar(
    url: str, identity: Identity, state: ReplState
) -> Callable[[], list[tuple[str, str]]]:
    """A bottom_toolbar callable for the prompt session. It reads ``state`` live,
    so row-limit / format / timing / editing-mode changes show up immediately."""
    host, who = _host(url), _identity_label(identity)

    def toolbar() -> list[tuple[str, str]]:
        fields = _toolbar_fields(host, who, state, get_app())
        frags: list[tuple[str, str]] = [("class:bottom-toolbar", " ")]
        for i, field in enumerate(fields):
            if i:
                frags.append(("class:bottom-toolbar.sep", " │ "))
            frags.append(("class:bottom-toolbar.item", field))
        return frags

    return toolbar
