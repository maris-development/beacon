"""Tests for the REPL bottom status bar."""

from __future__ import annotations

from types import SimpleNamespace

from prompt_toolkit.enums import EditingMode
from prompt_toolkit.key_binding.vi_state import InputMode

from beacon_cli.client import Identity
from beacon_cli.repl.state import ReplState
from beacon_cli.repl.toolbar import (
    _editing_label,
    _host,
    _identity_label,
    _toolbar_fields,
)


def _emacs_app():
    return SimpleNamespace(editing_mode=EditingMode.EMACS, vi_state=None)


def _vi_app(input_mode: InputMode):
    return SimpleNamespace(
        editing_mode=EditingMode.VI, vi_state=SimpleNamespace(input_mode=input_mode)
    )


def test_host_strips_scheme():
    assert _host("http://localhost:5001") == "localhost:5001"
    assert _host("https://beacon.example.com/") == "beacon.example.com/"


def test_identity_label():
    assert _identity_label(Identity("super-user", "admin")) == "super-user:admin"
    assert _identity_label(Identity("user", "alice")) == "user:alice"
    assert _identity_label(Identity("anonymous")) == "anonymous"


def test_editing_label_emacs_and_vi_modes():
    assert _editing_label(_emacs_app()) == "emacs"
    assert _editing_label(_vi_app(InputMode.INSERT)) == "vi:INSERT"
    assert _editing_label(_vi_app(InputMode.NAVIGATION)) == "vi:NORMAL"


def test_toolbar_fields_reflect_live_state():
    state = ReplState()
    state.max_rows = 50
    state.timing = False
    state.export_format = "parquet"
    fields = _toolbar_fields("localhost:5001", "super-user:admin", state, _emacs_app())
    assert fields == [
        "localhost:5001",
        "super-user:admin",
        "rows:50",
        "fmt:parquet",
        "timing:off",
        "emacs",
    ]


def test_toolbar_fields_defaults():
    fields = _toolbar_fields("h", "anonymous", ReplState(), _vi_app(InputMode.INSERT))
    assert "rows:100" in fields  # DEFAULT_MAX_ROWS
    assert "fmt:auto" in fields  # no export format set -> infer from extension
    assert "timing:on" in fields
    assert "vi:INSERT" in fields


def test_toolbar_fields_all_rows():
    state = ReplState()
    state.max_rows = -1
    assert "rows:all" in _toolbar_fields("h", "who", state, _emacs_app())
