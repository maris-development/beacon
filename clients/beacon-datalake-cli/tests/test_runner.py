"""Tests for the shell runner's cancelable query spinner."""

from __future__ import annotations

import io

import pytest
from rich.console import Console

from beacon_datalake_cli.repl.runner import run_with_spinner


def _console() -> Console:
    return Console(file=io.StringIO())


def test_run_with_spinner_returns_the_result():
    assert run_with_spinner(_console(), lambda: 42) == 42


def test_run_with_spinner_propagates_keyboard_interrupt():
    def cancel():
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        run_with_spinner(_console(), cancel)


def test_run_with_spinner_propagates_other_errors():
    def boom():
        raise ValueError("nope")

    with pytest.raises(ValueError, match="nope"):
        run_with_spinner(_console(), boom)
