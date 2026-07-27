"""Tests for the REPL prompt: identity encoding and continuation alignment."""

from __future__ import annotations

from prompt_toolkit.document import Document
from prompt_toolkit.formatted_text import fragment_list_to_text, to_formatted_text

from beacon_datalake_cli.client import Identity
from beacon_datalake_cli.repl import _prompt
from beacon_datalake_cli.repl.reader import (
    _key_bindings,
    _should_submit,
    _strip_terminator,
    ansi_fg,
    format_sql,
    read_lines,
)


class _FakeBuffer:
    def __init__(self, complete_state: object = None):
        self.complete_state = complete_state
        self.started = False
        self.advanced = False

    def start_completion(self, select_first: bool = False) -> None:
        self.started = True

    def complete_next(self) -> None:
        self.advanced = True


class _FakeEvent:
    def __init__(self, buff: _FakeBuffer):
        self.current_buffer = buff


def _handler_for(key: str):
    for binding in _key_bindings().bindings:
        if any(k.value == key for k in binding.keys):
            return binding.handler
    raise AssertionError(f"{key} binding not found")


def _ctrl_space_handler():
    return _handler_for("c-@")


class _FmtBuffer:
    """Stand-in buffer for the F2 formatter: exposes .text, captures .document."""

    def __init__(self, text: str):
        self.text = text
        self.document: object = None


class _Buf:
    """Minimal stand-in for a prompt_toolkit Buffer for _should_submit tests."""

    def __init__(self, text: str, cursor: int | None = None):
        self.text = text
        self.document = Document(text, cursor_position=len(text) if cursor is None else cursor)


def _plain(message: object) -> str:
    """Visible text of a prompt message (str or ANSI-formatted)."""
    if isinstance(message, str):
        return message
    return fragment_list_to_text(to_formatted_text(message))


def test_completion_triggers_are_bound():
    values = {k.value for b in _key_bindings().bindings for k in b.keys}
    # Tab ("c-i") is the portable trigger; Ctrl+Space is delivered as ControlAt
    # ("c-@"), with "c-space" normalising to the same key.
    assert "c-i" in values  # Tab — works in every terminal
    assert "c-@" in values  # Ctrl+Space — where the terminal supports it


def test_ctrl_space_opens_menu_when_none_active():
    buff = _FakeBuffer(complete_state=None)
    _ctrl_space_handler()(_FakeEvent(buff))
    assert buff.started and not buff.advanced


def test_ctrl_space_advances_when_menu_active():
    buff = _FakeBuffer(complete_state=object())
    _ctrl_space_handler()(_FakeEvent(buff))
    assert buff.advanced and not buff.started


def test_ansi_fg_builds_truecolor_escape():
    assert ansi_fg("#c3e88d") == "\x1b[38;2;195;232;141m"
    assert ansi_fg("#89ddff", bold=True) == "\x1b[1;38;2;137;221;255m"


def test_prompt_encodes_super_user():
    assert _prompt(Identity("super-user", "admin")) == "beacon (admin - super-user)> "


def test_prompt_encodes_named_user():
    assert _prompt(Identity("user", "alice")) == "beacon (alice - user)> "


def test_prompt_anonymous_has_no_username():
    assert _prompt(Identity("anonymous")) == "beacon (anonymous)> "


def test_prompt_unknown_falls_back_to_plain():
    assert _prompt(Identity("unknown", "admin")) == "beacon> "


# -- multi-line submit logic -------------------------------------------------

def test_should_submit_on_terminator_meta_and_quit():
    assert _should_submit(_Buf("SELECT 1;"))
    assert _should_submit(_Buf("\\dt"))
    assert _should_submit(_Buf("quit"))
    assert _should_submit(_Buf(""))  # empty buffer accepts (no-op)


def test_should_not_submit_mid_statement():
    # A non-terminated single line, or a line still being written, adds a newline.
    assert not _should_submit(_Buf("SELECT 1"))
    assert not _should_submit(_Buf("SELECT 1\nFROM t"))


def test_should_submit_on_trailing_blank_line():
    # Double-Enter: a blank line at the end submits (like psql).
    assert _should_submit(_Buf("SELECT 1\n"))


def test_strip_terminator():
    assert _strip_terminator("SELECT 1;") == "SELECT 1"
    assert _strip_terminator("  SELECT 1 ;\n") == "SELECT 1"
    assert _strip_terminator("SELECT *\nFROM t;") == "SELECT *\nFROM t"
    assert _strip_terminator("\\dt") == "\\dt"


# -- SQL formatting (F2) -----------------------------------------------------

def test_format_sql_reflows_and_uppercases():
    out = format_sql("select a, b from obs where depth<=10 order by a")
    assert out.split("\n", 1)[0].startswith("SELECT")  # keywords upper-cased
    assert "FROM obs" in out
    assert "\n" in out  # spread across multiple lines


def test_format_is_bound_to_ctrl_f_and_f2():
    values = {k.value for b in _key_bindings().bindings for k in b.keys}
    assert "c-f" in values and "f2" in values


def test_f2_formats_the_buffer():
    buff = _FmtBuffer("select a from obs where x=1")
    _handler_for("f2")(_FakeEvent(buff))
    assert buff.document is not None and buff.document.text.startswith("SELECT")
    assert "\n" in buff.document.text


def test_ctrl_f_also_formats_the_buffer():
    buff = _FmtBuffer("select a from obs")
    _handler_for("c-f")(_FakeEvent(buff))
    assert buff.document is not None and buff.document.text.startswith("SELECT")


def test_f2_leaves_meta_commands_untouched():
    buff = _FmtBuffer("\\dt")
    _handler_for("f2")(_FakeEvent(buff))
    assert buff.document is None  # not reformatted


# -- non-tty line-by-line fallback -------------------------------------------

def test_read_lines_uses_primary_then_aligned_continuation():
    prompts: list[str] = []
    lines = iter(["SELECT 1", "FROM t;"])

    def fake_prompt(text: str) -> str:
        prompts.append(text)
        return next(lines)

    primary = "beacon (admin - super-user)> "
    result = read_lines(fake_prompt, primary)

    assert result == "SELECT 1\nFROM t"
    assert prompts[0] == primary
    # The continuation is right-aligned to the primary width and ends in "...> ",
    # so the typed input stays in the same column across lines.
    assert prompts[1].endswith("...> ")
    assert len(prompts[1]) == len(primary)


def test_read_lines_colors_prompt_without_shifting_alignment():
    prompts: list[object] = []
    lines = iter(["SELECT 1", "FROM t;"])

    def fake_prompt(message: object) -> str:
        prompts.append(message)
        return next(lines)

    primary = "beacon (admin - super-user)> "
    read_lines(fake_prompt, primary, color="\x1b[1;32m")

    # Colour is applied (the message is no longer a bare str)...
    assert not isinstance(prompts[0], str)
    # ...but the visible text and its width are unchanged, so the column holds.
    assert _plain(prompts[0]) == primary
    assert _plain(prompts[1]).endswith("...> ")
    assert len(_plain(prompts[1])) == len(primary)
