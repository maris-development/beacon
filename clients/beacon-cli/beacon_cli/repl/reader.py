"""Input handling for the shell: history, the prompt session, and statement reads."""

from __future__ import annotations

import os
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import sqlparse
from prompt_toolkit import PromptSession
from prompt_toolkit.completion import Completer
from prompt_toolkit.document import Document
from prompt_toolkit.enums import EditingMode
from prompt_toolkit.formatted_text import ANSI, fragment_list_to_text, to_formatted_text
from prompt_toolkit.history import FileHistory
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.key_binding.bindings.named_commands import get_by_name
from prompt_toolkit.lexers import PygmentsLexer
from prompt_toolkit.styles import Style
from pygments.lexers.sql import SqlLexer

DIM = "\x1b[2m"
RESET = "\x1b[0m"


def ansi_fg(hex_color: str, bold: bool = False) -> str:
    """A truecolor foreground SGR escape from a ``#rrggbb`` hex string.

    Lets the prompt reuse the same palette as the SQL highlighting (which is
    defined in hex) instead of the basic 16-colour ANSI names.
    """
    r, g, b = (int(hex_color[i : i + 2], 16) for i in (1, 3, 5))
    return f"\x1b[{'1;' if bold else ''}38;2;{r};{g};{b}m"

# A dark, purple-forward palette for the live SQL highlighting (keywords in
# purple, the way Claude's UI leans). Maps Pygments token classes to true-colour
# hex; it layers over prompt_toolkit's default pygments style, so tokens we don't
# name here still get sensible defaults.
SQL_STYLE = Style.from_dict(
    {
        "pygments.keyword": "#b48ead bold",  # SELECT, FROM, WHERE, JOIN, ...
        "pygments.keyword.type": "#b48ead",  # INT, VARCHAR, ...
        "pygments.name.function": "#c39ac9",  # count(), read_netcdf(), ...
        "pygments.name.builtin": "#c39ac9",
        "pygments.literal.string": "#c3e88d",  # 'text' — bright enough for dark bg
        "pygments.literal.string.single": "#c3e88d",
        "pygments.literal.string.symbol": "#89ddff",  # "quoted ident" — readable cyan
        "pygments.literal.number": "#d08770",  # 42, 3.14
        "pygments.operator": "#81a1c1",  # = < > + ...
        "pygments.punctuation": "#c8d0e0",  # , ( ) ;
        "pygments.comment": "#6c7086 italic",  # -- comment
        # Bottom status bar.
        "bottom-toolbar": "bg:#313244 #cdd6f4",
        "bottom-toolbar.item": "bg:#313244 #cdd6f4",
        "bottom-toolbar.sep": "bg:#313244 #6c7086",
    }
)


def history_path() -> Path:
    if os.name == "nt":
        base = Path(os.environ.get("APPDATA", Path.home())) / "beacon-cli"
    else:
        base = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")) / "beacon-cli"
    base.mkdir(parents=True, exist_ok=True)
    return base / "history"


def _plain_prompt(message: Any) -> str:
    """Fallback prompt for non-terminals: render any formatted message as plain text."""
    if not isinstance(message, str):
        message = fragment_list_to_text(to_formatted_text(message))
    return input(message)


def _colorize(text: str, color: str | None) -> Any:
    """Wrap ``text`` in an ANSI colour for prompt_toolkit, or return it unchanged."""
    if not color:
        return text
    return ANSI(f"{color}{text}{RESET}")


def format_sql(sql: str) -> str:
    """Pretty-print a SQL statement across multiple lines (upper-cased keywords,
    reindented clauses). Returns the input unchanged if it isn't formattable."""
    formatted = sqlparse.format(
        sql, reindent=True, keyword_case="upper", strip_comments=False
    ).strip()
    return formatted or sql


def _should_submit(buffer: Any) -> bool:
    """Whether Enter should run the statement rather than insert a newline.

    Submits on: a terminating ``;``, a backslash meta-command or quit/exit, an
    empty buffer, or a blank line at the end (double-Enter, like psql). Otherwise
    Enter just adds a line, so statements can span multiple lines.
    """
    stripped = buffer.text.strip()
    if not stripped:
        return True
    if stripped.startswith("\\") or stripped.lower() in {"quit", "exit"}:
        return True
    if stripped.endswith(";"):
        return True
    doc = buffer.document
    return doc.is_cursor_at_the_end and doc.current_line.strip() == ""


def _key_bindings() -> KeyBindings:
    """Key bindings for the multi-line prompt: completion triggers + submit.

    Tab is the portable completion trigger — every terminal delivers it — so it
    gets prompt_toolkit's standard ``menu-complete`` (open / insert / cycle), with
    Shift+Tab to cycle backward. Ctrl+Space also opens the menu *without* inserting
    where the terminal sends a code for it (some, e.g. Git Bash's mintty, send
    nothing, so it's a no-op there — use Tab). Enter runs the statement or adds a
    line (see :func:`_should_submit`); Alt+Enter always runs it.
    """
    kb = KeyBindings()

    kb.add("c-i")(get_by_name("menu-complete"))  # Tab
    kb.add("s-tab")(get_by_name("menu-complete-backward"))

    # Ctrl+Space arrives as "c-space" on Windows and "c-@" (NUL) on Unix.
    @kb.add("c-space")
    @kb.add("c-@")
    def _(event: Any) -> None:
        buff = event.current_buffer
        if buff.complete_state:
            buff.complete_next()
        else:
            buff.start_completion(select_first=False)

    @kb.add("enter")
    def _(event: Any) -> None:
        buff = event.current_buffer
        # Accept a highlighted completion instead of submitting / adding a line.
        if buff.complete_state and buff.complete_state.current_completion:
            buff.apply_completion(buff.complete_state.current_completion)
        elif _should_submit(buff):
            buff.validate_and_handle()
        else:
            buff.newline()

    @kb.add("escape", "enter")  # Alt+Enter: force submit, even without a ';'
    def _(event: Any) -> None:
        event.current_buffer.validate_and_handle()

    @kb.add("c-f")  # Ctrl+F
    @kb.add("f2")  # Pretty-print the current SQL across multiple lines.
    def _(event: Any) -> None:
        buff = event.current_buffer
        text = buff.text
        if not text.strip() or text.lstrip().startswith("\\"):
            return  # nothing to format / it's a meta-command
        formatted = format_sql(text)
        if formatted != text:
            buff.document = Document(formatted, cursor_position=len(formatted))

    return kb


def _continuation(width: int, line_number: int, wrap_count: int) -> Any:
    """Prompt shown on the 2nd+ lines of a multi-line statement, aligned + dimmed."""
    return ANSI(f"{DIM}{'...> '.rjust(width)}{RESET}")


def make_prompt_session(
    completer: Completer | None = None,
    vi_mode: bool = False,
    bottom_toolbar: Any = None,
) -> PromptSession | None:
    """Build the interactive prompt session, or ``None`` for pipes / CI (no tty).

    The session is **multi-line**: a statement can span several lines, the cursor
    moves freely between them (Up/Down/Home/End), and it carries history, live SQL
    highlighting, and completion. ``vi_mode`` selects vi key bindings instead of
    the default emacs ones; ``bottom_toolbar`` is an optional status-bar callable.
    """
    if not (sys.stdin.isatty() and sys.stdout.isatty()):
        return None
    try:
        return PromptSession(
            history=FileHistory(str(history_path())),
            lexer=PygmentsLexer(SqlLexer),
            style=SQL_STYLE,
            completer=completer,
            complete_while_typing=True,
            # The completer is non-blocking (serves from memory, refreshes in the
            # background), so run it inline: no per-keystroke thread hand-off, the
            # menu appears in the same render frame.
            complete_in_thread=False,
            key_bindings=_key_bindings(),
            multiline=True,
            prompt_continuation=_continuation,
            editing_mode=EditingMode.VI if vi_mode else EditingMode.EMACS,
            bottom_toolbar=bottom_toolbar,
        )
    except Exception:
        return None


def _strip_terminator(text: str) -> str:
    """Trim surrounding whitespace and any trailing statement terminator(s)."""
    return text.strip().rstrip(";").rstrip()


def read_statement(
    session: PromptSession | None, primary: str = "beacon> ", color: str | None = None
) -> str | None:
    """Read one statement. ``primary`` is the first-line prompt (identity-encoded);
    ``color`` is an optional ANSI SGR tint. With a session this is one multi-line
    read; without one (no tty) it falls back to reading line-by-line via ``input``.
    """
    if session is None:
        return read_lines(_plain_prompt, primary, color)
    return _strip_terminator(session.prompt(_colorize(primary, color)))


def read_lines(
    prompt_fn: Callable[..., str], primary: str = "beacon> ", color: str | None = None
) -> str | None:
    """Non-terminal fallback: read a statement line-by-line until ';' or a blank
    line. Continuation lines use a ``...>`` prompt right-aligned to the primary
    width (computed on the plain text, so colour never shifts the column)."""
    lines: list[str] = []
    continuation = "...> ".rjust(len(primary))
    primary_msg = _colorize(primary, color)
    continuation_msg = _colorize(continuation, DIM if color else None)
    prompt = primary_msg
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
        prompt = continuation_msg
