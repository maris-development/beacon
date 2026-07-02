"""Tab-completion for the shell: SQL keywords, live table names, and meta-commands."""

from __future__ import annotations

import bisect
import re
import threading
import time
from collections.abc import Callable, Iterable, Iterator

from prompt_toolkit.completion import CompleteEvent, Completer, Completion
from prompt_toolkit.document import Document

from ..client import BeaconClient

# Curated SQL keywords (DataFusion dialect + Beacon statements). Offered
# upper-case; matching is case-insensitive.
SQL_KEYWORDS = sorted(
    {
        "SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "LIMIT", "OFFSET",
        "HAVING", "JOIN", "INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN",
        "CROSS JOIN", "ON", "USING", "AS", "AND", "OR", "NOT", "NULL", "IS",
        "IN", "LIKE", "ILIKE", "BETWEEN", "EXISTS", "DISTINCT", "ALL", "UNION",
        "INTERSECT", "EXCEPT", "WITH", "CASE", "WHEN", "THEN", "ELSE", "END",
        "CAST", "ASC", "DESC", "COUNT", "SUM", "AVG", "MIN", "MAX",
        "CREATE", "EXTERNAL", "TABLE", "VIEW", "MATERIALIZED VIEW", "OR REPLACE",
        "STORED AS", "LOCATION", "OPTIONS", "DROP", "INSERT", "INTO", "VALUES",
        "UPDATE", "DELETE", "SET", "REFRESH", "SHOW", "TABLES", "COLUMNS",
        "CRAWLERS", "EXPLAIN", "ANALYZE", "DESCRIBE", "CREATE CRAWLER",
        "RUN CRAWLER", "DROP CRAWLER",
    }
)

# Table functions — only meaningful in a table position (after FROM / JOIN).
TABLE_FUNCTIONS = sorted(
    ["read_netcdf", "read_parquet", "read_csv", "read_json", "read_odv"]
)

# Backslash meta-commands (mirrors repl.meta dispatch), offered when a line
# starts with '\'.
META_COMMANDS = sorted(
    [
        "\\dt", "\\dt+", "\\d", "\\df", "\\dft", "\\datasets", "\\crawlers",
        "\\run-crawler", "\\refresh", "\\info", "\\format", "\\export", "\\o",
        "\\i", "\\limit", "\\timing", "\\x", "\\vi", "\\emacs", "\\help", "\\q",
    ]
)

# An identifier ending at the cursor (word being typed).
_WORD_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*$")

# Table(s) referenced after FROM / JOIN on the (current) line.
_FROM_RE = re.compile(r"\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_.]*)", re.IGNORECASE)

# The cursor is at a table position: right after FROM / JOIN, optionally partway
# through the table name (so we suggest tables even before a prefix is typed).
_TABLE_POS_RE = re.compile(r"\b(?:FROM|JOIN)\s+[A-Za-z0-9_.]*$", re.IGNORECASE)

# How many columns to offer: a short teaser with no prefix (schemas can have
# thousands of columns), widening once the user starts typing to narrow them.
COLUMN_TEASER_LIMIT = 10
COLUMN_PREFIX_LIMIT = 50
# Cap for table suggestions after FROM (an instance may have very many tables).
TABLE_LIMIT = 50


def tables_in_scope(text: str) -> list[str]:
    """Table names referenced after FROM / JOIN in ``text`` (in order, de-duped)."""
    seen: dict[str, None] = {}
    for match in _FROM_RE.finditer(text):
        seen.setdefault(match.group(1), None)
    return list(seen)


class TableNames:
    """Cached, **non-blocking** table-name lookup.

    ``get()`` never touches the network: it returns the cached names immediately
    (empty until first loaded) and, when they're stale, kicks off a background
    refresh. So it's safe to call on the UI thread for every keystroke without
    ever stalling on a request. ``refresh_now()`` is the blocking variant used by
    the startup prefetch. Names are kept sorted for stable completion order.
    """

    def __init__(self, client: BeaconClient, ttl: float = 5.0):
        self._client = client
        self._ttl = ttl
        self._names: list[str] = []
        self._fetched_at: float | None = None
        self._lock = threading.Lock()
        self._fetching = False

    def get(self) -> list[str]:
        now = time.monotonic()
        with self._lock:
            fresh = self._fetched_at is not None and now - self._fetched_at < self._ttl
            names = self._names
            start = not fresh and not self._fetching
            if start:
                self._fetching = True
        if start:
            threading.Thread(target=self._background_refresh, daemon=True).start()
        return names

    def _background_refresh(self) -> None:
        try:
            self.refresh_now()
        finally:
            with self._lock:
                self._fetching = False

    def refresh_now(self) -> list[str]:
        """Blocking refresh — fetch the table list and update the cache."""
        try:
            names = sorted(self._client.tables())
        except Exception:
            # Best-effort warming from a background thread: swallow everything,
            # including the RuntimeError raised when the HTTP client is closed on
            # shutdown, so the thread never dies with an unhandled exception.
            with self._lock:
                return self._names  # keep the last known names
        with self._lock:
            self._names = names
            self._fetched_at = time.monotonic()
            return names


class ColumnIndex:
    """A sorted, prefix-searchable set of column names.

    Column names are lower-cased and sorted once, up front, so completion is a
    ``bisect`` (O(log n) to the first match, then a linear walk of only the
    matches) instead of scanning and lower-casing thousands of names per
    keystroke.
    """

    __slots__ = ("_lower", "_orig")

    def __init__(self, columns: Iterable[str]):
        pairs = sorted((c.lower(), c) for c in columns)
        self._lower = [lower for lower, _ in pairs]
        self._orig = [orig for _, orig in pairs]

    def prefix(self, prefix: str, limit: int) -> list[str]:
        """Up to ``limit`` column names matching ``prefix`` (already lower-case)."""
        if limit <= 0:
            return []
        if not prefix:
            return self._orig[:limit]  # teaser: first `limit` in sorted order
        out: list[str] = []
        i = bisect.bisect_left(self._lower, prefix)
        while i < len(self._lower) and self._lower[i].startswith(prefix) and len(out) < limit:
            out.append(self._orig[i])
            i += 1
        return out


_EMPTY_INDEX = ColumnIndex(())


class TableColumns:
    """Per-table :class:`ColumnIndex` lookup — cached, thread-safe, **non-blocking**.

    Like :class:`TableNames`, ``get()`` returns immediately (an empty index until
    the schema is loaded) and schedules a background fetch when missing/stale, so
    completion never blocks the UI on the network. ``refresh_now()`` is the
    blocking variant used by the startup prefetch. Schemas change less often than
    the table list, so this caches longer.
    """

    def __init__(self, client: BeaconClient, ttl: float = 60.0):
        self._client = client
        self._ttl = ttl
        self._cache: dict[str, tuple[float, ColumnIndex]] = {}
        self._lock = threading.Lock()
        self._fetching: set[str] = set()

    def get(self, table: str) -> ColumnIndex:
        now = time.monotonic()
        with self._lock:
            cached = self._cache.get(table)
            fresh = cached is not None and now - cached[0] < self._ttl
            index = cached[1] if cached else _EMPTY_INDEX
            start = not fresh and table not in self._fetching
            if start:
                self._fetching.add(table)
        if start:
            threading.Thread(
                target=self._background_refresh, args=(table,), daemon=True
            ).start()
        return index

    def _background_refresh(self, table: str) -> None:
        try:
            self.refresh_now(table)
        finally:
            with self._lock:
                self._fetching.discard(table)

    def refresh_now(self, table: str) -> ColumnIndex:
        """Blocking refresh — fetch ``table``'s schema and update the cache."""
        try:
            view = self._client.table_schema(table)
            index = ColumnIndex(f["name"] for f in view.get("fields", []))
        except Exception:
            # Best-effort warming from a background thread (see TableNames):
            # swallow everything, including the closed-client error on shutdown.
            with self._lock:
                cached = self._cache.get(table)
            return cached[1] if cached else _EMPTY_INDEX
        with self._lock:
            self._cache[table] = (time.monotonic(), index)
            return index


def warm_cache(tables: TableNames, columns: TableColumns) -> None:
    """Prefetch the table list and every table's columns, so completion is served
    from memory instead of waiting on the network. Meant to run in a background
    daemon thread; uses the blocking ``refresh_now`` and swallows failures via the
    caches themselves."""
    for name in tables.refresh_now():
        columns.refresh_now(name)


class BeaconCompleter(Completer):
    """Context-aware completer.

    - Backslash lines complete meta-commands.
    - Right after ``FROM`` / ``JOIN`` it suggests table names (even before a
      prefix is typed), plus table functions once you start typing.
    - Elsewhere, when the line binds a table via ``FROM`` / ``JOIN``, that
      table's columns are offered first (a short teaser with no prefix, widening
      as you type), then table names and keywords once there's a prefix.
    """

    def __init__(
        self,
        get_tables: Callable[[], Iterable[str]],
        get_columns: Callable[[str], ColumnIndex] | None = None,
    ):
        self._get_tables = get_tables
        self._get_columns = get_columns

    def get_completions(
        self, document: Document, complete_event: CompleteEvent
    ) -> Iterator[Completion]:
        text = document.text_before_cursor
        stripped = text.lstrip()

        # Empty-prefix suggestions (the column teaser, the full table list after
        # FROM) only pop when explicitly asked for — Tab / Ctrl+Space — not on
        # every keystroke. While typing, we suggest only once there's a prefix to
        # match, so the menu doesn't hover over every space (VS Code style).
        explicit = complete_event.completion_requested

        # Meta-command: complete the first token of a backslash line (case-insensitive).
        if stripped.startswith("\\") and " " not in stripped:
            cmd_prefix = stripped.lower()
            for cmd in META_COMMANDS:
                if cmd.startswith(cmd_prefix):
                    yield Completion(cmd, start_position=-len(stripped), display_meta="command")
            return

        match = _WORD_RE.search(text)
        word = match.group(0) if match else ""
        lower = word.lower()

        # Right after FROM / JOIN: suggest table names, plus table functions once
        # the user starts typing. No columns or general keywords here — only what
        # can follow FROM. The full list (no prefix) only shows on explicit request.
        if _TABLE_POS_RE.search(text):
            yield from self._table_completions(word, lower, allow_empty=explicit)
            for fn in TABLE_FUNCTIONS:
                if lower and fn.lower().startswith(lower):
                    yield Completion(fn, start_position=-len(word), display_meta="table function")
            return

        # Elsewhere: columns of any table bound via FROM / JOIN on the line — a
        # short teaser (explicit request only) that widens as the user types, so a
        # many-thousand-column schema narrows instead of dumping everything.
        yield from self._column_completions(document.text, word, lower, allow_empty=explicit)

        # With no word there is nothing more to prefix-match, so stop after the
        # column teaser rather than dumping every keyword and table.
        if not word:
            return

        yield from self._table_completions(word, lower, allow_empty=False)
        yield from self._keyword_completions(word, lower)

    def _table_completions(self, word: str, lower: str, allow_empty: bool) -> Iterator[Completion]:
        """Table names matching ``lower`` (or all, when ``allow_empty``), capped."""
        if not lower and not allow_empty:
            return
        count = 0
        for name in sorted(self._get_tables()):
            if lower and not name.lower().startswith(lower):
                continue
            yield Completion(name, start_position=-len(word), display_meta="table")
            count += 1
            if count >= TABLE_LIMIT:
                return

    def _keyword_completions(self, word: str, lower: str) -> Iterator[Completion]:
        """SQL keywords / table functions matching a (non-empty) ``lower`` prefix."""
        for kw in SQL_KEYWORDS:
            if kw.lower().startswith(lower):
                yield Completion(kw, start_position=-len(word), display_meta="keyword")

    def _column_completions(
        self, line: str, word: str, lower: str, allow_empty: bool
    ) -> Iterator[Completion]:
        """Columns of known FROM/JOIN tables on ``line``, prefix-filtered and capped."""
        if self._get_columns is None:
            return
        if not lower and not allow_empty:
            return  # don't auto-pop the empty-prefix teaser while typing
        tables = tables_in_scope(line)
        if not tables:
            return
        # Only fully-specified, real tables — skip half-typed names so we don't
        # fire schema lookups (or suggest columns) until a table is actually set.
        known = set(self._get_tables())
        limit = COLUMN_PREFIX_LIMIT if lower else COLUMN_TEASER_LIMIT
        emitted: set[str] = set()
        for table in tables:
            if table not in known:
                continue
            for col in self._get_columns(table).prefix(lower, limit - len(emitted)):
                if col in emitted:
                    continue
                emitted.add(col)
                yield Completion(col, start_position=-len(word), display_meta="column")
                if len(emitted) >= limit:
                    return
