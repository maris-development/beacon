"""Tests for REPL Tab-completion: SQL keywords, table names, and meta-commands."""

from __future__ import annotations

import threading
import time

from prompt_toolkit.completion import CompleteEvent
from prompt_toolkit.document import Document

from beacon_cli.repl.completion import (
    BeaconCompleter,
    ColumnIndex,
    TableColumns,
    TableNames,
    warm_cache,
)


def _complete(
    text: str,
    tables: list[str] | None = None,
    columns: dict[str, list[str]] | None = None,
    cursor: int | None = None,
    explicit: bool = True,
) -> list[str]:
    # explicit=True mimics a Tab/Ctrl+Space request; explicit=False mimics the
    # automatic complete-while-typing event.
    if columns is not None:
        indexes = {t: ColumnIndex(cols) for t, cols in columns.items()}
        get_columns = lambda t: indexes.get(t, ColumnIndex(()))  # noqa: E731
    else:
        get_columns = None
    completer = BeaconCompleter(lambda: tables or [], get_columns)
    doc = Document(text, cursor_position=len(text) if cursor is None else cursor)
    event = CompleteEvent(completion_requested=explicit)
    return [c.text for c in completer.get_completions(doc, event)]


def test_completes_sql_keyword():
    out = _complete("SEL")
    assert "SELECT" in out


def test_keyword_matching_is_case_insensitive():
    assert "SELECT" in _complete("sel")


def test_table_matching_is_case_insensitive():
    # Upper-case prefix still matches a lower-case table name.
    assert "ocean_profiles" in _complete("SELECT * FROM OC", tables=["ocean_profiles"])


def test_column_matching_is_case_insensitive():
    out = _complete(
        "SELECT * FROM obs WHERE TE", tables=["obs"], columns={"obs": ["temperature"]}
    )
    assert "temperature" in out


def test_meta_matching_is_case_insensitive():
    assert "\\dt" in _complete("\\D")


def test_completes_table_names_first():
    out = _complete("SELECT * FROM oc", tables=["ocean_profiles", "obs"])
    assert out[0] == "ocean_profiles"  # table before any keyword
    assert "obs" not in out  # 'obs' does not start with 'oc'


def test_completes_meta_commands_on_backslash():
    out = _complete("\\d")
    assert {"\\d", "\\dt", "\\dt+", "\\df", "\\dft", "\\datasets"} <= set(out)
    # SQL keywords are not offered for a backslash line.
    assert "SELECT" not in out


def test_meta_completion_stops_after_first_token():
    # Once past the command word, backslash lines get argument-style (non-meta)
    # completion, not another command list.
    assert "\\refresh" not in _complete("\\refresh ob", tables=["obs"])


def test_no_completion_without_a_word():
    assert _complete("SELECT * FROM obs ") == []


# -- table position (after FROM/JOIN) ----------------------------------------

def test_suggests_tables_after_from_with_no_prefix():
    out = _complete("SELECT * FROM ", tables=["obs", "ocean"])
    assert "obs" in out and "ocean" in out  # tables offered with no prefix typed
    assert "SELECT" not in out  # no keyword dump in a table position


def test_suggests_tables_after_join_with_prefix():
    out = _complete("SELECT * FROM obs JOIN oc", tables=["obs", "ocean_profiles"])
    assert out == ["ocean_profiles"]  # prefix-filtered to the JOIN target


def test_table_position_offers_table_functions_when_typing():
    assert "read_netcdf" in _complete("SELECT * FROM read_net", tables=["obs"])


# -- not-too-eager: empty-prefix suggestions only on explicit request --------

def test_no_auto_popup_of_empty_column_teaser():
    # In a WHERE with no prefix typed, complete-while-typing shows nothing...
    args = dict(tables=["obs"], columns={"obs": ["temperature", "depth"]})
    assert _complete("SELECT * FROM obs WHERE ", explicit=False, **args) == []
    # ...but pressing Tab / Ctrl+Space (explicit) surfaces the teaser.
    assert _complete("SELECT * FROM obs WHERE ", explicit=True, **args)


def test_no_auto_popup_of_full_table_list_after_from():
    assert _complete("SELECT * FROM ", tables=["obs", "ocean"], explicit=False) == []
    assert _complete("SELECT * FROM ", tables=["obs", "ocean"], explicit=True)


def test_typing_a_prefix_still_auto_completes():
    # A real prefix always completes, whether typed automatically or requested.
    assert "temperature" in _complete(
        "SELECT * FROM obs WHERE te",
        tables=["obs"],
        columns={"obs": ["temperature"]},
        explicit=False,
    )
    assert "ocean" in _complete("SELECT * FROM oc", tables=["ocean"], explicit=False)


# -- column completion -------------------------------------------------------

def test_columns_offered_once_table_in_from():
    out = _complete(
        "SELECT * FROM obs WHERE te",
        tables=["obs"],
        columns={"obs": ["temperature", "temp_flag", "salinity"]},
    )
    # Columns come first (before any keyword/table) and are prefix-filtered.
    assert set(out[:2]) == {"temperature", "temp_flag"}
    assert "salinity" not in out  # doesn't start with 'te'


def test_columns_in_select_clause_when_from_is_bound():
    # Cursor in the SELECT list, but FROM binds a table later on the line: the
    # column is resolved from the whole line, not just the text before the cursor.
    text = "SELECT te FROM obs"
    out = _complete(
        text,
        tables=["obs"],
        columns={"obs": ["temperature", "temp_flag", "depth"]},
        cursor=len("SELECT te"),
    )
    assert set(out) == {"temperature", "temp_flag"}  # prefix 'te', 'depth' excluded


def test_no_columns_without_a_from_table():
    # 'te' with no FROM in the line: no column suggestions (keywords/tables only).
    out = _complete("SELECT te", tables=["obs"], columns={"obs": ["temperature"]})
    assert "temperature" not in out


def test_columns_skipped_for_half_typed_table():
    # 'ob' is not a known table yet, so no schema lookup / column suggestions.
    out = _complete("SELECT * FROM ob", tables=["obs"], columns={"obs": ["temperature"]})
    assert "temperature" not in out
    assert "obs" in out  # still completes the table name


def test_empty_prefix_shows_capped_teaser():
    cols = [f"c{i:04d}" for i in range(1000)]
    out = _complete("SELECT * FROM big WHERE ", tables=["big"], columns={"big": cols})
    assert out == cols[:10]  # only the first 10 with no prefix to narrow on


def test_typing_widens_beyond_the_teaser():
    cols = [f"col_{i:04d}" for i in range(1000)]
    out = _complete("SELECT * FROM big WHERE col_", tables=["big"], columns={"big": cols})
    assert len(out) == 50  # widened cap while typing a prefix
    assert out[0] == "col_0000"


# -- ColumnIndex + prefetch --------------------------------------------------

def test_column_index_prefix_is_case_insensitive_and_capped():
    idx = ColumnIndex(["Temperature", "temp_flag", "Salinity", "depth"])
    assert idx.prefix("temp", 10) == ["temp_flag", "Temperature"]  # sorted, lower-cased key
    assert idx.prefix("", 2) == ["depth", "Salinity"]  # teaser: first 2 sorted
    assert idx.prefix("temp", 1) == ["temp_flag"]  # honours the limit
    assert idx.prefix("zzz", 10) == []


class _FakeClient:
    def __init__(self):
        self.table_calls = 0
        self.schema_calls: list[str] = []

    def tables(self) -> list[str]:
        self.table_calls += 1
        return ["obs", "argo"]

    def table_schema(self, table: str) -> dict:
        self.schema_calls.append(table)
        return {"fields": [{"name": "lat"}, {"name": "lon"}]}


def test_warm_cache_prefetches_tables_and_schemas():
    client = _FakeClient()
    names, cols = TableNames(client), TableColumns(client)
    warm_cache(names, cols)
    assert set(client.schema_calls) == {"obs", "argo"}  # every table's schema fetched
    # After warming, completion is served from cache (no further schema fetches).
    before = len(client.schema_calls)
    assert cols.get("obs").prefix("la", 5) == ["lat"]
    assert len(client.schema_calls) == before


def test_get_serves_from_cache_without_refetching():
    client = _FakeClient()
    names = TableNames(client)
    names.refresh_now()  # blocking prefetch
    calls = client.table_calls
    assert names.get() == ["argo", "obs"]  # sorted, from cache
    assert client.table_calls == calls  # get() didn't hit the client again


class _BlockingClient:
    """A client whose fetch blocks, to prove get() doesn't wait on it."""

    def __init__(self):
        self.release = threading.Event()

    def tables(self) -> list[str]:
        self.release.wait(timeout=2)
        return ["obs"]

    def table_schema(self, table: str) -> dict:
        self.release.wait(timeout=2)
        return {"fields": [{"name": "lat"}]}


def test_get_is_non_blocking():
    client = _BlockingClient()
    try:
        names = TableNames(client)
        cols = TableColumns(client)
        start = time.perf_counter()
        # Neither call may wait on the (blocked) fetch — they return what's cached
        # (empty) immediately and refresh in the background.
        assert names.get() == []
        assert cols.get("obs").prefix("", 5) == []
        assert time.perf_counter() - start < 0.5
    finally:
        client.release.set()  # let the background threads finish and exit
