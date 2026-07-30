"""Tests for the Relation returned by `con.sql()` / `con.table()` / `con.read_*()`.

A relation is lazy — `sql` is pure string assembly and nothing runs until a terminal method
(`fetchall`, `arrow`, `explain`, a `to_*` sink). It is no longer composable: relational chaining
was removed in favour of writing the SQL directly, so these tests cover the terminals, the
metadata properties, and the one rendering guarantee that still matters — that raw SQL runs
verbatim rather than being wrapped.
"""

from __future__ import annotations

import pytest

import beacondb


@pytest.fixture
def con():
    with beacondb.connect(":memory:") as connection:
        # A small fixed table to query against.
        connection.execute(
            """
            CREATE TABLE events AS
            SELECT * FROM (VALUES
                (1, 'click',  10),
                (2, 'click',  20),
                (3, 'view',   30),
                (1, 'click',  40),
                (2, 'view',   50)
            ) AS t(user_id, kind, amount)
            """
        )
        yield connection


# ----------------------------------------------------------------------------------------
# Laziness and verbatim SQL
# ----------------------------------------------------------------------------------------


def test_raw_sql_order_by_is_preserved(con):
    # A bare con.sql("... ORDER BY ...") must run verbatim, not wrapped in `SELECT * FROM (...)`
    # — an inner ORDER BY under an outer query is legally dropped by the optimizer, which once
    # silently unsorted every raw ordered query. The rendered SQL is the statement itself.
    rel = con.sql("SELECT amount FROM events ORDER BY amount")
    assert rel.sql == "SELECT amount FROM events ORDER BY amount"
    assert rel.fetchall() == [(10,), (20,), (30,), (40,), (50,)]


def test_raw_sql_group_by_order_is_deterministic(con):
    # The regression that surfaced the wrapping bug: grouped + ordered raw SQL returned rows in
    # nondeterministic order. Run it several times; it must be sorted every time.
    query = "SELECT kind, sum(amount) AS s FROM events GROUP BY kind ORDER BY kind"
    for _ in range(5):
        assert con.sql(query).fetchall() == [("click", 70), ("view", 80)]


def test_building_a_relation_executes_nothing(con):
    rel = con.sql("SELECT user_id FROM events WHERE kind = 'click'")
    # No terminal method was called; `sql` is pure string assembly.
    assert "events" in rel.sql
    assert isinstance(rel.sql, str)


def test_table_reads_the_named_table(con):
    assert con.table("events").fetchall() == [
        (1, "click", 10),
        (2, "click", 20),
        (3, "view", 30),
        (1, "click", 40),
        (2, "view", 50),
    ]


# Relational composition was removed in favour of writing SQL. Pinned as a test so it cannot
# creep back in unnoticed — re-exposing it is a deliberate decision, not an accident.
COMPOSITION_METHODS = [
    "filter",
    "project",
    "select",
    "aggregate",
    "order",
    "sort",
    "limit",
    "distinct",
    "join",
    "union",
    "union_all",
    "count",
    "sum",
    "min",
    "max",
    "mean",
]


@pytest.mark.parametrize("name", COMPOSITION_METHODS)
def test_composition_methods_are_not_exposed(con, name):
    rel = con.sql("SELECT * FROM events")
    assert not hasattr(rel, name), f"Relation.{name} should have been removed"


def test_terminals_and_metadata_are_still_exposed(con):
    rel = con.sql("SELECT * FROM events")
    for name in (
        "fetchone", "fetchmany", "fetchall",
        "arrow", "df", "pl", "record_batch",
        "explain", "show", "create", "create_view",
        "to_parquet", "to_csv", "to_netcdf", "to_odv",
        "sql", "columns", "types", "shape",
    ):
        assert hasattr(rel, name), f"Relation.{name} should still exist"


# ----------------------------------------------------------------------------------------
# Terminal methods
# ----------------------------------------------------------------------------------------


def test_scalar_aggregates_via_sql(con):
    assert con.sql("SELECT count(*) FROM events").fetchall() == [(5,)]
    assert con.sql("SELECT sum(amount) FROM events").fetchall() == [(150,)]
    assert con.sql("SELECT max(amount) FROM events").fetchall() == [(50,)]
    assert con.sql("SELECT min(amount) FROM events").fetchall() == [(10,)]


def test_query_with_a_cte(con):
    rel = con.sql(
        'WITH "e" AS (SELECT * FROM events) SELECT count(*) AS c FROM "e" WHERE amount > 20'
    )
    assert rel.fetchall() == [(3,)]


# ----------------------------------------------------------------------------------------
# Metadata
# ----------------------------------------------------------------------------------------


def test_columns_and_types_without_materializing(con):
    rel = con.sql("SELECT user_id, amount FROM events")
    assert rel.columns == ["user_id", "amount"]
    assert rel.types == ["Int64", "Int64"]


def test_shape_and_len(con):
    rel = con.sql("SELECT * FROM events WHERE kind = 'click'")
    assert rel.shape == (3, 3)
    assert len(rel) == 3


def test_explain_returns_plan_text(con):
    text = con.sql("SELECT * FROM events WHERE amount > 0").explain()
    assert "plan" in text.lower() or "Filter" in text


def test_explain_analyze_annotates_runtime_metrics(con):
    # EXPLAIN (no analyze) prints the plan but never runs it, so it carries no metrics; ANALYZE
    # runs the query and annotates each operator with real counters.
    plain = con.table("events").explain()
    analyzed = con.table("events").explain(analyze=True)
    assert "metrics=" not in plain
    assert "metrics=" in analyzed
    assert "output_rows" in analyzed


def test_arrow_and_df_on_a_relation(con):
    pytest.importorskip("pyarrow")
    table = con.sql("SELECT * FROM events WHERE kind = 'view'").arrow()
    assert table.num_rows == 2
    pytest.importorskip("pandas")
    frame = con.sql("SELECT * FROM events LIMIT 1").df()
    assert len(frame) == 1


def test_relation_is_arrow_c_stream_consumable(con):
    pa = pytest.importorskip("pyarrow")
    rel = con.sql("SELECT user_id FROM events ORDER BY user_id LIMIT 2")
    table = pa.table(rel)
    assert table.column("user_id").to_pylist() == [1, 1]


# ----------------------------------------------------------------------------------------
# Materialization semantics
# ----------------------------------------------------------------------------------------


def test_fetch_cursor_is_stable_across_calls(con):
    rel = con.sql("SELECT amount FROM events ORDER BY amount")
    assert rel.fetchone() == (10,)
    assert rel.fetchmany(2) == [(20,), (30,)]
    assert rel.fetchall() == [(40,), (50,)]


def test_create_table_from_relation(con):
    rel = con.sql("SELECT user_id, amount FROM events WHERE kind = 'click'")
    rel.create("clicks")
    assert con.sql("SELECT count(*) FROM clicks").fetchall() == [(3,)]
    # The new relation reads the persisted table.
    assert con.sql("SELECT sum(amount) FROM clicks").fetchall() == [(70,)]
