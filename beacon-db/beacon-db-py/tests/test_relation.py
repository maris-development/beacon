"""Tests for the lazy Relation — composition, correct SQL rendering, and terminal execution.

The rendering assertions matter as much as the row assertions: the whole reason for a clause
builder (rather than blind subquery nesting) is that `.order().limit()` must land both clauses
in one SELECT, or the ordering of a top-N result is left unspecified by SQL. So these tests pin
the emitted SQL shape, not only the rows it returns.
"""

from __future__ import annotations

import re

import pytest

import beacondb


@pytest.fixture
def con():
    with beacondb.connect(":memory:") as connection:
        # A small fixed table to compose against.
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


def norm(sql: str) -> str:
    """Collapse whitespace so SQL-shape assertions ignore formatting."""
    return re.sub(r"\s+", " ", sql).strip()


# ----------------------------------------------------------------------------------------
# Laziness
# ----------------------------------------------------------------------------------------


def test_building_a_relation_executes_nothing(con):
    rel = con.table("events").filter("kind = 'click'").project("user_id")
    # No terminal method was called; `sql` is pure string assembly.
    assert "events" in rel.sql
    assert isinstance(rel.sql, str)


def test_relational_methods_return_new_relations(con):
    base = con.table("events")
    filtered = base.filter("amount > 25")
    # The base is untouched — relations are immutable, so a relation is safe to branch.
    assert base.count().fetchall() == [(5,)]
    assert filtered.count().fetchall() == [(3,)]


# ----------------------------------------------------------------------------------------
# SQL shape — the correctness-critical part
# ----------------------------------------------------------------------------------------


def test_order_then_limit_land_in_one_select(con):
    rel = con.table("events").order("amount desc").limit(2)
    sql = norm(rel.sql)
    # Both clauses in the SAME select — not an inner ORDER BY under an outer LIMIT.
    assert re.search(r"ORDER BY amount desc LIMIT 2", sql), sql
    assert rel.fetchall() == [(2, "view", 50), (1, "click", 40)]


def test_filter_after_limit_wraps_so_it_applies_after(con):
    # limit(3) then filter must filter the limited rows, so the filter has to wrap.
    rel = con.table("events").limit(3).filter("amount > 15")
    sql = norm(rel.sql)
    assert "LIMIT 3) AS _rel WHERE" in sql, sql


def test_second_limit_wraps_rather_than_overwriting(con):
    rel = con.table("events").limit(4).limit(2)
    assert norm(rel.sql).count("LIMIT") == 2
    assert len(rel.fetchall()) == 2


def test_consecutive_filters_fold_into_one_where(con):
    rel = con.table("events").filter("amount > 10").filter("kind = 'click'")
    sql = norm(rel.sql)
    assert "WHERE (amount > 10) AND (kind = 'click')" in sql, sql
    assert rel.fetchall() == [(2, "click", 20), (1, "click", 40)]


def test_the_canonical_analytics_chain(con):
    # read → filter → aggregate → order → limit, the acceptance-checklist shape.
    rel = (
        con.table("events")
        .filter("kind = 'click'")
        .aggregate("user_id, count(*) AS n, sum(amount) AS total", "user_id")
        .order("total desc")
        .limit(2)
    )
    sql = norm(rel.sql)
    # The filter folds to a WHERE (evaluated before GROUP BY, i.e. pre-aggregation, which is
    # what filter-then-aggregate means), and order+limit ride on the same aggregate select.
    assert "WHERE (kind = 'click') GROUP BY user_id ORDER BY total desc LIMIT 2" in sql, sql
    assert rel.fetchall() == [(1, 2, 50), (2, 1, 20)]


# ----------------------------------------------------------------------------------------
# Terminal methods
# ----------------------------------------------------------------------------------------


def test_scalar_aggregates(con):
    assert con.table("events").count().fetchall() == [(5,)]
    assert con.table("events").sum("amount").fetchall() == [(150,)]
    assert con.table("events").max("amount").fetchall() == [(50,)]
    assert con.table("events").min("amount").fetchall() == [(10,)]


def test_distinct(con):
    kinds = con.table("events").project("kind").distinct().order("kind").fetchall()
    assert kinds == [("click",), ("view",)]


def test_union_all_and_union(con):
    a = con.sql("SELECT 1 AS x")
    b = con.sql("SELECT 2 AS x")
    assert sorted(a.union_all(b).fetchall()) == [(1,), (2,)]
    same = con.sql("SELECT 1 AS x")
    assert a.union(same).fetchall() == [(1,)]


def test_join(con):
    left = con.sql("SELECT 1 AS id, 'a' AS name")
    right = con.sql("SELECT 1 AS id, 100 AS score")
    joined = left.join(right, on="_l.id = _r.id", how="inner")
    assert joined.fetchall() == [(1, "a", 1, 100)]


def test_cross_join_needs_no_condition(con):
    left = con.sql("SELECT 1 AS a UNION ALL SELECT 2")
    right = con.sql("SELECT 10 AS b")
    assert sorted(left.join(right, how="cross").fetchall()) == [(1, 10), (2, 10)]


def test_join_rejects_unknown_type_and_missing_condition(con):
    left = con.sql("SELECT 1 AS id")
    right = con.sql("SELECT 1 AS id")
    with pytest.raises(beacondb.ProgrammingError):
        left.join(right, on="_l.id = _r.id", how="sideways")
    with pytest.raises(beacondb.ProgrammingError):
        left.join(right, how="inner")  # inner join with no `on`


def test_query_uses_a_cte_not_a_view(con):
    rel = con.table("events").query("e", "SELECT count(*) AS c FROM e WHERE amount > 20")
    assert "WITH \"e\" AS" in rel.sql
    assert rel.fetchall() == [(3,)]


# ----------------------------------------------------------------------------------------
# Metadata
# ----------------------------------------------------------------------------------------


def test_columns_and_types_without_materializing(con):
    rel = con.table("events").project("user_id, amount")
    assert rel.columns == ["user_id", "amount"]
    assert rel.types == ["Int64", "Int64"]


def test_shape_and_len(con):
    rel = con.table("events").filter("kind = 'click'")
    assert rel.shape == (3, 3)
    assert len(rel) == 3


def test_explain_returns_plan_text(con):
    text = con.table("events").filter("amount > 0").explain()
    assert "plan" in text.lower() or "Filter" in text


def test_arrow_and_df_on_a_relation(con):
    pytest.importorskip("pyarrow")
    table = con.table("events").filter("kind = 'view'").arrow()
    assert table.num_rows == 2
    pytest.importorskip("pandas")
    frame = con.table("events").limit(1).df()
    assert len(frame) == 1


def test_relation_is_arrow_c_stream_consumable(con):
    pa = pytest.importorskip("pyarrow")
    rel = con.table("events").project("user_id").order("user_id").limit(2)
    table = pa.table(rel)
    assert table.column("user_id").to_pylist() == [1, 1]


# ----------------------------------------------------------------------------------------
# Materialization semantics
# ----------------------------------------------------------------------------------------


def test_fetch_cursor_is_stable_across_calls(con):
    rel = con.table("events").order("amount").project("amount")
    assert rel.fetchone() == (10,)
    assert rel.fetchmany(2) == [(20,), (30,)]
    assert rel.fetchall() == [(40,), (50,)]


def test_create_table_from_relation(con):
    rel = con.table("events").filter("kind = 'click'").project("user_id, amount")
    created = rel.create("clicks")
    assert created.count().fetchall() == [(3,)]
    # The new relation reads the persisted table.
    assert con.table("clicks").sum("amount").fetchall() == [(70,)]
