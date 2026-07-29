"""Parameter binding — the DB-API `execute(sql, params)` path.

The point of binding (rather than string interpolation) is correctness *and* injection safety,
so the tests cover both: that placeholders resolve to the right values, and that a value which
looks like SQL is treated as data, never as syntax.
"""

from __future__ import annotations

import pytest

import beacondb


@pytest.fixture
def con():
    with beacondb.connect(":memory:") as connection:
        connection.execute(
            """
            CREATE TABLE people AS
            SELECT * FROM (VALUES (1,'alice'),(2,'bob'),(3,'alice')) AS t(id, name)
            """
        )
        yield connection


def test_qmark_positional_binding(con):
    assert con.execute("SELECT name FROM people WHERE id = ?", [2]).fetchall() == [("bob",)]


def test_multiple_parameters_bind_in_order(con):
    rows = con.execute(
        "SELECT id FROM people WHERE id >= ? AND name = ?", [1, "alice"]
    ).fetchall()
    assert rows == [(1,), (3,)]


def test_numeric_dollar_placeholders_are_accepted_too(con):
    assert con.execute("SELECT name FROM people WHERE id = $1", [3]).fetchall() == [("alice",)]


def test_binding_is_injection_safe(con):
    # A classic injection payload must bind as a plain string and match nothing, not alter the
    # query. If it were interpolated, this would return every row.
    payload = "alice' OR '1'='1"
    assert con.execute("SELECT * FROM people WHERE name = ?", [payload]).fetchall() == []


def test_question_mark_inside_a_string_literal_is_not_a_placeholder(con):
    rows = con.execute("SELECT 'a?b' AS s FROM people WHERE id = ?", [1]).fetchall()
    assert rows == [("a?b",)]


@pytest.mark.parametrize(
    "value, expected",
    [
        (42, 42),
        (3.5, 3.5),
        (True, True),
        ("hi", "hi"),
        (None, None),
    ],
)
def test_scalar_parameter_types_round_trip(con, value, expected):
    assert con.execute("SELECT ? AS v", [value]).fetchall() == [(expected,)]


def test_bytes_parameter(con):
    (row,) = con.execute("SELECT ? AS b", [b"\x00\x01\x02"]).fetchall()
    assert row[0] == b"\x00\x01\x02"


def test_executemany_bulk_insert(con):
    con.execute("CREATE TABLE nums (a BIGINT, b VARCHAR)")
    con.executemany("INSERT INTO nums VALUES (?, ?)", [(10, "x"), (20, "y"), (30, "z")])
    assert con.execute("SELECT count(*), sum(a) FROM nums").fetchall() == [(3, 60)]


def test_parameter_count_mismatch_is_a_programming_error(con):
    with pytest.raises(beacondb.ProgrammingError, match="placeholder"):
        con.execute("SELECT * FROM people WHERE id = ? AND name = ?", [1])


def test_unsupported_parameter_type_is_a_data_error(con):
    with pytest.raises(beacondb.DataError, match="cannot be bound"):
        con.execute("SELECT * FROM people WHERE id = ?", [{"not": "scalar"}])


def test_parameters_must_be_a_sequence(con):
    with pytest.raises(beacondb.ProgrammingError, match="list or tuple"):
        # A bare string must not be treated as a sequence of characters.
        con.execute("SELECT ?", "notalist")


def test_module_level_execute_takes_parameters():
    assert beacondb.execute("SELECT ? + ? AS s", [2, 3]).fetchall() == [(5,)]
