"""`SUMMARIZE` — a one-row-per-column data profile.

`SUMMARIZE <table>` / `SUMMARIZE <query>` lowers to a generated single-pass aggregate, so it is an
ordinary read-only query (works on a read-only connection, needs no privileges). Each row is a
column with min/max, distinct count, avg/std (numeric only), non-null count, and null percentage.
"""

from __future__ import annotations

import pytest

import beacondb


@pytest.fixture
def con():
    with beacondb.connect(":memory:") as connection:
        yield connection


@pytest.fixture
def obs(con):
    con.execute("CREATE TABLE obs (temperature DOUBLE, depth BIGINT, platform VARCHAR)")
    con.execute(
        "INSERT INTO obs VALUES (10.0,0,'A'),(20.0,50,'B'),(30.0,100,'A'),(NULL,100,'A')"
    )
    return con


def test_summarize_columns_and_order(obs):
    desc = [d[0] for d in obs.execute("SUMMARIZE obs").description]
    assert desc == [
        "column_name", "column_type", "min", "max", "distinct", "avg", "std",
        "count", "null_percentage",
    ]
    # rows come back in the table's column order, not alphabetical
    assert [r[0] for r in obs.sql("SUMMARIZE obs").fetchall()] == [
        "temperature", "depth", "platform",
    ]


def test_summarize_numeric_column(obs):
    rows = {r[0]: r for r in obs.sql("SUMMARIZE obs").fetchall()}
    temp = rows["temperature"]  # (name, type, min, max, distinct, avg, std, count, null%)
    assert temp[1] == "Float64"
    assert (temp[2], temp[3]) == ("10.0", "30.0")   # min, max (as text)
    assert temp[4] == 3                              # distinct
    assert temp[5] == 20.0                           # avg (10+20+30)/3
    assert temp[7] == 3                              # non-null count
    assert temp[8] == 25.0                           # null% (1 of 4)


def test_summarize_non_numeric_has_null_avg_std(obs):
    platform = {r[0]: r for r in obs.sql("SUMMARIZE obs").fetchall()}["platform"]
    assert platform[1] == "Utf8"
    assert (platform[2], platform[3]) == ("A", "B")  # min/max lexical
    assert platform[4] == 2                           # distinct
    assert platform[5] is None and platform[6] is None  # avg/std undefined for strings
    assert platform[8] == 0.0                         # no nulls


def test_summarize_a_query(obs):
    rows = obs.sql("SUMMARIZE SELECT depth FROM obs WHERE depth > 0").fetchall()
    assert len(rows) == 1
    depth = rows[0]
    assert depth[0] == "depth"
    assert (depth[2], depth[3]) == ("50", "100")
    assert depth[7] == 3   # three rows have depth > 0


def test_summarize_empty_table(con):
    con.execute("CREATE TABLE e (a BIGINT)")
    row = con.sql("SUMMARIZE e").fetchall()[0]
    assert row[0] == "a"
    assert row[7] == 0        # count
    assert row[8] == 0.0      # null% (no divide-by-zero)


def test_summarize_works_read_only(tmp_path):
    path = str(tmp_path / "beacon.db")
    w = beacondb.connect(path)
    w.execute("CREATE TABLE t (a BIGINT)")
    w.execute("INSERT INTO t VALUES (1), (2), (3)")
    w.close()

    ro = beacondb.connect(path, read_only=True)
    row = ro.sql("SUMMARIZE t").fetchall()[0]
    assert (row[0], row[2], row[3], row[7]) == ("a", "1", "3", 3)
    ro.close()
