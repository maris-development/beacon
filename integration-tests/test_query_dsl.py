"""The structured JSON DSL (the non-SQL form of POST /api/query).

``test_queries_parquet.py`` touches the DSL only minimally (a single legacy
``filters`` range). These tests drive the modern surface defined in
``beacon-core/src/query``: the singular ``filter`` tree (eq / gt / and / or /
is_not_null), aliased and function ``select`` items, ``sort_by`` (``Asc``/``Desc``),
and ``limit``/``offset`` — all over the in-place Parquet source.
"""

from __future__ import annotations

PARQUET_SRC = {"parquet": {"paths": ["obs/*.parquet"]}}


def _expected(rows, pred):
    return sum(1 for r in rows if pred(r))


def test_gt_filter(client, sample_data):
    rows = client.query_json_rows(
        {
            "from": PARQUET_SRC,
            "select": ["temperature"],
            "filter": {"column": "temperature", "gt": 20},
            "limit": 1_000_000,
        }
    )
    assert len(rows) - 1 == _expected(sample_data["rows"], lambda r: r["temperature"] > 20)


def test_eq_string_filter(client, sample_data):
    rows = client.query_json_rows(
        {
            "from": PARQUET_SRC,
            "select": ["platform"],
            "filter": {"column": "platform", "eq": "SHIP"},
            "limit": 1_000_000,
        }
    )
    assert len(rows) - 1 == _expected(sample_data["rows"], lambda r: r["platform"] == "SHIP")


def test_and_filter(client, sample_data):
    rows = client.query_json_rows(
        {
            "from": PARQUET_SRC,
            "select": ["platform", "temperature"],
            "filter": {
                "and": [
                    {"column": "temperature", "gt": 20},
                    {"column": "platform", "eq": "SHIP"},
                ]
            },
            "limit": 1_000_000,
        }
    )
    expected = _expected(
        sample_data["rows"],
        lambda r: r["temperature"] > 20 and r["platform"] == "SHIP",
    )
    assert expected > 0
    assert len(rows) - 1 == expected


def test_or_filter(client, sample_data):
    rows = client.query_json_rows(
        {
            "from": PARQUET_SRC,
            "select": ["platform"],
            "filter": {
                "or": [
                    {"column": "platform", "eq": "SHIP"},
                    {"column": "platform", "eq": "BUOY"},
                ]
            },
            "limit": 1_000_000,
        }
    )
    expected = _expected(
        sample_data["rows"], lambda r: r["platform"] in ("SHIP", "BUOY")
    )
    assert len(rows) - 1 == expected


def test_is_not_null_matches_all_rows(client, sample_data):
    rows = client.query_json_rows(
        {
            "from": PARQUET_SRC,
            "select": ["temperature"],
            "filter": {"is_not_null": {"column": "temperature"}},
            "limit": 1_000_000,
        }
    )
    assert len(rows) - 1 == sample_data["total"]


def test_sort_desc_limit_returns_max(client, sample_data):
    rows = client.query_json_rows(
        {
            "from": PARQUET_SRC,
            "select": ["temperature"],
            "sort_by": [{"Desc": "temperature"}],
            "limit": 1,
        }
    )
    top = float(rows[1][0])
    assert top == max(r["temperature"] for r in sample_data["rows"])


def test_sort_asc_offset_limit(client, sample_data):
    rows = client.query_json_rows(
        {
            "from": PARQUET_SRC,
            "select": ["time"],
            "sort_by": [{"Asc": "time"}],
            "offset": 10,
            "limit": 5,
        }
    )
    assert len(rows) - 1 == 5
    ordered_times = sorted(r["time"] for r in sample_data["rows"])
    assert int(rows[1][0]) == ordered_times[10]


def test_aliased_function_select(client):
    rows = client.query_json_rows(
        {
            "from": PARQUET_SRC,
            "select": [
                {"function": "abs", "args": [{"column": "latitude"}], "alias": "abs_lat"}
            ],
            "limit": 5,
        }
    )
    assert rows[0] == ["abs_lat"]
    assert all(float(r[0]) >= 0 for r in rows[1:])


def test_column_alias(client):
    rows = client.query_json_rows(
        {
            "from": PARQUET_SRC,
            "select": [{"column": "temperature", "alias": "temp_c"}],
            "limit": 3,
        }
    )
    assert rows[0] == ["temp_c"]
