"""The beacon convenience surface: the JSON query API and the system-schema helpers.

These are thin wrappers over things that already work as SQL, but they're what make the binding
feel like beacon rather than a generic DB-API driver — so they get their own coverage.
"""

from __future__ import annotations

import pytest

import beacondb


@pytest.fixture
def con():
    with beacondb.connect(":memory:") as connection:
        connection.execute(
            """
            CREATE TABLE obs AS SELECT * FROM (VALUES
                (10.0, 0.0,   'a'),
                (20.0, 50.0,  'b'),
                (30.0, 100.0, 'c')
            ) AS v(temperature, depth, name)
            """
        )
        yield connection


# ----------------------------------------------------------------------------------------
# JSON query API
# ----------------------------------------------------------------------------------------


def test_json_query_runs_the_structured_form(con):
    result = con.json_query(
        {
            "select": ["depth", "temperature"],
            "from": "obs",
            "filter": {"column": "depth", "gt_eq": 50, "lt_eq": 100},
            "sort_by": [{"Desc": "depth"}],
            "limit": 5,
        }
    )
    assert result.columns == ["depth", "temperature"]
    assert result.fetchall() == [(100.0, 30.0), (50.0, 20.0)]


def test_json_query_result_has_columnar_terminals(con):
    pytest.importorskip("pyarrow")
    result = con.json_query({"select": ["name"], "from": "obs"})
    assert result.arrow().num_rows == 3
    pytest.importorskip("pandas")
    assert list(result.df().columns) == ["name"]


def test_json_query_rejects_an_invalid_payload(con):
    with pytest.raises(beacondb.ProgrammingError, match="beacon JSON query"):
        con.json_query({"not": "a query"})


# ----------------------------------------------------------------------------------------
# System-schema helpers
# ----------------------------------------------------------------------------------------


def test_list_tables(con):
    tables = con.list_tables()
    assert "obs" in tables
    assert tables == sorted(tables)  # ordered


def test_functions_is_a_lazy_relation_over_the_catalog(con):
    functions = con.functions()
    assert isinstance(functions, beacondb.Relation)
    assert functions.columns[0] == "function_name"
    assert len(functions.fetchall()) > 0


def test_table_functions_lists_the_readers(con):
    names = con.table_functions()
    assert "read_parquet" in names and "read_netcdf" in names


def test_metrics_records_executed_queries(con):
    con.sql("SELECT count(*) FROM obs").fetchall()
    metrics = con.metrics()
    assert isinstance(metrics, beacondb.Relation)
    assert "query_id" in metrics.columns
    assert len(metrics.fetchall()) >= 1


def test_metrics_filtered_by_query_id_is_injection_safe(con):
    # A bogus/hostile id must bind as a literal and simply match nothing, never alter the query.
    rows = con.metrics(query_id="' OR '1'='1").fetchall()
    assert rows == []


# ----------------------------------------------------------------------------------------
# refresh
# ----------------------------------------------------------------------------------------


def test_refresh_is_wired_and_rejects_a_non_refreshable_table(con):
    # REFRESH applies to external tables and materialized views; a managed table is rejected.
    # This exercises the wiring and the error path without needing external files.
    with pytest.raises(beacondb.Error):
        con.refresh("obs")
