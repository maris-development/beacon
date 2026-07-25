"""The SQLAlchemy dialect (`beacondb://`).

The dialect is thin — it rides on the DB-API surface and beacon's information_schema — so these
tests focus on the seams: URL parsing, parameter binding through SQLAlchemy, reflection, and the
pandas.read_sql path (the main reason to have a dialect at all).
"""

from __future__ import annotations

import pytest

sa = pytest.importorskip("sqlalchemy")
from sqlalchemy import create_engine, inspect, text  # noqa: E402


@pytest.fixture
def engine(tmp_path):
    eng = create_engine(f"beacondb:///{tmp_path / 'demo.db'}")
    with eng.begin() as con:
        con.execute(
            text(
                "CREATE TABLE obs AS SELECT * FROM "
                "(VALUES (1,'a'),(2,'b'),(3,'a')) AS v(id, grp)"
            )
        )
    yield eng
    eng.dispose()


def test_dialect_is_registered():
    # create_engine resolving the URL proves the entry point is installed.
    eng = create_engine("beacondb://")
    assert eng.dialect.name == "beacondb"
    assert eng.dialect.paramstyle == "qmark"


def test_basic_query_and_parameters(engine):
    with engine.connect() as con:
        assert con.execute(text("SELECT 1 AS a")).fetchall() == [(1,)]
        # SQLAlchemy compiles :name to the driver's qmark placeholders.
        rows = con.execute(text("SELECT grp FROM obs WHERE id = :i"), {"i": 2}).fetchall()
        assert rows == [("b",)]


def test_in_memory_url():
    eng = create_engine("beacondb://")
    with eng.connect() as con:
        assert con.execute(text("SELECT 42 AS x")).fetchall() == [(42,)]


def test_reflection(engine):
    insp = inspect(engine)
    assert "public" in insp.get_schema_names()
    assert "obs" in insp.get_table_names()
    assert not insp.has_table("does_not_exist")
    assert insp.has_table("obs")

    columns = {c["name"]: c for c in insp.get_columns("obs")}
    assert set(columns) == {"id", "grp"}
    assert columns["id"]["type"].__class__.__name__ == "BIGINT"
    assert columns["grp"]["type"].__class__.__name__ == "VARCHAR"


def test_view_reflection(engine):
    with engine.begin() as con:
        con.execute(text("CREATE VIEW obs_a AS SELECT * FROM obs WHERE grp = 'a'"))
    insp = inspect(engine)
    assert "obs_a" in insp.get_view_names()


def test_pandas_read_sql(engine):
    pd = pytest.importorskip("pandas")
    df = pd.read_sql("SELECT grp, count(*) AS n FROM obs GROUP BY grp ORDER BY grp", engine)
    assert df.to_dict("records") == [{"grp": "a", "n": 2}, {"grp": "b", "n": 1}]


def test_url_query_params_map_to_connect_kwargs():
    # A file DB opened with auth on; the default super-user is off, so an anonymous session lands.
    from beacondb.sqlalchemy import BeacondbDialect

    url = sa.engine.make_url("beacondb:///x.db?auth=true&batch_size=1024&datasets=/tmp/data")
    _, kwargs = BeacondbDialect().create_connect_args(url)
    assert kwargs["database"] == "x.db"
    assert kwargs["auth"] is True
    assert kwargs["batch_size"] == 1024
    assert kwargs["datasets"] == "/tmp/data"
