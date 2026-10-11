"""DAP2 (OPeNDAP) endpoint, read with the pydap client.

The file skips itself when pydap is absent (see requirements-optional.txt).
"""

from __future__ import annotations

import pytest

from beacon_client import QueryError

pydap_client = pytest.importorskip("pydap.client")

TABLE = "itest_dap2"


@pytest.fixture(scope="module")
def dap_table(client):
    try:
        client.execute(f"DROP TABLE IF EXISTS {TABLE}", admin=True)
    except QueryError:
        pass
    client.execute(
        f"CREATE TABLE {TABLE} (id BIGINT, t TIMESTAMP, temp DOUBLE, platform VARCHAR)",
        admin=True,
    )
    client.execute(
        f"INSERT INTO {TABLE} VALUES "
        "(1, TIMESTAMP '2024-01-01 00:00:00', 10.5, 'SHIP'), "
        "(2, TIMESTAMP '2024-01-02 00:00:00', 4.25, 'BUOY'), "
        "(3, TIMESTAMP '2024-01-03 00:00:00', 7.0, 'BUOY')",
        admin=True,
    )
    yield TABLE
    try:
        client.execute(f"DROP TABLE IF EXISTS {TABLE}", admin=True)
    except QueryError:
        pass


def _sequence(client, table):
    dataset = pydap_client.open_url(f"{client.base_url}/dap/{table}", protocol="dap2")
    return dataset["s"]


def _ids(rows) -> list[int]:
    return sorted(int(row[0]) for row in rows)


def test_pydap_lists_the_columns_and_attributes(client, dap_table):
    seq = _sequence(client, dap_table)

    assert list(seq.keys()) == ["id", "t", "temp", "platform"]
    assert seq["t"].attributes["units"] == "seconds since 1970-01-01T00:00:00Z"


def test_pydap_numeric_filter_runs_on_the_server(client, dap_table):
    seq = _sequence(client, dap_table)

    rows = list(seq[seq["temp"] > 5].iterdata())

    assert _ids(rows) == [1, 3]
    assert len(rows) == client.count(f"SELECT * FROM {dap_table} WHERE temp > 5")


def test_pydap_text_filter(client, dap_table):
    seq = _sequence(client, dap_table)

    rows = list(seq[seq["platform"] == "BUOY"].iterdata())

    assert _ids(rows) == [2, 3]


def test_pydap_time_filter_uses_seconds(client, dap_table):
    seq = _sequence(client, dap_table)

    # 1704153600 is 2024-01-02T00:00:00Z.
    rows = list(seq[seq["t"] >= 1704153600].iterdata())

    assert _ids(rows) == [2, 3]


DOTTED_TABLE = "itest_dap2_dotted"


@pytest.fixture(scope="module")
def dotted_table(client, datasets_dir):
    # A managed table rejects a `.` in a column name, so the table is a CSV file.
    (datasets_dir / "itest_dap2_dotted.csv").write_text(
        "id,temp.units\n1,degC\n2,K\n3,degC\n", encoding="utf-8"
    )
    try:
        client.execute(f"DROP TABLE IF EXISTS {DOTTED_TABLE}", admin=True)
    except QueryError:
        pass
    client.execute(
        f"CREATE EXTERNAL TABLE {DOTTED_TABLE} STORED AS CSV LOCATION 'itest_dap2_dotted.csv'",
        admin=True,
    )
    yield DOTTED_TABLE
    try:
        client.execute(f"DROP TABLE IF EXISTS {DOTTED_TABLE}", admin=True)
    except QueryError:
        pass


def test_pydap_reads_and_filters_a_dotted_column(client, dotted_table):
    seq = _sequence(client, dotted_table)

    # pydap shows the DDS name, with the dot escaped as %2E.
    assert list(seq.keys()) == ["id", "temp%2Eunits"]
    rows = list(seq[seq["temp%2Eunits"] == "degC"].iterdata())

    assert _ids(rows) == [1, 3]
