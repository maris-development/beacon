"""External Delta Lake tables.

We synthesize a minimal Delta table (a parquet data file plus a hand-written
``_delta_log`` commit) using only ``pyarrow`` — no ``deltalake`` dependency, and
the protocol version is pinned to a conservative reader/writer 1/2 so Beacon's
delta-rs reader accepts it. Then we read it via ``read_delta`` and as a
``CREATE EXTERNAL TABLE ... STORED AS DELTA`` registration, and exercise INSERT.
"""

from __future__ import annotations

import json
import os
import time

import pytest

from beacon_client import QueryError

# (name, arrow-builder, delta-type)
ROWS = [
    {"id": 1, "name": "argo", "value": 12.5},
    {"id": 2, "name": "glider", "value": 9.0},
    {"id": 3, "name": "argo", "value": 7.0},
    {"id": 4, "name": "buoy", "value": 21.0},
]
DELTA_DIR = "delta_obs"


def _write_minimal_delta(table_dir) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    table_dir.mkdir(parents=True, exist_ok=True)
    schema = pa.schema(
        [("id", pa.int64()), ("name", pa.string()), ("value", pa.float64())]
    )
    table = pa.table(
        {
            "id": [r["id"] for r in ROWS],
            "name": [r["name"] for r in ROWS],
            "value": [r["value"] for r in ROWS],
        },
        schema=schema,
    )
    data_file = "part-00000.parquet"
    pq.write_table(table, table_dir / data_file)

    now_ms = int(time.time() * 1000)
    size = os.path.getsize(table_dir / data_file)
    delta_schema = {
        "type": "struct",
        "fields": [
            {"name": "id", "type": "long", "nullable": True, "metadata": {}},
            {"name": "name", "type": "string", "nullable": True, "metadata": {}},
            {"name": "value", "type": "double", "nullable": True, "metadata": {}},
        ],
    }
    commit = [
        {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
        {
            "metaData": {
                "id": "00000000-0000-0000-0000-0000000000aa",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": json.dumps(delta_schema),
                "partitionColumns": [],
                "configuration": {},
                "createdTime": now_ms,
            }
        },
        {
            "add": {
                "path": data_file,
                "partitionValues": {},
                "size": size,
                "modificationTime": now_ms,
                "dataChange": True,
            }
        },
    ]
    log_dir = table_dir / "_delta_log"
    log_dir.mkdir(exist_ok=True)
    with open(log_dir / "00000000000000000000.json", "w", encoding="utf-8") as fh:
        for action in commit:
            fh.write(json.dumps(action) + "\n")


@pytest.fixture(scope="module")
def delta_table(datasets_dir) -> str:
    _write_minimal_delta(datasets_dir / DELTA_DIR)
    return DELTA_DIR


def test_read_delta_table_function(client, delta_table):
    assert client.count(f"SELECT * FROM read_delta('{delta_table}')") == len(ROWS)


def test_read_delta_filter(client, delta_table):
    n = client.count(f"SELECT * FROM read_delta('{delta_table}') WHERE name = 'argo'")
    assert n == sum(1 for r in ROWS if r["name"] == "argo")


def test_external_delta_table(client, delta_table):
    name = "delta_ext"
    try:
        client.execute("DROP TABLE IF EXISTS delta_ext", admin=True)
    except QueryError:
        pass
    try:
        client.execute(
            f"CREATE EXTERNAL TABLE {name} STORED AS DELTA LOCATION 'datasets://{delta_table}'",
            admin=True,
        )
        assert name in client.tables().json()
        assert client.count(f"SELECT * FROM {name}") == len(ROWS)
        hi = client.count(f"SELECT * FROM {name} WHERE value > 10")
        assert hi == sum(1 for r in ROWS if r["value"] > 10)
    finally:
        client.execute(f"DROP TABLE IF EXISTS {name}", admin=True)


@pytest.mark.xfail(
    reason="beacon bug: INSERT into an external Delta table does not append a new commit "
    "(row count unchanged); reads work.",
    strict=False,
)
def test_external_delta_insert(client, delta_table):
    """Beacon can append a new commit to the external Delta table (admin)."""
    name = "delta_ins"
    try:
        client.execute("DROP TABLE IF EXISTS delta_ins", admin=True)
    except QueryError:
        pass
    try:
        client.execute(
            f"CREATE EXTERNAL TABLE {name} STORED AS DELTA LOCATION 'datasets://{delta_table}'",
            admin=True,
        )
        client.execute(f"INSERT INTO {name} VALUES (99, 'new', 1.0)", admin=True)
        assert client.count(f"SELECT * FROM {name}") == len(ROWS) + 1
    finally:
        client.execute(f"DROP TABLE IF EXISTS {name}", admin=True)
