"""External Apache Iceberg tables.

The table under ``test-datasets/iceberg-example`` was written by iceberg-rust and
committed, so this suite needs no Iceberg writer of its own (see
``beacon-db/beacon-core/tests/iceberg_tables.rs::regenerate_the_committed_fixture``).
Its metadata records the absolute paths of the warehouse it was written in, which
is exactly the normal case: Beacon reads a table another system wrote, mounted
somewhere else.

The metadata files are staged in two steps. The first five rows and three columns
are visible from the start; the last two metadata versions — a schema evolution
and the row that fills the new column — are copied in later, so the suite can
watch them land with no restart.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from beacon_client import QueryError

REPO_ROOT = Path(__file__).resolve().parent.parent
FIXTURE = REPO_ROOT / "test-datasets" / "iceberg-example"
ICEBERG_DIR = "iceberg_obs"

# How many rows the fixture holds at each staging step.
STAGED_ROWS = 5
EVOLVED_ROWS = 6
# Metadata versions withheld until the evolution step.
EVOLUTION_VERSIONS = ("00003", "00004")


def _stage(table_dir: Path, *, evolved: bool) -> None:
    """Copy the committed table into ``table_dir``.

    Data files and manifests are copied whole — a metadata version simply
    references the subset it knows about. Only the metadata files decide which
    version a reader sees, so those are what is withheld.
    """
    (table_dir / "data").mkdir(parents=True, exist_ok=True)
    (table_dir / "metadata").mkdir(parents=True, exist_ok=True)

    for data_file in (FIXTURE / "data").iterdir():
        shutil.copy(data_file, table_dir / "data" / data_file.name)

    for meta_file in (FIXTURE / "metadata").iterdir():
        withheld = not evolved and meta_file.name.startswith(EVOLUTION_VERSIONS)
        if withheld:
            continue
        shutil.copy(meta_file, table_dir / "metadata" / meta_file.name)


def _columns(client, table: str) -> list[str]:
    """The registered table's column names, in alphabetical order.

    Read from ``information_schema`` rather than from a ``LIMIT 0`` header: an
    empty result streams as a zero-length body, which carries no schema.
    """
    rows = client.sql_rows(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_name = '{table}' ORDER BY column_name",
        admin=True,
    )
    return [row[0] for row in rows[1:]]


@pytest.fixture(scope="module")
def iceberg_table(datasets_dir) -> str:
    _stage(datasets_dir / ICEBERG_DIR, evolved=False)
    return ICEBERG_DIR


def test_read_iceberg_table_function(client, iceberg_table):
    assert client.count(f"SELECT * FROM read_iceberg('{iceberg_table}')") == STAGED_ROWS


def test_read_iceberg_filter(client, iceberg_table):
    n = client.count(f"SELECT * FROM read_iceberg('{iceberg_table}') WHERE name = 'argo'")
    assert n == 2


def test_read_iceberg_without_a_table_fails_clearly(client):
    with pytest.raises(QueryError) as error:
        client.count("SELECT * FROM read_iceberg('obs')")
    assert "Iceberg metadata" in str(error.value)


def test_external_iceberg_table(client, iceberg_table):
    name = "iceberg_ext"
    try:
        client.execute(f"DROP TABLE IF EXISTS {name}", admin=True)
    except QueryError:
        pass
    try:
        client.execute(
            f"CREATE EXTERNAL TABLE {name} STORED AS ICEBERG "
            f"LOCATION 'datasets://{iceberg_table}'",
            admin=True,
        )
        assert name in client.tables().json()
        assert client.count(f"SELECT * FROM {name}") == STAGED_ROWS
        # The schema came from the table metadata, not from the DDL.
        assert _columns(client, name) == ["id", "name", "value"]
        assert client.count(f"SELECT * FROM {name} WHERE value > 10") == 2
    finally:
        client.execute(f"DROP TABLE IF EXISTS {name}", admin=True)


def test_iceberg_joins_parquet(client, iceberg_table):
    """One query over an Iceberg table and a Parquet dataset."""
    # The generated parquet rows carry platform_id = i % 50 over 1000 rows, so
    # every id in range matches exactly 20 of them.
    ids = [
        int(float(row[0]))
        for row in client.sql_rows(f"SELECT id FROM read_iceberg('{iceberg_table}')")[1:]
    ]
    expected = 20 * len([i for i in ids if 0 <= i < 50])
    n = client.count(
        f"SELECT * FROM read_iceberg('{iceberg_table}') i "
        "JOIN read_parquet('obs/*.parquet') p ON p.platform_id = i.id"
    )
    assert n == expected


def test_iceberg_pushes_the_filter_into_the_scan(client, iceberg_table):
    """A `WHERE` clause reaches the Iceberg scan, which is what prunes data files."""
    rows = client.sql_rows(
        f"EXPLAIN SELECT * FROM read_iceberg('{iceberg_table}') WHERE id > 3", admin=True
    )
    plan = "\n".join(cell for row in rows for cell in row)
    assert "IcebergTableScan" in plan, plan
    assert "predicate:[" in plan and "id" in plan, plan
    # The pruned scan still returns the right rows.
    assert client.count(f"SELECT * FROM read_iceberg('{iceberg_table}') WHERE id > 3") == 2


def test_schema_evolution_shows_up_without_a_restart(client, datasets_dir, iceberg_table):
    """The writer adds a column and a row; the next query sees both.

    This runs last: it moves the fixture to its final version for good.
    """
    name = "iceberg_evolving"
    try:
        client.execute(f"DROP TABLE IF EXISTS {name}", admin=True)
    except QueryError:
        pass
    try:
        client.execute(
            f"CREATE EXTERNAL TABLE {name} STORED AS ICEBERG "
            f"LOCATION 'datasets://{iceberg_table}'",
            admin=True,
        )
        assert client.count(f"SELECT * FROM {name}") == STAGED_ROWS
        assert "qc_flag" not in _columns(client, name)

        # Another writer commits a schema evolution and a row that fills it.
        _stage(datasets_dir / ICEBERG_DIR, evolved=True)

        assert "qc_flag" in _columns(client, name), (
            "the added column should show with no restart"
        )
        assert client.count(f"SELECT * FROM {name}") == EVOLVED_ROWS
        assert client.count(f"SELECT * FROM {name} WHERE qc_flag = 1") == 1
    finally:
        client.execute(f"DROP TABLE IF EXISTS {name}", admin=True)
