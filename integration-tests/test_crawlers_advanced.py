"""Advanced crawler behavior beyond the basic CRUD in test_admin_crawlers.py:
format-filter exclusion, table-naming strategies, re-run idempotency, and the
SQL crawler DDL (CREATE / RUN / SHOW / DROP CRAWLER).
"""

from __future__ import annotations

import pytest

from beacon_client import QueryError


def _drop_table(client, name):
    try:
        client.execute(f"DROP TABLE IF EXISTS {name}", admin=True)
    except QueryError:
        pass


@pytest.fixture
def mixed_dir(datasets_dir):
    """The ``mixed/`` dir (parquet + csv) created at container start in conftest, so
    the crawler's initial dataset listing sees it."""
    return "mixed/"


def test_format_filter_excludes_non_matching(client, mixed_dir, sample_data):
    name = "fmt_crawler"
    client.admin_delete(f"/api/admin/crawlers/{name}")
    _drop_table(client, "mixed")
    try:
        resp = client.admin_post(
            "/api/admin/crawlers",
            {
                "name": name,
                "target_prefix": "mixed/",
                "format_filter": ["parquet"],
                "table_naming": "leaf_prefix",
            },
        )
        assert resp.status_code == 200, resp.text

        report = client.admin_post(f"/api/admin/crawlers/{name}/run").json()
        # The parquet group is discovered. (Files excluded by `format_filter` are
        # dropped before the extension check, so they are NOT counted in
        # `skipped_files` — that counter only tracks extension/format mismatches.)
        assert report["discovered"] >= 1, report
        assert "mixed" in client.tables().json()
        # The decisive check that the csv was excluded: only the parquet rows
        # (both parts) were registered, not the csv.
        assert client.count("SELECT * FROM mixed") == sample_data["total"]
    finally:
        client.admin_delete(f"/api/admin/crawlers/{name}")
        _drop_table(client, "mixed")


def test_leaf_prefix_naming_and_idempotent_rerun(client, sample_data):
    name = "leaf_cr"
    client.admin_delete(f"/api/admin/crawlers/{name}")
    _drop_table(client, "obs")
    try:
        client.admin_post(
            "/api/admin/crawlers",
            {"name": name, "target_prefix": "obs/", "format_filter": ["parquet"],
             "table_naming": "leaf_prefix"},
        )
        first = client.admin_post(f"/api/admin/crawlers/{name}/run").json()
        # leaf_prefix names the table after the prefix leaf: obs/ -> "obs".
        assert "obs" in first["created"]
        assert client.count("SELECT * FROM obs") == sample_data["total"]

        # Re-running is idempotent: the crawler-owned table is updated, not recreated.
        second = client.admin_post(f"/api/admin/crawlers/{name}/run").json()
        assert "obs" in second["updated"]
        assert client.count("SELECT * FROM obs") == sample_data["total"]
    finally:
        client.admin_delete(f"/api/admin/crawlers/{name}")
        _drop_table(client, "obs")


def test_sql_crawler_ddl(client, sample_data):
    """CREATE / RUN / SHOW / DROP CRAWLER over the SQL endpoint."""
    name = "sqlcr"
    table = f"{name}_obs"
    try:
        client.execute(f"DROP CRAWLER {name}", admin=True)
    except QueryError:
        pass
    _drop_table(client, table)
    try:
        client.execute(
            f"CREATE CRAWLER {name} ON 'obs/' "
            "WITH ('format' 'parquet', 'table_naming' 'crawler_prefixed')",
            admin=True,
        )
        crawlers = client.sql_rows("SHOW CRAWLERS", admin=True)
        assert name in {cell for row in crawlers[1:] for cell in row}

        client.execute(f"RUN CRAWLER {name}", admin=True)
        assert table in client.tables().json()
        assert client.count(f"SELECT * FROM {table}") == sample_data["total"]

        # Dropping the crawler leaves the discovered table in place.
        client.execute(f"DROP CRAWLER {name}", admin=True)
        show = client.sql_rows("SHOW CRAWLERS", admin=True)
        assert name not in {cell for row in show[1:] for cell in row}
        assert table in client.tables().json()
    finally:
        try:
            client.execute(f"DROP CRAWLER {name}", admin=True)
        except QueryError:
            pass
        _drop_table(client, table)


def test_create_crawler_requires_admin(client):
    assert client.status("CREATE CRAWLER nope ON 'obs/'", admin=False) == 400
