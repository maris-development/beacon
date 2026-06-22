"""Admin crawler endpoints: create -> list -> get -> run -> drop over /api/admin/*.

Also asserts the basic-auth gate: the admin surface rejects unauthenticated calls
with 401 (it never silently downgrades to anonymous like the read-only query path).
"""

from __future__ import annotations

import pytest

CRAWLER = "obs_crawler"


@pytest.fixture
def crawler(client):
    """Define the crawler once per test, dropping it (and reruns' leftovers) around."""
    client.admin_delete(f"/api/admin/crawlers/{CRAWLER}")  # idempotent pre-clean
    body = {
        "name": CRAWLER,
        "target_prefix": "obs/",
        "format_filter": ["parquet"],
        "table_naming": "crawler_prefixed",
    }
    resp = client.admin_post("/api/admin/crawlers", body)
    assert resp.status_code == 200, resp.text
    yield CRAWLER
    client.admin_delete(f"/api/admin/crawlers/{CRAWLER}")
    # The crawl registers `<crawler>_obs`; drop it so it does not leak across runs.
    client.execute(f"DROP TABLE IF EXISTS {CRAWLER}_obs", admin=True)


def test_crawler_listed_and_retrievable(client, crawler):
    listed = client.admin_get("/api/admin/crawlers")
    assert listed.status_code == 200
    names = [c["name"] for c in listed.json()]
    assert crawler in names

    single = client.admin_get(f"/api/admin/crawlers/{crawler}")
    assert single.status_code == 200
    body = single.json()
    assert body["name"] == crawler
    assert body["target_prefix"] == "obs/"
    assert body["table_naming"] == "crawler_prefixed"
    assert body["format_filter"] == ["parquet"]


def test_get_unknown_crawler_is_404(client):
    resp = client.admin_get("/api/admin/crawlers/does_not_exist")
    assert resp.status_code == 404


def test_run_crawler_discovers_and_registers_table(client, crawler):
    resp = client.admin_post(f"/api/admin/crawlers/{crawler}/run")
    assert resp.status_code == 200, resp.text
    report = resp.json()
    assert report["crawler"] == crawler
    assert report["discovered"] >= 1
    # The discovered obs parquet group becomes `<crawler>_obs`.
    table = f"{crawler}_obs"
    assert table in report["created"] or table in report["updated"]

    tables = client.tables()
    assert tables.status_code == 200
    assert table in tables.json()


def test_drop_crawler_removes_it(client, crawler):
    assert client.admin_delete(f"/api/admin/crawlers/{crawler}").status_code == 200
    assert client.admin_get(f"/api/admin/crawlers/{crawler}").status_code == 404
    # Dropping a non-existent crawler is a 400 (the manager errors).
    assert client.admin_delete(f"/api/admin/crawlers/{crawler}").status_code == 400


def test_admin_crawler_endpoints_require_auth(client):
    assert client.admin_get("/api/admin/crawlers", admin=False).status_code == 401
    assert (
        client.admin_post("/api/admin/crawlers", {"name": "x", "target_prefix": "obs/"}, admin=False).status_code
        == 401
    )
