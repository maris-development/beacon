"""Croissant export, read with the mlcroissant library.

The file skips itself when mlcroissant is absent (see requirements-optional.txt).
Set ``BEACON_BASE_URL`` to run it against an already-running Beacon instead of Docker.
"""

from __future__ import annotations

import os

import pytest
import requests

from beacon_client import BeaconHTTPClient, QueryError

mlc = pytest.importorskip("mlcroissant")

TABLE = "itest_croissant"
ADMIN_USER = os.environ.get("BEACON_ADMIN_USERNAME", "admin")
ADMIN_PW = os.environ.get("BEACON_ADMIN_PASSWORD", "securepassword")


@pytest.fixture(scope="module")
def base_url(request) -> str:
    # Resolve the Docker container only when no external server is set.
    url = os.environ.get("BEACON_BASE_URL")
    if url:
        return url.rstrip("/")
    return request.getfixturevalue("beacon_container")


@pytest.fixture(scope="module")
def admin(base_url) -> BeaconHTTPClient:
    return BeaconHTTPClient(base_url, ADMIN_USER, ADMIN_PW)


def _drop(admin):
    try:
        admin.execute(f"DROP TABLE IF EXISTS {TABLE}", admin=True)
    except QueryError:
        pass


@pytest.fixture(scope="module")
def croissant_table(admin):
    _drop(admin)
    for sql in (
        f"CREATE TABLE {TABLE} (id BIGINT, t TIMESTAMP, temp DOUBLE, ok BOOLEAN, platform VARCHAR)",
        f"INSERT INTO {TABLE} VALUES "
        "(1, TIMESTAMP '2024-01-01 00:00:00', 10.5, true, 'SHIP'), "
        "(2, TIMESTAMP '2024-01-02 00:00:00', 4.25, false, 'BUOY'), "
        "(3, TIMESTAMP '2024-01-03 00:00:00', 7.0, true, 'BUOY')",
        f"COMMENT ON TABLE {TABLE} IS 'Croissant integration test observations'",
        f"COMMENT ON COLUMN {TABLE}.temp IS 'sea water temperature'",
    ):
        admin.execute(sql, admin=True)
    yield TABLE
    _drop(admin)


@pytest.fixture(scope="module")
def dataset(base_url, croissant_table):
    return mlc.Dataset(jsonld=f"{base_url}/api/croissant/{croissant_table}")


def _errors(ds) -> list[str]:
    return sorted(str(error) for error in ds.metadata.ctx.issues.errors)


def test_document_validates(dataset):
    # Warnings about license, creator and datePublished are expected.
    assert _errors(dataset) == []


def test_records_load(dataset, croissant_table):
    records = list(dataset.records(record_set=croissant_table))

    assert len(records) == 3
    by_id = sorted(records, key=lambda r: r[f"{croissant_table}/id"])
    assert [r[f"{croissant_table}/temp"] for r in by_id] == [10.5, 4.25, 7.0]


def test_catalog_lists_the_table(base_url, croissant_table):
    resp = requests.get(f"{base_url}/api/croissant", timeout=30)

    assert resp.status_code == 200, resp.text
    names = [entry["name"] for entry in resp.json()["dataset"]]
    assert croissant_table in names
