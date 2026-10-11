"""ERDDAP reads a Beacon table over DAP2 (EDDTableFromDapSequence).

Manual: set BEACON_ERDDAP_E2E=1 and follow README.md. Otherwise the file skips itself.
"""

from __future__ import annotations

import csv
import io
import os
import time
import urllib.parse

import pytest
import requests

if os.environ.get("BEACON_ERDDAP_E2E") != "1":
    pytest.skip("set BEACON_ERDDAP_E2E=1 to run the ERDDAP test", allow_module_level=True)

BEACON = os.environ.get("BEACON_URL", "http://localhost:5001")
ERDDAP = os.environ.get("ERDDAP_URL", "http://localhost:8090/erddap")
ADMIN = (
    os.environ.get("BEACON_ADMIN_USERNAME", "admin"),
    os.environ.get("BEACON_ADMIN_PASSWORD", "securepassword"),
)
DATASET = "beacon_dap2_obs"
LOAD_TIMEOUT_S = 300


def _sql(sql: str) -> None:
    response = requests.post(f"{BEACON}/api/query", json={"sql": sql}, auth=ADMIN, timeout=60)
    assert response.status_code == 200, f"{sql}: {response.text}"


@pytest.fixture(scope="module")
def erddap_dataset():
    _sql("DROP TABLE IF EXISTS erddap_obs")
    _sql("CREATE TABLE erddap_obs (t TIMESTAMP, temp DOUBLE, platform VARCHAR)")
    _sql(
        "INSERT INTO erddap_obs VALUES "
        "(TIMESTAMP '2024-01-01 00:00:00', 10.5, 'SHIP'), "
        "(TIMESTAMP '2024-01-02 00:00:00', 6.0, 'BUOY'), "
        "(TIMESTAMP '2024-01-03 00:00:00', 4.0, 'BUOY')"
    )
    deadline = time.monotonic() + LOAD_TIMEOUT_S
    while time.monotonic() < deadline:
        response = requests.get(f"{ERDDAP}/tabledap/{DATASET}.das", timeout=30)
        if response.status_code == 200:
            return DATASET
        time.sleep(10)
    pytest.fail(f"ERDDAP did not load {DATASET} in {LOAD_TIMEOUT_S} s; see its log.txt")


def test_erddap_filters_through_beacon(erddap_dataset):
    query = urllib.parse.quote('time,temp,platform&temp>5&platform="BUOY"', safe="&=,")

    response = requests.get(f"{ERDDAP}/tabledap/{erddap_dataset}.csv?{query}", timeout=60)

    assert response.status_code == 200, response.text
    rows = list(csv.reader(io.StringIO(response.text)))
    # Row 0 holds the names and row 1 the units.
    assert rows[0] == ["time", "temp", "platform"]
    assert rows[2:] == [["2024-01-02T00:00:00Z", "6.0", "BUOY"]]
