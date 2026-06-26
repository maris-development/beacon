"""Concurrency: many simultaneous queries must all return consistent results.

Each worker uses its own client (own ``requests`` session) against the shared
container, mirroring independent API clients hitting one Beacon instance.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from beacon_client import BeaconHTTPClient
from conftest import ADMIN_USERNAME, ADMIN_PASSWORD

N_WORKERS = 16


def _client(base_url) -> BeaconHTTPClient:
    return BeaconHTTPClient(base_url, ADMIN_USERNAME, ADMIN_PASSWORD)


def test_parallel_counts_are_consistent(beacon_container, sample_data):
    def one(_):
        return _client(beacon_container).count("SELECT * FROM read_parquet(['obs/*.parquet'])")

    with ThreadPoolExecutor(max_workers=N_WORKERS) as pool:
        results = list(pool.map(one, range(N_WORKERS)))

    assert results == [sample_data["total"]] * N_WORKERS


def test_parallel_distinct_aggregations(beacon_container, sample_data):
    """Different aggregate queries running concurrently each return the right answer."""
    warm_sql = "SELECT * FROM read_parquet(['obs/*.parquet']) WHERE temperature > 20"

    def task(i):
        c = _client(beacon_container)
        if i % 2 == 0:
            return ("total", c.count("SELECT * FROM read_parquet(['obs/*.parquet'])"))
        return ("warm", c.count(warm_sql))

    with ThreadPoolExecutor(max_workers=N_WORKERS) as pool:
        results = list(pool.map(task, range(N_WORKERS)))

    for kind, value in results:
        if kind == "total":
            assert value == sample_data["total"]
        else:
            assert value == sample_data["warm_count"]
