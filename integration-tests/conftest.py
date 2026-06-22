"""Pytest fixtures that build the Beacon image, generate sample data, run the
container, and hand tests an HTTP client.

The whole suite is skipped when the ``docker`` CLI is unavailable. Set
``BEACON_IMAGE`` to reuse a prebuilt image instead of building the local Dockerfile
(fast reruns); leave it unset to build ``beacon-integration:local`` from the repo root.
"""

from __future__ import annotations

import base64
import os
import shutil
import subprocess
import time
from pathlib import Path

import pytest

from beacon_client import BeaconHTTPClient

REPO_ROOT = Path(__file__).resolve().parent.parent
LOCAL_IMAGE_TAG = "beacon-integration:local"

ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = "securepassword"

# Container internal ports.
HTTP_PORT = 5001
FLIGHT_PORT = 32011

CONTAINER_NAME = "beacon-integration-test"
NETWORK_NAME = "beacon-integration-net"
# Base64 32-byte master key so the beacon container can encrypt external-database
# credentials at rest (required by `STORED AS POSTGRES/MYSQL` with a password).
SECRETS_KEY = base64.b64encode(bytes(range(32))).decode()
HEALTH_TIMEOUT_S = 180
BUILD_TIMEOUT_S = 3600

# Generated dataset: number of rows split across two parquet files so the tests
# exercise multi-file globs. Kept tiny so the suite stays fast.
ROWS_FILE_A = 600
ROWS_FILE_B = 400
TOTAL_ROWS = ROWS_FILE_A + ROWS_FILE_B
PLATFORMS = ["SHIP", "BUOY", "GLIDER", "FLOAT"]


def _docker_available() -> bool:
    return shutil.which("docker") is not None


def _run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, **kwargs)


pytestmark = pytest.mark.skipif(not _docker_available(), reason="docker CLI not available")


@pytest.fixture(scope="session")
def beacon_image() -> str:
    """The image to run: an override via ``BEACON_IMAGE`` or a freshly built local one."""
    override = os.environ.get("BEACON_IMAGE")
    if override:
        return override

    print(f"\n[integration] building image {LOCAL_IMAGE_TAG} from {REPO_ROOT} ...")
    result = _run(
        ["docker", "build", "-t", LOCAL_IMAGE_TAG, str(REPO_ROOT)],
        timeout=BUILD_TIMEOUT_S,
    )
    if result.returncode != 0:
        pytest.fail(f"docker build failed:\n{result.stdout}\n{result.stderr}")
    return LOCAL_IMAGE_TAG


@pytest.fixture(scope="session")
def sample_data():
    """Generate deterministic oceanographic-style rows shared by all tests.

    Returns a dict with the in-memory column data plus expected aggregates the tests
    assert against, so the expectations live next to the generator.
    """
    n = TOTAL_ROWS
    rows = []
    for i in range(n):
        rows.append(
            {
                "time": 1_700_000_000 + i * 3600,
                "latitude": -60.0 + (i % 120) * 1.0,
                "longitude": -180.0 + (i % 360) * 1.0,
                "depth": float(i % 500),
                "platform": PLATFORMS[i % len(PLATFORMS)],
                "platform_id": i % 50,
                # Spans 5.0..34.0 so the > 20 filter splits the data meaningfully.
                "temperature": 5.0 + float(i % 30),
                "salinity": 30.0 + (i % 10) * 0.4,
            }
        )
    warm = [r for r in rows if r["temperature"] > 20]
    return {
        "rows": rows,
        "total": n,
        "warm_count": len(warm),
        "platforms": PLATFORMS,
    }


def _write_parquet(rows: list[dict], path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    if not rows:
        return
    cols = {k: [r[k] for r in rows] for k in rows[0].keys()}
    schema = pa.schema(
        [
            ("time", pa.int64()),
            ("latitude", pa.float64()),
            ("longitude", pa.float64()),
            ("depth", pa.float32()),
            ("platform", pa.string()),
            ("platform_id", pa.int32()),
            ("temperature", pa.float32()),
            ("salinity", pa.float32()),
        ]
    )
    table = pa.table(cols, schema=schema)
    pq.write_table(table, path)


@pytest.fixture(scope="session")
def datasets_dir(tmp_path_factory, sample_data) -> Path:
    """A datasets directory mounted into the container at /beacon/data/datasets."""
    root = tmp_path_factory.mktemp("beacon-datasets")
    obs = root / "obs"
    obs.mkdir()
    rows = sample_data["rows"]
    _write_parquet(rows[:ROWS_FILE_A], obs / "part-0.parquet")
    _write_parquet(rows[ROWS_FILE_A:], obs / "part-1.parquet")

    # A small CSV so the CSV reader is exercised too.
    csv_path = root / "stations.csv"
    csv_path.write_text(
        "platform,description\n" + "\n".join(f"{p},{p.lower()} platform" for p in PLATFORMS) + "\n",
        encoding="utf-8",
    )
    return root


@pytest.fixture(scope="session")
def tables_dir(tmp_path_factory) -> Path:
    """Empty directory for managed (Iceberg) table persistence."""
    return tmp_path_factory.mktemp("beacon-tables")


@pytest.fixture(scope="session")
def docker_network():
    """A user-defined bridge network so the beacon container can reach sidecar
    containers (e.g. PostgreSQL) by name."""
    _run(["docker", "network", "rm", NETWORK_NAME])
    created = _run(["docker", "network", "create", NETWORK_NAME])
    if created.returncode != 0:
        pytest.fail(f"docker network create failed:\n{created.stdout}\n{created.stderr}")
    yield NETWORK_NAME
    _run(["docker", "network", "rm", NETWORK_NAME])


@pytest.fixture(scope="session")
def beacon_container(request, beacon_image, datasets_dir, tables_dir, docker_network):
    """Run Beacon in a container and yield its base HTTP URL."""
    # Remove any stale container from a previous interrupted run.
    _run(["docker", "rm", "-f", CONTAINER_NAME])

    run_cmd = [
        "docker", "run", "-d",
        "--name", CONTAINER_NAME,
        "--network", docker_network,
        "-p", f"{HTTP_PORT}",          # publish container HTTP port to a random host port
        "-p", f"{FLIGHT_PORT}",
        "-v", f"{datasets_dir}:/beacon/data/datasets",
        "-v", f"{tables_dir}:/beacon/data/tables",
        "-e", "BEACON_ENABLE_SQL=true",
        "-e", f"BEACON_ADMIN_USERNAME={ADMIN_USERNAME}",
        "-e", f"BEACON_ADMIN_PASSWORD={ADMIN_PASSWORD}",
        "-e", f"BEACON_SECRETS_KEY={SECRETS_KEY}",
        "-e", "BEACON_LOG_LEVEL=info",
        beacon_image,
    ]
    started = _run(run_cmd)
    if started.returncode != 0:
        pytest.fail(f"docker run failed:\n{started.stdout}\n{started.stderr}")

    def _dump_logs_and_remove():
        # Dump logs when the session had failures, to aid debugging in CI/local.
        if request.session.testsfailed:
            logs = _run(["docker", "logs", CONTAINER_NAME])
            print("\n===== beacon container logs =====")
            print(logs.stdout)
            print(logs.stderr)
            print("===== end logs =====")
        _run(["docker", "rm", "-f", CONTAINER_NAME])

    request.addfinalizer(_dump_logs_and_remove)

    host_port = _resolve_host_port(CONTAINER_NAME, HTTP_PORT)
    base_url = f"http://127.0.0.1:{host_port}"
    _wait_healthy(base_url, CONTAINER_NAME)
    return base_url


def _resolve_host_port(container: str, container_port: int) -> int:
    result = _run(["docker", "port", container, f"{container_port}/tcp"])
    if result.returncode != 0 or not result.stdout.strip():
        pytest.fail(f"could not resolve host port for {container_port}:\n{result.stderr}")
    # Output looks like "0.0.0.0:49154" (possibly multiple lines for ipv4/ipv6).
    first = result.stdout.strip().splitlines()[0]
    return int(first.rsplit(":", 1)[1])


def _wait_healthy(base_url: str, container: str) -> None:
    import requests

    deadline = time.time() + HEALTH_TIMEOUT_S
    last_err = None
    while time.time() < deadline:
        # Bail early with logs if the container died.
        state = _run(["docker", "inspect", "-f", "{{.State.Running}}", container])
        if state.stdout.strip() != "true":
            logs = _run(["docker", "logs", container])
            pytest.fail(f"beacon container exited early:\n{logs.stdout}\n{logs.stderr}")
        try:
            resp = requests.get(f"{base_url}/api/health", timeout=5)
            if resp.status_code == 200:
                return
            last_err = f"status {resp.status_code}"
        except Exception as exc:  # noqa: BLE001 - just record and retry
            last_err = str(exc)
        time.sleep(2)
    pytest.fail(f"beacon did not become healthy within {HEALTH_TIMEOUT_S}s (last: {last_err})")


@pytest.fixture(scope="session")
def client(beacon_container) -> BeaconHTTPClient:
    return BeaconHTTPClient(beacon_container, ADMIN_USERNAME, ADMIN_PASSWORD)
