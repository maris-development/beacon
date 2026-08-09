"""An Iceberg table on an S3 bucket, read with no local copy.

Beacon's datasets store is either a local directory or one S3-compatible
bucket, chosen at startup. Every other Iceberg test uses the local form, so
this one covers the other: a MinIO sidecar holds the table, a second Beacon
runs with ``BEACON_S3_DATASETS``, and its own datasets directory stays empty
while it answers the queries.

The table is the one committed under ``test-datasets/iceberg-example`` — the
same bytes the local suite reads, uploaded to a bucket instead. Its metadata
still records the absolute paths of the warehouse it was written in, so this
also covers rebasing those onto a bucket key prefix.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from beacon_client import BeaconHTTPClient, QueryError
from conftest import ADMIN_PASSWORD, ADMIN_USERNAME, _run, run_beacon_container

REPO_ROOT = Path(__file__).resolve().parent.parent
FIXTURE = REPO_ROOT / "test-datasets" / "iceberg-example"

MINIO_CONTAINER = "beacon-it-minio"
MINIO_HOST_ALIAS = "beacon-it-minio"  # resolvable on docker_network
MINIO_IMAGE = "minio/minio:RELEASE.2025-09-07T16-13-09Z"
MC_IMAGE = "minio/mc:latest"
MINIO_KEY = "minioadmin"
MINIO_SECRET = "minioadmin"
BUCKET = "datasets"
# Key prefix of the table inside the bucket. Beacon addresses it by this name,
# because the bucket itself is the datasets root.
TABLE_PREFIX = "iceberg-example"

FIXTURE_ROWS = 6


ALIAS = f"mc alias set m http://{MINIO_HOST_ALIAS}:9000 {MINIO_KEY} {MINIO_SECRET}"


def _mc(network: str, script: str, *, mount_fixture: bool = False):
    """Run one `mc` script against the sidecar, on the shared network."""
    cmd = ["docker", "run", "--rm", "--network", network]
    if mount_fixture:
        cmd += ["-v", f"{FIXTURE.parent}:/src:ro"]
    cmd += ["--entrypoint", "sh", MC_IMAGE, "-c", script]
    return _run(cmd)


@pytest.fixture(scope="module")
def minio_container(docker_network):
    """Run MinIO on the shared network and upload the Iceberg table to it."""
    _run(["docker", "rm", "-f", MINIO_CONTAINER])
    started = _run(
        [
            "docker", "run", "-d",
            "--name", MINIO_CONTAINER,
            "--network", docker_network,
            "--network-alias", MINIO_HOST_ALIAS,
            "-e", f"MINIO_ROOT_USER={MINIO_KEY}",
            "-e", f"MINIO_ROOT_PASSWORD={MINIO_SECRET}",
            MINIO_IMAGE, "server", "/data",
        ]
    )
    if started.returncode != 0:
        pytest.fail(f"docker run minio failed:\n{started.stdout}\n{started.stderr}")

    # Ready when `mc` can reach the API.
    deadline = time.time() + 120
    while time.time() < deadline:
        if _mc(docker_network, ALIAS).returncode == 0:
            break
        time.sleep(2)
    else:
        logs = _run(["docker", "logs", MINIO_CONTAINER])
        _run(["docker", "rm", "-f", MINIO_CONTAINER])
        pytest.fail(f"minio did not become ready:\n{logs.stdout}\n{logs.stderr}")

    upload = _mc(
        docker_network,
        f"{ALIAS} && mc mb --ignore-existing m/{BUCKET} && "
        f"mc cp --recursive /src/{FIXTURE.name} m/{BUCKET}/",
        mount_fixture=True,
    )
    if upload.returncode != 0:
        _run(["docker", "rm", "-f", MINIO_CONTAINER])
        pytest.fail(f"uploading the Iceberg fixture failed:\n{upload.stdout}\n{upload.stderr}")

    yield {"endpoint": f"http://{MINIO_HOST_ALIAS}:9000", "bucket": BUCKET}
    _run(["docker", "rm", "-f", MINIO_CONTAINER])


@pytest.fixture(scope="module")
def s3_datasets_dir(tmp_path_factory) -> Path:
    """The S3-backed server's *local* datasets directory. It must stay empty."""
    return tmp_path_factory.mktemp("beacon-s3-datasets")


@pytest.fixture(scope="module")
def s3_client(
    request, beacon_image, docker_network, minio_container, s3_datasets_dir, tmp_path_factory
) -> BeaconHTTPClient:
    """A Beacon whose datasets store is the MinIO bucket."""
    base_url = run_beacon_container(
        request,
        name="beacon-integration-s3",
        image=beacon_image,
        network=docker_network,
        datasets_dir=s3_datasets_dir,
        tables_dir=tmp_path_factory.mktemp("beacon-s3-tables"),
        extra_env={
            "BEACON_S3_DATASETS": "true",
            "BEACON_S3_BUCKET": minio_container["bucket"],
            "BEACON_S3_ALLOW_HTTP": "true",
            "AWS_ACCESS_KEY_ID": MINIO_KEY,
            "AWS_SECRET_ACCESS_KEY": MINIO_SECRET,
            "AWS_REGION": "us-east-1",
            "AWS_ENDPOINT": minio_container["endpoint"],
            "AWS_ALLOW_HTTP": "true",
        },
    )
    return BeaconHTTPClient(base_url, ADMIN_USERNAME, ADMIN_PASSWORD)


def test_read_iceberg_from_a_bucket(s3_client):
    assert s3_client.count(f"SELECT * FROM read_iceberg('{TABLE_PREFIX}')") == FIXTURE_ROWS


def test_external_iceberg_table_on_a_bucket(s3_client):
    name = "s3_iceberg"
    try:
        s3_client.execute(f"DROP TABLE IF EXISTS {name}", admin=True)
    except QueryError:
        pass
    try:
        s3_client.execute(
            f"CREATE EXTERNAL TABLE {name} STORED AS ICEBERG "
            f"LOCATION 'datasets://{TABLE_PREFIX}'",
            admin=True,
        )
        assert s3_client.count(f"SELECT * FROM {name}") == FIXTURE_ROWS
        # The values arrive, including the column a later commit added.
        rows = s3_client.sql_rows(f"SELECT id, qc_flag FROM {name} ORDER BY id")[1:]
        assert [row[0] for row in rows] == ["1", "2", "3", "4", "5", "6"]
        assert rows[-1][1] == "1", rows
        # A filter still prunes correctly against the bucket.
        assert s3_client.count(f"SELECT * FROM {name} WHERE value > 10") == 2
    finally:
        s3_client.execute(f"DROP TABLE IF EXISTS {name}", admin=True)


def test_nothing_was_copied_to_local_disk(s3_client, s3_datasets_dir):
    """The read is served from the bucket, not from a staged local copy."""
    # Force a full read first, so any staging would already have happened.
    assert s3_client.count(f"SELECT * FROM read_iceberg('{TABLE_PREFIX}')") == FIXTURE_ROWS
    staged = list(s3_datasets_dir.rglob("*"))
    assert staged == [], f"the datasets directory should stay empty, found: {staged}"
