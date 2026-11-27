"""An Iceberg table on an S3 bucket, read with no local copy.

Beacon's datasets store is either a local directory or one S3-compatible
bucket, chosen at startup. Every other Iceberg test uses the local form, so
this one covers the other: a SeaweedFS sidecar holds the table, a second Beacon
runs with ``BEACON_S3_DATASETS``, and its own datasets directory stays empty
while it answers the queries.

The table is the one committed under ``test-datasets/iceberg-example`` — the
same bytes the local suite reads, uploaded to a bucket instead. Its metadata
still records the absolute paths of the warehouse it was written in, so this
also covers rebasing those onto a bucket key prefix.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import boto3
import pytest
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError

from beacon_client import BeaconHTTPClient, QueryError
from conftest import ADMIN_PASSWORD, ADMIN_USERNAME, _resolve_host_port, _run, run_beacon_container

REPO_ROOT = Path(__file__).resolve().parent.parent
FIXTURE = REPO_ROOT / "test-datasets" / "iceberg-example"

S3_CONTAINER = "beacon-it-seaweedfs"
S3_HOST_ALIAS = "beacon-it-seaweedfs"  # resolvable on docker_network
S3_IMAGE = "chrislusf/seaweedfs:4.52"
S3_PORT = 8333
S3_KEY = "beacon-it-key"
S3_SECRET = "beacon-it-secret"
S3_REGION = "us-east-1"
BUCKET = "datasets"
# Key prefix of the table inside the bucket. Beacon addresses it by this name,
# because the bucket itself is the datasets root.
TABLE_PREFIX = "iceberg-example"

FIXTURE_ROWS = 6


def _s3_config() -> dict:
    """The SeaweedFS S3 identity file: one key pair with full access."""
    return {
        "identities": [
            {
                "name": "beacon-it",
                "credentials": [{"accessKey": S3_KEY, "secretKey": S3_SECRET}],
                "actions": ["Admin", "Read", "List", "Tagging", "Write"],
            }
        ]
    }


def _boto_client(endpoint: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=S3_KEY,
        aws_secret_access_key=S3_SECRET,
        region_name=S3_REGION,
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1}),
    )


def _upload_fixture(client) -> None:
    """Make the bucket and copy the Iceberg table into it under TABLE_PREFIX."""
    client.create_bucket(Bucket=BUCKET)
    files = [f for f in FIXTURE.rglob("*") if f.is_file()]
    if not any(f.suffix == ".parquet" for f in files):
        pytest.fail(f"the Iceberg fixture has no data files under {FIXTURE}")
    for path in files:
        key = f"{TABLE_PREFIX}/{path.relative_to(FIXTURE).as_posix()}"
        client.upload_file(str(path), BUCKET, key)


@pytest.fixture(scope="module")
def s3_server(docker_network, tmp_path_factory):
    """Run SeaweedFS on the shared network and upload the Iceberg table to it."""
    config_dir = tmp_path_factory.mktemp("seaweedfs-config")
    (config_dir / "s3.json").write_text(json.dumps(_s3_config()), encoding="utf-8")

    _run(["docker", "rm", "-f", S3_CONTAINER])
    started = _run(
        [
            "docker", "run", "-d",
            "--name", S3_CONTAINER,
            "--network", docker_network,
            "--network-alias", S3_HOST_ALIAS,
            "-p", f"{S3_PORT}",  # the test process uploads through this port
            "-v", f"{config_dir}:/etc/seaweedfs-it:ro",
            S3_IMAGE, "server",
            "-dir=/data",
            "-s3",
            f"-s3.port={S3_PORT}",
            "-s3.config=/etc/seaweedfs-it/s3.json",
            # Small volumes keep the disk use of the sidecar low.
            "-master.volumeSizeLimitMB=64",
        ]
    )
    if started.returncode != 0:
        pytest.fail(f"docker run seaweedfs failed:\n{started.stdout}\n{started.stderr}")

    def _fail(message: str):
        logs = _run(["docker", "logs", S3_CONTAINER])
        _run(["docker", "rm", "-f", S3_CONTAINER])
        pytest.fail(f"{message}\n{logs.stdout}\n{logs.stderr}")

    client = _boto_client(f"http://127.0.0.1:{_resolve_host_port(S3_CONTAINER, S3_PORT)}")

    # Ready when the gateway answers an authenticated request.
    deadline = time.time() + 120
    while time.time() < deadline:
        try:
            client.list_buckets()
            break
        except (BotoCoreError, ClientError):
            time.sleep(2)
    else:
        _fail("seaweedfs did not become ready:")

    try:
        _upload_fixture(client)
    except (BotoCoreError, ClientError) as exc:
        _fail(f"uploading the Iceberg fixture failed: {exc}")

    yield {"endpoint": f"http://{S3_HOST_ALIAS}:{S3_PORT}", "bucket": BUCKET}
    _run(["docker", "rm", "-f", S3_CONTAINER])


@pytest.fixture(scope="module")
def s3_datasets_dir(tmp_path_factory) -> Path:
    """The S3-backed server's *local* datasets directory. It must stay empty."""
    return tmp_path_factory.mktemp("beacon-s3-datasets")


@pytest.fixture(scope="module")
def s3_client(
    request, beacon_image, docker_network, s3_server, s3_datasets_dir, tmp_path_factory
) -> BeaconHTTPClient:
    """A Beacon whose datasets store is the SeaweedFS bucket."""
    base_url = run_beacon_container(
        request,
        name="beacon-integration-s3",
        image=beacon_image,
        network=docker_network,
        datasets_dir=s3_datasets_dir,
        tables_dir=tmp_path_factory.mktemp("beacon-s3-tables"),
        extra_env={
            "BEACON_S3_DATASETS": "true",
            "BEACON_S3_BUCKET": s3_server["bucket"],
            "BEACON_S3_ALLOW_HTTP": "true",
            "AWS_ACCESS_KEY_ID": S3_KEY,
            "AWS_SECRET_ACCESS_KEY": S3_SECRET,
            "AWS_REGION": S3_REGION,
            "AWS_ENDPOINT": s3_server["endpoint"],
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
