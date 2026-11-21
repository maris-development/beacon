"""Lance external tables on an S3 bucket served by SeaweedFS.

A SeaweedFS sidecar runs only its S3 gateway. A second Beacon uses the bucket
as its datasets store (``BEACON_S3_DATASETS``), so a relative ``LOCATION`` of a
``STORED AS LANCE`` table is a key prefix in the bucket. The test process
makes the bucket and reads the keys with ``boto3``, and reads and writes the
same datasets with pylance.

Lance commits a new version with a conditional PUT (``If-None-Match: *``). The
fixture checks that the server refuses a second conditional PUT on one key
before any test runs. Without that check, two writers could overwrite the same
manifest and the concurrency test would prove nothing.
"""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path

import pytest

boto3 = pytest.importorskip("boto3")
lance = pytest.importorskip("lance")

import pyarrow as pa  # noqa: E402
from botocore.client import Config  # noqa: E402
from botocore.exceptions import BotoCoreError, ClientError  # noqa: E402

from beacon_client import BeaconHTTPClient, QueryError  # noqa: E402
from conftest import (  # noqa: E402
    ADMIN_PASSWORD,
    ADMIN_USERNAME,
    _resolve_host_port,
    _run,
    run_beacon_container,
)

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
S3_CONFIG_DIR = HERE / "seaweedfs"
NETCDF_FIXTURE = REPO_ROOT / "test-datasets" / "test_file.nc"

S3_CONTAINER = "beacon-it-seaweedfs-lance"
S3_HOST_ALIAS = S3_CONTAINER  # resolvable on docker_network
S3_IMAGE = "chrislusf/seaweedfs:4.52"
S3_PORT = 8333
S3_REGION = "us-east-1"
BUCKET = "datasets"
PROBE_KEY = "_probe/conditional-put"
CONDITIONAL_PUT_MESSAGE = (
    "S3 test server does not support conditional PUT; Lance commits are not safe on it"
)


def _s3_keys() -> tuple[str, str]:
    """The one key pair of the SeaweedFS identity file."""
    config = json.loads((S3_CONFIG_DIR / "s3.json").read_text(encoding="utf-8"))
    credentials = config["identities"][0]["credentials"][0]
    return credentials["accessKey"], credentials["secretKey"]


S3_KEY, S3_SECRET = _s3_keys()


def _boto_client(endpoint: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=S3_KEY,
        aws_secret_access_key=S3_SECRET,
        region_name=S3_REGION,
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1}),
    )


def _check_conditional_put(client) -> None:
    """Fail the module when a second ``If-None-Match: *`` PUT on one key succeeds."""
    try:
        client.put_object(Bucket=BUCKET, Key=PROBE_KEY, Body=b"first", IfNoneMatch="*")
    except ClientError as exc:
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        pytest.fail(f"{CONDITIONAL_PUT_MESSAGE} (first PUT gave HTTP {status}: {exc})")
    try:
        client.put_object(Bucket=BUCKET, Key=PROBE_KEY, Body=b"second", IfNoneMatch="*")
    except ClientError as exc:
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 412:
            return
        pytest.fail(f"{CONDITIONAL_PUT_MESSAGE} (second PUT gave HTTP {status}: {exc})")
    pytest.fail(CONDITIONAL_PUT_MESSAGE)


@pytest.fixture(scope="module")
def seaweedfs_container(docker_network):
    """Run the SeaweedFS S3 gateway on the shared network, with the bucket made."""
    _run(["docker", "rm", "-f", S3_CONTAINER])
    started = _run(
        [
            "docker", "run", "-d",
            "--name", S3_CONTAINER,
            "--network", docker_network,
            "--network-alias", S3_HOST_ALIAS,
            "-p", f"{S3_PORT}",  # the test process uses the bucket through this port
            "-v", f"{S3_CONFIG_DIR}:/etc/seaweedfs-it:ro",
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
        pytest.fail(f"{message}\n{logs.stdout}\n{logs.stderr}")

    # Any failure before the yield removes the container; teardown does not run then.
    try:
        host_endpoint = f"http://127.0.0.1:{_resolve_host_port(S3_CONTAINER, S3_PORT)}"
        client = _boto_client(host_endpoint)

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
            client.create_bucket(Bucket=BUCKET)
        except (BotoCoreError, ClientError) as exc:
            _fail(f"making the bucket failed: {exc}")

        _check_conditional_put(client)
    except BaseException:
        _run(["docker", "rm", "-f", S3_CONTAINER])
        raise

    yield {
        "endpoint": f"http://{S3_HOST_ALIAS}:{S3_PORT}",
        "host_endpoint": host_endpoint,
        "bucket": BUCKET,
        "s3": client,
    }
    _run(["docker", "rm", "-f", S3_CONTAINER])


def _start_lance_s3_beacon(request, name, beacon_image, docker_network, server, tmp_path_factory):
    """Start a Beacon whose datasets store is the SeaweedFS bucket."""
    base_url = run_beacon_container(
        request,
        name=name,
        image=beacon_image,
        network=docker_network,
        datasets_dir=tmp_path_factory.mktemp(f"{name}-datasets"),
        tables_dir=tmp_path_factory.mktemp(f"{name}-tables"),
        extra_env={
            "BEACON_S3_DATASETS": "true",
            "BEACON_S3_BUCKET": server["bucket"],
            "BEACON_S3_ALLOW_HTTP": "true",
            "AWS_ACCESS_KEY_ID": S3_KEY,
            "AWS_SECRET_ACCESS_KEY": S3_SECRET,
            "AWS_REGION": S3_REGION,
            "AWS_ENDPOINT": server["endpoint"],
            "AWS_ALLOW_HTTP": "true",
        },
    )
    return BeaconHTTPClient(base_url, ADMIN_USERNAME, ADMIN_PASSWORD)


@pytest.fixture(scope="module")
def lance_s3_client(
    request, beacon_image, docker_network, seaweedfs_container, tmp_path_factory
) -> BeaconHTTPClient:
    """A Beacon on the SeaweedFS bucket."""
    return _start_lance_s3_beacon(
        request,
        "beacon-integration-lance-s3",
        beacon_image,
        docker_network,
        seaweedfs_container,
        tmp_path_factory,
    )


def _drop(client, name):
    try:
        client.execute(f"DROP TABLE IF EXISTS {name}", admin=True)
    except QueryError:
        pass


def _keys(server, prefix: str) -> list[str]:
    s3 = server["s3"]
    keys: list[str] = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=server["bucket"], Prefix=prefix):
        keys += [item["Key"] for item in page.get("Contents", [])]
    return keys


def _storage_options(server) -> dict:
    return {
        "aws_access_key_id": S3_KEY,
        "aws_secret_access_key": S3_SECRET,
        "aws_region": S3_REGION,
        "aws_endpoint": server["host_endpoint"],
        "allow_http": "true",
    }


def _dataset_uri(server, location: str) -> str:
    return f"s3://{server['bucket']}/{location}"


def test_seaweedfs_supports_conditional_put(seaweedfs_container):
    """The fixture probe ran; the probe key holds the first PUT."""
    body = seaweedfs_container["s3"].get_object(Bucket=BUCKET, Key=PROBE_KEY)["Body"].read()
    assert body == b"first"


def test_create_lance_table_in_bucket(lance_s3_client, seaweedfs_container):
    name = "s3_ext"
    _drop(lance_s3_client, name)
    try:
        lance_s3_client.execute(
            f"CREATE EXTERNAL TABLE {name} (id BIGINT) STORED AS LANCE LOCATION 'ext.lance'",
            admin=True,
        )
        assert _keys(seaweedfs_container, "ext.lance/_versions/"), "no Lance manifest in the bucket"
        assert lance_s3_client.count(f"SELECT * FROM {name}") == 0
    finally:
        _drop(lance_s3_client, name)


def test_insert_by_name_from_netcdf(lance_s3_client, seaweedfs_container):
    seaweedfs_container["s3"].upload_file(
        str(NETCDF_FIXTURE), BUCKET, f"nc_src/{NETCDF_FIXTURE.name}"
    )
    source, target = "s3_nc_src", "s3_nc_lance"
    # The reader drops the 2-D N_LEVELS variables and counts the DATE_TIME string dimension.
    columns = "LATITUDE, LONGITUDE, CYCLE_NUMBER"
    for name in (source, target):
        _drop(lance_s3_client, name)
    try:
        lance_s3_client.execute(
            f"CREATE EXTERNAL TABLE {source} STORED AS NC LOCATION 'nc_src/*.nc'", admin=True
        )
        lance_s3_client.execute(
            f"CREATE EXTERNAL TABLE {target} (id BIGINT) STORED AS LANCE "
            "LOCATION 'nc_target.lance'",
            admin=True,
        )
        # Read the rows: count(*) prunes the projection, and the nd scan then builds another grid.
        source_rows = sorted(lance_s3_client.sql_rows(f"SELECT {columns} FROM {source}")[1:])
        expected = len(source_rows)
        assert expected > 0

        lance_s3_client.execute(
            f"INSERT INTO {target} BY NAME SELECT {columns} FROM {source}", admin=True
        )

        assert lance_s3_client.count(f"SELECT * FROM {target}") == expected
        assert sorted(lance_s3_client.sql_rows(f"SELECT {columns} FROM {target}")[1:]) == source_rows
        # The NetCDF variables became Lance columns; the source has no id.
        assert lance_s3_client.count(f"SELECT {columns} FROM {target} WHERE id IS NULL") == expected

        # SELECT * also brings attribute columns such as TEMP.units; Lance stores them as TEMP_units.
        star_rows = len(lance_s3_client.sql_rows(f"SELECT * FROM {source}")) - 1
        assert star_rows > 0
        uri = _dataset_uri(seaweedfs_container, "nc_target.lance")
        options = _storage_options(seaweedfs_container)
        lance_s3_client.execute(f"INSERT INTO {target} BY NAME SELECT * FROM {source}", admin=True)

        names = lance.dataset(uri, storage_options=options).schema.names
        assert {"TEMP_units", "LATITUDE_units", "CYCLE_NUMBER_long_name"} <= set(names), names
        assert not [n for n in names if "." in n], names
        assert lance_s3_client.count(f"SELECT * FROM {target}") == expected + star_rows
        assert lance_s3_client.count(f'SELECT "TEMP_units" FROM {target}') == expected + star_rows

        # A second SELECT * matches the renamed columns and adds none.
        lance_s3_client.execute(f"INSERT INTO {target} BY NAME SELECT * FROM {source}", admin=True)
        assert lance.dataset(uri, storage_options=options).schema.names == names
        assert lance_s3_client.count(f"SELECT * FROM {target}") == expected + 2 * star_rows
    finally:
        for name in (target, source):
            _drop(lance_s3_client, name)


def test_delete_update_on_s3(lance_s3_client, seaweedfs_container):
    name = "s3_rw"
    _drop(lance_s3_client, name)
    try:
        lance_s3_client.execute(
            f"CREATE EXTERNAL TABLE {name} (id BIGINT, name VARCHAR) STORED AS LANCE "
            "LOCATION 'rw.lance'",
            admin=True,
        )
        assert _keys(seaweedfs_container, "rw.lance/_versions/")
        lance_s3_client.execute(
            f"INSERT INTO {name} VALUES (1, 'a'), (2, 'b'), (3, 'b')", admin=True
        )
        assert lance_s3_client.count(f"SELECT * FROM {name}") == 3

        lance_s3_client.execute(f"DELETE FROM {name} WHERE name = 'a'", admin=True)
        assert lance_s3_client.count(f"SELECT * FROM {name}") == 2

        lance_s3_client.execute(f"UPDATE {name} SET name = 'c' WHERE id = 3", admin=True)
        assert lance_s3_client.scalar(f"SELECT name FROM {name} WHERE id = 3") == "c"
    finally:
        _drop(lance_s3_client, name)


def test_drop_keeps_objects(lance_s3_client, seaweedfs_container):
    name = "s3_drop"
    _drop(lance_s3_client, name)
    try:
        lance_s3_client.execute(
            f"CREATE EXTERNAL TABLE {name} (id BIGINT) STORED AS LANCE LOCATION 'drop.lance'",
            admin=True,
        )
        lance_s3_client.execute(f"INSERT INTO {name} VALUES (1), (2)", admin=True)
        before = set(_keys(seaweedfs_container, "drop.lance/"))
        assert before

        lance_s3_client.execute(f"DROP TABLE {name}", admin=True)

        with pytest.raises(QueryError):
            lance_s3_client.sql_rows(f"SELECT * FROM {name}")
        after = set(_keys(seaweedfs_container, "drop.lance/"))
        assert before <= after, f"DROP TABLE removed objects: {sorted(before - after)}"
    finally:
        _drop(lance_s3_client, name)


def test_pylance_round_trip(lance_s3_client, seaweedfs_container):
    location = "pylance.lance"
    uri = _dataset_uri(seaweedfs_container, location)
    options = _storage_options(seaweedfs_container)
    lance.write_dataset(
        pa.table({"id": pa.array([1, 2, 3], pa.int64())}), uri, storage_options=options
    )

    name = "s3_pylance"
    _drop(lance_s3_client, name)
    try:
        lance_s3_client.execute(
            f"CREATE EXTERNAL TABLE {name} STORED AS LANCE LOCATION '{location}'", admin=True
        )
        assert lance_s3_client.count(f"SELECT * FROM {name}") == 3

        lance_s3_client.execute(f"INSERT INTO {name} VALUES (4), (5)", admin=True)

        assert lance.dataset(uri, storage_options=options).count_rows() == 5
    finally:
        _drop(lance_s3_client, name)


def test_two_beacons_append(
    request, lance_s3_client, beacon_image, docker_network, seaweedfs_container, tmp_path_factory
):
    other = _start_lance_s3_beacon(
        request,
        "beacon-integration-lance-s3-b",
        beacon_image,
        docker_network,
        seaweedfs_container,
        tmp_path_factory,
    )
    name, location = "s3_append", "append.lance"
    for client in (lance_s3_client, other):
        _drop(client, name)
    lance_s3_client.execute(
        f"CREATE EXTERNAL TABLE {name} (id BIGINT) STORED AS LANCE LOCATION '{location}'",
        admin=True,
    )
    # The dataset exists now, so the second Beacon takes its columns from it.
    other.execute(f"CREATE EXTERNAL TABLE {name} STORED AS LANCE LOCATION '{location}'", admin=True)

    errors: list[BaseException] = []
    start = threading.Barrier(2, timeout=60)

    def _append(client, first_id: int):
        try:
            start.wait()
            for batch in range(5):
                base = first_id + batch * 10
                values = ", ".join(f"({base + i})" for i in range(10))
                client.execute(f"INSERT INTO {name} VALUES {values}", admin=True)
        except BaseException as exc:  # noqa: BLE001 - reported in the main thread
            errors.append(exc)

    try:
        threads = [
            threading.Thread(target=_append, args=(lance_s3_client, 0)),
            threading.Thread(target=_append, args=(other, 1000)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=300)
        assert not any(thread.is_alive() for thread in threads), "an insert thread hung"
        assert not errors, errors

        uri = _dataset_uri(seaweedfs_container, location)
        dataset = lance.dataset(uri, storage_options=_storage_options(seaweedfs_container))
        assert dataset.count_rows() == 100
        assert lance_s3_client.count(f"SELECT DISTINCT id FROM {name}") == 100
        assert other.count(f"SELECT * FROM {name}") == 100
    finally:
        for client in (lance_s3_client, other):
            _drop(client, name)
