import numpy as np
import fsspec
import pytest

from beacon_binary_format import Collection, CollectionReader


def test_roundtrip_readback(tmp_path):
    collection = Collection(str(tmp_path), "example")
    partition = collection.create_partition("p0")
    partition.write_entry(
        "entry-0",
        {
            "a": np.array([1, 2, 3], dtype=np.int32),
            "b": (np.array([[1.0, 2.0]], dtype=np.float64), ["y", "x"]),
        },
    )
    partition.finish()

    reader = CollectionReader(str(tmp_path), "example")
    partition_reader = reader.open_partition("p0")
    entries = partition_reader.read_entries()
    assert len(entries) == 1
    entry = entries[0]
    assert entry["__entry_key"]["data"] == "entry-0"
    assert np.array_equal(entry["a"]["data"], np.array([1, 2, 3], dtype=np.int32))
    assert entry["a"]["dims"] == ["dim0"]
    assert entry["b"]["dims"] == ["y", "x"]
    assert np.array_equal(
        entry["b"]["data"],
        np.array([[1.0, 2.0]], dtype=np.float64),
    )


def test_storage_options_file_scheme(tmp_path):
    base_uri = f"file://{tmp_path}"
    storage_options = {
        "region_name": "us-east-1",
        "client_kwargs": {
            "endpoint_url": "http://localhost:9000",
            "allow_http": True,
        },
    }
    collection = Collection(base_uri, "with-storage", storage_options=storage_options)
    partition = collection.create_partition("sp0")
    partition.write_entry("entry-0", {"a": np.array([1], dtype=np.int32)})
    partition.finish()

    reader = CollectionReader(base_uri, "with-storage", storage_options=storage_options)
    entries = reader.open_partition("sp0").read_entries()
    assert entries[0]["__entry_key"]["data"] == "entry-0"
    assert np.array_equal(entries[0]["a"]["data"], np.array([1], dtype=np.int32))


def test_filesystem_instance_roundtrip(tmp_path):
    fs = fsspec.filesystem("file")
    base_path = str(tmp_path)

    collection = Collection(base_path, "fs-example", filesystem=fs)
    partition = collection.create_partition("p0")
    partition.write_entry("entry-0", {"value": np.array([42], dtype=np.int64)})
    partition.finish()

    reader = CollectionReader(base_path, "fs-example", filesystem=fs)
    entries = reader.open_partition("p0").read_entries()
    assert entries[0]["__entry_key"]["data"] == "entry-0"
    assert np.array_equal(entries[0]["value"]["data"], np.array([42], dtype=np.int64))
