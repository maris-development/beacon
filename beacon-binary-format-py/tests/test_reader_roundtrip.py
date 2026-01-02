import numpy as np
import pytest

from beacon_binary_format import Collection, CollectionReader, ObjectStore


def test_roundtrip_readback(tmp_path):
    store = ObjectStore.local(str(tmp_path))
    collection = Collection(store, "example")
    partition = collection.create_partition("p0")
    partition.write_entry(
        "entry-0",
        {
            "a": np.array([1, 2, 3], dtype=np.int32),
            "b": (np.array([[1.0, 2.0]], dtype=np.float64), ["y", "x"]),
        },
    )
    partition.finish()

    reader = CollectionReader(store, "example")
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


def test_object_store_local_roundtrip(tmp_path):
    store = ObjectStore.local(str(tmp_path))
    collection = Collection(store, "with-storage")
    partition = collection.create_partition("sp0")
    partition.write_entry("entry-0", {"a": np.array([1], dtype=np.int32)})
    partition.finish()

    reader = CollectionReader(store, "with-storage")
    entries = reader.open_partition("sp0").read_entries()
    assert entries[0]["__entry_key"]["data"] == "entry-0"
    assert np.array_equal(entries[0]["a"]["data"], np.array([1], dtype=np.int32))


def test_object_store_local_roundtrip_second_collection(tmp_path):
    store = ObjectStore.local(str(tmp_path))
    collection = Collection(store, "fs-example")
    partition = collection.create_partition("p0")
    partition.write_entry("entry-0", {"value": np.array([42], dtype=np.int64)})
    partition.finish()

    reader = CollectionReader(store, "fs-example")
    entries = reader.open_partition("p0").read_entries()
    assert entries[0]["__entry_key"]["data"] == "entry-0"
    assert np.array_equal(entries[0]["value"]["data"], np.array([42], dtype=np.int64))
