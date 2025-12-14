import numpy as np
import pytest

from beacon_bbf import Collection, CollectionReader


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
