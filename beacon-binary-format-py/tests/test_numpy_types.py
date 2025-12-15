import numpy as np
import pytest

import beacon_binary_format as bbf


def test_collection_supports_multiple_partitions(tmp_path):
    collection = bbf.Collection(str(tmp_path), "collection")
    partition = collection.create_partition("partition-0")

    arrays = {
        "bools": np.array([True, False, True], dtype=np.bool_),
        "int8s": np.array([-8, 9], dtype=np.int8),
        "int16s": np.array([-1000, 1000], dtype=np.int16),
        "int32s": np.array([-1, 2, 3], dtype=np.int32),
        "int64s": np.array([1, 2, 3], dtype=np.int64),
        "uint8s": np.array([1, 255], dtype=np.uint8),
        "uint16s": np.array([1, 500], dtype=np.uint16),
        "uint32s": np.array([1, 2, 3], dtype=np.uint32),
        "uint64s": np.array([1, 2, 3], dtype=np.uint64),
        "float32s": np.array([1.5, 2.5], dtype=np.float32),
        "float64s": np.array([1.5, 2.5], dtype=np.float64),
        "unicode_values": np.array(["alpha", "beta"], dtype="U10"),
        "binary_values": np.array([b"alpha", b"beta"], dtype="S5"),
        "datetimes": np.array([
            "2020-01-01T00:00:00",
            "2020-01-02T12:34:56",
        ], dtype="datetime64[ms]"),
    }

    partition.write_entry("row-0", arrays)
    partition.finish()

    second = collection.create_partition()
    second_arrays = {
        "bools": np.array([False, True, False], dtype=np.bool_),
        "int8s": np.array([1, 2], dtype=np.int8),
        "int16s": np.array([500, -500], dtype=np.int16),
        "int32s": np.array([4, 5, 6], dtype=np.int32),
        "int64s": np.array([4, 5, 6], dtype=np.int64),
        "uint8s": np.array([5, 6], dtype=np.uint8),
        "uint16s": np.array([600, 700], dtype=np.uint16),
        "uint32s": np.array([4, 5, 6], dtype=np.uint32),
        "uint64s": np.array([4, 5, 6], dtype=np.uint64),
        "float32s": np.array([3.5, 4.5], dtype=np.float32),
        "float64s": np.array([3.5, 4.5], dtype=np.float64),
        "unicode_values": np.array(["gamma", "delta"], dtype="U10"),
        "binary_values": np.array([b"gamma", b"delta"], dtype="S5"),
        "datetimes": np.array([
            "2020-01-03T00:00:00",
            "2020-01-04T12:34:56",
        ], dtype="datetime64[ms]"),
    }
    second.write_entry("row-1", second_arrays)
    second.finish()

    version = collection.library_version()
    assert isinstance(version, str)
    assert version


def test_numpy_arrays_support_named_dimensions(tmp_path):
    collection = bbf.Collection(str(tmp_path), "dims")
    partition = collection.create_partition("partition-dims")

    grid = np.ones((2, 3, 4), dtype=np.float32)

    partition.write_entry(
        "row-0",
        {
            "grid": (grid, ["depth", "lat", "lon"]),
        },
    )

    partition.write_entry(
        "row-1",
        {
            "grid": {
                "data": grid,
                "dims": ["depth", "lat", "lon"],
            }
        },
    )

    with pytest.raises(ValueError):
        partition.write_entry("row-2", {"grid": (grid, ["only_depth"])})

    with pytest.raises(ValueError):
        partition.write_entry("row-3", {"grid": {"data": grid, "dims": ["depth", 1]}})

    partition.finish()


def test_numpy_masked_arrays_respect_nulls(tmp_path):
    collection = bbf.Collection(str(tmp_path), "masked")
    partition = collection.create_partition("partition-masked")

    masked_floats = np.ma.array(
        np.array([1.0, 2.0, 3.0, 4.0], dtype=np.float32),
        mask=[False, True, False, True],
    )
    masked_unicode = np.ma.array(
        np.array(["alpha", "beta", "gamma"], dtype="U10"),
        mask=[False, False, True],
    )

    partition.write_entry(
        "row-0",
        {
            "masked_floats": masked_floats,
            "masked_unicode": masked_unicode,
        },
    )

    partition.finish()
