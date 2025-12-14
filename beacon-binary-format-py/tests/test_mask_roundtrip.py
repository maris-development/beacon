import numpy as np

from beacon_binary_format import Collection, CollectionReader


def test_masked_values_roundtrip(tmp_path):
    collection = Collection(str(tmp_path), "masked")
    partition = collection.create_partition("p0")
    masked = np.ma.array([1.0, 2.0, 3.0], mask=[False, True, False])
    partition.write_entry("entry-0", {"masked": masked})
    partition.finish()

    reader = CollectionReader(str(tmp_path), "masked")
    partition_reader = reader.open_partition("p0")
    entries = partition_reader.read_entries()
    entry = entries[0]
    masked_result = entry["masked"]["data"]
    assert masked_result.mask.tolist() == [False, True, False]
    np.testing.assert_allclose(masked_result.data, np.array([1.0, 0.0, 3.0]))
