# Beacon Binary Format Python Bindings

This crate exposes the Beacon Binary Format to Python via [PyO3](https://pyo3.rs/) and ships as a native extension built with [maturin](https://www.maturin.rs/).

## Usage

### Writing

The module now exposes a `Collection` object that can host any number of partitions. Each partition is managed by a `PartitionBuilder` which mirrors the original single-partition workflow:

```python
from pathlib import Path
import numpy as np
from beacon_binary_format import Collection, ObjectStore

tmp_dir = Path("/tmp/beacon")
store = ObjectStore.local(str(tmp_dir))
collection = Collection(store, "example")

partition = collection.create_partition("partition-0")
partition.write_entry(
	"dataset-0",
	{
		"salinity": np.array([34.5, 34.6], dtype=np.float32),
		"temperature": np.array([9.2, 9.0], dtype=np.float32),
	},
)
partition.finish()

# Materialize a second partition later on the same collection
second = collection.create_partition()
second.write_entry("dataset-1", {"flags": np.array([True, False])})
second.finish()

print(collection.library_version())

```

### Reading

Use `CollectionReader` to open a collection and read entries back as dictionaries of NumPy payloads. Every field is returned as `{"data": <ndarray|masked array>, "dims": [...], "shape": [...]}` so dimension metadata survives round-trips.

```python
from beacon_binary_format import CollectionReader

store = ObjectStore.local(str(tmp_dir))
reader = CollectionReader(store, "example")

print(reader.partition_names())

partition = reader.open_partition("partition-0")
entries = partition.read_entries(
	projection=["temperature"],
	max_concurrent_reads=32,
)

for entry in entries:
	print(entry["__entry_key"]["data"], entry["temperature"]["dims"])
```

#### Entry selection

`entry_selection` lets you provide an explicit boolean selection mask (length must equal `partition.num_entries()`). This is applied in addition to any persisted logical deletes.

```python
partition = reader.open_partition("partition-0")
selection = [(i % 2) == 0 for i in range(partition.num_entries())]
entries = partition.read_entries(entry_selection=selection)
```

### Named dimensions

NumPy arrays default to synthetic dimension names (`dim0`, `dim1`, ...). You can override them per array by wrapping the data in either a tuple or a tiny mapping when calling `write_entry`:

```python

partition.write_entry(
	"dataset-2",
	{
		# tuple form -> (<array-like>, <sequence-of-dimension-names>)
		"salinity_grid": (np.ones((2, 3, 4), dtype=np.float32), ["depth", "lat", "lon"]),
		# mapping form -> keys `data` + `dims`
		"temperature_grid": {
			"data": np.ones((2, 3, 4), dtype=np.float32),
			"dims": ["depth", "lat", "lon"],
		},
	},
)

```

The number of names must match the rank of the array (or be exactly one for scalars). Any mismatch raises a `ValueError`, which keeps the partition builder in a valid state for additional writes.

### NumPy masked arrays

Masked arrays created via `numpy.ma.array` automatically translate their mask into the Arrow validity bitmap used by Beacon. You only need to pass the masked array instance; there is no additional API surface:

```python

masked = np.ma.array(
	[1.0, 2.0, 3.0, 4.0],
	mask=[False, True, False, True],
)
partition.write_entry("row-0", {"masked": masked})

```

Any position masked in NumPy is written as a null value in the corresponding Arrow array.

### Object stores

The bindings expose an `ObjectStore` helper that can be configured up-front and then passed into writers/readers.

Local filesystem:

```python
from beacon_binary_format import ObjectStore, Collection

store = ObjectStore.local("/tmp/beacon")
collection = Collection(store, "example")
```

S3 / S3-compatible:

```python
from beacon_binary_format import ObjectStore, Collection

store = ObjectStore.s3(
	bucket="beacon-dev",
	prefix="datasets",
	region="us-east-1",
	endpoint_url="http://localhost:9000",  # optional
	access_key_id="minio-access-key",      # optional
	secret_access_key="minio-secret-key",  # optional
	allow_http=True,                         # optional
)

collection = Collection(store, "planning/profiles")
```

## File format layout

On disk, a Beacon collection is stored under the `collection_path` you pass to `Collection`/`CollectionReader`. The root contains a `bbf.json` metadata file and a `partitions/` directory.

```text
<collection_root>/<collection_path>/
	bbf.json
	partitions/
		<partition_name>/
			partition_blob.bbb
			resolution.json
			pruning_index.bbpi        (optional)
			entry_mask.bbem           (optional)
```

- `bbf.json` tracks collection-level schema/metadata and the set of partitions.
- `partition_blob.bbb` stores Arrow IPC payloads for arrays concatenated into a single blob.
- `pruning_index.bbpi` stores concatenated Arrow IPC pruning indices (one slice per array that has a pruning index).
- `entry_mask.bbem` stores an Arrow IPC boolean array used for logical deletes.
- `resolution.json` maps content hashes (array hash, pruning-index hash, entry-mask hash) to `{offset,size}` slices inside the corresponding blob, enabling efficient object-store range reads.

The legacy `CollectionBuilder` class remains available for scripts that only ever deal with a single partition, but the `Collection`/`PartitionBuilder` pair is the recommended interface moving forward. Shipping `.pyi` stubs and `py.typed` ensures editors and static type checkers understand the API surface.

## Development

### Prerequisites

- Rust toolchain that matches the workspace (`rustup` suggested)
- Python 3.9+ with development headers available
- `pip install maturin`

If your default `python3` differs from the interpreter you want to target, set `PYO3_PYTHON` to its absolute path before building:

```shell
export PYO3_PYTHON=$(which python3.11)
```

### Local install

Run the following inside `beacon-binary-format-py/`:

```shell
maturin develop --release
```

This builds the Rust crate in release mode and installs the resulting Python module (`beacon_binary_format`) into your active virtual environment. Re-run the command whenever you change the Rust sources.
