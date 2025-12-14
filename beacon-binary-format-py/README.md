# Beacon Binary Format Python Bindings

This crate exposes the Beacon Binary Format collection writer to Python via [PyO3](https://pyo3.rs/) and ships as a native extension built with [maturin](https://www.maturin.rs/).

## Prerequisites

- Rust toolchain that matches the workspace (`rustup` suggested)
- Python 3.9+ with development headers available
- `pip install maturin`

If your default `python3` differs from the interpreter you want to target, set `PYO3_PYTHON` to its absolute path before building:

```shell
export PYO3_PYTHON=$(which python3.11)
```

## Local install

Run the following inside `beacon-binary-format-py/`:

```shell
maturin develop --release
```

This builds the Rust crate in release mode and installs the resulting Python module (`beacon_binary_format`) into your active virtual environment. Re-run the command whenever you change the Rust sources.

## Usage

The module now exposes a `Collection` object that can host any number of partitions. Each partition is managed by a `PartitionBuilder` which mirrors the original single-partition workflow:

```python
from pathlib import Path
import numpy as np
from beacon_binary_format import Collection

tmp_dir = Path("/tmp/beacon")
collection = Collection(str(tmp_dir), "example")

partition = collection.create_partition("partition-0")
partition.write_entry(
	"row-0",
	{
		"salinity": np.array([34.5, 34.6], dtype=np.float32),
		"temperature": np.array([9.2, 9.0], dtype=np.float32),
	},
)
partition.finish()

# Materialize a second partition later on the same collection
second = collection.create_partition()
second.write_entry("row-1", {"flags": np.array([True, False])})
second.finish()

print(collection.library_version())

### Named dimensions

NumPy arrays default to synthetic dimension names (`dim0`, `dim1`, ...). You can override them per array by wrapping the data in either a tuple or a tiny mapping when calling `write_entry`:

```python
partition.write_entry(
	"row-2",
	{
		# tuple form -> (<array-like>, <sequence-of-dimension-names>)
		"salinity_grid": (np.ones((2, 3), dtype=np.float32), ["depth", "lat", "lon"]),
		# mapping form -> keys `data` + `dims`
		"temperature_grid": {
			"data": np.ones((2, 3), dtype=np.float32),
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

### Reading collections back into NumPy

A matching `CollectionReader` can reconstruct logical entries as dictionaries of NumPy payloads. Every field is returned as `{"data": <ndarray|masked array>, "dims": [...], "shape": [...]}` so dimension metadata survives round-trips.

```python
from beacon_binary_format import CollectionReader

reader = CollectionReader(str(tmp_dir), "example")
partition = reader.open_partition("partition-0")
entries = partition.read_entries()

for entry in entries:
    print(entry["__entry_key"]["data"], entry["temperature"]["dims"])
```

Optional `projection=["temperature", "salinity"]` narrows the arrays fetched from disk, and `max_concurrent_reads` lets you tune object-store fanout.

### Object stores and `fsspec`-style options

`Collection`, `CollectionBuilder`, and `CollectionReader` each accept an optional `storage_options` mapping along with richer `base_dir` URIs. Supplying "s3://bucket/prefix" switches the backend to AWS S3 via the Rust `object_store` crate, while ordinary filesystem paths (or `file://` URLs) continue to use `LocalFileSystem`.

`storage_options` mirrors the values you would normally pass to [`fsspec`](https://filesystem-spec.readthedocs.io/), which means existing credential dictionaries can be reused verbatim:

```python
from beacon_binary_format import Collection

collection = Collection(
    base_dir="s3://beacon-dev/datasets",
    collection_path="planning/profiles",
    storage_options={
        "key": "minio-access-key",
        "secret": "minio-secret-key",
        "region_name": "us-east-1",
        "client_kwargs": {
            "endpoint_url": "http://localhost:9000",
            "allow_http": True,
        },
    },
)
```

The same configuration works for readers and builders, and you can even forward `fs.storage_options` from an existing `fsspec` filesystem for consistency across libraries.

Prefer to pass the filesystem object itself? Provide it via the `filesystem` keyword alongside a path scoped to that instance:

```python
import numpy as np
import fsspec
from beacon_binary_format import Collection, CollectionReader

fs = fsspec.filesystem(
	"s3",
	key="minio-access-key",
	secret="minio-secret-key",
	client_kwargs={"endpoint_url": "http://localhost:9000", "allow_http": True},
)

collection = Collection(
	base_dir="beacon-dev/datasets",  # interpreted relative to `fs`
	collection_path="planning/profiles",
	filesystem=fs,
)
partition = collection.create_partition("p0")
partition.write_entry("row-0", {"temperature": np.array([9.2], dtype=np.float32)})
partition.finish()

reader = CollectionReader(
	base_dir="beacon-dev/datasets",
	collection_path="planning/profiles",
	filesystem=fs,
)
entries = reader.open_partition("p0").read_entries()
print(entries[0]["temperature"]["data"])
```

When the supplied path lacks a scheme, the constructor infers it from `filesystem.protocol`, so `s3`, `gcs`, `abfs`, etc. remain fully supported. Explicit `storage_options` still take precedence if you need to override anything pulled from the filesystem instance.

The legacy `CollectionBuilder` class remains available for scripts that only ever deal with a single partition, but the `Collection`/`PartitionBuilder` pair is the recommended interface moving forward. Shipping `.pyi` stubs and `py.typed` ensures editors and static type checkers understand the API surface.

````
