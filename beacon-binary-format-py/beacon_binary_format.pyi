from __future__ import annotations

from typing import Mapping, Any, Optional, Sequence, Union, TypedDict, Iterable, List

ArrayLike = Any
DimensionNames = Sequence[str]
ArrayWithDimensions = Mapping[str, Any]
ArrayValue = Union[ArrayLike, tuple[ArrayLike, DimensionNames], ArrayWithDimensions]
ArrayMap = Mapping[str, ArrayValue]

class ArrayPayload(TypedDict, total=False):
    data: ArrayLike
    dims: Sequence[str]
    shape: Sequence[int]
    mask: ArrayLike

class CollectionMetadata(TypedDict):
    collection_byte_size: int
    collection_num_elements: int
    partition_count: int
    library_version: str

EntryPayload = Mapping[str, ArrayPayload]

class Collection:
    """Logical Beacon collection that can host multiple partitions."""
    def __init__(
        self,
        base_dir: str,
        collection_path: str,
        storage_options: Optional[Mapping[str, Any]] = ...,
        filesystem: Optional[Any] = ...,
    ) -> None: ...
    def create_partition(
        self,
        partition_name: Optional[str] = ..., 
        max_group_size: Optional[int] = ...,
    ) -> PartitionBuilder:
        """Create a new partition; names default to `partition-{n}` when omitted."""
    def library_version(self) -> str:
        """Return the Beacon Binary Format library version baked into the writer."""

class PartitionBuilder:
    """Builder that gathers per-entry Arrow arrays before flushing to disk."""
    def write_entry(self, entry_key: str, arrays: ArrayMap) -> None:
        """Write a logical row using bare NumPy arrays, named-dimension tuples, or masked arrays."""
    def finish(self) -> None:
        """Flush buffered data and attach the partition to its parent collection."""

class CollectionBuilder:
    """Backwards-compatible façade that opens one partition immediately."""
    def __init__(
        self,
        base_dir: str,
        collection_path: str,
        partition_name: Optional[str] = ..., 
        max_group_size: Optional[int] = ...,
        storage_options: Optional[Mapping[str, Any]] = ...,
        filesystem: Optional[Any] = ...,
    ) -> None: ...
    def write_entry(self, entry_key: str, arrays: ArrayMap) -> None:
        """Shortcut to the single-partition workflow retained for legacy scripts."""
    def finish(self) -> None:
        """Finalize the eagerly-created partition and release internal resources."""
    def library_version(self) -> str:
        """Mirror of :meth:`Collection.library_version`."""

class CollectionReader:
    """Read Beacon Binary Format collections back into NumPy payloads."""

    def __init__(
        self,
        base_dir: str,
        collection_path: str,
        cache_bytes: Optional[int] = ...,
        storage_options: Optional[Mapping[str, Any]] = ...,
        filesystem: Optional[Any] = ...,
    ) -> None: ...

    def metadata(self) -> CollectionMetadata: ...
    def partition_names(self) -> List[str]: ...
    def open_partition(self, partition_name: str) -> PartitionReader: ...

class PartitionReader:
    """Read logical entries inside a specific collection partition."""

    def name(self) -> str: ...
    def num_entries(self) -> int: ...
    def read_entries(
        self,
        projection: Optional[Iterable[str]] = ...,
        max_concurrent_reads: Optional[int] = ...,
    ) -> List[EntryPayload]: ...


def crate_version() -> str:
    """Return the underlying Rust crate version bundled with beacon_binary_format."""
