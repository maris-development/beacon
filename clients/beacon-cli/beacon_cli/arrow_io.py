"""Arrow IPC decoding for query results.

In-memory query results come back from the server as a zstd-compressed Arrow IPC
*stream*, which ``pyarrow`` decodes natively. The stream is consumed
incrementally: :func:`collect_ipc_stream` reads record batches one at a time and
can stop early once a row budget is reached, so rendering the first N rows of a
huge result does not download the whole thing. A side-effecting statement
(DDL/DML) returns no IPC frames at all, which we surface as an empty table.
"""

from __future__ import annotations

import io
from collections.abc import Iterable

import pyarrow as pa


class QueryResult:
    """A decoded in-terminal query result.

    ``truncated`` is True when the fetch was stopped early because the row budget
    was reached — i.e. the server has more rows than were retrieved.
    """

    def __init__(
        self,
        table: pa.Table,
        query_id: str | None,
        elapsed: float,
        truncated: bool = False,
    ):
        self.table = table
        self.query_id = query_id
        self.elapsed = elapsed
        self.truncated = truncated

    @property
    def num_rows(self) -> int:
        return self.table.num_rows

    @property
    def is_empty(self) -> bool:
        """True for a side-effecting statement (no rows and no columns)."""
        return self.table.num_rows == 0 and self.table.num_columns == 0


class _ChunkStream(io.RawIOBase):
    """Adapt an iterator of byte chunks (e.g. ``httpx`` ``iter_bytes``) into a
    readable, non-seekable binary stream that pyarrow can pull from on demand."""

    def __init__(self, chunks: Iterable[bytes]):
        self._it = iter(chunks)
        self._buf = bytearray()

    def readable(self) -> bool:
        return True

    def readinto(self, b) -> int:  # noqa: ANN001 - matches RawIOBase signature
        need = len(b)
        while len(self._buf) < need:
            try:
                self._buf += next(self._it)
            except StopIteration:
                break
        n = min(need, len(self._buf))
        b[:n] = self._buf[:n]
        del self._buf[:n]
        return n


def collect_ipc_stream(
    chunks: Iterable[bytes], row_limit: int | None
) -> tuple[pa.Table, bool]:
    """Read an Arrow IPC stream from ``chunks`` into a table.

    Stops after the first batch that pushes the row count past ``row_limit``
    (returning ``truncated=True``); a ``row_limit`` of ``None`` or negative reads
    the whole stream. A stream with no schema message (a side-effecting
    statement) yields an empty 0-column table.
    """
    source = io.BufferedReader(_ChunkStream(chunks))
    try:
        reader = pa.ipc.open_stream(source)
    except pa.ArrowInvalid:
        return pa.table({}), False

    unlimited = row_limit is None or row_limit < 0
    batches: list[pa.RecordBatch] = []
    total = 0
    truncated = False
    while True:
        try:
            batch = reader.read_next_batch()
        except StopIteration:
            break
        batches.append(batch)
        total += batch.num_rows
        if not unlimited and total > row_limit:
            truncated = True
            break

    if batches:
        table = pa.Table.from_batches(batches, reader.schema)
    else:
        table = reader.schema.empty_table()
    return table, truncated


def decode_ipc_stream(content: bytes) -> pa.Table:
    """Decode a fully-buffered Arrow IPC stream (zstd-compressed) into a table."""
    table, _ = collect_ipc_stream([content] if content else [], None)
    return table
