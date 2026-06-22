/**
 * Bridge to `apache-arrow` (a required dependency) with zstd support.
 *
 * Beacon's default in-memory query response is a *zstd-compressed* Arrow IPC
 * stream. apache-arrow 21+ decodes compressed IPC, but only once a codec is
 * registered for the compression type (it bundles no zstd implementation). We
 * register one backed by `fzstd` — a tiny, pure-JS, isomorphic zstd
 * decompressor — the first time the decoder is loaded.
 *
 * The imports are dynamic so callers that only hit metadata endpoints don't pull
 * Arrow into their bundle until the first query.
 */

/** Minimal structural view of an apache-arrow `Table`, to avoid a type dependency. */
export interface ArrowTable {
  readonly numRows: number;
  readonly numCols: number;
  toArray(): unknown[];
}

/** Minimal structural view of an apache-arrow `RecordBatch`. */
export interface ArrowRecordBatch {
  readonly numRows: number;
  toArray(): unknown[];
}

interface ArrowDecoder {
  /** Decodes a complete Arrow IPC payload (stream or file) into a Table. */
  tableFromIPC(bytes: Uint8Array): ArrowTable;
  /** Opens a streaming reader over a chunked Arrow IPC source. */
  readStream(source: AsyncIterable<Uint8Array>): Promise<AsyncIterable<ArrowRecordBatch>>;
}

// Shape of the bits of the apache-arrow module we touch.
interface ArrowModule {
  tableFromIPC(bytes: Uint8Array): ArrowTable;
  RecordBatchReader: {
    from(source: unknown): Promise<AsyncIterable<ArrowRecordBatch>> | AsyncIterable<ArrowRecordBatch>;
  };
  CompressionType: { ZSTD: number };
  compressionRegistry: {
    get(type: number): { decode?: (b: Uint8Array) => Uint8Array } | null;
    set(type: number, codec: { decode?: (b: Uint8Array) => Uint8Array }): void;
  };
}

let loaded: ArrowDecoder | undefined;

/** Loads apache-arrow (and registers the zstd codec on first use). */
export async function getArrowDecoder(): Promise<ArrowDecoder> {
  if (loaded) return loaded;
  const arrow = (await import("apache-arrow")) as unknown as ArrowModule;
  await registerZstd(arrow);
  loaded = {
    tableFromIPC: (bytes) => arrow.tableFromIPC(bytes),
    readStream: async (source) => arrow.RecordBatchReader.from(source),
  };
  return loaded;
}

/** Registers an fzstd-backed ZSTD decode codec with apache-arrow (idempotent). */
async function registerZstd(arrow: ArrowModule): Promise<void> {
  const zstdType = arrow.CompressionType.ZSTD;
  if (arrow.compressionRegistry.get(zstdType)?.decode) return;
  const { decompress } = (await import("fzstd")) as { decompress: (b: Uint8Array) => Uint8Array };
  arrow.compressionRegistry.set(zstdType, { decode: (b) => decompress(b) });
}

/** Converts an Arrow Table to plain JS row objects. */
export function rowsFromTable<T>(table: ArrowTable): T[] {
  return table.toArray().map(toPlainRow) as T[];
}

function toPlainRow(row: unknown): unknown {
  if (row && typeof (row as { toJSON?: unknown }).toJSON === "function") {
    return (row as { toJSON: () => unknown }).toJSON();
  }
  return { ...(row as object) };
}
