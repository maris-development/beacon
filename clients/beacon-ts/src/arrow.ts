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
  /** The batch's Arrow schema (carries field names and types). */
  readonly schema?: unknown;
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
  tableFromArrays(arrays: Record<string, unknown>): unknown;
  tableToIPC(table: unknown, variant: string): Uint8Array;
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
  patchBufferAlignment(arrow);
  loaded = {
    tableFromIPC: (bytes) => arrow.tableFromIPC(bytes),
    readStream: async (source) => arrow.RecordBatchReader.from(source),
  };
  return loaded;
}

const ALIGN_PATCH_FLAG = "__beaconAligned8";

/** Copies a byte buffer to a fresh, 8-byte-aligned backing buffer when needed. */
function align8(buf: Uint8Array): Uint8Array {
  if (buf.byteOffset % 8 === 0) return buf;
  const copy = new Uint8Array(buf.byteLength);
  copy.set(buf);
  return copy;
}

/**
 * Works around an apache-arrow decoding bug. When a record batch is compressed
 * but an individual buffer is stored *uncompressed* — the `-1` length prefix
 * Beacon emits for small/incompressible buffers (e.g. a 10-row Float64 column) —
 * the reader returns a sub-view of the message body whose `byteOffset` is not a
 * multiple of 8. Building a typed-array view (e.g. `Float64Array`) over it then
 * throws "start offset of Float64Array should be a multiple of 8".
 *
 * We wrap the reader's internal `_decompressBuffers` to realign the buffers it
 * produces. The patch reaches the internal prototype via a throwaway reader and
 * is fully guarded: if Arrow's internals change it silently no-ops, leaving the
 * default behavior (compressed buffers already decode correctly).
 */
function patchBufferAlignment(arrow: ArrowModule): void {
  try {
    const ipc = arrow.tableToIPC(arrow.tableFromArrays({ x: Uint8Array.from([0]) }), "stream");
    const reader = arrow.RecordBatchReader.from(ipc) as unknown as { _impl?: object };
    let proto: object | null = reader._impl ? Object.getPrototypeOf(reader._impl) : null;
    while (proto && !Object.prototype.hasOwnProperty.call(proto, "_decompressBuffers")) {
      proto = Object.getPrototypeOf(proto);
    }
    const target = proto as
      | (Record<string, unknown> & {
          _decompressBuffers?: (...args: unknown[]) => { decommpressedBody?: Uint8Array[] };
        })
      | null;
    if (!target || target[ALIGN_PATCH_FLAG] || typeof target._decompressBuffers !== "function") {
      return;
    }
    const original = target._decompressBuffers;
    target._decompressBuffers = function (this: unknown, ...args: unknown[]) {
      const result = original.apply(this, args);
      if (result && Array.isArray(result.decommpressedBody)) {
        result.decommpressedBody = result.decommpressedBody.map(align8);
      }
      return result;
    };
    target[ALIGN_PATCH_FLAG] = true;
  } catch {
    // Arrow internals changed — leave default behavior in place.
  }
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

/** Converts a single Arrow RecordBatch to plain JS row objects. */
export function rowsFromBatch<T>(batch: ArrowRecordBatch): T[] {
  return batch.toArray().map(toPlainRow) as T[];
}

function toPlainRow(row: unknown): unknown {
  if (row && typeof (row as { toJSON?: unknown }).toJSON === "function") {
    return (row as { toJSON: () => unknown }).toJSON();
  }
  return { ...(row as object) };
}
