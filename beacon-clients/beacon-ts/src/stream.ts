/** Isomorphic helpers for consuming a `fetch` `Response` body as bytes. */

/**
 * Adapts a `Response` body to an `AsyncIterable<Uint8Array>`.
 *
 * Node's `ReadableStream` is async-iterable, but browsers' is not (yet), so we
 * drive the reader manually for portability. An early stop cancels the body.
 */
export function responseByteStream(res: Response): AsyncGenerator<Uint8Array> {
  return openByteStream(res).chunks;
}

/**
 * Like {@link responseByteStream}, plus a `cancel()` that closes the body at any
 * time. A `return()` on the generator waits for a pending read; `cancel()` does not.
 */
export function openByteStream(res: Response): {
  chunks: AsyncGenerator<Uint8Array>;
  cancel: () => void;
} {
  const reader = res.body?.getReader();
  let settled = false;
  const cancel = () => {
    if (settled || !reader) return;
    settled = true;
    // Also resolves a pending read, so the generator ends.
    reader.cancel().catch(() => {});
  };

  async function* chunks(): AsyncGenerator<Uint8Array> {
    if (!reader) {
      // Some runtimes omit a streaming body; fall back to a single chunk.
      yield new Uint8Array(await res.arrayBuffer());
      return;
    }
    try {
      for (;;) {
        const chunk = await reader.read();
        if (chunk.done) {
          settled = true;
          break;
        }
        if (chunk.value) yield chunk.value;
      }
    } finally {
      // An early stop must close the body, or the request and the server query stay open.
      cancel();
      reader.releaseLock();
    }
  }

  return { chunks: chunks(), cancel };
}
