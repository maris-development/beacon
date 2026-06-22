/** Isomorphic helpers for consuming a `fetch` `Response` body as bytes. */

/**
 * Adapts a `Response` body to an `AsyncIterable<Uint8Array>`.
 *
 * Node's `ReadableStream` is async-iterable, but browsers' is not (yet), so we
 * drive the reader manually for portability.
 */
export async function* responseByteStream(res: Response): AsyncGenerator<Uint8Array> {
  if (!res.body) {
    // Some runtimes omit a streaming body; fall back to a single chunk.
    yield new Uint8Array(await res.arrayBuffer());
    return;
  }
  const reader = res.body.getReader();
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value) yield value;
    }
  } finally {
    reader.releaseLock();
  }
}
