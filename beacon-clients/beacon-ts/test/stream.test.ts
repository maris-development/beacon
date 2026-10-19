import { tableFromArrays, tableToIPC } from "apache-arrow";
import { describe, expect, it, vi } from "vitest";

import { BeaconClient, responseByteStream } from "../src/index.js";

/**
 * One Arrow IPC batch, then only the continuation marker of the next message.
 * apache-arrow yields a batch once it reads that marker; it then waits for more.
 */
const firstBatch = () => {
  const ipc = tableToIPC(tableFromArrays({ n: Int32Array.from([1, 2]) }), "stream");
  return ipc.subarray(0, ipc.length - 4);
};

/**
 * A fetch whose body sends `chunk`, then never ends by itself. Like native fetch,
 * an abort of the request signal errors the body.
 */
function openEndedFetch(chunk: Uint8Array) {
  const cancel = vi.fn();
  const fetchImpl = ((_url: string | URL | Request, init?: RequestInit) => {
    const signal = init?.signal;
    if (signal?.aborted) return Promise.reject(signal.reason);
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(chunk);
        signal?.addEventListener("abort", () => controller.error(signal.reason), { once: true });
      },
      pull: () => new Promise<void>(() => {}),
      cancel,
    });
    return Promise.resolve(new Response(body));
  }) as unknown as typeof fetch;
  return { fetchImpl, cancel };
}

describe("responseByteStream", () => {
  it("cancels the body when the consumer stops early", async () => {
    const { fetchImpl, cancel } = openEndedFetch(new Uint8Array([1, 2, 3]));
    const res = await fetchImpl("http://beacon.test");
    for await (const chunk of responseByteStream(res)) {
      expect(chunk).toEqual(new Uint8Array([1, 2, 3]));
      break;
    }
    expect(cancel).toHaveBeenCalledOnce();
  });
});

describe("queryBatches cancellation", () => {
  it("ends the batch stream when the signal aborts after the headers", async () => {
    const { fetchImpl } = openEndedFetch(firstBatch());
    const client = new BeaconClient({ url: "http://beacon.test", fetch: fetchImpl });
    const ac = new AbortController();
    const { batches } = await client.queryBatches("SELECT n", ac.signal);

    const rows: unknown[] = [];
    const consume = (async () => {
      for await (const batch of batches) {
        rows.push(...batch.toArray().map((r) => r.toJSON()));
        ac.abort();
      }
    })();

    await expect(consume).rejects.toMatchObject({ name: "AbortError" });
    expect(rows).toEqual([{ n: 1 }, { n: 2 }]);
  });

  it("cancels the body when the consumer breaks after the first batch", async () => {
    const { fetchImpl, cancel } = openEndedFetch(firstBatch());
    const client = new BeaconClient({ url: "http://beacon.test", fetch: fetchImpl });
    const { batches } = await client.queryBatches("SELECT n");

    for await (const batch of batches) {
      expect(batch.numRows).toBe(2);
      break;
    }
    expect(cancel).toHaveBeenCalledOnce();
  });

  it("rejects at once when the signal is already aborted", async () => {
    const { fetchImpl } = openEndedFetch(firstBatch());
    const client = new BeaconClient({ url: "http://beacon.test", fetch: fetchImpl });
    const ac = new AbortController();
    ac.abort();
    await expect(client.queryBatches("SELECT n", ac.signal)).rejects.toMatchObject({
      name: "AbortError",
    });
  });
});
