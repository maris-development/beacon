import { describe, expect, it } from "vitest";

import { BeaconClient, TimeoutError, basicAuthHeader } from "../src/index.js";

describe("basicAuthHeader", () => {
  it("base64-encodes the UTF-8 bytes of credentials (non-Latin1 safe)", () => {
    const expected = "Basic " + Buffer.from("admin:pä$$wörd", "utf-8").toString("base64");
    expect(basicAuthHeader("admin", "pä$$wörd")).toBe(expected);
  });
});

/** A fetch that never resolves on its own; it rejects only when the signal aborts. */
const hangingFetch = ((_url: string | URL | Request, init?: RequestInit) =>
  new Promise<Response>((_resolve, reject) => {
    const signal = init?.signal;
    const fail = () => reject(signal?.reason ?? new DOMException("Aborted", "AbortError"));
    if (signal?.aborted) fail();
    else signal?.addEventListener("abort", fail, { once: true });
  })) as unknown as typeof fetch;

describe("abort classification", () => {
  it("raises TimeoutError when the request times out", async () => {
    const client = new BeaconClient({ url: "http://beacon.test", fetch: hangingFetch, timeoutMs: 10 });
    await expect(client.query("SELECT 1", { format: "csv" })).rejects.toBeInstanceOf(TimeoutError);
  });

  it("preserves the caller's AbortError on external cancellation (not a TimeoutError)", async () => {
    const client = new BeaconClient({ url: "http://beacon.test", fetch: hangingFetch, timeoutMs: 0 });
    const ac = new AbortController();
    const promise = client.query("SELECT 1", { format: "csv", signal: ac.signal });
    ac.abort();
    await expect(promise).rejects.toMatchObject({ name: "AbortError" });
    await expect(promise).rejects.not.toBeInstanceOf(TimeoutError);
  });
});
