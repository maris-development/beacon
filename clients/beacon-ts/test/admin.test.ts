import { describe, expect, it, vi } from "vitest";

import { BeaconClient } from "../src/index.js";

/** A fetch stub that routes by URL and records every call. */
function routerFetch() {
  const calls: { method: string; url: string; body: unknown }[] = [];
  const fn = vi.fn(async (url: string | URL | Request, init?: RequestInit) => {
    const u = String(url);
    const method = (init?.method ?? "GET").toUpperCase();
    calls.push({ method, url: u, body: init?.body });

    if (u.includes("/datasets/upload/initiate")) {
      return new Response(JSON.stringify({ upload_id: "u1", part_size: 4 }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    }
    if (u.includes("/datasets/upload/part")) {
      return new Response(null, { status: 204 });
    }
    if (u.includes("/datasets/upload/complete")) {
      return new Response(JSON.stringify({ path: "a/big.parquet", size: 10 }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    }
    // single-shot upload
    return new Response(JSON.stringify({ path: "a/small.parquet", size: 3 }), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  });
  return { fn: fn as unknown as typeof fetch, calls };
}

function adminClient(fetchFn: typeof fetch) {
  return new BeaconClient({
    url: "http://beacon.test",
    username: "admin",
    password: "pw",
    fetch: fetchFn,
  });
}

describe("AdminClient.uploadDataset", () => {
  it("uses a single request for files at or below the threshold", async () => {
    const { fn, calls } = routerFetch();
    const client = adminClient(fn);

    const res = await client.admin.uploadDataset("a/small.parquet", new Uint8Array(3));

    expect(res).toEqual({ path: "a/small.parquet", size: 3 });
    expect(calls).toHaveLength(1);
    expect(calls[0]!.method).toBe("POST");
    expect(calls[0]!.url).toContain("/api/admin/datasets/upload?");
    expect(calls[0]!.url).toContain("path=a%2Fsmall.parquet");
  });

  it("switches to the chunked protocol above the threshold", async () => {
    const { fn, calls } = routerFetch();
    const client = adminClient(fn);
    const progress: number[] = [];

    const res = await client.admin.uploadDataset("a/big.parquet", new Uint8Array(10), {
      thresholdBytes: 4,
      onProgress: (p) => progress.push(p.uploaded),
    });

    expect(res).toEqual({ path: "a/big.parquet", size: 10 });

    // initiate, 3 parts (4 + 4 + 2 bytes), complete.
    const kinds = calls.map((c) => `${c.method} ${new URL(c.url).pathname}`);
    expect(kinds).toEqual([
      "POST /api/admin/datasets/upload/initiate",
      "PUT /api/admin/datasets/upload/part",
      "PUT /api/admin/datasets/upload/part",
      "PUT /api/admin/datasets/upload/part",
      "POST /api/admin/datasets/upload/complete",
    ]);
    // Parts are numbered in order.
    const parts = calls
      .filter((c) => c.url.includes("/upload/part"))
      .map((c) => new URL(c.url).searchParams.get("part_number"));
    expect(parts).toEqual(["1", "2", "3"]);
    // Progress reports cumulative bytes.
    expect(progress).toEqual([4, 8, 10]);
  });

  it("aborts the session if a part fails", async () => {
    const calls: string[] = [];
    const fn = vi.fn(async (url: string | URL | Request, init?: RequestInit) => {
      const u = String(url);
      const method = (init?.method ?? "GET").toUpperCase();
      calls.push(`${method} ${new URL(u).pathname}`);
      if (u.includes("/upload/initiate")) {
        return new Response(JSON.stringify({ upload_id: "u1", part_size: 4 }), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }
      if (u.includes("/upload/part")) {
        return new Response("boom", { status: 500 });
      }
      return new Response(null, { status: 204 });
    });
    const client = adminClient(fn as unknown as typeof fetch);

    await expect(
      client.admin.uploadDataset("a/big.parquet", new Uint8Array(10), {
        thresholdBytes: 4,
        partRetries: 0,
      }),
    ).rejects.toThrow();

    // Initiate, the failed part, then a best-effort DELETE to abort the session.
    expect(calls).toContain("DELETE /api/admin/datasets/upload");
  });
});
