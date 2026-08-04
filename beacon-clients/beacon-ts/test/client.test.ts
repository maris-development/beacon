import { tableFromArrays, tableToIPC } from "apache-arrow";
import { describe, expect, it, vi } from "vitest";

import { ApiError, BeaconClient, basicAuthHeader } from "../src/index.js";

/** Builds a fetch stub that records calls and returns a fresh `response` each time. */
function stubFetch(makeResponse: () => Response) {
  const calls: { url: string; init: RequestInit }[] = [];
  const fn = vi.fn(async (url: string | URL | Request, init?: RequestInit) => {
    calls.push({ url: String(url), init: init ?? {} });
    return makeResponse();
  });
  return { fn: fn as unknown as typeof fetch, calls };
}

function jsonResponse(body: unknown, init: ResponseInit = {}): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { "content-type": "application/json", ...init.headers },
    ...init,
  });
}

/** An uncompressed Arrow IPC stream the way the server (minus zstd) would emit. */
function arrowResponse(columns: Record<string, unknown[]>): Response {
  const ipc = tableToIPC(tableFromArrays(columns), "stream");
  return new Response(ipc, {
    status: 200,
    headers: { "x-beacon-query-id": "q-1" },
  });
}

describe("basicAuthHeader", () => {
  it("base64-encodes credentials", () => {
    expect(basicAuthHeader("admin", "secret")).toBe("Basic YWRtaW46c2VjcmV0");
  });
});

describe("BeaconClient.query (Arrow path)", () => {
  it("posts the default body and decodes the Arrow stream into rows", async () => {
    const { fn, calls } = stubFetch(() =>
      arrowResponse({ n: Int32Array.from([1, 2]), label: ["a", "b"] }),
    );
    const client = new BeaconClient({ url: "http://beacon.test", fetch: fn });

    const result = await client.query("SELECT 1");

    expect(result.rows).toEqual([
      { n: 1, label: "a" },
      { n: 2, label: "b" },
    ]);
    expect(result.queryId).toBe("q-1");
    expect(result.table?.numRows).toBe(2);
    // No output format => default (compressed) Arrow stream requested.
    const body = JSON.parse(calls[0]!.init.body as string);
    expect(body).toEqual({ sql: "SELECT 1" });
  });

  it("queryArrow returns the decoded Table", async () => {
    const { fn } = stubFetch(() => arrowResponse({ n: Int32Array.from([7]) }));
    const client = new BeaconClient({ url: "http://beacon.test", fetch: fn });
    const table = await client.queryArrow("SELECT 7");
    expect(table.numRows).toBe(1);
  });
});

describe("BeaconClient.query (CSV fallback / queryCsv)", () => {
  it("requests CSV and parses rows when format: 'csv'", async () => {
    const { fn, calls } = stubFetch(
      () =>
        new Response('a,b\n1,"x,y"\n2,"line\nbreak"\n', {
          status: 200,
          headers: { "x-beacon-query-id": "csv-1" },
        }),
    );
    const client = new BeaconClient({ url: "http://beacon.test", fetch: fn });

    const result = await client.query("SELECT 1", { format: "csv" });

    expect(result.rows).toEqual([
      { a: "1", b: "x,y" },
      { a: "2", b: "line\nbreak" },
    ]);
    expect(result.queryId).toBe("csv-1");
    expect(result.table).toBeUndefined();
    expect(JSON.parse(calls[0]!.init.body as string)).toEqual({
      sql: "SELECT 1",
      output: { format: "csv" },
    });
  });

  it("returns no rows for an empty (DDL) CSV response", async () => {
    const { fn } = stubFetch(() => new Response("", { status: 200 }));
    const client = new BeaconClient({ url: "http://beacon.test", fetch: fn });
    const result = await client.query("CREATE TABLE t (a INT)", { format: "csv" });
    expect(result.rows).toEqual([]);
  });
});

describe("BeaconClient structured queries", () => {
  it("sends a structured DSL body verbatim", async () => {
    const { fn, calls } = stubFetch(() => arrowResponse({ temp: Float64Array.from([1.5]) }));
    const client = new BeaconClient({ url: "http://beacon.test", fetch: fn });
    await client.query({ select: ["temp"], limit: 10 });
    expect(JSON.parse(calls[0]!.init.body as string)).toEqual({ select: ["temp"], limit: 10 });
  });
});

describe("BeaconClient error handling", () => {
  it("throws ApiError with the unwrapped message on non-2xx", async () => {
    const { fn } = stubFetch(() => new Response(JSON.stringify("bad query"), { status: 400 }));
    const client = new BeaconClient({ url: "http://beacon.test", fetch: fn });
    await expect(client.query("SELECT bogus")).rejects.toMatchObject({
      constructor: ApiError,
      status: 400,
      body: "bad query",
    });
  });
});

describe("BeaconClient auth", () => {
  it("attaches a basic-auth header when credentials are configured", async () => {
    const { fn, calls } = stubFetch(() => jsonResponse([]));
    const client = new BeaconClient({
      url: "http://beacon.test",
      username: "admin",
      password: "secret",
      fetch: fn,
    });
    await client.tables();
    const headers = new Headers(calls[0]!.init.headers);
    expect(headers.get("authorization")).toBe(basicAuthHeader("admin", "secret"));
  });
});

describe("BeaconClient metadata paths", () => {
  it("encodes query params for table-schema", async () => {
    const { fn, calls } = stubFetch(() => jsonResponse({}));
    const client = new BeaconClient({ url: "http://beacon.test", fetch: fn });
    await client.tableSchema("my table");
    expect(calls[0]!.url).toBe("http://beacon.test/api/table-schema?table_name=my+table");
  });

  it("respects basePath", async () => {
    const { fn, calls } = stubFetch(() => jsonResponse([]));
    const client = new BeaconClient({ url: "http://beacon.test", basePath: "/beacon", fetch: fn });
    await client.tables();
    expect(calls[0]!.url).toBe("http://beacon.test/beacon/api/tables");
  });
});
