import {
  CompressionType,
  compressionRegistry,
  tableFromArrays,
  tableToIPC,
} from "apache-arrow";
import { describe, expect, it, vi } from "vitest";

import { BeaconClient, parseCsv } from "../src/index.js";

describe("parseCsv", () => {
  it("parses a simple table", () => {
    expect(parseCsv("a,b\n1,2\n3,4\n")).toEqual([
      { a: "1", b: "2" },
      { a: "3", b: "4" },
    ]);
  });

  it("handles quoted fields with commas, newlines, and escaped quotes", () => {
    const csv = 'name,note\n"Doe, Jane","say ""hi""\nthere"\n';
    expect(parseCsv(csv)).toEqual([{ name: "Doe, Jane", note: 'say "hi"\nthere' }]);
  });

  it("returns [] for empty input and header-only input", () => {
    expect(parseCsv("")).toEqual([]);
    expect(parseCsv("a,b\n")).toEqual([]);
  });

  it("tolerates a missing trailing newline and short rows", () => {
    expect(parseCsv("a,b,c\n1,2")).toEqual([{ a: "1", b: "2", c: "" }]);
  });
});

describe("zstd codec registration", () => {
  it("registers a ZSTD decode codec with apache-arrow on the Arrow path", async () => {
    const ipc = tableToIPC(tableFromArrays({ n: Int32Array.from([1]) }), "stream");
    const fn = vi.fn(async () => new Response(ipc)) as unknown as typeof fetch;
    const client = new BeaconClient({ url: "http://beacon.test", fetch: fn });

    await client.query("SELECT 1");

    const codec = compressionRegistry.get(CompressionType.ZSTD);
    expect(typeof codec?.decode).toBe("function");
  });
});
