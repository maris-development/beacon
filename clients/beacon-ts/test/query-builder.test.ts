import { describe, expect, it, vi } from "vitest";
import { tableFromArrays, tableToIPC } from "apache-arrow";

import { BeaconClient, QueryBuilder, and, col, column, func } from "../src/index.js";

/** Build the DSL without a client. */
function qb() {
  return new QueryBuilder();
}

describe("QueryBuilder.build", () => {
  it("emits a minimal select", () => {
    expect(qb().select("TEMP", "PSAL").build()).toEqual({ select: ["TEMP", "PSAL"] });
  });

  it("maps from-helpers to the DSL source shape", () => {
    expect(qb().select("a").fromParquet(["x.parquet"]).build().from).toEqual({
      parquet: { paths: ["x.parquet"] },
    });
    expect(qb().select("a").fromTable("t").build().from).toBe("t");
    expect(qb().select("a").fromCsv("y.csv", ";").build().from).toEqual({
      csv: { paths: ["y.csv"], delimiter: ";" },
    });
  });

  it("translates a single where predicate", () => {
    const built = qb()
      .select("TEMP")
      .where((x) => x.depth.gte(0))
      .build();
    expect(built.filter).toEqual({ column: "depth", gt_eq: 0 });
  });

  it("combines chained comparisons via and()", () => {
    const built = qb()
      .select("TEMP")
      .where((x) => x.depth.gte(0).and(x.depth.lte(100)))
      .build();
    expect(built.filter).toEqual({
      and: [
        { column: "depth", gt_eq: 0 },
        { column: "depth", lt_eq: 100 },
      ],
    });
  });

  it("ANDs multiple where() calls together", () => {
    const built = qb()
      .select("a")
      .where(col("x").eq(1))
      .where(col("y").neq("z"))
      .build();
    expect(built.filter).toEqual({
      and: [
        { column: "x", eq: 1 },
        { column: "y", neq: "z" },
      ],
    });
  });

  it("supports between, isNull, and free-standing and()", () => {
    const built = qb()
      .select("a")
      .where(and(col("d").between(0, 10), col("flag").isNull()))
      .build();
    expect(built.filter).toEqual({
      and: [
        { column: "d", gt_eq: 0, lt_eq: 10 },
        { is_null: { column: "flag" } },
      ],
    });
  });

  it("builds ordering, paging, distinct, and aliased/function projections", () => {
    const built = qb()
      .select(column("TEMP", "t"), func("avg", ["PSAL"], "mean"))
      .orderBy("TEMP")
      .thenByDescending("PSAL")
      .skip(5)
      .take(10)
      .distinct(["TEMP"])
      .build();
    expect(built).toEqual({
      select: [
        { column: "TEMP", alias: "t" },
        { function: "avg", args: ["PSAL"], alias: "mean" },
      ],
      sort_by: [{ Asc: "TEMP" }, { Desc: "PSAL" }],
      offset: 5,
      limit: 10,
      distinct: { on: ["TEMP"], select: ["TEMP"] },
    });
  });

  it("throws if executed without a bound client", async () => {
    await expect(qb().select("a").toArray()).rejects.toThrow(/not bound to a client/);
  });
});

describe("BeaconClient builder integration", () => {
  it("first() requests limit 1 without mutating the builder", async () => {
    const ipc = tableToIPC(tableFromArrays({ TEMP: Float64Array.from([1]) }), "stream");
    const bodies: unknown[] = [];
    const fn = vi.fn(async (_url: string | URL | Request, init?: RequestInit) => {
      bodies.push(JSON.parse(init?.body as string));
      return new Response(ipc);
    }) as unknown as typeof fetch;
    const client = new BeaconClient({ url: "http://beacon.test", fetch: fn });

    const builder = client.from("ctd").select("TEMP");
    await builder.first();
    await builder.execute();

    expect(bodies[0]).toEqual({ select: ["TEMP"], from: "ctd", limit: 1 }); // first() → limit 1
    expect(bodies[1]).toEqual({ select: ["TEMP"], from: "ctd" }); // builder unchanged → no limit
    expect(builder.build().limit).toBeUndefined();
  });

  it("from().where().execute() posts the built DSL and decodes rows", async () => {
    const ipc = tableToIPC(tableFromArrays({ TEMP: Float64Array.from([4.2]) }), "stream");
    const calls: RequestInit[] = [];
    const fn = vi.fn(async (_url: string | URL | Request, init?: RequestInit) => {
      calls.push(init ?? {});
      return new Response(ipc);
    }) as unknown as typeof fetch;
    const client = new BeaconClient({ url: "http://beacon.test", fetch: fn });

    const { rows } = await client
      .from({ netcdf: { paths: ["a.nc"] } })
      .select("TEMP")
      .where((x) => x.depth.gte(0))
      .take(1)
      .execute();

    expect(rows).toEqual([{ TEMP: 4.2 }]);
    expect(JSON.parse(calls[0]!.body as string)).toEqual({
      select: ["TEMP"],
      filter: { column: "depth", gt_eq: 0 },
      from: { netcdf: { paths: ["a.nc"] } },
      limit: 1,
    });
  });
});
