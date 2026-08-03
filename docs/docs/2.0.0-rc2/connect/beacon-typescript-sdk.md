---
description: "@beacon/client is a TypeScript SDK for Beacon. Run SQL or the JSON query DSL from Node.js or the browser. Build queries with a fluent builder."
---

# Beacon TypeScript SDK

`@beacon/client` is a TypeScript and JavaScript SDK for a Beacon server. It runs
in **Node.js 18 and later** and in the **browser**. It uses the global `fetch`.
It runs SQL and the [JSON query DSL](/docs/2.0.0-rc2/api/querying/json). It
decodes the zstd-compressed Arrow IPC results of Beacon into plain JS row
objects. It also gives a fluent query builder in the style of EF Core and LINQ.

The SDK lives in the Beacon repository, under
[`beacon-datalake-clients/beacon-ts`](https://github.com/maris-development/beacon/tree/main/beacon-datalake-clients/beacon-ts).

## Install

```bash
npm install @beacon/client
```

The SDK has two normal dependencies. `apache-arrow` decodes the result. `fzstd`
is a small zstd decompressor in pure JavaScript. Both install automatically. You
set up nothing. Beacon returns zstd-compressed Arrow IPC. The SDK registers an
fzstd codec with `apache-arrow`. The results therefore decode at once.

## Quick start

```ts
import { BeaconClient } from "@beacon/client";

const beacon = new BeaconClient({ url: "http://localhost:5001" });

// Run SQL and get plain JS row objects (decoded from the Arrow stream).
const { rows, queryId, table } = await beacon.query(
  "SELECT TEMP, PSAL FROM read_netcdf(['file.nc']) LIMIT 5",
);

// Or use the structured JSON query DSL.
const result = await beacon.query({
  select: [{ column: "TEMP", alias: "temperature" }, "PSAL"],
  filter: { column: "depth", gt_eq: 0, lt_eq: 100 },
  limit: 100,
});

// Inspect the catalog.
const tables = await beacon.tables();
const info = await beacon.info();
```

Every query returns `{ rows, queryId, table }`. `rows` holds the decoded JS
objects. `queryId` is the `x-beacon-query-id` of the server. Use it to fetch the
query metrics. `table` is the [Arrow](https://arrow.apache.org/) `Table`. It
gives zero-copy access.

## Query builder (EF Core / LINQ-style)

The fluent builder writes the JSON DSL for you. JavaScript cannot overload an
operator. A predicate therefore uses a method, as in `col("d").gte(0)`. It does
not use `d >= 0`. The rest reads like the method syntax of EF Core.

```ts
import { column, func, col } from "@beacon/client";

const { rows } = await beacon
  .from({ netcdf: { paths: ["argo.nc"] } })       // or .fromNetcdf("argo.nc"), .fromTable("t")
  .select("TEMP", column("PSAL", "salinity"), func("avg", ["TEMP"], "mean"))
  .where((x) => x.depth.gte(0).and(x.depth.lte(100)))  // x.<col> → comparison methods
  .orderByDescending("TEMP")
  .skip(0)
  .take(100)
  .execute();                                     // → { rows, queryId, table }

// EF-style terminals:
const list = await beacon.from("ctd").select("*").toArray();      // just rows
const one = await beacon.select("TEMP").where(col("id").eq(7)).first();

// Other outputs from the same builder:
const arrow = await beacon.from("ctd").select("TEMP").toArrow();  // Arrow Table
for await (const batch of beacon.from("ctd").select("TEMP").stream()) {
  /* RecordBatch */
}

// Inspect or reuse the DSL without running it:
const dsl = beacon.from("ctd").select("TEMP").where(col("d").gte(0)).build();
```

## Admin operations

Give the client admin Basic auth credentials. It can then manage the data lake.
It registers external tables. It manages crawlers. It also drops tables:

```ts
const beacon = new BeaconClient({
  url: "http://localhost:5001",
  username: "beacon-admin",
  password: "beacon-password",
});
```

The client sends the credentials on every request. The server checks them with
`GET /api/admin/check`. The [REST API reference](/docs/2.0.0-rc2/api/) lists every
admin endpoint.

## Use in a browser

The SDK works with a bundler. It runs in the browser without a change. The
default CORS policy of Beacon is `*`, and it allows the `Authorization` header. A
browser application can therefore call the server directly. The
[Admin Web UI](/docs/2.0.0-rc2/connect/web-admin-ui) uses this SDK only. It is a
good example.
