---
description: "@beacon/client is an isomorphic TypeScript SDK for Beacon — run SQL or the JSON query DSL from Node.js or the browser, decode the Arrow result stream to plain JS objects, and build queries with an EF Core-style fluent builder."
---

# Beacon TypeScript SDK

`@beacon/client` is an isomorphic TypeScript/JavaScript SDK for a running Beacon
server. It runs in **Node.js (18+)** and the **browser**, built on the global
`fetch`. It runs SQL or the [JSON query DSL](/docs/1.8.0/api/querying/json),
decodes Beacon's zstd-compressed Arrow IPC results into plain JS row objects, and
ships a fluent, EF Core / LINQ-style query builder.

It lives in the Beacon repository under [`clients/beacon-ts`](https://github.com/maris-development/beacon/tree/main/clients/beacon-ts).

## Install

```bash
npm install @beacon/client
```

`apache-arrow` (the result decoder) and `fzstd` (a tiny pure-JS zstd
decompressor) are regular dependencies and install automatically — no extra
setup. Beacon's results are zstd-compressed Arrow IPC; the SDK registers an
fzstd-backed zstd codec with `apache-arrow` so they decode out of the box.

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

Every query returns `{ rows, queryId, table }`: `rows` are decoded JS objects,
`queryId` is the server's `x-beacon-query-id` (use it to fetch query metrics),
and `table` is the underlying [Arrow](https://arrow.apache.org/) `Table` for
zero-copy access.

## Query builder (EF Core / LINQ-style)

A fluent builder produces the JSON DSL for you. JavaScript can't overload
operators, so predicates use methods (`col("d").gte(0)`) rather than `d >= 0`;
otherwise it reads like EF Core's method syntax.

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

When constructed with admin Basic-auth credentials, the client can manage the
data lake — register external tables, manage crawlers, and drop tables:

```ts
const beacon = new BeaconClient({
  url: "http://localhost:5001",
  username: "beacon-admin",
  password: "beacon-password",
});
```

The credentials are sent on every request and are verified server-side against
`GET /api/admin/check`. See the [REST API reference](/docs/1.8.0/api/) for the
full set of admin endpoints.

## Browser usage

The SDK is bundler-friendly and runs unmodified in the browser. Beacon's default
CORS policy (`*`, with the `Authorization` header allowed) lets a browser app
call the server directly. The bundled [Admin Web UI](/docs/1.8.0/connect/web-admin-ui)
is built entirely on this SDK and is a good reference for browser usage.
