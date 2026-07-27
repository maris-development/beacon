# @beacon/client

Isomorphic TypeScript SDK for managing and querying a [Beacon](../../README.md)
instance. Runs in Node.js (18+) and the browser, built on the global `fetch`.

## Install

```bash
npm install @beacon/client
```

`apache-arrow` (the result decoder) and `fzstd` (a tiny pure-JS zstd
decompressor) are regular dependencies, installed automatically — no extra
setup. Beacon's results are zstd-compressed Arrow IPC; the SDK registers an
fzstd-backed zstd codec with apache-arrow so they decode out of the box.

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

## Query builder (EF Core / LINQ-style)

A fluent builder produces the JSON DSL for you. JavaScript can't overload
operators, so predicates use methods (`col("d").gte(0)`) rather than `d >= 0`;
otherwise it reads like EF Core's method syntax.

```ts
import { column, func } from "@beacon/client";

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
const table = await beacon.from("ctd").select("TEMP").toArrow();  // Arrow Table
for await (const batch of beacon.from("ctd").select("TEMP").stream()) { /* RecordBatch */ }

// Inspect or reuse the DSL without running it:
const dsl = beacon.from("ctd").select("TEMP").where(col("d").gte(0)).build();
```

Predicate building blocks (importable): `col(name)` →
`.eq/.neq/.gt/.gte/.lt/.lte/.between/.isNull/.isNotNull`, combined with
`.and(...)`, `.or(...)` or the free functions `and(...)`, `or(...)`. There is no
`not` (Beacon's DSL has none) — use `neq` / `isNotNull`. Projection helpers:
`column(name, alias?)`, `func(name, args, alias?)`, `literal(value, alias?)`.

### Optional typed columns and completion

Typing is **opt-in**. By default the builder is fully untyped — no type
parameter, no annotations, every column name and comparison value accepted:

```ts
// Works as-is, no types required:
const { rows } = await beacon
  .from("ctd")
  .select("TEMP")
  .where((x) => x.depth.gte(0))   // `x` is inferred; no annotation needed
  .execute();
```

*If* you want completion and checking, pass a row type — then `where`, `select`,
`orderBy`/`thenBy`, and `distinct` all complete column names, and `where`
comparison values are checked against each column's type:

```ts
interface Ctd { TEMP: number; depth: number; station: string }

beacon.from<Ctd>("ctd")
  .select("TEMP", "depth")        // ✅ completes TEMP | depth | station
  .where((x) => x.depth.gte(0))   // ✅ `x.` completes columns; gte() wants a number
  .where((x) => x.dpeth.gte(0))   // ❌ no column `dpeth`
  .where((x) => x.TEMP.gte("hi")) // ❌ TEMP is a number, not a string
  .orderByDescending("TEMP");     // ✅ completes columns
```

`where(x => …)` is strict — a misspelled column is a compile error. `select`,
`orderBy`, and `distinct` complete known columns but still accept any string, so
`"*"`, aliases, and expressions keep working. Without a type argument the builder
is fully untyped (any column name, any comparison value) — the escape hatch for
ad-hoc queries.

### Authenticated (admin) operations

Supplying credentials elevates every request to super-user, enabling DDL/DML over
`query()` and the `admin.*` endpoints:

```ts
const beacon = new BeaconClient({
  url: "http://localhost:5001",
  username: "beacon-admin",
  password: "beacon-password",
});

await beacon.query("CREATE EXTERNAL TABLE ... STORED AS PARQUET ...");

const crawlers = await beacon.admin.listCrawlers();
await beacon.admin.runCrawler("my-crawler");
await beacon.admin.createExternalTable({ /* spec */ });
```

## Result formats

Beacon's default `/api/query` response is a **zstd-compressed Arrow IPC stream**,
which the SDK decodes via `apache-arrow` + the auto-registered `fzstd` zstd
codec. There is **no JSON output format**; CSV is the dependency-light
alternative.

| Method | Server output | Returns |
| --- | --- | --- |
| `query()` | default Arrow stream | `{ rows, queryId, table }` — plain objects + the Arrow `table` |
| `query(q, { format: "csv" })` | `csv` | `{ rows, queryId }` — all values are strings |
| `queryArrow()` | default Arrow stream | an `apache-arrow` `Table` |
| `queryStream()` | default Arrow stream | `AsyncGenerator<RecordBatch>` — streamed, unbuffered |
| `queryCsv()` | `csv` | `{ rows, queryId }` — string-valued rows |
| `queryRaw(query, format?)` | any (or default stream) | the raw `fetch` `Response` |

Use `queryRaw` to stream large results or write a materialized format (CSV,
Parquet, NetCDF, GeoParquet, ODV, …) to disk:

```ts
import { createWriteStream } from "node:fs";
import { Readable } from "node:stream";

const res = await beacon.queryRaw("SELECT * FROM big_table", "parquet");
await new Promise((resolve, reject) => {
  Readable.fromWeb(res.body!).pipe(createWriteStream("out.parquet")).on("finish", resolve).on("error", reject);
});
```

## Typed schemas from your server

The hand-written types in [`src/types.ts`](src/types.ts) cover the query DSL and
common shapes. For exact request/response types generated from a specific
server's OpenAPI document:

```bash
BEACON_URL=http://localhost:5001 npm run codegen
```

This writes `src/generated/schema.d.ts` from that server's `/openapi.json`.

## Development

```bash
npm install
npm run typecheck
npm test
npm run build
```

## API surface

- **Query builder:** `from`, `select` → fluent `QueryBuilder` (see above)
- **Query:** `query`, `queryArrow`, `queryStream`, `queryCsv`, `queryRaw`, `parseQuery`, `explainQuery`, `queryMetrics`
- **Tables:** `tables`, `tablesWithSchema`, `tableSchema`, `tableConfig`, `defaultTable`, `defaultTableSchema`
- **Datasets:** `datasets`, `datasetSchema`, `totalDatasets`
- **Functions / info:** `functions`, `tableFunctions`, `info`, `health`
- **Admin (`beacon.admin`):** `check`, `listCrawlers`, `createCrawler`, `getCrawler`, `runCrawler`, `dropCrawler`, `createExternalTable`
