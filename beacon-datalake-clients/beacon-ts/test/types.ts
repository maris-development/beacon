/**
 * Compile-time tests for the query builder's typing. Checked by
 * `npm run typecheck` (this file is in tsconfig's `include`). The
 * `@ts-expect-error` lines fail the build if they ever stop erroring.
 */

import { BeaconClient } from "../src/index.js";

const beacon = new BeaconClient({ url: "http://x" });

interface Ctd {
  TEMP: number;
  depth: number;
  station: string;
}

// --- where() predicate ---

// Untyped: any column name is accepted (escape hatch, no completion).
void beacon.from("ctd").select("TEMP").where((x) => x.anything.gte(0));

// Typed: known columns resolve and complete, and chain with and()/or().
void beacon.from<Ctd>("ctd").select("TEMP").where((x) => x.TEMP.gte(0).and(x.depth.lte(100)));

// Typed: comparison values are checked against the column's type.
void beacon.from<Ctd>("ctd").where((x) => x.station.eq("BTL01"));

// Typed: a misspelled column is a compile error.
// @ts-expect-error - `tmep` is not a column of Ctd
void beacon.from<Ctd>("ctd").where((x) => x.tmep.gte(0));

// Typed: a wrong-typed comparison value is a compile error.
// @ts-expect-error - TEMP is a number, not a string
void beacon.from<Ctd>("ctd").where((x) => x.TEMP.gte("hot"));

// --- select() / orderBy() / distinct() column completion ---

// Typed columns complete and are accepted.
void beacon.from<Ctd>("ctd").select("TEMP", "depth").orderBy("TEMP").thenByDescending("depth");

// Any string is still allowed (for "*", aliases, expressions) — no error.
void beacon.from<Ctd>("ctd").select("*", "TEMP AS t").orderBy("some_alias");
