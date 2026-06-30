/**
 * Fetches the OpenAPI document from a running Beacon server and writes it to
 * `openapi.json` for `openapi-typescript` to consume.
 *
 * Usage: `BEACON_URL=http://localhost:5001 npm run codegen`
 */

import { writeFile } from "node:fs/promises";

const url = process.env.BEACON_URL ?? "http://localhost:5001";
const specUrl = `${url.replace(/\/+$/, "")}/openapi.json`;

const res = await fetch(specUrl);
if (!res.ok) {
  console.error(`Failed to fetch ${specUrl}: ${res.status} ${res.statusText}`);
  process.exit(1);
}

const spec = await res.text();
await writeFile("openapi.json", spec);
console.log(`Wrote openapi.json from ${specUrl}`);
