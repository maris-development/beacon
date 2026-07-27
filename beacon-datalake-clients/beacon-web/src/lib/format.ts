/** Display helpers for query rows and Arrow values in the results grid. */

import type { Row } from "@beacon/client";

/** Derives ordered column names from the decoded rows (Arrow column order). */
export function columnsOf(rows: Row[]): string[] {
  const seen = new Set<string>();
  for (const row of rows) for (const key of Object.keys(row)) seen.add(key);
  return [...seen];
}

/** Formats an arbitrary cell value (BigInt, Date, typed-array, object) as text. */
export function formatCell(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "bigint") return value.toString();
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "object") {
    try {
      return JSON.stringify(value, (_k, v) => (typeof v === "bigint" ? v.toString() : v));
    } catch {
      return String(value);
    }
  }
  return String(value);
}

/**
 * Maps each Arrow `Timestamp` column to its unit (SECOND/MILLISECOND/…), read
 * from the decoded table's schema. apache-arrow returns timestamp values as raw
 * numbers, so we need the type to render them as dates. Defensive: returns an
 * empty map if the table or its schema is unavailable.
 */
export function timestampColumns(table: unknown): Map<string, string> {
  const map = new Map<string, string>();
  const fields = (table as { schema?: { fields?: unknown[] } } | undefined)?.schema?.fields;
  if (!Array.isArray(fields)) return map;
  for (const f of fields) {
    const field = f as { name?: unknown; type?: unknown };
    const match = /^Timestamp<(\w+)>/.exec(String(field.type ?? ""));
    if (match && typeof field.name === "string") map.set(field.name, match[1]);
  }
  return map;
}

const TS_UNIT_TO_MS: Record<string, number> = {
  SECOND: 1000,
  MILLISECOND: 1,
  MICROSECOND: 1 / 1000,
  NANOSECOND: 1 / 1_000_000,
};

/** Formats a raw Arrow timestamp value (in `unit`) as an ISO-8601 UTC string. */
export function formatTimestamp(value: unknown, unit: string): string {
  if (value === null || value === undefined) return "";
  const raw = typeof value === "bigint" ? Number(value) : Number(value);
  if (!Number.isFinite(raw)) return formatCell(value);
  const date = new Date(raw * (TS_UNIT_TO_MS[unit] ?? 1));
  return Number.isNaN(date.getTime()) ? formatCell(value) : date.toISOString();
}

/** Compact byte-size label, e.g. 1536 -> "1.5 KB". */
export function formatBytes(bytes: number): string {
  if (!Number.isFinite(bytes)) return "—";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let n = bytes;
  let i = 0;
  while (n >= 1024 && i < units.length - 1) {
    n /= 1024;
    i++;
  }
  return `${i === 0 ? n : n.toFixed(1)} ${units[i]}`;
}
