/** Display helpers for query rows and Arrow values in the results grid. */

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
 * Formats an Arrow timestamp value as an ISO-8601 UTC string.
 *
 * apache-arrow's element getters already normalise every timestamp to
 * **milliseconds** regardless of the column's unit (`visitor/get.ts`:
 * SECOND ×1000, MICROSECOND ÷1e3, NANOSECOND ÷1e6). Scaling by the unit again
 * here would apply the conversion twice — a nanosecond column then landed
 * within half an hour of the epoch, so every row rendered as 1969/1970.
 */
export function formatTimestamp(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (value instanceof Date) return value.toISOString();
  const millis = typeof value === "bigint" ? Number(value) : Number(value);
  if (!Number.isFinite(millis)) return formatCell(value);
  const date = new Date(millis);
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
