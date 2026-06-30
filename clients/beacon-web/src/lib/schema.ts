/** Defensive parsing of Beacon's table/dataset schema responses. */

/** How many columns to render at once before a "show more" control. */
export const COLUMN_PAGE_SIZE = 500;

export interface SchemaColumn {
  name: string;
  dataType: string;
  nullable?: boolean;
}

interface RawField {
  name?: string;
  data_type?: unknown;
  dataType?: unknown;
  type?: unknown;
  nullable?: boolean;
}

/** Extracts a flat column list from any Beacon schema shape (`fields` or `columns`). */
export function parseSchema(schema: unknown): SchemaColumn[] {
  if (!schema || typeof schema !== "object") return [];
  const obj = schema as Record<string, unknown>;
  const raw = (Array.isArray(obj.fields) && obj.fields) ||
    (Array.isArray(obj.columns) && obj.columns) ||
    [];
  return (raw as RawField[])
    .filter((f) => typeof f.name === "string")
    .map((f) => ({
      name: f.name as string,
      dataType: stringifyType(f.data_type ?? f.dataType ?? f.type),
      nullable: f.nullable,
    }));
}

function stringifyType(t: unknown): string {
  if (t == null) return "";
  if (typeof t === "string") return t;
  if (typeof t === "object") {
    // Arrow data types may serialize as a single-key object, e.g. { "Timestamp": ... }.
    const keys = Object.keys(t as Record<string, unknown>);
    if (keys.length === 1) return keys[0];
    try {
      return JSON.stringify(t);
    } catch {
      return String(t);
    }
  }
  return String(t);
}
