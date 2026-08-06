/** Reading Beacon's schema responses, which are Arrow schemas as Arrow serializes them. */

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

/**
 * Renders an Arrow data type as the text a person reads in a column list.
 *
 * Arrow serializes a simple type as a bare string (`"Float64"`) and a
 * parameterized one as a single-key object carrying its arguments:
 *
 * - `{ "Timestamp": ["Microsecond", null] }` → `Timestamp(Microsecond)`
 * - `{ "Timestamp": ["Millisecond", "+00:00"] }` → `Timestamp(Millisecond, +00:00)`
 * - `{ "List": { "name": "item", "data_type": "Utf8", … } }` → `List<Utf8>`
 * - `{ "Decimal128": [10, 2] }` → `Decimal128(10, 2)`
 *
 * Nulls in the argument list are dropped — an absent timezone is not worth a
 * `, null` in the UI. Anything unrecognized falls back to its JSON.
 */
export function stringifyType(type: unknown): string {
  if (type == null) return "";
  if (typeof type === "string") return type;
  if (typeof type !== "object") return String(type);

  const entries = Object.entries(type as Record<string, unknown>);
  if (entries.length !== 1) return safeJson(type);
  const [name, args] = entries[0];

  // A nested field (List, LargeList, FixedSizeList, Map): render its element type.
  if (args && typeof args === "object" && !Array.isArray(args) && "data_type" in args) {
    return `${name}<${stringifyType((args as RawField).data_type)}>`;
  }
  if (Array.isArray(args)) {
    const rendered = args
      .filter((arg) => arg != null)
      .map((arg) =>
        typeof arg === "object" ? stringifyType(arg) : String(arg),
      );
    return rendered.length > 0 ? `${name}(${rendered.join(", ")})` : name;
  }
  if (args == null) return name;
  return `${name}(${typeof args === "object" ? stringifyType(args) : String(args)})`;
}

function safeJson(value: unknown): string {
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}
