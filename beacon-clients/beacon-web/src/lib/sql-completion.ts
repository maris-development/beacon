/**
 * What the SQL editor completes, and where it comes from.
 *
 * The suggestions are the runtime's live metadata:
 * - table names, including the `catalog.schema.table` path of every catalog and
 *   schema (from `catalogs`), so tables outside the default schema —
 *   `beacon.system`, `information_schema`, attached remotes — are reachable too,
 * - scalar, aggregate, and window functions (from `functions`), inserted with a
 *   `(` and documented with their signature, description, and parameters. Table
 *   functions (`read_parquet`, …) are absent: DataFusion does not catalog UDTFs,
 *   so the server has no list to serve,
 * - SQL keywords.
 *
 * **Not** column names. A beacon table can carry 100K+ columns — an
 * N-dimensional dataset flattened into one — so completing them would mean
 * downloading schemas the browser cannot afford. Columns are browsed instead:
 * expand a table in the data panel, or open it on the Tables page.
 */

import * as React from "react";
import { useQuery } from "@tanstack/react-query";

import { useBeacon } from "@/lib/beacon-context";
import { isDefaultSchema, sqlName, useCatalogTree } from "@/lib/catalog";

/** Parsed function metadata used to build completions and their documentation. */
export interface FnMeta {
  name: string;
  description?: string;
  returnType?: string;
  params: { name: string; dataType?: string; description?: string }[];
}

/** Everything the editor's completion provider offers. */
export interface SqlMetadata {
  /** Table names as they would be written in SQL (qualified when they must be). */
  tables: string[];
  functions: FnMeta[];
}

export const EMPTY_METADATA: SqlMetadata = { tables: [], functions: [] };

/** Pull a function name out of a metadata object (the API isn't uniform). */
function fnName(o: Record<string, unknown>): string | undefined {
  const v = o.function_name ?? o.name ?? o.function ?? o.id;
  return typeof v === "string" ? v : undefined;
}

/** True for the API's placeholder text, which we don't want to render as docs. */
export function isPlaceholder(s: string | undefined): boolean {
  return !s || /^No (documentation|description) available$/i.test(s);
}

/** Parse a raw `/api/functions` entry into {@link FnMeta}. */
function parseFn(o: Record<string, unknown>): FnMeta | null {
  const name = fnName(o);
  if (!name) return null;
  const rawParams = Array.isArray(o.params) ? (o.params as Record<string, unknown>[]) : [];
  return {
    name,
    description: typeof o.description === "string" ? o.description : undefined,
    returnType: typeof o.return_type === "string" ? o.return_type : undefined,
    params: rawParams.map((p) => ({
      name: typeof p.name === "string" ? p.name : "",
      dataType: typeof p.data_type === "string" ? p.data_type : undefined,
      description: typeof p.description === "string" ? p.description : undefined,
    })),
  };
}

/** `name(a: T, b: U) → R`, the signature line of a function's documentation. */
export function fnSignature(fn: FnMeta): string {
  const params = fn.params
    .map((p) => (p.dataType ? `${p.name}: ${p.dataType}` : p.name))
    .join(", ");
  return `${fn.name}(${params})${fn.returnType ? ` → ${fn.returnType}` : ""}`;
}

/** A function's documentation as markdown, for the suggestion's detail pane. */
export function fnDocumentation(fn: FnMeta): string {
  const lines = [`\`\`\`\n${fnSignature(fn)}\n\`\`\``];
  if (!isPlaceholder(fn.description)) lines.push(fn.description as string);
  const documented = fn.params.filter((p) => p.name && !isPlaceholder(p.description));
  if (documented.length > 0) {
    lines.push(documented.map((p) => `- \`${p.name}\` — ${p.description}`).join("\n"));
  }
  return lines.join("\n\n");
}

/**
 * Loads the metadata the completion provider offers.
 *
 * Both sources are small and cached: the catalog tree is names only, and the
 * function catalog is a few hundred entries fetched once per session.
 */
export function useSqlMetadata(enabled = true): SqlMetadata {
  const beacon = useBeacon();
  const { tree } = useCatalogTree(enabled);

  const fnQuery = useQuery({
    queryKey: ["sql-function-docs"],
    queryFn: async () => {
      const functions = await beacon.functions<Record<string, unknown>[]>();
      const seen = new Set<string>();
      const out: FnMeta[] = [];
      for (const o of functions) {
        const fn = parseFn(o);
        if (!fn || seen.has(fn.name)) continue;
        seen.add(fn.name);
        out.push(fn);
      }
      return out.sort((a, b) => a.name.localeCompare(b.name));
    },
    staleTime: 60_000,
    enabled,
  });

  return React.useMemo(() => {
    if (!enabled) return EMPTY_METADATA;
    // A table in the default schema completes bare; anything else completes as
    // the qualified name that would actually resolve.
    const tables = tree.tables.map((table) =>
      isDefaultSchema(table, tree.defaults) ? table.name : sqlName(table, tree.defaults),
    );
    return { tables, functions: fnQuery.data ?? [] };
  }, [enabled, tree, fnQuery.data]);
}

/**
 * The keywords offered alongside the metadata. Monaco's SQL mode is a tokenizer
 * only — it highlights keywords but suggests nothing — so the list lives here.
 */
export const SQL_KEYWORDS = [
  "SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "HAVING", "LIMIT", "OFFSET",
  "JOIN", "INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN", "CROSS JOIN", "ON", "USING",
  "AS", "AND", "OR", "NOT", "IN", "IS NULL", "IS NOT NULL", "BETWEEN", "LIKE", "ILIKE",
  "CASE", "WHEN", "THEN", "ELSE", "END", "DISTINCT", "UNION", "UNION ALL",
  "INTERSECT", "EXCEPT", "WITH", "ASC", "DESC", "NULLS FIRST", "NULLS LAST",
  "CREATE TABLE", "CREATE VIEW", "CREATE MATERIALIZED VIEW", "CREATE EXTERNAL TABLE",
  "STORED AS", "LOCATION", "OPTIONS", "INSERT INTO", "VALUES", "UPDATE", "SET",
  "DELETE FROM", "DROP TABLE", "ALTER TABLE", "REFRESH", "EXPLAIN", "ANALYZE",
  "SHOW TABLES", "SHOW EXTENSIONS FOR", "DESCRIBE",
];
