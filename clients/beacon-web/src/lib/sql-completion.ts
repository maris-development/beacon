import * as React from "react";
import { useQuery } from "@tanstack/react-query";
import { sql, type SQLNamespace } from "@codemirror/lang-sql";
import type { Extension } from "@codemirror/state";
import type {
  Completion,
  CompletionContext,
  CompletionResult,
} from "@codemirror/autocomplete";

import { useBeacon } from "@/lib/beacon-context";

/** Stable empty-extension identity (a fresh `[]` each render reconfigures CodeMirror). */
const NO_EXTENSIONS: Extension[] = [];

/**
 * Max column completions to build per table. A wide table (we've seen 100K+
 * columns) would otherwise allocate that many `Completion` objects and feed them
 * all to `sql({ schema })` synchronously on the main thread — enough to hang the
 * renderer. Beyond this many columns the dropdown is unusable to a human anyway,
 * so we take the first N; the table name itself still completes.
 */
const MAX_COMPLETION_COLUMNS_PER_TABLE = 100;

/** Shape of one entry from `GET /api/tables-with-schema`. */
interface TableWithSchema {
  table_name: string;
  columns: { name: string; data_type?: string; nullable?: boolean }[];
}

/** Parsed function metadata used to build completions and their doc tooltips. */
interface FnMeta {
  name: string;
  description?: string;
  returnType?: string;
  params: { name: string; dataType?: string; description?: string }[];
}

/** Pull a function name out of a metadata object (the API isn't uniform). */
function fnName(o: Record<string, unknown>): string | undefined {
  const v = o.function_name ?? o.name ?? o.function ?? o.id;
  return typeof v === "string" ? v : undefined;
}

/** True for the API's placeholder text, which we don't want to render as docs. */
function isPlaceholder(s: string | undefined): boolean {
  return !s || /^No (documentation|description) available$/i.test(s);
}

/** Parse a raw `/api/functions` (or table-functions) entry into {@link FnMeta}. */
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

/** Build a documentation tooltip (signature + description + params) for a function. */
function renderFnDoc(fn: FnMeta): HTMLElement {
  const dom = document.createElement("div");
  dom.style.maxWidth = "32rem";

  const paramSig = fn.params
    .map((p) => (p.dataType ? `${p.name}: ${p.dataType}` : p.name))
    .join(", ");
  const sig = document.createElement("div");
  sig.style.fontFamily = "monospace";
  sig.style.fontWeight = "600";
  sig.textContent = `${fn.name}(${paramSig})${fn.returnType ? ` → ${fn.returnType}` : ""}`;
  dom.appendChild(sig);

  if (!isPlaceholder(fn.description)) {
    const desc = document.createElement("div");
    desc.style.marginTop = "4px";
    desc.textContent = fn.description as string;
    dom.appendChild(desc);
  }

  const documented = fn.params.filter((p) => p.name && !isPlaceholder(p.description));
  if (documented.length > 0) {
    const ul = document.createElement("ul");
    ul.style.margin = "6px 0 0";
    ul.style.paddingLeft = "1.1rem";
    for (const p of documented) {
      const li = document.createElement("li");
      const code = document.createElement("code");
      code.textContent = p.name;
      li.appendChild(code);
      li.appendChild(document.createTextNode(` — ${p.description}`));
      ul.appendChild(li);
    }
    dom.appendChild(ul);
  }
  return dom;
}

/**
 * CodeMirror SQL completion fed by the runtime's live metadata:
 * - table names and their columns (from `tables-with-schema`) — including
 *   `table.column` / `alias.column` completion, with each column's data type,
 * - scalar/aggregate and table-valued function names (inserted with a `(`), each
 *   with a documentation tooltip (signature, description, parameters),
 * - SQL keywords (provided by `@codemirror/lang-sql` out of the box).
 *
 * Returns a CodeMirror extension that updates as the metadata loads.
 */
export function useSqlCompletion(enabled = true) {
  const beacon = useBeacon();

  const tablesQuery = useQuery({
    queryKey: ["tables-with-schema"],
    queryFn: () => beacon.tablesWithSchema<TableWithSchema[]>(),
    staleTime: 60_000,
    enabled,
  });

  const fnQuery = useQuery({
    queryKey: ["sql-function-docs"],
    queryFn: async () => {
      const [scalar, table] = await Promise.all([
        beacon.functions<Record<string, unknown>[]>(),
        beacon.tableFunctions<Record<string, unknown>[]>(),
      ]);
      const parse = (arr: Record<string, unknown>[]) => {
        const seen = new Set<string>();
        const out: FnMeta[] = [];
        for (const o of arr) {
          const fn = parseFn(o);
          if (!fn || seen.has(fn.name)) continue;
          seen.add(fn.name);
          out.push(fn);
        }
        return out.sort((a, b) => a.name.localeCompare(b.name));
      };
      return { scalar: parse(scalar), table: parse(table) };
    },
    staleTime: 60_000,
    enabled,
  });

  return React.useMemo(() => {
    // When autocomplete is disabled we never fetched metadata; skip building the
    // (potentially very large) completion schema entirely.
    if (!enabled) return NO_EXTENSIONS;
    // Build the table → columns namespace. Columns are Completion objects so the
    // popup shows each column's data type (detail) and a small info tooltip.
    const schema: SQLNamespace = {};
    for (const t of tablesQuery.data ?? []) {
      const named = (t.columns ?? []).filter((c) => c.name);
      // Cap very wide tables so building completions can't lock up the renderer.
      const cols =
        named.length > MAX_COMPLETION_COLUMNS_PER_TABLE
          ? named.slice(0, MAX_COMPLETION_COLUMNS_PER_TABLE)
          : named;
      (schema as Record<string, Completion[]>)[t.table_name] = cols.map((c) => ({
        label: c.name,
        type: "property",
        detail: c.data_type,
        info: c.data_type
          ? `${t.table_name}.${c.name} — ${c.data_type}${c.nullable === false ? " (not null)" : ""}`
          : undefined,
      }));
    }

    // Function completions: insert `name(` and attach a doc tooltip. Scalar
    // functions show their return type as detail; table functions are labelled.
    const toOption = (f: FnMeta, detail: string | undefined): Completion => ({
      label: f.name,
      type: "function",
      detail,
      apply: `${f.name}(`,
      info: () => renderFnDoc(f),
    });
    const fnOptions: Completion[] = [
      ...(fnQuery.data?.scalar ?? []).map((f) => toOption(f, f.returnType)),
      ...(fnQuery.data?.table ?? []).map((f) => toOption(f, "table function")),
    ];

    const lang = sql({ schema });
    const fnSource = (ctx: CompletionContext): CompletionResult | null => {
      if (fnOptions.length === 0) return null;
      const word = ctx.matchBefore(/\w+/);
      if (!word || (word.from === word.to && !ctx.explicit)) return null;
      // Don't suggest functions in member-access position (after `table.`/`alias.`),
      // where only the resolved table's columns — handled by the language's own
      // source, including FROM-clause alias resolution — should appear.
      if (word.from > 0 && ctx.state.sliceDoc(word.from - 1, word.from) === ".") return null;
      return { from: word.from, options: fnOptions, validFor: /^\w*$/ };
    };

    // Merge our function source with the language's built-in (tables/columns/keywords).
    return [lang, lang.language.data.of({ autocomplete: fnSource })];
  }, [enabled, tablesQuery.data, fnQuery.data]);
}
