import * as React from "react";
import { ChevronRight } from "lucide-react";

import { cn } from "@/lib/utils";

/**
 * Renders a DataFusion/Postgres-style EXPLAIN plan as a collapsible tree.
 *
 * The plan is `[{ Plan: node }]` where each node has a `Node Type`, a `Plans`
 * array of children, and assorted scalar/array detail fields.
 */
export function PlanTree({ plan }: { plan: unknown }) {
  const root = extractRoot(plan);
  if (!root) {
    return (
      <div className="p-4 text-sm text-muted-foreground">
        No plan to display.
      </div>
    );
  }
  return (
    <div className="p-4 font-mono text-xs">
      <PlanNode node={root} depth={0} defaultOpen />
    </div>
  );
}

interface RawNode {
  [key: string]: unknown;
  "Node Type"?: string;
  Plans?: unknown[];
}

/** Finds the top plan node within the explain response. */
function extractRoot(plan: unknown): RawNode | null {
  let obj: unknown = plan;
  if (Array.isArray(plan) && plan.length > 0) obj = plan[0];
  if (obj && typeof obj === "object" && "Plan" in obj) {
    obj = (obj as Record<string, unknown>).Plan;
  }
  return obj && typeof obj === "object" ? (obj as RawNode) : null;
}

function PlanNode({
  node,
  depth,
  defaultOpen = false,
}: {
  node: RawNode;
  depth: number;
  defaultOpen?: boolean;
}) {
  const [open, setOpen] = React.useState(defaultOpen || depth < 3);
  const children = Array.isArray(node.Plans) ? (node.Plans as RawNode[]) : [];
  const nodeType = typeof node["Node Type"] === "string" ? (node["Node Type"] as string) : "Node";

  // EXPLAIN ANALYZE metrics, surfaced as always-visible badges on the node.
  const actualRows = node["Actual Rows"];
  const actualTime = node["Actual Total Time"]; // operator compute time (ms)
  const wallMs = wallTimeMs(node.Extras); // wall time from start/end timestamps
  const metricKeys = new Set(["Node Type", "Plans", "Actual Rows", "Actual Total Time"]);

  // Detail fields = everything except the node type, child list, and the
  // metrics already shown as badges.
  const details = Object.entries(node).filter(([k]) => !metricKeys.has(k));

  return (
    <div className={cn(depth > 0 && "ml-3 border-l border-border pl-3")}>
      <div className="flex items-start gap-1 py-0.5">
        <button
          type="button"
          onClick={() => setOpen((o) => !o)}
          className={cn(
            "mt-0.5 flex h-4 w-4 shrink-0 items-center justify-center text-muted-foreground",
            children.length === 0 && "invisible",
          )}
          aria-label={open ? "Collapse" : "Expand"}
        >
          <ChevronRight className={cn("h-3.5 w-3.5 transition-transform", open && "rotate-90")} />
        </button>
        <div className="min-w-0">
          <span className="rounded bg-primary/10 px-1.5 py-0.5 font-semibold text-primary">
            {nodeType}
          </span>
          {actualRows !== undefined && (
            <span className="ml-1.5 rounded bg-secondary px-1.5 py-0.5 text-[11px] text-muted-foreground">
              {formatValue(actualRows)} rows
            </span>
          )}
          {actualTime !== undefined && (
            <span
              className="ml-1.5 rounded bg-accent/15 px-1.5 py-0.5 text-[11px] font-medium text-accent"
              title="Operator compute time (elapsed_compute)"
            >
              compute {formatTime(actualTime)}
            </span>
          )}
          {wallMs !== undefined && (
            <span
              className="ml-1.5 rounded bg-sky-500/15 px-1.5 py-0.5 text-[11px] font-medium text-sky-600 dark:text-sky-400"
              title="Wall-clock time (end − start timestamp)"
            >
              wall {formatTime(wallMs)}
            </span>
          )}
          {open && details.length > 0 && (
            <div className="mt-1 space-y-0.5">
              {details.map(([key, value]) => (
                <div key={key} className="flex gap-1.5">
                  <span className="shrink-0 text-muted-foreground">{key}:</span>
                  <span className="break-all text-foreground">{formatValue(value)}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {open && children.length > 0 && (
        <div className="mt-0.5">
          {children.map((child, i) => (
            <PlanNode key={i} node={child} depth={depth + 1} />
          ))}
        </div>
      )}
    </div>
  );
}

/**
 * Wall-clock time (ms) of a node, derived from its `Extras` start/end epoch-ns
 * timestamps. Returns undefined when the timestamps are absent or unusable.
 */
function wallTimeMs(extras: unknown): number | undefined {
  if (!extras || typeof extras !== "object") return undefined;
  const e = extras as Record<string, unknown>;
  const start = Number(e.start_timestamp);
  const end = Number(e.end_timestamp);
  if (!Number.isFinite(start) || !Number.isFinite(end) || end < start) return undefined;
  return (end - start) / 1_000_000;
}

/**
 * Formats an "Actual Total Time" value (milliseconds) as a compact label,
 * adapting the unit so sub-millisecond times don't collapse to "0.000 ms".
 * Note this is the operator's compute time; scan operators report ~0 here and
 * record their I/O cost under `Extras` (`time_elapsed_*`).
 */
function formatTime(value: unknown): string {
  const n = Number(value);
  if (!Number.isFinite(n)) return `${formatValue(value)} ms`;
  if (n === 0) return "0 ms";
  if (n >= 1000) return `${(n / 1000).toFixed(2)} s`;
  if (n >= 1) return `${n.toFixed(2)} ms`;
  if (n >= 0.001) return `${(n * 1000).toFixed(1)} µs`;
  return `${Math.round(n * 1_000_000)} ns`;
}

function formatValue(value: unknown): string {
  if (value === null || value === undefined) return "—";
  if (Array.isArray(value)) return value.map((v) => formatValue(v)).join(", ");
  if (typeof value === "object") {
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }
  return String(value);
}
