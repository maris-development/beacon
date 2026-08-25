import * as React from "react";
import { useLocation } from "react-router-dom";
import {
  AlertCircle,
  Bookmark,
  Download,
  FolderOpen,
  Gauge,
  Loader2,
  Network,
  Play,
  Square,
  Trash2,
} from "lucide-react";

import { appendChunk, EMPTY_RESULT, type ArrowResult } from "@/lib/arrow-result";
import { useBeacon } from "@/lib/beacon-context";
import { errorMessage } from "@/lib/errors";
import { formatBytes } from "@/lib/format";
import {
  deleteSavedQuery,
  listSavedQueries,
  saveQuery,
  type SavedQuery,
} from "@/lib/saved-queries";
import { DataPanel } from "@/components/data-panel";
import {
  disposeEditorModel,
  SqlEditor,
  type SqlEditorHandle,
} from "@/components/sql-editor-lazy";
import { QueryTabs } from "@/components/query-tabs";
import { useQueryTabs } from "@/lib/query-tabs";
import { ResultsGrid } from "@/components/results-grid";
import { PlanTree } from "@/components/plan-tree";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";

interface RunResult {
  /** The rows that have streamed in so far, kept in Arrow's columnar form. */
  view: ArrowResult;
  queryId: string | null;
  elapsedMs: number;
  /** True when the preview row limit was hit and the query was stopped early. */
  truncated?: boolean;
  /** True when the user cancelled the query mid-stream. */
  cancelled?: boolean;
}

type ViewMode = "results" | "explain";

interface DownloadFormat {
  format: "csv" | "parquet" | "arrow" | "netcdf";
  label: string;
  ext: string;
}

/** Output formats offered in the Download menu (see SDK `OutputFormat`). */
const DOWNLOAD_FORMATS: DownloadFormat[] = [
  { format: "csv", label: "CSV", ext: "csv" },
  { format: "parquet", label: "Parquet", ext: "parquet" },
  { format: "arrow", label: "Arrow IPC", ext: "arrow" },
  { format: "netcdf", label: "NetCDF", ext: "nc" },
];

/**
 * How many rows to render for a result preview. Once this many have streamed in
 * the query is aborted, so a `SELECT *` over a huge table fills the grid quickly
 * instead of downloading (and buffering) the entire result.
 */
const PREVIEW_ROW_LIMIT = 500;

/**
 * What the workbench is doing right now.
 *
 * Run, Explain, Analyze and Download all describe the SQL of the active tab and
 * write into the one panel below it, so the workbench does one at a time: a
 * second action cancels the first instead of racing it. Two of them in flight
 * only doubled the work the server did, and whichever finished last took the
 * panel — an EXPLAIN ANALYZE that failed after a Run had already drawn its rows
 * replaced them with its own error, and the editor looked stuck.
 */
type Action = "run" | "explain" | "analyze" | "download";

/** Tooltip note on the buttons that cancel whatever else is running. */
const SUPERSEDES_TITLE = "Cancels the query that is still running, if any.";

/** What the results header says while each action is in flight. */
const BUSY_LABEL: Record<Action, string> = {
  run: "Running…",
  explain: "Explaining…",
  analyze: "Analyzing…",
  download: "Preparing download…",
};

export function WorkbenchPage() {
  const beacon = useBeacon();
  const location = useLocation();
  const editorRef = React.useRef<SqlEditorHandle>(null);
  // The in-flight action, so the user (or the next action) can cancel it. Held
  // in a ref as well as in `busy` because the async bodies below test ownership
  // of the panel after every await, and state they closed over is stale by then.
  const inflight = React.useRef<AbortController | null>(null);
  // Open queries, one per tab, kept in local storage so they survive leaving the
  // page (and the browser).
  const queryTabs = useQueryTabs();
  const { active, activeId, setSql: setTabSql, open: openTab, close: closeTab } = queryTabs;
  const sql = active.sql;
  const setSql = React.useCallback(
    (next: string) => setTabSql(activeId, next),
    [setTabSql, activeId],
  );

  const [busy, setBusy] = React.useState<Action | null>(null);
  const [mode, setMode] = React.useState<ViewMode>("results");
  const [result, setResult] = React.useState<RunResult | null>(null);
  const [plan, setPlan] = React.useState<unknown>(null);
  const [analyzed, setAnalyzed] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  const running = busy === "run";
  const explaining = busy === "explain";
  const analyzing = busy === "analyze";
  const downloading = busy === "download";

  /** Cancels whatever is running and hands the panel to `action`. */
  const begin = React.useCallback((action: Action) => {
    inflight.current?.abort();
    const controller = new AbortController();
    inflight.current = controller;
    setBusy(action);
    setError(null);
    return controller;
  }, []);

  /**
   * Whether `controller`'s action still owns the panel. A superseded action must
   * write no state at all: its result belongs to SQL the user has moved on from.
   */
  const owns = React.useCallback(
    (controller: AbortController) => inflight.current === controller,
    [],
  );

  /** Releases the panel, unless a newer action has already taken it. */
  const end = React.useCallback((controller: AbortController) => {
    if (inflight.current !== controller) return;
    inflight.current = null;
    setBusy(null);
  }, []);

  // Another page (e.g. Datasets → "Query") can open the editor pre-filled by
  // navigating to `/query` with `{ state: { sql } }`. It lands in a tab of its
  // own rather than overwriting whatever was being written.
  const handedOver = (location.state as { sql?: string } | null)?.sql;
  const handedOverRef = React.useRef<string | null>(null);
  React.useEffect(() => {
    if (!handedOver || handedOverRef.current === handedOver) return;
    handedOverRef.current = handedOver;
    openTab(handedOver);
  }, [handedOver, openTab]);

  const [saveOpen, setSaveOpen] = React.useState(false);
  const [savedOpen, setSavedOpen] = React.useState(false);
  const [metricsId, setMetricsId] = React.useState<string | null>(null);

  // Results describe the query that produced them, so switching tabs clears the
  // panel — and cancels the work still filling it — rather than showing one
  // tab's rows under another tab's SQL.
  React.useEffect(() => {
    inflight.current?.abort();
    inflight.current = null;
    setBusy(null);
    setResult(null);
    setPlan(null);
    setError(null);
    setMode("results");
  }, [activeId]);

  const run = React.useCallback(async () => {
    const text = sql.trim();
    if (!text) return;
    const controller = begin("run");
    setMode("results");
    setResult(null);
    const started = performance.now();
    // Stream record batches and render them as they arrive, stopping (and
    // aborting the server query) once the preview limit is reached — or when the
    // user cancels.
    try {
      const { queryId, batches } = await beacon.queryBatches(text, controller.signal);
      let view = EMPTY_RESULT;
      let truncated = false;
      for await (const batch of batches) {
        if (!owns(controller)) return;
        // The batch is kept as Arrow columns; nothing is decoded into JS objects
        // until the grid renders a cell.
        truncated = batch.numRows > PREVIEW_ROW_LIMIT - view.numRows;
        view = appendChunk(view, batch, PREVIEW_ROW_LIMIT);
        // A new result reference makes React render the rows so far.
        setResult({ view, queryId, elapsedMs: performance.now() - started, truncated });
        if (truncated) {
          controller.abort(); // we have our preview; stop the query
          break;
        }
      }
      if (!owns(controller)) return;
      // No batches arrived (DDL/DML or an empty result): surface a zero-row result.
      setResult(
        (prev) =>
          prev ?? {
            view: EMPTY_RESULT,
            queryId,
            elapsedMs: performance.now() - started,
            truncated: false,
          },
      );
    } catch (err) {
      if (!owns(controller)) return;
      if (controller.signal.aborted) {
        // User cancelled mid-stream: keep whatever rows already arrived. (Don't
        // relabel a result that stopped because it hit the preview limit.)
        setResult((prev) => (prev && !prev.truncated ? { ...prev, cancelled: true } : prev));
      } else {
        setResult(null);
        setError(errorMessage(err));
      }
    } finally {
      end(controller);
    }
  }, [beacon, sql, begin, owns, end]);

  /** Aborts the in-flight action (if any); partial results stay on screen. */
  const cancel = React.useCallback(() => {
    inflight.current?.abort();
  }, []);

  // Abort any in-flight work if the page unmounts.
  React.useEffect(() => () => inflight.current?.abort(), []);

  const explain = React.useCallback(async () => {
    const text = sql.trim();
    if (!text) return;
    const controller = begin("explain");
    setMode("explain");
    setAnalyzed(false);
    // Drop the previous plan: leaving it up makes a second Explain look like it
    // did nothing, because the pane shows progress only while it is empty.
    setPlan(null);
    try {
      const explained = await beacon.explainQuery(text, controller.signal);
      if (owns(controller)) setPlan(explained);
    } catch (err) {
      // Swallow cancellation; only surface real failures.
      if (owns(controller) && !controller.signal.aborted) setError(errorMessage(err));
    } finally {
      end(controller);
    }
  }, [beacon, sql, begin, owns, end]);

  const analyze = React.useCallback(async () => {
    const text = sql.trim();
    if (!text) return;
    const controller = begin("analyze");
    setMode("explain");
    setAnalyzed(true);
    setPlan(null);
    try {
      const analyzedPlan = await beacon.explainAnalyzeQuery(text, controller.signal);
      if (owns(controller)) setPlan(analyzedPlan);
    } catch (err) {
      if (owns(controller) && !controller.signal.aborted) setError(errorMessage(err));
    } finally {
      end(controller);
    }
  }, [beacon, sql, begin, owns, end]);

  const download = React.useCallback(
    async (format: DownloadFormat["format"], ext: string) => {
      const text = sql.trim();
      if (!text) return;
      const controller = begin("download");
      try {
        const res = await beacon.queryRaw(text, format, controller.signal);
        const blob = await res.blob();
        if (!owns(controller)) return;
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = `beacon-result.${ext}`;
        document.body.appendChild(a);
        a.click();
        a.remove();
        URL.revokeObjectURL(url);
      } catch (err) {
        if (owns(controller) && !controller.signal.aborted) setError(errorMessage(err));
      } finally {
        end(controller);
      }
    },
    [beacon, sql, begin, owns, end],
  );

  /** Closing a tab drops its editor model too, undo history and all. */
  const closeWithModel = React.useCallback(
    (id: string) => {
      closeTab(id);
      void disposeEditorModel(id);
    },
    [closeTab],
  );

  function insert(textToInsert: string) {
    if (editorRef.current) {
      editorRef.current.insert(textToInsert);
    } else {
      // Before the editor has mounted there is no cursor to insert at.
      setSql(sql ? `${sql} ${textToInsert}` : textToInsert);
    }
  }

  return (
    <div className="flex h-full min-h-0">
      <div className="w-64 shrink-0">
        <DataPanel onInsert={insert} />
      </div>

      <div className="flex min-h-0 min-w-0 flex-1 flex-col">
        <QueryTabs {...queryTabs} close={closeWithModel} />

        {/* Toolbar */}
        <div className="flex items-center gap-2 border-b bg-card px-4 py-2">
          {running ? (
            <Button onClick={cancel} variant="destructive" size="sm" className="gap-1.5">
              <Square className="h-4 w-4" />
              Stop
            </Button>
          ) : (
            <Button onClick={run} size="sm" className="gap-1.5" title={SUPERSEDES_TITLE}>
              <Play className="h-4 w-4" />
              Run
            </Button>
          )}
          <Button
            onClick={explain}
            disabled={explaining}
            variant="outline"
            size="sm"
            className="gap-1.5"
            title={SUPERSEDES_TITLE}
          >
            {explaining ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <Network className="h-4 w-4" />
            )}
            Explain
          </Button>
          {analyzing ? (
            <Button
              onClick={cancel}
              variant="destructive"
              size="sm"
              className="gap-1.5"
              title="Stop the running EXPLAIN ANALYZE"
            >
              <Square className="h-4 w-4" />
              Stop
            </Button>
          ) : (
            <Button
              onClick={analyze}
              variant="outline"
              size="sm"
              className="gap-1.5"
              title={`Run the query and show its plan with execution metrics. ${SUPERSEDES_TITLE}`}
            >
              <Gauge className="h-4 w-4" />
              Analyze
            </Button>
          )}
          <span className="text-xs text-muted-foreground">⌘/Ctrl + Enter</span>

          <div className="ml-auto flex items-center gap-2">
            <Button variant="ghost" size="sm" onClick={() => setSaveOpen(true)} className="gap-1.5">
              <Bookmark className="h-4 w-4" /> Save
            </Button>
            <Button
              variant="ghost"
              size="sm"
              onClick={() => setSavedOpen(true)}
              className="gap-1.5"
            >
              <FolderOpen className="h-4 w-4" /> Saved
            </Button>
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button variant="outline" size="sm" disabled={downloading} className="gap-1.5">
                  {downloading ? (
                    <Loader2 className="h-4 w-4 animate-spin" />
                  ) : (
                    <Download className="h-4 w-4" />
                  )}
                  Download
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end">
                {DOWNLOAD_FORMATS.map((f) => (
                  <DropdownMenuItem key={f.format} onClick={() => download(f.format, f.ext)}>
                    {f.label} (.{f.ext})
                  </DropdownMenuItem>
                ))}
              </DropdownMenuContent>
            </DropdownMenu>
          </div>
        </div>

        {/* Editor */}
        <div className="h-[38%] min-h-[120px] border-b">
          <SqlEditor
            ref={editorRef}
            modelKey={activeId}
            value={sql}
            onChange={setSql}
            onRun={run}
          />
        </div>

        {/* Results / plan header */}
        <div className="flex items-center gap-3 border-b bg-secondary/40 px-4 py-1.5 text-xs">
          <span className="font-semibold">{mode === "explain" ? "Query plan" : "Results"}</span>
          {/* One indicator for the one action in flight. It names that action, so
              a slow query says which of them the server is still working on. */}
          {busy && (
            <span className="flex items-center gap-1.5 text-muted-foreground">
              <Loader2 className="h-3 w-3 animate-spin" />
              {BUSY_LABEL[busy]}
            </span>
          )}
          {mode === "results" && result && (
            <>
              <span className="text-muted-foreground">
                {result.view.numRows} rows
                {result.truncated && ` (first ${PREVIEW_ROW_LIMIT} — query stopped)`}
                {result.cancelled && " (cancelled)"}
              </span>
              <span className="text-muted-foreground">{result.elapsedMs.toFixed(0)} ms</span>
              {result.queryId && (
                <button
                  type="button"
                  onClick={() => setMetricsId(result.queryId)}
                  title="View execution metrics"
                  className="ml-auto flex items-center gap-1 font-mono text-[11px] text-muted-foreground hover:text-foreground"
                >
                  <Gauge className="h-3.5 w-3.5" />
                  {result.queryId}
                </button>
              )}
            </>
          )}
          {mode === "explain" && plan != null && (
            <span className="text-muted-foreground">
              {analyzed ? "physical plan · execution metrics" : "logical plan"}
            </span>
          )}
        </div>

        {/* Body */}
        <div className="min-h-0 flex-1 overflow-auto bg-card">
          {error ? (
            <div className="m-4 flex items-start gap-2 rounded-md border border-destructive/30 bg-destructive/10 p-3 text-sm text-destructive">
              <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
              <pre className="overflow-auto whitespace-pre-wrap break-words font-mono text-xs">
                {error}
              </pre>
            </div>
          ) : mode === "explain" ? (
            plan != null ? (
              <PlanTree plan={plan} />
            ) : explaining || analyzing ? (
              <Empty>
                <Loader2 className="mr-2 inline h-4 w-4 animate-spin" />
                {analyzing ? "Analyzing…" : "Explaining…"}
              </Empty>
            ) : (
              <Empty>Run Explain to see the query plan.</Empty>
            )
          ) : result ? (
            <ResultsGrid result={result.view} />
          ) : running ? (
            <Empty>
              <Loader2 className="mr-2 inline h-4 w-4 animate-spin" />
              Running…
            </Empty>
          ) : (
            <Empty>Run a query to see results.</Empty>
          )}
        </div>
      </div>

      <QueryMetricsDialog queryId={metricsId} onClose={() => setMetricsId(null)} />
      <SaveQueryDialog open={saveOpen} onOpenChange={setSaveOpen} sql={sql} />
      <SavedQueriesDialog
        open={savedOpen}
        onOpenChange={setSavedOpen}
        onLoad={(q) => {
          setSql(q.sql);
          setSavedOpen(false);
        }}
      />
    </div>
  );
}

function Empty({ children }: { children: React.ReactNode }) {
  return (
    <div className="flex h-full items-center justify-center text-sm text-muted-foreground">
      {children}
    </div>
  );
}

interface QueryMetrics {
  input_rows?: number;
  input_bytes?: number;
  result_num_rows?: number;
  result_size_in_bytes?: number;
  [key: string]: unknown;
}

/** Fetches and shows `/api/query/metrics/{id}` for a completed query. */
function QueryMetricsDialog({
  queryId,
  onClose,
}: {
  queryId: string | null;
  onClose: () => void;
}) {
  const beacon = useBeacon();
  const [data, setData] = React.useState<QueryMetrics | null>(null);
  const [error, setError] = React.useState<string | null>(null);
  const [loading, setLoading] = React.useState(false);

  React.useEffect(() => {
    if (!queryId) return;
    setData(null);
    setError(null);
    setLoading(true);
    let cancelled = false;
    beacon
      .queryMetrics(queryId)
      .then((m) => !cancelled && setData(m as QueryMetrics))
      .catch((e) => !cancelled && setError(errorMessage(e)))
      .finally(() => !cancelled && setLoading(false));
    return () => {
      cancelled = true;
    };
  }, [queryId, beacon]);

  // Known scalar fields surfaced as tiles; anything else is shown as raw JSON.
  const known = ["input_rows", "input_bytes", "result_num_rows", "result_size_in_bytes"];
  const extras = data
    ? Object.fromEntries(Object.entries(data).filter(([k]) => !known.includes(k)))
    : {};

  return (
    <Dialog open={queryId != null} onOpenChange={(o) => !o && onClose()}>
      <DialogContent className="max-w-lg">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Gauge className="h-4 w-4" /> Query metrics
          </DialogTitle>
          <DialogDescription className="font-mono text-[11px]">{queryId}</DialogDescription>
        </DialogHeader>

        {loading && (
          <div className="flex items-center gap-2 py-4 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" /> Loading metrics…
          </div>
        )}
        {error && <p className="text-sm text-destructive">{error}</p>}
        {data && (
          <div className="space-y-3">
            <div className="grid grid-cols-2 gap-3">
              <MetricTile label="Input rows" value={(data.input_rows ?? 0).toLocaleString()} />
              <MetricTile label="Input bytes" value={formatBytes(data.input_bytes ?? 0)} />
              <MetricTile label="Result rows" value={(data.result_num_rows ?? 0).toLocaleString()} />
              <MetricTile label="Result size" value={formatBytes(data.result_size_in_bytes ?? 0)} />
            </div>
            {Object.keys(extras).length > 0 && (
              <pre className="max-h-48 overflow-auto rounded-md bg-secondary/50 p-3 font-mono text-xs">
                {JSON.stringify(extras, null, 2)}
              </pre>
            )}
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}

function MetricTile({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-md border bg-card p-3">
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className="mt-0.5 text-lg font-semibold tabular-nums">{value}</div>
    </div>
  );
}

function SaveQueryDialog({
  open,
  onOpenChange,
  sql,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  sql: string;
}) {
  const [name, setName] = React.useState("");

  React.useEffect(() => {
    if (open) setName("");
  }, [open]);

  function submit() {
    const trimmed = name.trim();
    if (!trimmed) return;
    saveQuery(trimmed, sql);
    onOpenChange(false);
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle>Save query</DialogTitle>
          <DialogDescription>
            Saved queries are stored in this browser. Reusing a name overwrites it.
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-1.5">
          <Input
            autoFocus
            value={name}
            onChange={(e) => setName(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && submit()}
            placeholder="Query name"
          />
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button onClick={submit} disabled={!name.trim()}>
            <Bookmark className="h-4 w-4" /> Save
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function SavedQueriesDialog({
  open,
  onOpenChange,
  onLoad,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onLoad: (q: SavedQuery) => void;
}) {
  const [queries, setQueries] = React.useState<SavedQuery[]>([]);

  React.useEffect(() => {
    if (open) setQueries(listSavedQueries());
  }, [open]);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[80vh] max-w-2xl overflow-y-auto">
        <DialogHeader>
          <DialogTitle>Saved queries</DialogTitle>
          <DialogDescription>Load a saved query into the editor.</DialogDescription>
        </DialogHeader>
        {queries.length === 0 ? (
          <p className="py-6 text-center text-sm text-muted-foreground">
            No saved queries yet. Use <span className="font-medium">Save</span> to add one.
          </p>
        ) : (
          <div className="space-y-2">
            {queries.map((q) => (
              <div
                key={q.id}
                className="flex items-start gap-3 rounded-md border p-2.5 hover:bg-secondary/40"
              >
                <button
                  type="button"
                  onClick={() => onLoad(q)}
                  className="min-w-0 flex-1 text-left"
                >
                  <div className="text-sm font-medium">{q.name}</div>
                  <div className="truncate font-mono text-xs text-muted-foreground">{q.sql}</div>
                </button>
                <Button variant="outline" size="sm" onClick={() => onLoad(q)}>
                  Load
                </Button>
                <Button
                  variant="ghost"
                  size="icon"
                  className="h-8 w-8 text-destructive hover:bg-destructive/10 hover:text-destructive"
                  onClick={() => setQueries(deleteSavedQuery(q.id))}
                  aria-label={`Delete ${q.name}`}
                >
                  <Trash2 className="h-4 w-4" />
                </Button>
              </div>
            ))}
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}
