import * as React from "react";
import { useLocation } from "react-router-dom";
import type { ReactCodeMirrorRef } from "@uiw/react-codemirror";
import { rowsFromBatch, type ArrowRecordBatch, type ArrowTable, type Row } from "@beacon/client";
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
import { SqlEditor } from "@/components/sql-editor";
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
  rows: Row[];
  /** First record batch (or full table) — carries the schema for timestamp rendering. */
  table?: ArrowTable | ArrowRecordBatch;
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

const STARTER_SQL = "SELECT 1 AS n";

/**
 * How many rows to render for a result preview. Once this many have streamed in
 * the query is aborted, so a `SELECT *` over a huge table fills the grid quickly
 * instead of downloading (and buffering) the entire result.
 */
const PREVIEW_ROW_LIMIT = 500;

export function WorkbenchPage() {
  const beacon = useBeacon();
  const location = useLocation();
  const editorRef = React.useRef<ReactCodeMirrorRef>(null);
  // Tracks the in-flight streaming query so it can be cancelled by the user.
  const abortRef = React.useRef<AbortController | null>(null);
  // Tracks the in-flight EXPLAIN ANALYZE run so it can be cancelled.
  const analyzeAbortRef = React.useRef<AbortController | null>(null);
  // Another page (e.g. Datasets → "Query") can open the editor pre-filled by
  // navigating to `/query` with `{ state: { sql } }`.
  const initialSql = (location.state as { sql?: string } | null)?.sql;
  const [sql, setSql] = React.useState(initialSql ?? STARTER_SQL);
  const [running, setRunning] = React.useState(false);
  const [explaining, setExplaining] = React.useState(false);
  const [analyzing, setAnalyzing] = React.useState(false);
  const [downloading, setDownloading] = React.useState(false);
  const [mode, setMode] = React.useState<ViewMode>("results");
  const [result, setResult] = React.useState<RunResult | null>(null);
  const [plan, setPlan] = React.useState<unknown>(null);
  const [analyzed, setAnalyzed] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  const [saveOpen, setSaveOpen] = React.useState(false);
  const [savedOpen, setSavedOpen] = React.useState(false);
  const [metricsId, setMetricsId] = React.useState<string | null>(null);

  const run = React.useCallback(async () => {
    const text = sql.trim();
    if (!text || running) return;
    setRunning(true);
    setError(null);
    setMode("results");
    setResult(null);
    const started = performance.now();
    // Stream record batches and render them as they arrive, stopping (and
    // aborting the server query) once the preview limit is reached — or when the
    // user cancels.
    const controller = new AbortController();
    abortRef.current = controller;
    try {
      const { queryId, batches } = await beacon.queryBatches(text, controller.signal);
      const rows: Row[] = [];
      let schema: ArrowRecordBatch | undefined;
      let truncated = false;
      for await (const batch of batches) {
        if (!schema) schema = batch; // the first batch carries the Arrow schema
        for (const row of rowsFromBatch<Row>(batch)) {
          if (rows.length >= PREVIEW_ROW_LIMIT) {
            truncated = true;
            break;
          }
          rows.push(row);
        }
        // New array reference so React re-renders with the rows so far.
        setResult({
          rows: rows.slice(),
          table: schema,
          queryId,
          elapsedMs: performance.now() - started,
          truncated,
        });
        if (truncated) {
          controller.abort(); // we have our preview; stop the query
          break;
        }
      }
      // No batches arrived (DDL/DML or an empty result): surface a zero-row result.
      setResult(
        (prev) =>
          prev ?? { rows: [], queryId, elapsedMs: performance.now() - started, truncated: false },
      );
    } catch (err) {
      if (controller.signal.aborted) {
        // User cancelled mid-stream: keep whatever rows already arrived. (Don't
        // relabel a result that stopped because it hit the preview limit.)
        setResult((prev) => (prev && !prev.truncated ? { ...prev, cancelled: true } : prev));
      } else {
        setResult(null);
        setError(errorMessage(err));
      }
    } finally {
      if (abortRef.current === controller) abortRef.current = null;
      setRunning(false);
    }
  }, [beacon, sql, running]);

  /** Aborts the in-flight query (if any); partial results stay on screen. */
  const cancel = React.useCallback(() => {
    abortRef.current?.abort();
  }, []);

  /** Aborts the in-flight EXPLAIN ANALYZE run (if any). */
  const cancelAnalyze = React.useCallback(() => {
    analyzeAbortRef.current?.abort();
  }, []);

  // Abort any in-flight work if the page unmounts.
  React.useEffect(
    () => () => {
      abortRef.current?.abort();
      analyzeAbortRef.current?.abort();
    },
    [],
  );

  async function explain() {
    const text = sql.trim();
    if (!text || explaining) return;
    setExplaining(true);
    setError(null);
    setMode("explain");
    setAnalyzed(false);
    try {
      setPlan(await beacon.explainQuery(text));
    } catch (err) {
      setPlan(null);
      setError(errorMessage(err));
    } finally {
      setExplaining(false);
    }
  }

  async function analyze() {
    const text = sql.trim();
    if (!text || analyzing) return;
    setAnalyzing(true);
    setError(null);
    setMode("explain");
    setAnalyzed(true);
    const controller = new AbortController();
    analyzeAbortRef.current = controller;
    try {
      setPlan(await beacon.explainAnalyzeQuery(text, controller.signal));
    } catch (err) {
      setPlan(null);
      // Swallow user cancellation; only surface real failures.
      if (!controller.signal.aborted) setError(errorMessage(err));
    } finally {
      if (analyzeAbortRef.current === controller) analyzeAbortRef.current = null;
      setAnalyzing(false);
    }
  }

  async function download(format: DownloadFormat["format"], ext: string) {
    const text = sql.trim();
    if (!text) return;
    setDownloading(true);
    setError(null);
    try {
      const res = await beacon.queryRaw(text, format);
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `beacon-result.${ext}`;
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setDownloading(false);
    }
  }

  function insert(textToInsert: string) {
    const view = editorRef.current?.view;
    if (view) {
      const { from, to } = view.state.selection.main;
      view.dispatch({
        changes: { from, to, insert: textToInsert },
        selection: { anchor: from + textToInsert.length },
      });
      view.focus();
    } else {
      setSql((prev) => (prev ? `${prev} ${textToInsert}` : textToInsert));
    }
  }

  return (
    <div className="flex h-full min-h-0">
      <div className="w-64 shrink-0">
        <DataPanel onInsert={insert} />
      </div>

      <div className="flex min-h-0 min-w-0 flex-1 flex-col">
        {/* Toolbar */}
        <div className="flex items-center gap-2 border-b bg-card px-4 py-2">
          {running ? (
            <Button onClick={cancel} variant="destructive" size="sm" className="gap-1.5">
              <Square className="h-4 w-4" />
              Stop
            </Button>
          ) : (
            <Button onClick={run} size="sm" className="gap-1.5">
              <Play className="h-4 w-4" />
              Run
            </Button>
          )}
          <Button onClick={explain} disabled={explaining} variant="outline" size="sm" className="gap-1.5">
            {explaining ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <Network className="h-4 w-4" />
            )}
            Explain
          </Button>
          {analyzing ? (
            <Button
              onClick={cancelAnalyze}
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
              title="Run the query and show its plan with execution metrics"
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
          <SqlEditor ref={editorRef} value={sql} onChange={setSql} onRun={run} />
        </div>

        {/* Results / plan header */}
        <div className="flex items-center gap-3 border-b bg-secondary/40 px-4 py-1.5 text-xs">
          <span className="font-semibold">{mode === "explain" ? "Query plan" : "Results"}</span>
          {mode === "results" && result && (
            <>
              <span className="text-muted-foreground">
                {result.rows.length} rows
                {result.truncated && ` (first ${PREVIEW_ROW_LIMIT} — query stopped)`}
                {result.cancelled && " (cancelled)"}
              </span>
              {running && <Loader2 className="h-3 w-3 animate-spin text-muted-foreground" />}
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
            <ResultsGrid rows={result.rows} table={result.table} />
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
