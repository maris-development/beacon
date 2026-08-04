import * as React from "react";
import { useNavigate } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  ArrowDown,
  ArrowUp,
  Check,
  ChevronRight,
  CornerLeftUp,
  Download,
  Eye,
  FileStack,
  Folder,
  Loader2,
  MoreHorizontal,
  Search,
  Table2,
  Terminal,
  Trash2,
  Upload,
  X,
} from "lucide-react";

import { cn } from "@/lib/utils";
import { useBeacon } from "@/lib/beacon-context";
import { COLUMN_PAGE_SIZE, parseSchema } from "@/lib/schema";
import { errorMessage } from "@/lib/errors";
import { PageContainer } from "@/components/app-shell";
import { InfoBanner } from "@/components/info-banner";
import { ResultsGrid } from "@/components/results-grid";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
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
import { Label } from "@/components/ui/label";
import { CheckboxField } from "@/components/ui/checkbox-field";
import { formatBytes } from "@/lib/format";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";

/** Number of rows to fetch for a dataset preview. */
const PREVIEW_ROWS = 10;

interface DatasetItem {
  path: string;
  format?: string;
  size?: number;
  lastModified?: string;
}

/** Maps a dataset `format` id to the query DSL's `from` format key. */
function fromKey(format: string | undefined): string {
  if (!format) return "parquet";
  const map: Record<string, string> = { nc: "netcdf" };
  return map[format] ?? format;
}

/**
 * Maps a dataset `format` id to the table function that reads it. Note the names
 * are not always `read_<format>` (e.g. ODV's `txt` → `read_odv_ascii`).
 */
const READ_FUNCTIONS: Record<string, string> = {
  nc: "read_netcdf",
  parquet: "read_parquet",
  csv: "read_csv",
  arrow: "read_arrow",
  zarr: "read_zarr",
  geoparquet: "read_geoparquet",
  txt: "read_odv_ascii",
  tiff: "read_tiff",
  bbf: "read_bbf",
  atlas: "read_atlas",
};

/**
 * Build a `SELECT * FROM read_<fmt>(['<path>']) LIMIT 100` query for a dataset
 * file, escaping single quotes in the path. Falls back to `read_parquet` for an
 * undetected format (consistent with the preview's default).
 */
function readQuery(item: DatasetItem): string {
  const fn = (item.format && READ_FUNCTIONS[item.format]) ?? "read_parquet";
  const escaped = item.path.replace(/'/g, "''");
  return `SELECT *\nFROM ${fn}(['${escaped}'])\nLIMIT 100;`;
}

/** The dialog target: a dataset path/format and which tab to open. */
interface DialogTarget {
  path: string;
  format?: string;
  tab: "preview" | "schema";
}

function normalize(raw: unknown): DatasetItem[] {
  if (!Array.isArray(raw)) return [];
  return raw.map((item) => {
    if (typeof item === "string") return { path: item };
    const obj = item as Record<string, unknown>;
    const path = (obj.path ?? obj.file ?? obj.file_path ?? "") as string;
    const format = (obj.format ?? obj.file_format) as string | undefined;
    const size = typeof obj.size === "number" ? obj.size : undefined;
    const lastModified = typeof obj.last_modified === "string" ? obj.last_modified : undefined;
    return { path, format, size, lastModified };
  });
}

interface FolderEntry {
  name: string;
  count: number;
}

/** Splits the datasets under `prefix` into immediate sub-folders and files. */
function browse(items: DatasetItem[], prefix: string, filterText: string) {
  const folderCounts = new Map<string, number>();
  const files: DatasetItem[] = [];
  for (const it of items) {
    if (prefix && !it.path.startsWith(prefix)) continue;
    const rest = it.path.slice(prefix.length);
    if (!rest) continue;
    const slash = rest.indexOf("/");
    if (slash >= 0) {
      const folder = rest.slice(0, slash);
      folderCounts.set(folder, (folderCounts.get(folder) ?? 0) + 1);
    } else {
      files.push(it);
    }
  }
  const f = filterText.trim().toLowerCase();
  let folders: FolderEntry[] = [...folderCounts.entries()]
    .map(([name, count]) => ({ name, count }))
    .sort((a, b) => a.name.localeCompare(b.name));
  files.sort((a, b) => a.path.localeCompare(b.path));
  let fileList = files;
  if (f) {
    folders = folders.filter((x) => x.name.toLowerCase().includes(f));
    fileList = files.filter((x) => baseName(x.path).toLowerCase().includes(f));
  }
  return { folders, files: fileList };
}

const baseName = (p: string) => p.slice(p.lastIndexOf("/") + 1);

/** The parent folder prefix of a folder prefix ("a/b/" -> "a/", "a/" -> ""). */
function parentPrefix(p: string): string {
  const trimmed = p.replace(/\/$/, "");
  const idx = trimmed.lastIndexOf("/");
  return idx >= 0 ? trimmed.slice(0, idx + 1) : "";
}

/** One rendered row: the up (..) entry, a sub-folder, or a file. */
type ListRow =
  | { type: "up" }
  | { type: "folder"; name: string; count: number }
  | { type: "file"; item: DatasetItem; label: string };

/** Fixed row height (px) and shared column grid — must match between header and rows. */
const ROW_H = 40;
const ROW_GRID =
  "grid grid-cols-[minmax(0,1fr)_4rem_5rem_7rem_11rem] items-center gap-2";

function rowKey(row: ListRow): React.Key {
  if (row.type === "file") return row.item.path;
  if (row.type === "folder") return "dir:" + row.name;
  return "up";
}

/** Sortable columns and the active sort state. */
type SortKey = "name" | "size" | "modified";
interface SortState {
  key: SortKey;
  dir: "asc" | "desc";
}

/** Compact last-modified label, e.g. "Jun 28, 20:10". Empty when unknown. */
function formatModified(iso: string | undefined): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

/** Order two files by the active sort (folders are ordered separately, by name). */
function compareFiles(a: DatasetItem, b: DatasetItem, sort: SortState): number {
  let cmp: number;
  if (sort.key === "size") cmp = (a.size ?? -1) - (b.size ?? -1);
  else if (sort.key === "modified") cmp = (a.lastModified ?? "").localeCompare(b.lastModified ?? "");
  else cmp = a.path.localeCompare(b.path);
  return sort.dir === "asc" ? cmp : -cmp;
}

/**
 * Minimal fixed-height window: renders only the rows overlapping the viewport
 * (plus a small overscan), positioned inside a full-height spacer. Keeps the
 * datasets list responsive even with tens of thousands of rows, without pulling
 * in a virtualization dependency. Remount (via `key`) to reset scroll.
 */
function VirtualList<T>({
  items,
  rowHeight,
  height,
  getKey,
  renderRow,
}: {
  items: T[];
  rowHeight: number;
  height: number;
  getKey: (item: T) => React.Key;
  renderRow: (item: T) => React.ReactNode;
}) {
  const [scrollTop, setScrollTop] = React.useState(0);
  const overscan = 8;
  const start = Math.max(0, Math.floor(scrollTop / rowHeight) - overscan);
  const end = Math.min(items.length, start + Math.ceil(height / rowHeight) + overscan * 2);
  return (
    <div
      className="overflow-auto"
      style={{ height }}
      onScroll={(e) => setScrollTop(e.currentTarget.scrollTop)}
    >
      <div style={{ height: items.length * rowHeight, position: "relative" }}>
        <div style={{ transform: `translateY(${start * rowHeight}px)` }}>
          {items.slice(start, end).map((item) => (
            <div key={getKey(item)} style={{ height: rowHeight }}>
              {renderRow(item)}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

export function DatasetsPage() {
  const beacon = useBeacon();
  const qc = useQueryClient();
  const navigate = useNavigate();
  const [path, setPath] = React.useState(""); // current folder prefix, "" = root, else ends with "/"
  const [search, setSearch] = React.useState(""); // global path search; non-empty switches to flat results
  const [sort, setSort] = React.useState<SortState>({ key: "name", dir: "asc" });
  const [dialog, setDialog] = React.useState<DialogTarget | null>(null);
  const [banner, setBanner] = React.useState<{ kind: "error" | "info"; text: string } | null>(null);
  const [uploadOpen, setUploadOpen] = React.useState(false);
  const [droppedItems, setDroppedItems] = React.useState<UploadItem[]>([]);
  const [dragging, setDragging] = React.useState(false);
  // Depth counter so child elements firing dragenter/dragleave don't flicker the
  // overlay; it only clears when we've truly left the drop zone.
  const dragDepth = React.useRef(0);

  const refreshDatasets = React.useCallback(() => {
    qc.invalidateQueries({ queryKey: ["datasets-all"] });
    qc.invalidateQueries({ queryKey: ["total-datasets"] });
  }, [qc]);

  const deleteMutation = useMutation({
    mutationFn: (p: string) => beacon.admin.deleteDataset(p),
    onSuccess: () => {
      setBanner({ kind: "info", text: "File deleted." });
      refreshDatasets();
    },
    onError: (e) => setBanner({ kind: "error", text: errorMessage(e) }),
  });

  async function download(p: string) {
    setBanner(null);
    try {
      const blob = await beacon.admin.downloadDataset(p);
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = baseName(p);
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
    } catch (e) {
      setBanner({ kind: "error", text: errorMessage(e) });
    }
  }

  // Open the query editor pre-filled with a table-function query for this file.
  function openInQuery(item: DatasetItem) {
    navigate("/query", { state: { sql: readQuery(item) } });
  }

  function openUpload(items: UploadItem[]) {
    setDroppedItems(items);
    setUploadOpen(true);
  }

  // Drag-and-drop: dropping files opens the upload dialog pre-loaded with them and
  // the current folder as the destination (so you can still review before upload).
  const isFileDrag = (e: React.DragEvent) => e.dataTransfer.types.includes("Files");
  function onDragEnter(e: React.DragEvent) {
    if (!isFileDrag(e)) return;
    e.preventDefault();
    dragDepth.current += 1;
    setDragging(true);
  }
  function onDragOver(e: React.DragEvent) {
    if (isFileDrag(e)) e.preventDefault(); // mark this a valid drop target
  }
  function onDragLeave() {
    dragDepth.current = Math.max(0, dragDepth.current - 1);
    if (dragDepth.current === 0) setDragging(false);
  }
  function onDrop(e: React.DragEvent) {
    if (!isFileDrag(e)) return;
    e.preventDefault();
    dragDepth.current = 0;
    setDragging(false);
    // Expand folders (async) into a flat list of items; entries are read
    // synchronously inside itemsFromDataTransfer before any await.
    void itemsFromDataTransfer(e.dataTransfer).then((items) => {
      if (items.length > 0) openUpload(items);
    });
  }

  function onUploaded(folder: string, uploaded: number, failed: number) {
    // Refresh the listing and move to the destination folder so the new files are
    // visible behind the dialog. The dialog itself stays open (showing the per-file
    // ✓/✗ status and summary) until the user dismisses it.
    refreshDatasets();
    goTo(folder);
    setBanner(
      failed > 0
        ? {
            kind: "error",
            text: `${failed} file${failed === 1 ? "" : "s"} failed to upload${
              uploaded > 0 ? `, ${uploaded} succeeded` : ""
            } — see the list for details.`,
          }
        : { kind: "info", text: `Uploaded ${uploaded} file${uploaded === 1 ? "" : "s"}.` },
    );
  }

  const totalQuery = useQuery({ queryKey: ["total-datasets"], queryFn: () => beacon.totalDatasets() });
  const datasetsQuery = useQuery({
    queryKey: ["datasets-all"],
    queryFn: async () => normalize(await beacon.datasets({ limit: 100_000 })),
  });

  const items = datasetsQuery.data ?? [];
  const searching = search.trim().length > 0;
  const crumbs = path ? path.replace(/\/$/, "").split("/") : [];

  // Unified, virtualizable row list. A non-empty search switches to a flat list
  // of all matching files (by full path) across every folder; otherwise it's the
  // current folder's sub-folders and files.
  const rows = React.useMemo<ListRow[]>(() => {
    if (searching) {
      const q = search.trim().toLowerCase();
      return items
        .filter((it) => it.path.toLowerCase().includes(q))
        .sort((a, b) => compareFiles(a, b, sort))
        .map((item) => ({ type: "file", item, label: item.path }));
    }
    const { folders, files } = browse(items, path, "");
    const list: ListRow[] = [];
    if (path) list.push({ type: "up" });
    // Folders stay grouped on top (name-ordered); files follow the active sort.
    for (const f of folders) list.push({ type: "folder", name: f.name, count: f.count });
    for (const file of [...files].sort((a, b) => compareFiles(a, b, sort)))
      list.push({ type: "file", item: file, label: baseName(file.path) });
    return list;
  }, [items, path, search, searching, sort]);

  function toggleSort(key: SortKey) {
    setSort((s) => (s.key === key ? { key, dir: s.dir === "asc" ? "desc" : "asc" } : { key, dir: "asc" }));
  }

  function sortHeader(label: string, col: SortKey, align: "left" | "right" = "left") {
    const active = sort.key === col;
    return (
      <button
        onClick={() => toggleSort(col)}
        className={cn(
          "flex items-center gap-1 hover:text-foreground",
          align === "right" && "justify-end",
        )}
      >
        {label}
        {active &&
          (sort.dir === "asc" ? (
            <ArrowUp className="h-3 w-3" />
          ) : (
            <ArrowDown className="h-3 w-3" />
          ))}
      </button>
    );
  }

  function enter(folder: string) {
    setPath(path + folder + "/");
    setSearch("");
  }
  function goTo(prefix: string) {
    setPath(prefix);
    setSearch("");
  }

  function renderRow(row: ListRow): React.ReactNode {
    const rowClass = ROW_GRID + " h-full w-full border-b px-3 text-xs hover:bg-muted/50";
    if (row.type === "up") {
      return (
        <button onClick={() => goTo(parentPrefix(path))} className={rowClass + " text-left"}>
          <span className="flex items-center gap-1.5 font-mono text-muted-foreground">
            <CornerLeftUp className="h-3.5 w-3.5 shrink-0" /> ..
          </span>
          <span />
          <span />
          <span />
          <span />
        </button>
      );
    }
    if (row.type === "folder") {
      return (
        <button onClick={() => enter(row.name)} className={rowClass + " text-left"}>
          <span className="flex min-w-0 items-center gap-1.5 font-mono">
            <Folder className="h-3.5 w-3.5 shrink-0 text-primary" />
            <span className="truncate">{row.name}</span>
          </span>
          <span className="text-muted-foreground">
            {row.count} item{row.count === 1 ? "" : "s"}
          </span>
          <span />
          <span />
          <span />
        </button>
      );
    }
    const d = row.item;
    return (
      <div className={rowClass}>
        <button
          className="flex min-w-0 items-center gap-1.5 text-left font-mono hover:text-primary hover:underline"
          onClick={() => setDialog({ path: d.path, format: d.format, tab: "preview" })}
          title={d.path}
        >
          <FileStack className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
          <span className="truncate">{row.label}</span>
        </button>
        <span className="min-w-0 truncate">
          {d.format ? <Badge variant="muted">{d.format}</Badge> : "—"}
        </span>
        <span className="truncate text-right tabular-nums text-muted-foreground">
          {typeof d.size === "number" ? formatBytes(d.size) : "—"}
        </span>
        <span className="truncate text-muted-foreground" title={d.lastModified}>
          {formatModified(d.lastModified)}
        </span>
        <div className="flex justify-end gap-1">
          <Button
            variant="outline"
            size="sm"
            className="h-6 gap-1 px-2 text-[11px]"
            onClick={() => setDialog({ path: d.path, format: d.format, tab: "preview" })}
          >
            <Eye className="h-3 w-3" /> Preview
          </Button>
          <Button
            variant="outline"
            size="sm"
            className="h-6 gap-1 px-2 text-[11px]"
            onClick={() => setDialog({ path: d.path, format: d.format, tab: "schema" })}
          >
            <Table2 className="h-3 w-3" /> Schema
          </Button>
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="ghost" size="sm" className="h-6 w-6 p-0" title="More actions">
                <MoreHorizontal className="h-4 w-4" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              <DropdownMenuItem onClick={() => openInQuery(d)}>
                <Terminal className="mr-2 h-3.5 w-3.5" /> Query
              </DropdownMenuItem>
              <DropdownMenuItem onClick={() => download(d.path)}>
                <Download className="mr-2 h-3.5 w-3.5" /> Download
              </DropdownMenuItem>
              <DropdownMenuItem
                className="text-destructive focus:text-destructive"
                disabled={deleteMutation.isPending}
                onClick={() => {
                  if (
                    window.confirm(
                      `Delete "${baseName(d.path)}"? This permanently removes the file from the datasets store.`,
                    )
                  ) {
                    setBanner(null);
                    deleteMutation.mutate(d.path);
                  }
                }}
              >
                <Trash2 className="mr-2 h-3.5 w-3.5" /> Delete
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
      </div>
    );
  }

  return (
    <PageContainer
      title="Datasets"
      description="Browse the datasets store. Preview rows or inspect a file's schema."
      actions={
        <div className="flex items-center gap-2">
          {typeof totalQuery.data === "number" && (
            <Badge variant="secondary">{totalQuery.data} total</Badge>
          )}
          <Button size="sm" className="gap-1.5" onClick={() => openUpload([])}>
            <Upload className="h-4 w-4" />
            Upload
          </Button>
        </div>
      }
    >
      <InfoBanner>
        Datasets are the raw files discovered in the datasets store. Browse folders, preview the
        first rows, or inspect a file&rsquo;s schema. To query a file, hit <strong>Query</strong> to
        open the editor on a table function (e.g.{" "}
        <span className="font-mono">read_netcdf(&hellip;)</span>), or register a reusable table from
        the <strong>Tables</strong> page (External table) or via a Crawler. <strong>Upload</strong>{" "}
        adds files at any path you choose &mdash; type a destination folder to drop them into an
        existing folder or create a new one.
      </InfoBanner>
      {banner && (
        <div
          className={cn(
            "mb-3 rounded-md border px-3 py-2 text-sm",
            banner.kind === "error"
              ? "border-destructive/40 text-destructive"
              : "border-border text-muted-foreground",
          )}
        >
          {banner.text}
        </div>
      )}
      <div className="mb-3 flex items-center gap-3">
        <nav className="flex flex-wrap items-center gap-1 text-sm">
          <button
            onClick={() => goTo("")}
            className={cn("hover:text-primary", path === "" ? "font-medium" : "text-muted-foreground")}
          >
            datasets
          </button>
          {crumbs.map((c, i) => (
            <span key={i} className="flex items-center gap-1">
              <ChevronRight className="h-3.5 w-3.5 text-muted-foreground" />
              <button
                onClick={() => goTo(crumbs.slice(0, i + 1).join("/") + "/")}
                className={cn(
                  "hover:text-primary",
                  i === crumbs.length - 1 ? "font-medium" : "text-muted-foreground",
                )}
              >
                {c}
              </button>
            </span>
          ))}
        </nav>
        <div className="relative ml-auto w-72">
          <Search className="absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Search all files by path…"
            className="h-8 pl-8"
          />
        </div>
      </div>

      <div
        className="relative"
        onDragEnter={onDragEnter}
        onDragOver={onDragOver}
        onDragLeave={onDragLeave}
        onDrop={onDrop}
      >
        {dragging && (
          <div className="pointer-events-none absolute inset-0 z-10 flex items-center justify-center rounded-lg border-2 border-dashed border-primary bg-primary/5 text-sm font-medium text-primary">
            <span className="flex items-center gap-2">
              <Upload className="h-4 w-4" />
              Drop to upload to {path || "the datasets root"}
            </span>
          </div>
        )}
        <Card className="overflow-hidden">
          {datasetsQuery.isLoading && (
            <div className="flex items-center gap-2 p-4 text-sm text-muted-foreground">
              <Loader2 className="h-4 w-4 animate-spin" /> Loading datasets…
            </div>
          )}
        {datasetsQuery.isError && (
          <div className="p-4 text-sm text-destructive">{errorMessage(datasetsQuery.error)}</div>
        )}
        {datasetsQuery.data && rows.length === 0 && (
          <div className="p-4 text-sm text-muted-foreground">
            {searching ? `No files match “${search.trim()}”.` : "This folder is empty."}
          </div>
        )}
        {datasetsQuery.data && rows.length > 0 && (
          <>
            {searching && (
              <div className="border-b px-3 py-1.5 text-xs text-muted-foreground">
                {rows.length.toLocaleString()} match{rows.length === 1 ? "" : "es"} across all folders
              </div>
            )}
            <div className={ROW_GRID + " border-b bg-muted/30 px-3 py-2 text-xs font-medium text-muted-foreground"}>
              {sortHeader("Name", "name")}
              <span>Format</span>
              {sortHeader("Size", "size", "right")}
              {sortHeader("Modified", "modified")}
              <span className="text-right">Actions</span>
            </div>
            <VirtualList
              key={searching ? "search:" + search.trim() : "path:" + path}
              items={rows}
              rowHeight={ROW_H}
              height={Math.min(rows.length * ROW_H, 560)}
              getKey={rowKey}
              renderRow={renderRow}
            />
          </>
        )}
        </Card>
      </div>

      <Dialog open={dialog !== null} onOpenChange={(o) => !o && setDialog(null)}>
        <DialogContent className="max-w-4xl grid-cols-1">
          <DialogHeader>
            <DialogTitle className="break-all font-mono text-sm">{dialog?.path}</DialogTitle>
          </DialogHeader>
          {dialog && (
            <Tabs defaultValue={dialog.tab} key={dialog.path}>
              <TabsList>
                <TabsTrigger value="preview">Preview</TabsTrigger>
                <TabsTrigger value="schema">Schema</TabsTrigger>
              </TabsList>
              <TabsContent value="preview">
                <DatasetPreview path={dialog.path} format={dialog.format} />
              </TabsContent>
              <TabsContent value="schema">
                <DatasetSchema file={dialog.path} />
              </TabsContent>
            </Tabs>
          )}
        </DialogContent>
      </Dialog>

      <UploadDialog
        open={uploadOpen}
        onOpenChange={setUploadOpen}
        initialFolder={path}
        initialItems={droppedItems}
        onUploaded={onUploaded}
        onError={(text) => setBanner({ kind: "error", text })}
      />
    </PageContainer>
  );
}

/** A file plus its path relative to the destination, preserving dropped/picked folder structure. */
interface UploadItem {
  file: File;
  relPath: string;
}

/** Per-file upload status, keyed by `relPath`, shown in the dialog's file list. */
interface ItemStatus {
  state: "uploading" | "done" | "error";
  error?: string;
}

const totalBytes = (items: UploadItem[]) => items.reduce((sum, it) => sum + it.file.size, 0);

/** Map a flat FileList to upload items, honoring `webkitRelativePath` (set when a folder is picked). */
function itemsFromFileList(list: FileList): UploadItem[] {
  return Array.from(list).map((file) => ({ file, relPath: file.webkitRelativePath || file.name }));
}

/** Recursively expand a dropped file-system entry into upload items under `prefix`. */
async function expandEntry(entry: FileSystemEntry, prefix: string): Promise<UploadItem[]> {
  if (entry.isFile) {
    const file = await new Promise<File>((resolve, reject) =>
      (entry as FileSystemFileEntry).file(resolve, reject),
    );
    return [{ file, relPath: prefix + entry.name }];
  }
  const reader = (entry as FileSystemDirectoryEntry).createReader();
  // readEntries returns the directory in batches; call until it yields none.
  const readBatch = () =>
    new Promise<FileSystemEntry[]>((resolve, reject) => reader.readEntries(resolve, reject));
  const items: UploadItem[] = [];
  for (let batch = await readBatch(); batch.length > 0; batch = await readBatch()) {
    for (const child of batch) {
      items.push(...(await expandEntry(child, `${prefix}${entry.name}/`)));
    }
  }
  return items;
}

/**
 * Turn a drop's `DataTransfer` into upload items, expanding any dropped folders.
 * Entries are only valid during the drop event, so they must be captured
 * synchronously (the loop below) before the async directory traversal.
 */
async function itemsFromDataTransfer(dt: DataTransfer): Promise<UploadItem[]> {
  const entries: FileSystemEntry[] = [];
  const looseFiles: File[] = [];
  for (const item of Array.from(dt.items)) {
    if (item.kind !== "file") continue;
    const entry = item.webkitGetAsEntry?.();
    if (entry) entries.push(entry);
    else {
      const file = item.getAsFile();
      if (file) looseFiles.push(file);
    }
  }
  const nested = (await Promise.all(entries.map((entry) => expandEntry(entry, "")))).flat();
  return [...nested, ...looseFiles.map((file) => ({ file, relPath: file.name }))];
}

/**
 * Normalize a user-typed destination folder into an object-store key prefix:
 * trims, drops leading slashes, collapses repeats, and ensures a single trailing
 * slash. "" means the datasets root.
 */
function normalizeFolder(input: string): string {
  const p = input.trim().replace(/^\/+/, "").replace(/\/{2,}/g, "/");
  if (!p) return "";
  return p.endsWith("/") ? p : p + "/";
}

/**
 * Upload dialog: pick files and choose the destination folder. Files are written
 * to `<folder><filename>`, so typing a new (possibly nested) folder is how you
 * create one — object storage has no empty folders. Large files use the SDK's
 * resumable chunked path automatically; a per-file progress bar is shown.
 */
function UploadDialog({
  open,
  onOpenChange,
  initialFolder,
  initialItems,
  onUploaded,
  onError,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  initialFolder: string;
  initialItems?: UploadItem[];
  onUploaded: (folder: string, uploaded: number, failed: number) => void;
  onError: (message: string) => void;
}) {
  const beacon = useBeacon();
  const [folder, setFolder] = React.useState(initialFolder);
  const [overwrite, setOverwrite] = React.useState(false);
  const [selected, setSelected] = React.useState<UploadItem[]>([]);
  const [progress, setProgress] = React.useState<{
    name: string;
    index: number;
    count: number;
    pct: number;
  } | null>(null);
  const [statuses, setStatuses] = React.useState<Record<string, ItemStatus>>({});
  const fileInputRef = React.useRef<HTMLInputElement>(null);
  const folderInputRef = React.useRef<HTMLInputElement | null>(null);

  // Seed the folder/items from the props ONLY when the dialog opens. Keying this
  // on `open` alone (not initialFolder/initialItems) is deliberate: after a
  // successful upload the page navigates to the destination folder, which changes
  // `initialFolder` — if that re-ran the reset, it would wipe the just-finished
  // ✓/✗ status while the dialog is still open. The dialog must persist its result
  // until the user dismisses it.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  React.useEffect(() => {
    if (open) {
      setFolder(initialFolder);
      setSelected(initialItems ?? []);
      setStatuses({});
      setOverwrite(false);
      setProgress(null);
    }
  }, [open]);

  // `webkitdirectory` is not a typed React prop, so set it via a callback ref the
  // moment the element mounts — more reliable than an effect, which can run before
  // the (portal-mounted) input is attached. With it set, the second picker selects
  // a whole folder; files keep their relative paths via `webkitRelativePath`.
  const folderInputCallbackRef = React.useCallback((el: HTMLInputElement | null) => {
    folderInputRef.current = el;
    if (el) {
      el.setAttribute("webkitdirectory", "");
      el.setAttribute("directory", "");
    }
  }, []);

  const dest = normalizeFolder(folder);
  const bytes = totalBytes(selected);

  const hasErrors = Object.values(statuses).some((s) => s.state === "error");

  // Live tally for the summary line, by per-file status.
  const counts = selected.reduce(
    (acc, it) => {
      const state = statuses[it.relPath]?.state;
      if (state === "done") acc.done += 1;
      else if (state === "error") acc.failed += 1;
      else if (state === "uploading") acc.uploading += 1;
      else acc.pending += 1;
      return acc;
    },
    { done: 0, failed: 0, uploading: 0, pending: 0 },
  );
  const started = counts.done + counts.failed + counts.uploading > 0;
  const allDone = selected.length > 0 && counts.done === selected.length;

  const upload = useMutation({
    // Upload each file independently: a failure marks just that file and the run
    // continues, so one bad file in a big folder doesn't abort the rest. Overall
    // progress is completed bytes plus the current file's reported bytes, over the
    // grand total. Files already uploaded in a prior attempt are skipped (so the
    // Upload button doubles as "retry the failed ones").
    mutationFn: async () => {
      let bytesDone = 0;
      let uploaded = 0;
      let failed = 0;
      for (const [index, item] of selected.entries()) {
        if (statuses[item.relPath]?.state === "done") {
          bytesDone += item.file.size;
          uploaded += 1;
          continue;
        }
        const update = (extra: number) =>
          setProgress({
            name: item.relPath,
            index,
            count: selected.length,
            pct: bytes > 0 ? Math.round(((bytesDone + extra) / bytes) * 100) : 100,
          });
        update(0); // mark the file started — small (single-shot) files emit no onProgress
        setStatuses((s) => ({ ...s, [item.relPath]: { state: "uploading" } }));
        try {
          await beacon.admin.uploadDataset(dest + item.relPath, item.file, {
            overwrite,
            onProgress: ({ uploaded: u }) => update(u),
          });
          setStatuses((s) => ({ ...s, [item.relPath]: { state: "done" } }));
          uploaded += 1;
        } catch (e) {
          setStatuses((s) => ({ ...s, [item.relPath]: { state: "error", error: errorMessage(e) } }));
          failed += 1;
        }
        bytesDone += item.file.size;
      }
      return { uploaded, failed };
    },
    // Per-file errors are caught above, so this resolves even on partial failure;
    // the page decides whether to close the dialog (only on a fully clean run).
    onSuccess: ({ uploaded, failed }) => onUploaded(dest, uploaded, failed),
    onError: (e) => onError(errorMessage(e)),
    onSettled: () => setProgress(null),
  });

  return (
    <Dialog open={open} onOpenChange={(o) => !upload.isPending && onOpenChange(o)}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Upload files</DialogTitle>
          <DialogDescription>
            Files are written into the destination folder, keeping the structure of any folders you
            add. A new folder is created automatically when the first file lands in it.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4">
          <div className="space-y-1.5">
            <Label htmlFor="upload-dest">Destination folder</Label>
            <Input
              id="upload-dest"
              value={folder}
              placeholder="(datasets root)"
              onChange={(e) => setFolder(e.target.value)}
              className="font-mono text-xs"
            />
            <p className="text-xs text-muted-foreground">
              Use <span className="font-mono">/</span> to nest, e.g.{" "}
              <span className="font-mono">ctd/cruise42/</span>. Leave empty for the root.
            </p>
          </div>

          <div>
            <input
              ref={fileInputRef}
              type="file"
              multiple
              className="hidden"
              onChange={(e) => {
                if (e.target.files) setSelected(itemsFromFileList(e.target.files));
                e.target.value = "";
              }}
            />
            <input
              ref={folderInputCallbackRef}
              type="file"
              multiple
              className="hidden"
              onChange={(e) => {
                if (e.target.files) setSelected(itemsFromFileList(e.target.files));
                e.target.value = "";
              }}
            />
            <div className="flex flex-wrap items-center gap-2">
              <Button
                type="button"
                variant="outline"
                size="sm"
                onClick={() => fileInputRef.current?.click()}
              >
                Choose files…
              </Button>
              <Button
                type="button"
                variant="outline"
                size="sm"
                onClick={() => folderInputRef.current?.click()}
              >
                Choose folder…
              </Button>
              <span className="text-xs text-muted-foreground">
                or drag &amp; drop files/folders onto the list
              </span>
            </div>
            {selected.length > 0 && (
              <>
                <p className="mt-2 text-xs text-muted-foreground">
                  {selected.length} file{selected.length === 1 ? "" : "s"} · {formatBytes(bytes)}
                  {started && (
                    <>
                      {" — "}
                      <span className="text-emerald-600">{counts.done} done</span>
                      {" · "}
                      <span className={counts.failed > 0 ? "text-destructive" : undefined}>
                        {counts.failed} failed
                      </span>
                      {counts.uploading > 0 && <>{` · ${counts.uploading} uploading`}</>}
                      {" · "}
                      {counts.pending} pending
                    </>
                  )}
                </p>
                <ul className="mt-1 max-h-44 space-y-1 overflow-auto rounded-md border p-2 text-xs">
                  {selected.map((it) => {
                    const st = statuses[it.relPath];
                    return (
                      <li
                        key={it.relPath}
                        className="flex items-center justify-between gap-3 font-mono"
                      >
                        <span className="flex min-w-0 items-center gap-1.5">
                          {st?.state === "uploading" && (
                            <Loader2 className="h-3 w-3 shrink-0 animate-spin text-muted-foreground" />
                          )}
                          {st?.state === "done" && (
                            <Check className="h-3 w-3 shrink-0 text-emerald-500" />
                          )}
                          {st?.state === "error" && (
                            <X className="h-3 w-3 shrink-0 text-destructive" />
                          )}
                          <span className="truncate" title={dest + it.relPath}>
                            {dest}
                            {it.relPath}
                          </span>
                        </span>
                        <span className="shrink-0 text-muted-foreground">
                          {st?.state === "error" ? (
                            <span className="text-destructive" title={st.error}>
                              failed
                            </span>
                          ) : (
                            formatBytes(it.file.size)
                          )}
                        </span>
                      </li>
                    );
                  })}
                </ul>
              </>
            )}
          </div>

          <CheckboxField
            checked={overwrite}
            onChange={setOverwrite}
            label="Overwrite existing files"
            hint="Replace a file if one already exists at the same path (otherwise the upload fails)."
          />

          {progress && (
            <div className="rounded-md border border-border px-3 py-2">
              <div className="mb-1 flex items-center justify-between gap-2 text-xs text-muted-foreground">
                <span className="truncate font-mono">
                  Uploading {Math.min(progress.index + 1, progress.count)} of {progress.count}:{" "}
                  {progress.name}
                </span>
                <span className="tabular-nums">{progress.pct}%</span>
              </div>
              <div className="h-1.5 overflow-hidden rounded-full bg-muted">
                <div
                  className="h-full bg-primary transition-[width] duration-200"
                  style={{ width: `${progress.pct}%` }}
                />
              </div>
            </div>
          )}
        </div>

        <DialogFooter>
          <Button variant="ghost" onClick={() => onOpenChange(false)} disabled={upload.isPending}>
            {started && !upload.isPending ? "Close" : "Cancel"}
          </Button>
          <Button
            className="gap-1.5"
            disabled={selected.length === 0 || upload.isPending || allDone}
            onClick={() => upload.mutate()}
          >
            {upload.isPending ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : allDone ? (
              <Check className="h-4 w-4" />
            ) : (
              <Upload className="h-4 w-4" />
            )}
            {allDone
              ? "Uploaded"
              : hasErrors
                ? "Retry failed"
                : `Upload${selected.length > 0 ? ` ${selected.length} file${selected.length === 1 ? "" : "s"}` : ""}`}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function DatasetPreview({ path, format }: { path: string; format?: string }) {
  const beacon = useBeacon();
  const query = useQuery({
    queryKey: ["dataset-preview", path],
    queryFn: async () => {
      const cols = parseSchema(await beacon.datasetSchema(path)).map((c) => c.name);
      if (cols.length === 0) return { rows: [] as Record<string, unknown>[], table: undefined };
      const { rows, table } = await beacon.query({
        select: cols.map((name) => ({ column: name })),
        from: { [fromKey(format)]: { paths: [path] } },
        limit: PREVIEW_ROWS,
      });
      return { rows, table };
    },
  });

  if (query.isLoading)
    return (
      <div className="flex items-center gap-2 py-6 text-sm text-muted-foreground">
        <Loader2 className="h-4 w-4 animate-spin" /> Loading preview…
      </div>
    );
  if (query.isError)
    return (
      <div className="py-4 text-sm text-destructive">
        Could not preview this file: {errorMessage(query.error)}
      </div>
    );

  const rows = query.data?.rows ?? [];
  return (
    <div className="space-y-2">
      <div className="max-h-[60vh] overflow-auto rounded-md border">
        <ResultsGrid rows={rows} table={query.data?.table} />
      </div>
      {rows.length > 0 && (
        <p className="text-xs text-muted-foreground">First {rows.length} rows.</p>
      )}
    </div>
  );
}

function DatasetSchema({ file }: { file: string }) {
  const beacon = useBeacon();
  const query = useQuery({
    queryKey: ["dataset-schema", file],
    queryFn: async () => parseSchema(await beacon.datasetSchema(file)),
  });
  const [filter, setFilter] = React.useState("");
  const [visible, setVisible] = React.useState(COLUMN_PAGE_SIZE);

  if (query.isLoading)
    return (
      <div className="flex items-center gap-2 py-4 text-sm text-muted-foreground">
        <Loader2 className="h-4 w-4 animate-spin" /> Reading schema…
      </div>
    );
  if (query.isError) return <div className="py-4 text-sm text-destructive">{errorMessage(query.error)}</div>;

  const columns = query.data ?? [];
  const needle = filter.trim().toLowerCase();
  const filtered = needle
    ? columns.filter(
        (c) => c.name.toLowerCase().includes(needle) || c.dataType.toLowerCase().includes(needle),
      )
    : columns;

  return (
    <div className="space-y-2">
      <div className="relative">
        <Search className="absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
        <Input
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder="Filter columns (e.g. TEMP)"
          className="h-8 pl-8"
        />
      </div>
      <div className="max-h-[55vh] overflow-auto rounded-md border">
        <Table className="text-[13px] [&_td]:py-1 [&_th]:h-8">
          <TableHeader>
            <TableRow>
              <TableHead>Column</TableHead>
              <TableHead>Type</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {filtered.length === 0 && (
              <TableRow>
                <TableCell colSpan={2} className="py-4 text-center text-muted-foreground">
                  No columns match “{filter}”.
                </TableCell>
              </TableRow>
            )}
            {filtered.slice(0, visible).map((c) => (
              <TableRow key={c.name}>
                <TableCell className="font-mono">{c.name}</TableCell>
                <TableCell className="font-mono text-muted-foreground">{c.dataType}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
      {filtered.length > 0 && (
        <div className="flex items-center gap-3 text-xs text-muted-foreground">
          <span>
            Showing {Math.min(visible, filtered.length).toLocaleString()} of{" "}
            {filtered.length.toLocaleString()}
            {needle ? ` matching columns (${columns.length.toLocaleString()} total)` : " columns"}
          </span>
          {filtered.length > visible && (
            <>
              <Button
                variant="outline"
                size="sm"
                onClick={() => setVisible((v) => v + COLUMN_PAGE_SIZE)}
              >
                Show {Math.min(filtered.length - visible, COLUMN_PAGE_SIZE)} more
              </Button>
              <Button variant="ghost" size="sm" onClick={() => setVisible(filtered.length)}>
                Show all
              </Button>
            </>
          )}
        </div>
      )}
    </div>
  );
}
