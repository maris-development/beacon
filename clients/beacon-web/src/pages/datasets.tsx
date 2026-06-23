import * as React from "react";
import { useQuery } from "@tanstack/react-query";
import { ChevronRight, CornerLeftUp, Eye, FileStack, Folder, Loader2, Search, Table2 } from "lucide-react";

import { cn } from "@/lib/utils";
import { useBeacon } from "@/lib/beacon-context";
import { COLUMN_PAGE_SIZE, parseSchema } from "@/lib/schema";
import { errorMessage } from "@/lib/errors";
import { PageContainer } from "@/components/app-shell";
import { ResultsGrid } from "@/components/results-grid";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";

/** Number of rows to fetch for a dataset preview. */
const PREVIEW_ROWS = 10;

interface DatasetItem {
  path: string;
  format?: string;
}

/** Maps a dataset `format` id to the query DSL's `from` format key. */
function fromKey(format: string | undefined): string {
  if (!format) return "parquet";
  const map: Record<string, string> = { nc: "netcdf" };
  return map[format] ?? format;
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
    return { path, format };
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

export function DatasetsPage() {
  const beacon = useBeacon();
  const [path, setPath] = React.useState(""); // current folder prefix, "" = root, else ends with "/"
  const [filter, setFilter] = React.useState("");
  const [dialog, setDialog] = React.useState<DialogTarget | null>(null);

  const totalQuery = useQuery({ queryKey: ["total-datasets"], queryFn: () => beacon.totalDatasets() });
  const datasetsQuery = useQuery({
    queryKey: ["datasets-all"],
    queryFn: async () => normalize(await beacon.datasets({ limit: 100_000 })),
  });

  const items = datasetsQuery.data ?? [];
  const { folders, files } = React.useMemo(() => browse(items, path, filter), [items, path, filter]);
  const crumbs = path ? path.replace(/\/$/, "").split("/") : [];

  function enter(folder: string) {
    setPath(path + folder + "/");
    setFilter("");
  }
  function goTo(prefix: string) {
    setPath(prefix);
    setFilter("");
  }

  return (
    <PageContainer
      title="Datasets"
      description="Browse the datasets store. Preview rows or inspect a file's schema."
      actions={
        typeof totalQuery.data === "number" ? (
          <Badge variant="secondary">{totalQuery.data} total</Badge>
        ) : undefined
      }
    >
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
        <div className="relative ml-auto w-64">
          <Search className="absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            placeholder="Filter this folder"
            className="h-8 pl-8"
          />
        </div>
      </div>

      <Card className="overflow-hidden">
        {datasetsQuery.isLoading && (
          <div className="flex items-center gap-2 p-4 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" /> Loading datasets…
          </div>
        )}
        {datasetsQuery.isError && (
          <div className="p-4 text-sm text-destructive">{errorMessage(datasetsQuery.error)}</div>
        )}
        {datasetsQuery.data && folders.length === 0 && files.length === 0 && (
          <div className="p-4 text-sm text-muted-foreground">
            {filter ? "Nothing matches in this folder." : "This folder is empty."}
          </div>
        )}
        {datasetsQuery.data && (folders.length > 0 || files.length > 0) && (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead className="w-28">Format</TableHead>
                <TableHead className="w-56 text-right">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {path !== "" && (
                <TableRow className="cursor-pointer" onClick={() => goTo(parentPrefix(path))}>
                  <TableCell className="font-mono text-xs">
                    <span className="flex items-center gap-1.5 text-muted-foreground">
                      <CornerLeftUp className="h-3.5 w-3.5 shrink-0" /> ..
                    </span>
                  </TableCell>
                  <TableCell />
                  <TableCell />
                </TableRow>
              )}
              {folders.map((d) => (
                <TableRow key={"dir:" + d.name} className="cursor-pointer" onClick={() => enter(d.name)}>
                  <TableCell className="font-mono text-xs">
                    <span className="flex items-center gap-1.5">
                      <Folder className="h-3.5 w-3.5 shrink-0 text-primary" />
                      {d.name}
                    </span>
                  </TableCell>
                  <TableCell className="text-xs text-muted-foreground">
                    {d.count} item{d.count === 1 ? "" : "s"}
                  </TableCell>
                  <TableCell />
                </TableRow>
              ))}
              {files.map((d) => (
                <TableRow key={d.path}>

                  <TableCell className="font-mono text-xs">
                    <button
                      className="flex items-center gap-1.5 text-left hover:text-primary hover:underline"
                      onClick={() => setDialog({ path: d.path, format: d.format, tab: "preview" })}
                      title="Preview rows"
                    >
                      <FileStack className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                      {baseName(d.path)}
                    </button>
                  </TableCell>
                  <TableCell className="py-0">
                    {d.format ? <Badge variant="muted">{d.format}</Badge> : "—"}
                  </TableCell>
                  <TableCell className="py-0 text-right">
                    <div className="flex justify-end gap-2">
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
                    </div>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        )}
      </Card>

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
    </PageContainer>
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
        <Table>
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
