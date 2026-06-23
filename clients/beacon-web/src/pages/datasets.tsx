import * as React from "react";
import { useQuery } from "@tanstack/react-query";
import { Eye, FileStack, Loader2, Search, Table2 } from "lucide-react";

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

export function DatasetsPage() {
  const beacon = useBeacon();
  const [pattern, setPattern] = React.useState("");
  const [applied, setApplied] = React.useState("");
  const [dialog, setDialog] = React.useState<DialogTarget | null>(null);

  const totalQuery = useQuery({ queryKey: ["total-datasets"], queryFn: () => beacon.totalDatasets() });
  const datasetsQuery = useQuery({
    queryKey: ["datasets", applied],
    queryFn: async () => normalize(await beacon.datasets({ pattern: applied || undefined, limit: 200 })),
  });

  return (
    <PageContainer
      title="Datasets"
      description="Files discovered in the datasets store. Preview rows or inspect a file's schema."
      actions={
        typeof totalQuery.data === "number" ? (
          <Badge variant="secondary">{totalQuery.data} total</Badge>
        ) : undefined
      }
    >
      <form
        className="mb-4 flex max-w-md items-center gap-2"
        onSubmit={(e) => {
          e.preventDefault();
          setApplied(pattern.trim());
        }}
      >
        <div className="relative flex-1">
          <Search className="absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            value={pattern}
            onChange={(e) => setPattern(e.target.value)}
            placeholder="Glob pattern, e.g. **/*.nc"
            className="pl-8"
          />
        </div>
        <Button type="submit" variant="outline">
          Filter
        </Button>
      </form>

      <Card className="overflow-hidden">
        {datasetsQuery.isLoading && (
          <div className="flex items-center gap-2 p-4 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" /> Loading datasets…
          </div>
        )}
        {datasetsQuery.isError && (
          <div className="p-4 text-sm text-destructive">{errorMessage(datasetsQuery.error)}</div>
        )}
        {datasetsQuery.data && datasetsQuery.data.length === 0 && (
          <div className="p-4 text-sm text-muted-foreground">No datasets match.</div>
        )}
        {datasetsQuery.data && datasetsQuery.data.length > 0 && (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Path</TableHead>
                <TableHead className="w-28">Format</TableHead>
                <TableHead className="w-56 text-right">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {datasetsQuery.data.map((d) => (
                <TableRow key={d.path}>
                  <TableCell className="font-mono text-xs">
                    <button
                      className="flex items-center gap-1.5 text-left hover:text-primary hover:underline"
                      onClick={() => setDialog({ path: d.path, format: d.format, tab: "preview" })}
                      title="Preview rows"
                    >
                      <FileStack className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                      {d.path}
                    </button>
                  </TableCell>
                  <TableCell>
                    {d.format ? <Badge variant="muted">{d.format}</Badge> : "—"}
                  </TableCell>
                  <TableCell className="text-right">
                    <div className="flex justify-end gap-2">
                      <Button
                        variant="outline"
                        size="sm"
                        className="gap-1.5"
                        onClick={() => setDialog({ path: d.path, format: d.format, tab: "preview" })}
                      >
                        <Eye className="h-3.5 w-3.5" /> Preview
                      </Button>
                      <Button
                        variant="outline"
                        size="sm"
                        className="gap-1.5"
                        onClick={() => setDialog({ path: d.path, format: d.format, tab: "schema" })}
                      >
                        <Table2 className="h-3.5 w-3.5" /> Schema
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
