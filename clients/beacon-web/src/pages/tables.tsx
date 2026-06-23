import * as React from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, Loader2, Search, Table2, Trash2 } from "lucide-react";

import { cn } from "@/lib/utils";
import { useBeacon } from "@/lib/beacon-context";
import { COLUMN_PAGE_SIZE, parseSchema } from "@/lib/schema";
import { errorMessage } from "@/lib/errors";
import { PageContainer } from "@/components/app-shell";
import { JsonView } from "@/components/json-view";
import { ResultsGrid } from "@/components/results-grid";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";

/** Friendly labels for Beacon's `definition_type` (typetag) values. */
const KIND_LABELS: Record<string, string> = {
  listing_table: "External",
  iceberg: "Iceberg",
  delta_table: "Delta",
  materialized_view: "Materialized view",
  view_table: "View",
  logical: "Logical",
  remote_table: "Remote",
  sql_database_table: "SQL database",
};

/** Derives a human-readable table-kind label from a table-config object. */
function tableKind(config: unknown): string | null {
  if (!config || typeof config !== "object") return null;
  const c = config as Record<string, unknown>;
  const dt = c.definition_type;
  if (typeof dt !== "string") return null;
  const base = KIND_LABELS[dt] ?? dt.replace(/_/g, " ");
  // External file-backed tables carry the concrete format in `file_type`.
  if (dt === "listing_table" && typeof c.file_type === "string" && c.file_type) {
    return `${base} · ${c.file_type.toUpperCase()}`;
  }
  return base;
}

export function TablesPage() {
  const beacon = useBeacon();
  const [selected, setSelected] = React.useState<string | null>(null);

  const tablesQuery = useQuery({ queryKey: ["tables"], queryFn: () => beacon.tables() });

  React.useEffect(() => {
    if (!selected && tablesQuery.data && tablesQuery.data.length > 0) {
      setSelected(tablesQuery.data[0]);
    }
  }, [selected, tablesQuery.data]);

  return (
    <PageContainer title="Tables" description="Registered tables, their schemas, and configuration.">
      <div className="flex h-full min-h-0 gap-4">
        <Card className="flex w-64 shrink-0 flex-col overflow-hidden">
          <div className="border-b px-3 py-2 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
            {tablesQuery.data?.length ?? 0} tables
          </div>
          <div className="min-h-0 flex-1 overflow-auto p-1.5">
            {tablesQuery.isLoading && (
              <div className="flex items-center gap-2 p-2 text-sm text-muted-foreground">
                <Loader2 className="h-4 w-4 animate-spin" /> Loading…
              </div>
            )}
            {tablesQuery.isError && (
              <div className="p-2 text-sm text-destructive">{errorMessage(tablesQuery.error)}</div>
            )}
            {tablesQuery.data?.map((name) => (
              <button
                key={name}
                onClick={() => setSelected(name)}
                className={cn(
                  "flex w-full items-center gap-2 rounded px-2 py-1.5 text-left text-sm",
                  selected === name
                    ? "bg-secondary font-medium"
                    : "hover:bg-secondary/60 text-muted-foreground",
                )}
              >
                <Table2 className="h-4 w-4 shrink-0 text-primary" />
                <span className="truncate">{name}</span>
              </button>
            ))}
          </div>
        </Card>

        <div className="min-h-0 min-w-0 flex-1 overflow-auto">
          {selected ? <TableDetail name={selected} onDeleted={() => setSelected(null)} /> : null}
        </div>
      </div>
    </PageContainer>
  );
}

function TableDetail({ name, onDeleted }: { name: string; onDeleted: () => void }) {
  const beacon = useBeacon();

  const schemaQuery = useQuery({
    queryKey: ["table-schema", name],
    queryFn: async () => parseSchema(await beacon.tableSchema(name)),
  });
  const configQuery = useQuery({
    queryKey: ["table-config", name],
    queryFn: () => beacon.tableConfig(name),
  });

  const columns = schemaQuery.data ?? [];
  const [visible, setVisible] = React.useState(COLUMN_PAGE_SIZE);
  const [filter, setFilter] = React.useState("");

  // Reset the view when switching to a different table.
  React.useEffect(() => {
    setVisible(COLUMN_PAGE_SIZE);
    setFilter("");
  }, [name]);

  const needle = filter.trim().toLowerCase();
  const filtered = needle
    ? columns.filter(
        (c) =>
          c.name.toLowerCase().includes(needle) || c.dataType.toLowerCase().includes(needle),
      )
    : columns;

  return (
    <Card className="p-4">
      <div className="mb-3 flex items-center gap-2">
        <h2 className="flex items-center gap-2 text-base font-semibold">
          <Table2 className="h-4 w-4 text-primary" /> {name}
        </h2>
        {tableKind(configQuery.data) && (
          <Badge variant="secondary">{tableKind(configQuery.data)}</Badge>
        )}
        <DeleteTableDialog name={name} onDeleted={onDeleted} />
      </div>
      <Tabs defaultValue="schema" key={name}>
        <TabsList>
          <TabsTrigger value="schema">Schema</TabsTrigger>
          <TabsTrigger value="preview">Preview</TabsTrigger>
          <TabsTrigger value="config">Configuration</TabsTrigger>
        </TabsList>

        <TabsContent value="preview">
          <TablePreview name={name} />
        </TabsContent>

        <TabsContent value="schema">
          {schemaQuery.isLoading && <Spinner />}
          {schemaQuery.isError && <Err msg={errorMessage(schemaQuery.error)} />}
          {schemaQuery.data && (
            <>
              <div className="relative mb-2 max-w-xs">
                <Search className="absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                <Input
                  value={filter}
                  onChange={(e) => setFilter(e.target.value)}
                  placeholder="Filter columns (e.g. TEMP)"
                  className="h-8 pl-8"
                />
              </div>
              <div className="overflow-auto rounded-md border">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Column</TableHead>
                      <TableHead>Type</TableHead>
                      <TableHead>Nullable</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {filtered.length === 0 && (
                      <TableRow>
                        <TableCell colSpan={3} className="py-4 text-center text-muted-foreground">
                          No columns match “{filter}”.
                        </TableCell>
                      </TableRow>
                    )}
                    {filtered.slice(0, visible).map((c) => (
                      <TableRow key={c.name}>
                        <TableCell className="font-mono">{c.name}</TableCell>
                        <TableCell className="font-mono text-muted-foreground">
                          {c.dataType}
                        </TableCell>
                        <TableCell>
                          {c.nullable === undefined ? (
                            "—"
                          ) : (
                            <Badge variant={c.nullable ? "muted" : "secondary"}>
                              {c.nullable ? "nullable" : "required"}
                            </Badge>
                          )}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
              {filtered.length > 0 && (
                <div className="mt-2 flex items-center gap-3 text-xs text-muted-foreground">
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
            </>
          )}
        </TabsContent>

        <TabsContent value="config">
          {configQuery.isLoading && <Spinner />}
          {configQuery.isError && <Err msg={errorMessage(configQuery.error)} />}
          {configQuery.data != null && <JsonView value={configQuery.data} />}
        </TabsContent>
      </Tabs>
    </Card>
  );
}

/** Double-quotes a SQL identifier, escaping embedded quotes. */
function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

const PREVIEW_ROWS = 10;

function TablePreview({ name }: { name: string }) {
  const beacon = useBeacon();
  const query = useQuery({
    queryKey: ["table-preview", name],
    queryFn: async () =>
      (await beacon.query(`SELECT * FROM ${quoteIdent(name)} LIMIT ${PREVIEW_ROWS}`)).rows,
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
        Could not preview this table: {errorMessage(query.error)}
      </div>
    );

  const rows = query.data ?? [];
  return (
    <div className="space-y-2">
      <div className="max-h-[60vh] overflow-auto rounded-md border">
        <ResultsGrid rows={rows} />
      </div>
      {rows.length > 0 && (
        <p className="text-xs text-muted-foreground">First {rows.length} rows.</p>
      )}
    </div>
  );
}

function DeleteTableDialog({ name, onDeleted }: { name: string; onDeleted: () => void }) {
  const beacon = useBeacon();
  const qc = useQueryClient();
  const [open, setOpen] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  const dropMutation = useMutation({
    mutationFn: () => beacon.query(`DROP TABLE IF EXISTS ${quoteIdent(name)}`),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["tables"] });
      setOpen(false);
      onDeleted();
    },
    onError: (e) => setError(errorMessage(e)),
  });

  return (
    <Dialog
      open={open}
      onOpenChange={(o) => {
        setOpen(o);
        if (!o) setError(null);
      }}
    >
      <Button
        variant="ghost"
        size="sm"
        className="ml-auto text-destructive hover:bg-destructive/10 hover:text-destructive"
        onClick={() => setOpen(true)}
      >
        <Trash2 className="h-4 w-4" /> Delete
      </Button>
      <DialogContent>
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <AlertTriangle className="h-5 w-5 text-destructive" /> Delete table
          </DialogTitle>
          <DialogDescription>
            This removes the table{" "}
            <span className="font-mono font-medium text-foreground">{name}</span> from the catalog
            via <span className="font-mono">DROP TABLE</span>. The underlying dataset files are left
            in place. This cannot be undone.
          </DialogDescription>
        </DialogHeader>
        {error && <p className="text-sm text-destructive">{error}</p>}
        <DialogFooter>
          <Button variant="outline" onClick={() => setOpen(false)}>
            Cancel
          </Button>
          <Button
            variant="destructive"
            onClick={() => {
              setError(null);
              dropMutation.mutate();
            }}
            disabled={dropMutation.isPending}
          >
            {dropMutation.isPending && <Loader2 className="h-4 w-4 animate-spin" />}
            Delete table
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function Spinner() {
  return (
    <div className="flex items-center gap-2 py-4 text-sm text-muted-foreground">
      <Loader2 className="h-4 w-4 animate-spin" /> Loading…
    </div>
  );
}

function Err({ msg }: { msg: string }) {
  return <div className="py-4 text-sm text-destructive">{msg}</div>;
}
