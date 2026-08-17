import * as React from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  AlertTriangle,
  ChevronDown,
  ChevronRight,
  Database,
  Loader2,
  Plus,
  RefreshCw,
  Search,
  Table2,
  Trash2,
} from "lucide-react";

import { cn } from "@/lib/utils";
import { EMPTY_RESULT, resultFromTable } from "@/lib/arrow-result";
import { useBeacon } from "@/lib/beacon-context";
import { COLUMN_PAGE_SIZE, parseSchema } from "@/lib/schema";
import { errorMessage } from "@/lib/errors";
import {
  filterTree,
  firstTable,
  isDefaultSchema,
  isSystemSchema,
  refKey,
  sameTable,
  schemaLabel,
  sqlIdent,
  sqlName,
  useCatalogTree,
  type CatalogDefaults,
  type CatalogTree,
  type TableRef,
} from "@/lib/catalog";
import { PageContainer } from "@/components/app-shell";
import { ResultsGrid } from "@/components/results-grid";
import { InfoBanner } from "@/components/info-banner";
import { CreateViewDialog } from "@/components/create-view-dialog";
import { ExternalTableDialog } from "@/components/external-table-dialog";
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
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";

/** The kind badge, from the only kind information the server still reports. */
function tableKind(table: TableRef): string {
  return table.tableType?.toUpperCase() === "VIEW" ? "View" : "Table";
}

type CreateTarget =
  | { kind: "view" }
  | { kind: "materialized" }
  | { kind: "external"; format: string };

export function TablesPage() {
  const [selected, setSelected] = React.useState<TableRef | null>(null);
  const [create, setCreate] = React.useState<CreateTarget | null>(null);

  const catalogsQuery = useCatalogTree();
  const tree = catalogsQuery.tree;

  React.useEffect(() => {
    if (!selected) setSelected(firstTable(tree));
  }, [selected, tree]);

  return (
    <PageContainer
      title="Tables"
      description="Registered tables and their schemas, grouped by catalog."
      actions={
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button size="sm" className="gap-1.5">
              <Plus className="h-4 w-4" /> Create <ChevronDown className="h-4 w-4" />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end">
            <DropdownMenuItem onClick={() => setCreate({ kind: "view" })}>View</DropdownMenuItem>
            <DropdownMenuItem onClick={() => setCreate({ kind: "materialized" })}>
              Materialized view
            </DropdownMenuItem>
            <DropdownMenuSeparator />
            <DropdownMenuItem onClick={() => setCreate({ kind: "external", format: "PARQUET" })}>
              External table
            </DropdownMenuItem>
            <DropdownMenuItem onClick={() => setCreate({ kind: "external", format: "DELTA" })}>
              Delta Lake table
            </DropdownMenuItem>
            <DropdownMenuItem onClick={() => setCreate({ kind: "external", format: "ICEBERG" })}>
              Iceberg table
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      }
    >
      <div className="flex h-full min-h-0 flex-col gap-3">
        <InfoBanner>
          Tables are the named, queryable datasets in Beacon — external (file-backed), views,
          materialized views, Delta/Iceberg, and more. They are grouped by catalog and schema:
          yours live in <span className="font-mono">{tree.defaults.catalog}.{tree.defaults.schema}</span>,
          alongside Beacon&rsquo;s own metadata schemas and any attached remote. Use{" "}
          <strong>Create</strong> to add one; the tag next to a table&rsquo;s name shows its kind.
        </InfoBanner>
        <div className="flex min-h-0 flex-1 gap-4">
        <CatalogList
          tree={tree}
          isLoading={catalogsQuery.isLoading}
          error={catalogsQuery.isError ? errorMessage(catalogsQuery.error) : null}
          selected={selected}
          onSelect={setSelected}
        />

        <div className="min-h-0 min-w-0 flex-1 overflow-auto">
          {selected ? (
            <TableDetail
              table={selected}
              defaults={tree.defaults}
              onDeleted={() => setSelected(null)}
            />
          ) : null}
        </div>
        </div>
      </div>

      <CreateViewDialog
        open={create?.kind === "view" || create?.kind === "materialized"}
        materialized={create?.kind === "materialized"}
        onOpenChange={(o) => !o && setCreate(null)}
        onCreated={(name) => setSelected({ ...tree.defaults, name })}
      />
      <ExternalTableDialog
        open={create?.kind === "external"}
        presetFormat={create?.kind === "external" ? create.format : undefined}
        onOpenChange={(o) => !o && setCreate(null)}
        onCreated={(name) => setSelected({ ...tree.defaults, name })}
      />
    </PageContainer>
  );
}

/** The left-hand browser: catalogs → schemas → tables, filtered by name. */
function CatalogList({
  tree,
  isLoading,
  error,
  selected,
  onSelect,
}: {
  tree: CatalogTree;
  isLoading: boolean;
  error: string | null;
  selected: TableRef | null;
  onSelect: (table: TableRef) => void;
}) {
  const [filter, setFilter] = React.useState("");
  const filtered = filterTree(tree, filter);
  const searching = filter.trim().length > 0;

  return (
    <Card className="flex w-72 shrink-0 flex-col overflow-hidden">
      <div className="border-b px-3 py-2 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
        {tree.tables.length} tables in {tree.catalogs.length}{" "}
        {tree.catalogs.length === 1 ? "catalog" : "catalogs"}
      </div>
      <div className="relative border-b px-2 py-2">
        <Search className="absolute left-4 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
        <Input
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder="Filter tables"
          className="h-8 pl-7 text-xs"
        />
      </div>
      <div className="min-h-0 flex-1 overflow-auto p-1.5">
        {isLoading && (
          <div className="flex items-center gap-2 p-2 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" /> Loading…
          </div>
        )}
        {error && <div className="p-2 text-sm text-destructive">{error}</div>}
        {!isLoading && filtered.catalogs.length === 0 && (
          <div className="p-2 text-sm text-muted-foreground">
            {searching ? "No matches." : "No tables."}
          </div>
        )}
        {filtered.catalogs.map((catalog) => (
          <div key={catalog.name} className="mb-1">
            <div className="flex items-center gap-1.5 px-2 py-1 text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
              <Database className="h-3.5 w-3.5" />
              <span className="truncate">{catalog.name}</span>
            </div>
            {catalog.schemas.map((schema) => (
              <SchemaGroup
                key={schema.name}
                catalog={catalog.name}
                schema={schema.name}
                tables={schema.tables}
                defaults={tree.defaults}
                forceOpen={searching}
                selected={selected}
                onSelect={onSelect}
              />
            ))}
          </div>
        ))}
      </div>
    </Card>
  );
}

/** One schema's tables, collapsed unless it is the default schema (or we're filtering). */
function SchemaGroup({
  catalog,
  schema,
  tables,
  defaults,
  forceOpen,
  selected,
  onSelect,
}: {
  catalog: string;
  schema: string;
  tables: { name: string; table_type: string }[];
  defaults: CatalogDefaults;
  forceOpen: boolean;
  selected: TableRef | null;
  onSelect: (table: TableRef) => void;
}) {
  const isDefault = isDefaultSchema({ catalog, schema }, defaults);
  const [open, setOpen] = React.useState(isDefault);
  const expanded = forceOpen || open;

  return (
    <div className="ml-1">
      <button
        onClick={() => setOpen((o) => !o)}
        title={`Schema ${catalog}.${schema}`}
        className="flex w-full items-center gap-1 rounded px-1 py-1 text-left text-[13px] hover:bg-secondary/60"
      >
        <ChevronRight
          className={cn(
            "h-3.5 w-3.5 shrink-0 text-muted-foreground transition-transform",
            expanded && "rotate-90",
          )}
        />
        <span className={cn("truncate", isSystemSchema(schema) && "text-muted-foreground")}>
          {schema}
        </span>
        <span className="ml-auto shrink-0 text-[10px] text-muted-foreground">{tables.length}</span>
      </button>
      {expanded && (
        <div className="ml-3 border-l pl-1">
          {tables.map((table) => {
            const ref = { catalog, schema, name: table.name, tableType: table.table_type };
            return (
              <button
                key={table.name}
                onClick={() => onSelect(ref)}
                title={`${catalog}.${schema}.${table.name}`}
                className={cn(
                  "flex w-full items-center gap-2 rounded px-2 py-1 text-left text-[13px]",
                  sameTable(selected, ref)
                    ? "bg-secondary font-medium"
                    : "hover:bg-secondary/60 text-muted-foreground",
                )}
              >
                <Table2 className="h-4 w-4 shrink-0 text-primary" />
                <span className="truncate">{table.name}</span>
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
}

function TableDetail({
  table,
  defaults,
  onDeleted,
}: {
  table: TableRef;
  defaults: CatalogDefaults;
  onDeleted: () => void;
}) {
  const beacon = useBeacon();
  const key = refKey(table);
  const name = table.name;
  // Configuration, DROP, and REFRESH all address a table by its bare name in the
  // default schema. Beacon's metadata schemas and attached remotes are read-only
  // here, so those controls are hidden for them rather than failing on click.
  const manageable = isDefaultSchema(table, defaults);

  const schemaQuery = useQuery({
    queryKey: ["table-schema", key],
    queryFn: async () =>
      parseSchema(await beacon.tableSchema(name, { catalog: table.catalog, schema: table.schema })),
  });
  const columns = schemaQuery.data ?? [];
  const [visible, setVisible] = React.useState(COLUMN_PAGE_SIZE);
  const [filter, setFilter] = React.useState("");

  // Reset the view when switching to a different table.
  React.useEffect(() => {
    setVisible(COLUMN_PAGE_SIZE);
    setFilter("");
  }, [key]);

  const needle = filter.trim().toLowerCase();
  const filtered = needle
    ? columns.filter(
        (c) =>
          c.name.toLowerCase().includes(needle) || c.dataType.toLowerCase().includes(needle),
      )
    : columns;

  // A materialized view is a stored table, indistinguishable from any other one
  // through the catalog, so Refresh is offered for every managed table and the
  // server rejects it (visibly, on the button) when the table is not one.
  const refreshable = manageable && table.tableType?.toUpperCase() !== "VIEW";

  return (
    <Card className="p-4">
      <div className="mb-3 flex items-center gap-2">
        <h2 className="flex items-center gap-2 text-base font-semibold">
          <Table2 className="h-4 w-4 text-primary" /> {name}
        </h2>
        <span className="font-mono text-xs text-muted-foreground">{schemaLabel(table)}</span>
        <Badge variant="secondary">{tableKind(table)}</Badge>
        <div className="ml-auto flex items-center gap-2">
          {refreshable && <RefreshMvButton table={table} />}
          {manageable && <DeleteTableDialog name={name} onDeleted={onDeleted} />}
        </div>
      </div>
      <Tabs defaultValue="schema" key={key}>
        <TabsList>
          <TabsTrigger value="schema">Schema</TabsTrigger>
          <TabsTrigger value="preview">Preview</TabsTrigger>
        </TabsList>

        <TabsContent value="preview">
          <TablePreview table={table} defaults={defaults} />
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
                <Table className="text-[13px] [&_td]:py-1 [&_th]:h-8">
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

      </Tabs>
    </Card>
  );
}

/** Re-materializes a materialized view (`REFRESH <name>`). */
function RefreshMvButton({ table }: { table: TableRef }) {
  const beacon = useBeacon();
  const qc = useQueryClient();
  const [error, setError] = React.useState<string | null>(null);

  const refresh = useMutation({
    mutationFn: () => beacon.query(`REFRESH ${sqlIdent(table.name)}`),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["table-preview", refKey(table)] });
      qc.invalidateQueries({ queryKey: ["table-schema", refKey(table)] });
    },
    onError: (e) => setError(errorMessage(e)),
  });

  return (
    <Button
      variant="outline"
      size="sm"
      className="gap-1.5"
      disabled={refresh.isPending}
      title={error ?? "Re-run the view's query and store the result"}
      onClick={() => {
        setError(null);
        refresh.mutate();
      }}
    >
      <RefreshCw className={cn("h-4 w-4", refresh.isPending && "animate-spin")} />
      Refresh
    </Button>
  );
}

const PREVIEW_ROWS = 10;

function TablePreview({ table, defaults }: { table: TableRef; defaults: CatalogDefaults }) {
  const beacon = useBeacon();
  const query = useQuery({
    queryKey: ["table-preview", refKey(table)],
    queryFn: async () =>
      resultFromTable(
        await beacon.queryArrow(`SELECT * FROM ${sqlName(table, defaults)} LIMIT ${PREVIEW_ROWS}`),
      ),
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

  const result = query.data ?? EMPTY_RESULT;
  return (
    <div className="space-y-2">
      <div className="max-h-[60vh] overflow-auto rounded-md border">
        <ResultsGrid result={result} />
      </div>
      {result.numRows > 0 && (
        <p className="text-xs text-muted-foreground">First {result.numRows} rows.</p>
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
    mutationFn: () => beacon.query(`DROP TABLE IF EXISTS ${sqlIdent(name)}`),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["tables"] });
      qc.invalidateQueries({ queryKey: ["catalogs"] });
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
        className="text-destructive hover:bg-destructive/10 hover:text-destructive"
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
