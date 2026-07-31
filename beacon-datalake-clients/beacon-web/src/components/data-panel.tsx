import * as React from "react";
import { useQuery } from "@tanstack/react-query";
import {
  ChevronRight,
  Columns3,
  Database,
  FolderTree,
  Loader2,
  RefreshCw,
  Search,
  Table2,
} from "lucide-react";

import { cn } from "@/lib/utils";
import { useBeacon } from "@/lib/beacon-context";
import { COLUMN_PAGE_SIZE, parseSchema } from "@/lib/schema";
import { errorMessage } from "@/lib/errors";
import {
  filterTree,
  isDefaultSchema,
  isSystemSchema,
  refKey,
  sqlName,
  useCatalogTree,
  type CatalogDefaults,
  type TableRef,
} from "@/lib/catalog";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";

interface DataPanelProps {
  /** Called when the user clicks a table or column name, to insert it into the editor. */
  onInsert: (text: string) => void;
}

/**
 * Athena-style left data panel: the whole catalog namespace as a searchable
 * tree — catalog → schema → table → columns.
 *
 * The default catalog and schema are expanded on load; the rest (beacon's
 * `system` schema, `information_schema`, attached remotes) are collapsed but
 * present. Typing in the filter expands whatever matches.
 */
export function DataPanel({ onInsert }: DataPanelProps) {
  const [filter, setFilter] = React.useState("");
  const catalogsQuery = useCatalogTree();
  const tree = filterTree(catalogsQuery.tree, filter);
  const searching = filter.trim().length > 0;

  return (
    <div className="flex h-full flex-col border-r bg-card">
      <div className="flex items-center justify-between gap-2 border-b px-3 py-2">
        <span className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
          Catalogs
        </span>
        <Button
          variant="ghost"
          size="icon"
          className="h-6 w-6"
          onClick={() => catalogsQuery.refetch()}
          title="Refresh"
        >
          <RefreshCw className={cn("h-3.5 w-3.5", catalogsQuery.isFetching && "animate-spin")} />
        </Button>
      </div>
      <div className="relative px-3 py-2">
        <Search className="absolute left-5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
        <Input
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder="Filter tables"
          className="h-8 pl-7 text-xs"
        />
      </div>

      <div className="min-h-0 flex-1 overflow-auto px-1.5 pb-2">
        {catalogsQuery.isLoading && (
          <div className="flex items-center gap-2 px-2 py-3 text-xs text-muted-foreground">
            <Loader2 className="h-3.5 w-3.5 animate-spin" /> Loading…
          </div>
        )}
        {catalogsQuery.isError && (
          <div className="px-2 py-3 text-xs text-destructive">
            {errorMessage(catalogsQuery.error)}
          </div>
        )}
        {!catalogsQuery.isLoading && tree.catalogs.length === 0 && (
          <div className="px-2 py-3 text-xs text-muted-foreground">
            {searching ? "No matches." : "No tables."}
          </div>
        )}
        {tree.catalogs.map((catalog) => (
          <CatalogNode
            key={catalog.name}
            name={catalog.name}
            schemas={catalog.schemas}
            defaults={tree.defaults}
            searching={searching}
            onInsert={onInsert}
          />
        ))}
      </div>
    </div>
  );
}

/** Chevron that rotates when its node is open. */
function Twisty({ open }: { open: boolean }) {
  return (
    <span className="flex h-6 w-5 shrink-0 items-center justify-center text-muted-foreground">
      <ChevronRight className={cn("h-3.5 w-3.5 transition-transform", open && "rotate-90")} />
    </span>
  );
}

function CatalogNode({
  name,
  schemas,
  defaults,
  searching,
  onInsert,
}: {
  name: string;
  schemas: { name: string; tables: { name: string }[] }[];
  defaults: CatalogDefaults;
  searching: boolean;
  onInsert: (text: string) => void;
}) {
  const [open, setOpen] = React.useState(name === defaults.catalog);
  // While filtering, everything that survived the filter is worth showing.
  const expanded = searching || open;

  return (
    <div>
      <button
        onClick={() => setOpen((o) => !o)}
        title={`Catalog ${name}`}
        className="flex w-full items-center gap-1 rounded py-1 pr-2 text-left text-[13px] hover:bg-secondary/60"
      >
        <Twisty open={expanded} />
        <Database className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
        <span className="truncate font-medium">{name}</span>
      </button>
      {expanded && (
        <div className="ml-3 border-l pl-1">
          {schemas.map((schema) => (
            <SchemaNode
              key={schema.name}
              catalog={name}
              name={schema.name}
              tables={schema.tables}
              defaults={defaults}
              searching={searching}
              onInsert={onInsert}
            />
          ))}
        </div>
      )}
    </div>
  );
}

function SchemaNode({
  catalog,
  name,
  tables,
  defaults,
  searching,
  onInsert,
}: {
  catalog: string;
  name: string;
  tables: { name: string }[];
  defaults: CatalogDefaults;
  searching: boolean;
  onInsert: (text: string) => void;
}) {
  const isDefault = isDefaultSchema({ catalog, schema: name }, defaults);
  const [open, setOpen] = React.useState(isDefault);
  const expanded = searching || open;

  return (
    <div>
      <button
        onClick={() => setOpen((o) => !o)}
        title={`Schema ${catalog}.${name}`}
        className="flex w-full items-center gap-1 rounded py-1 pr-2 text-left text-[13px] hover:bg-secondary/60"
      >
        <Twisty open={expanded} />
        <FolderTree className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
        <span className={cn("truncate", isSystemSchema(name) && "text-muted-foreground")}>
          {name}
        </span>
        <span className="ml-auto shrink-0 text-[10px] text-muted-foreground">{tables.length}</span>
      </button>
      {expanded && (
        <div className="ml-3 border-l pl-1">
          {tables.map((table) => (
            <TableNode
              key={table.name}
              table={{ catalog, schema: name, name: table.name }}
              defaults={defaults}
              onInsert={onInsert}
            />
          ))}
        </div>
      )}
    </div>
  );
}

function TableNode({
  table,
  defaults,
  onInsert,
}: {
  table: TableRef;
  defaults: CatalogDefaults;
  onInsert: (text: string) => void;
}) {
  const beacon = useBeacon();
  const [open, setOpen] = React.useState(false);
  const [visible, setVisible] = React.useState(COLUMN_PAGE_SIZE);

  const schemaQuery = useQuery({
    queryKey: ["table-schema", refKey(table)],
    queryFn: async () =>
      parseSchema(
        await beacon.tableSchema(table.name, { catalog: table.catalog, schema: table.schema }),
      ),
    enabled: open,
  });

  const columns = schemaQuery.data ?? [];
  const hidden = Math.max(0, columns.length - visible);
  // Inserting a table outside the default schema needs its qualified name.
  const insertName = sqlName(table, defaults);

  return (
    <div>
      <div className="flex items-center rounded hover:bg-secondary/60">
        <button
          onClick={() => setOpen((o) => !o)}
          className="flex h-7 w-5 items-center justify-center text-muted-foreground"
          aria-label={open ? "Collapse" : "Expand"}
        >
          <ChevronRight className={cn("h-3.5 w-3.5 transition-transform", open && "rotate-90")} />
        </button>
        <button
          onClick={() => onInsert(insertName)}
          className="flex min-w-0 flex-1 items-center gap-1.5 py-1 pr-2 text-left text-[13px]"
          title={`Insert "${insertName}"`}
        >
          <Table2 className="h-3.5 w-3.5 shrink-0 text-primary" />
          <span className="truncate">{table.name}</span>
        </button>
      </div>

      {open && (
        <div className="ml-6 border-l pl-2">
          {schemaQuery.isLoading && (
            <div className="flex items-center gap-1.5 py-1 text-[11px] text-muted-foreground">
              <Loader2 className="h-3 w-3 animate-spin" /> Loading columns…
            </div>
          )}
          {schemaQuery.isError && (
            <div className="py-1 text-[11px] text-destructive">
              {errorMessage(schemaQuery.error)}
            </div>
          )}
          {columns.slice(0, visible).map((col) => (
            <button
              key={col.name}
              onClick={() => onInsert(col.name)}
              className="flex w-full items-center gap-1.5 rounded py-0.5 pr-2 text-left text-[12px] hover:bg-secondary/60"
              title={`Insert "${col.name}"`}
            >
              <Columns3 className="h-3 w-3 shrink-0 text-muted-foreground" />
              <span className="truncate">{col.name}</span>
              <span className="ml-auto shrink-0 text-[10px] text-muted-foreground">
                {col.dataType}
              </span>
            </button>
          ))}
          {hidden > 0 && (
            <button
              onClick={() => setVisible((v) => v + COLUMN_PAGE_SIZE)}
              className="mt-0.5 w-full rounded px-1 py-1 text-left text-[11px] font-medium text-primary hover:bg-secondary/60"
            >
              Show {Math.min(hidden, COLUMN_PAGE_SIZE)} more ({columns.length.toLocaleString()} total)
            </button>
          )}
          {schemaQuery.data?.length === 0 && (
            <div className="py-1 text-[11px] text-muted-foreground">No columns.</div>
          )}
        </div>
      )}
    </div>
  );
}
