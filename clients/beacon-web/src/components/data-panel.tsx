import * as React from "react";
import { useQuery } from "@tanstack/react-query";
import { ChevronRight, Columns3, Loader2, RefreshCw, Search, Table2 } from "lucide-react";

import { cn } from "@/lib/utils";
import { useBeacon } from "@/lib/beacon-context";
import { parseSchema } from "@/lib/schema";
import { errorMessage } from "@/lib/errors";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";

interface DataPanelProps {
  /** Called when the user clicks a table or column name, to insert it into the editor. */
  onInsert: (text: string) => void;
}

/** Athena-style left data panel: searchable table list with expandable columns. */
export function DataPanel({ onInsert }: DataPanelProps) {
  const beacon = useBeacon();
  const [filter, setFilter] = React.useState("");

  const tablesQuery = useQuery({
    queryKey: ["tables"],
    queryFn: () => beacon.tables(),
  });

  const tables = (tablesQuery.data ?? []).filter((t) =>
    t.toLowerCase().includes(filter.toLowerCase()),
  );

  return (
    <div className="flex h-full flex-col border-r bg-card">
      <div className="flex items-center justify-between gap-2 border-b px-3 py-2">
        <span className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
          Tables
        </span>
        <Button
          variant="ghost"
          size="icon"
          className="h-6 w-6"
          onClick={() => tablesQuery.refetch()}
          title="Refresh"
        >
          <RefreshCw className={cn("h-3.5 w-3.5", tablesQuery.isFetching && "animate-spin")} />
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
        {tablesQuery.isLoading && (
          <div className="flex items-center gap-2 px-2 py-3 text-xs text-muted-foreground">
            <Loader2 className="h-3.5 w-3.5 animate-spin" /> Loading…
          </div>
        )}
        {tablesQuery.isError && (
          <div className="px-2 py-3 text-xs text-destructive">
            {errorMessage(tablesQuery.error)}
          </div>
        )}
        {!tablesQuery.isLoading && tables.length === 0 && (
          <div className="px-2 py-3 text-xs text-muted-foreground">No tables.</div>
        )}
        {tables.map((name) => (
          <TableNode key={name} name={name} onInsert={onInsert} />
        ))}
      </div>
    </div>
  );
}

function TableNode({ name, onInsert }: { name: string; onInsert: (text: string) => void }) {
  const beacon = useBeacon();
  const [open, setOpen] = React.useState(false);

  const schemaQuery = useQuery({
    queryKey: ["table-schema", name],
    queryFn: async () => parseSchema(await beacon.tableSchema(name)),
    enabled: open,
  });

  return (
    <div>
      <div className="flex items-center rounded hover:bg-secondary/60">
        <button
          onClick={() => setOpen((o) => !o)}
          className="flex h-7 w-6 items-center justify-center text-muted-foreground"
          aria-label={open ? "Collapse" : "Expand"}
        >
          <ChevronRight className={cn("h-3.5 w-3.5 transition-transform", open && "rotate-90")} />
        </button>
        <button
          onClick={() => onInsert(name)}
          className="flex min-w-0 flex-1 items-center gap-1.5 py-1 pr-2 text-left text-[13px]"
          title={`Insert "${name}"`}
        >
          <Table2 className="h-3.5 w-3.5 shrink-0 text-primary" />
          <span className="truncate">{name}</span>
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
          {schemaQuery.data?.map((col) => (
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
          {schemaQuery.data?.length === 0 && (
            <div className="py-1 text-[11px] text-muted-foreground">No columns.</div>
          )}
        </div>
      )}
    </div>
  );
}
