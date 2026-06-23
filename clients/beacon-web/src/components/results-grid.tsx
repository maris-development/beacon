import { useMemo } from "react";
import type { Row } from "@beacon/client";

import { columnsOf, formatCell } from "@/lib/format";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";

interface ResultsGridProps {
  rows: Row[];
}

/** A scrollable, monospaced grid over decoded query rows. */
export function ResultsGrid({ rows }: ResultsGridProps) {
  const columns = useMemo(() => columnsOf(rows), [rows]);

  if (rows.length === 0) {
    return (
      <div className="flex h-full items-center justify-center p-6 text-sm text-muted-foreground">
        Query returned no rows.
      </div>
    );
  }

  // The table sizes to its content (`w-max`) so a wide result overflows and the
  // surrounding scroll container (workbench body / dialog) scrolls sideways.
  return (
    <Table className="w-max min-w-full font-mono text-xs">
      <TableHeader className="sticky top-0 z-10 bg-secondary">
        <TableRow>
          <TableHead className="w-12 text-right text-muted-foreground">#</TableHead>
          {columns.map((col) => (
            <TableHead key={col} className="whitespace-nowrap font-mono">
              {col}
            </TableHead>
          ))}
        </TableRow>
      </TableHeader>
      <TableBody>
        {rows.map((row, i) => (
          <TableRow key={i}>
            <TableCell className="text-right text-muted-foreground">{i + 1}</TableCell>
            {columns.map((col) => {
              const text = formatCell(row[col]);
              return (
                <TableCell
                  key={col}
                  className="max-w-[28rem] truncate whitespace-nowrap"
                  title={text}
                >
                  {text}
                </TableCell>
              );
            })}
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}
