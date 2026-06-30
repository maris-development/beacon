import { useMemo } from "react";
import type { ArrowRecordBatch, ArrowTable, Row } from "@beacon/client";

import { columnsOf, formatCell, formatTimestamp, timestampColumns } from "@/lib/format";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";

interface ResultsGridProps {
  rows: Row[];
  /**
   * Decoded Arrow table or record batch — its schema is used to render timestamp
   * columns as date strings.
   */
  table?: ArrowTable | ArrowRecordBatch;
}

/** A scrollable, monospaced grid over decoded query rows. */
export function ResultsGrid({ rows, table }: ResultsGridProps) {
  const columns = useMemo(() => columnsOf(rows), [rows]);
  const tsColumns = useMemo(() => timestampColumns(table), [table]);

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
              const tsUnit = tsColumns.get(col);
              const text =
                tsUnit !== undefined ? formatTimestamp(row[col], tsUnit) : formatCell(row[col]);
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
