import { EMPTY_RESULT, type ArrowResult } from "@/lib/arrow-result";
import { formatCell, formatTimestamp } from "@/lib/format";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";

interface ResultsGridProps {
  /** The rows to show, in Arrow's columnar form. */
  result?: ArrowResult;
}

/**
 * A scrollable, monospaced grid over an Arrow query result.
 *
 * Cells are read from the Arrow vectors while rendering, so a streaming query
 * shows each batch as it lands without first decoding it into JS row objects.
 */
export function ResultsGrid({ result = EMPTY_RESULT }: ResultsGridProps) {
  if (result.numRows === 0) {
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
          {result.columns.map((col, c) => (
            // Arrow allows duplicate field names, so columns are keyed by position.
            <TableHead key={c} className="whitespace-nowrap font-mono">
              {col.name}
            </TableHead>
          ))}
        </TableRow>
      </TableHeader>
      <TableBody>
        {result.chunks.map((chunk, ci) =>
          Array.from({ length: chunk.numRows }, (_, i) => (
            <TableRow key={`${ci}:${i}`}>
              <TableCell className="text-right text-muted-foreground">
                {chunk.offset + i + 1}
              </TableCell>
              {result.columns.map((col, c) => {
                const value = chunk.vectors[c]?.get(i);
                const text = col.timestamp ? formatTimestamp(value) : formatCell(value);
                return (
                  <TableCell
                    key={c}
                    className="max-w-[28rem] truncate whitespace-nowrap"
                    title={text}
                  >
                    {text}
                  </TableCell>
                );
              })}
            </TableRow>
          )),
        )}
      </TableBody>
    </Table>
  );
}
