/** A columnar view over the Arrow chunks a query returns. */

import type { ArrowRecordBatch, ArrowTable, ArrowVector } from "@beacon/client";

/** A decoded Arrow chunk: one streamed record batch, or a whole table. */
export type ArrowChunk = ArrowTable | ArrowRecordBatch;

/** A column of the result, named by the Arrow schema. */
export interface ResultColumn {
  name: string;
  /** An Arrow `Timestamp` column, which the grid renders as a date. */
  timestamp: boolean;
}

/**
 * One chunk of the result, with its columns resolved on arrival so the grid
 * reads cells straight out of the Arrow vectors.
 */
export interface ResultChunk {
  /** Row index of this chunk's first row within the whole result. */
  offset: number;
  /** Rows to take from this chunk. The last chunk is clipped at the row limit. */
  numRows: number;
  /** One vector per entry of {@link ArrowResult.columns}; null when the chunk lacks it. */
  vectors: Array<ArrowVector | null>;
}

/**
 * The rows a query returned, held in Arrow's columnar form.
 *
 * Nothing here builds a JS object per row. The grid reads one cell as
 * `chunk.vectors[column].get(row)`, so a result costs only the cells that are
 * rendered — a beacon table can carry 100K+ columns, of which a preview shows a
 * few hundred rows.
 */
export interface ArrowResult {
  readonly columns: readonly ResultColumn[];
  readonly chunks: readonly ResultChunk[];
  readonly numRows: number;
}

/** A result with no columns and no rows. */
export const EMPTY_RESULT: ArrowResult = { columns: [], chunks: [], numRows: 0 };

/**
 * Appends `chunk`, keeping at most `maxRows` rows in total, and returns a new
 * result. The reference is new, so React re-renders with the rows so far while
 * the query still streams.
 */
export function appendChunk(
  result: ArrowResult,
  chunk: ArrowChunk,
  maxRows = Number.POSITIVE_INFINITY,
): ArrowResult {
  const numRows = Math.min(chunk.numRows, Math.max(0, maxRows - result.numRows));
  if (numRows === 0) return result;
  // Every batch of one Arrow IPC stream shares the schema of the first.
  const columns = result.columns.length > 0 ? result.columns : columnsOf(chunk);
  const vectors = columns.map((_, i) => chunk.getChildAt(i));
  return {
    columns,
    chunks: [...result.chunks, { offset: result.numRows, numRows, vectors }],
    numRows: result.numRows + numRows,
  };
}

/** Builds a result from a single already-decoded table (the non-streaming path). */
export function resultFromTable(
  table: ArrowTable | undefined,
  maxRows = Number.POSITIVE_INFINITY,
): ArrowResult {
  return table ? appendChunk(EMPTY_RESULT, table, maxRows) : EMPTY_RESULT;
}

/**
 * Reads the column names from the chunk's Arrow schema, marking the `Timestamp`
 * ones. The unit is not needed — apache-arrow's getters already normalize every
 * timestamp to milliseconds (see `formatTimestamp`) — only which columns are
 * timestamps, so the grid renders them as dates and not as bare numbers.
 */
function columnsOf(chunk: ArrowChunk): ResultColumn[] {
  const fields = chunk.schema?.fields;
  if (!Array.isArray(fields)) return [];
  // A `Timestamp` type prints as `Timestamp<MICROSECOND>`, or with a time zone
  // as `Timestamp<MICROSECOND, UTC>` — match the name, not the parameters.
  return fields.map((field) => ({
    name: String(field.name),
    timestamp: String(field.type).startsWith("Timestamp<"),
  }));
}
