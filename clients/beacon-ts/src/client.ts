/** The main Beacon client: query execution and metadata discovery. */

import { AdminClient } from "./admin.js";
import {
  getArrowDecoder,
  rowsFromTable,
  type ArrowRecordBatch,
  type ArrowTable,
} from "./arrow.js";
import { parseCsv } from "./csv.js";
import { Http, type ClientOptions } from "./http.js";
import { QueryBuilder } from "./query-builder.js";
import { responseByteStream } from "./stream.js";
import type {
  From,
  Output,
  OutputFormat,
  QueryInput,
  QueryMetricsView,
  Row,
  Select,
} from "./types.js";

const QUERY_ID_HEADER = "x-beacon-query-id";

/** A query result decoded into rows, with the server-assigned query id. */
export interface QueryResult<T = Row> {
  rows: T[];
  /** Server-assigned UUID for this query (from the `x-beacon-query-id` header). */
  queryId: string | null;
  /**
   * The decoded Arrow Table, present when the Arrow path was used (i.e.
   * `apache-arrow` is installed). Absent when the CSV fallback produced the rows.
   */
  table?: ArrowTable;
}

/** Options for `query()`. */
export interface QueryOptions {
  /**
   * Force a decode path. `"arrow"` requires `apache-arrow`; `"csv"` uses the
   * dependency-free CSV fallback. Omit to use Arrow when available and fall back
   * to CSV otherwise.
   */
  format?: "arrow" | "csv";
  signal?: AbortSignal;
}

/**
 * Client for a single Beacon server.
 *
 * ```ts
 * const beacon = new BeaconClient({ url: "http://localhost:5001" });
 * const { rows } = await beacon.query("SELECT 1 AS n");
 * ```
 *
 * Supplying `username`/`password` elevates every request to super-user, enabling
 * DDL/DML over `query()` and the `admin` endpoints.
 */
export class BeaconClient {
  private readonly http: Http;
  /** Authenticated administrative endpoints (`/api/admin/*`). */
  readonly admin: AdminClient;

  constructor(options: ClientOptions) {
    this.http = new Http(options);
    this.admin = new AdminClient(this.http);
  }

  // -- query ------------------------------------------------------------------

  /** Starts a fluent query against `source` (a table name or `{ format: { paths } }`). */
  from<T = Row>(source?: From): QueryBuilder<T> {
    return new QueryBuilder<T>(this, source);
  }

  /** Starts a fluent query with the given projection (shorthand for `from().select(...)`). */
  select<T = Row>(...items: Array<string | Select>): QueryBuilder<T> {
    return new QueryBuilder<T>(this).select(...items);
  }

  /**
   * Runs a query and returns the rows as plain JS objects.
   *
   * Decodes the server's zstd-compressed Arrow IPC stream via `apache-arrow`
   * (the result also exposes the Arrow `table`). Pass `{ format: "csv" }` to
   * fetch CSV output instead and parse it (all values become strings, `table`
   * is absent). Works for SELECTs; DDL/DML yields an empty `rows`.
   */
  async query<T = Row>(query: QueryInput, options: QueryOptions = {}): Promise<QueryResult<T>> {
    if (options.format === "csv") {
      const res = await this.queryRaw(query, "csv", options.signal);
      return { rows: parseCsv(await res.text()) as T[], queryId: res.headers.get(QUERY_ID_HEADER) };
    }
    const decoder = await getArrowDecoder();
    const res = await this.queryRaw(query, undefined, options.signal);
    const queryId = res.headers.get(QUERY_ID_HEADER);
    const table = decoder.tableFromIPC(new Uint8Array(await res.arrayBuffer()));
    return { rows: rowsFromTable<T>(table), queryId, table };
  }

  /**
   * Runs a query and decodes the default zstd-compressed Arrow IPC stream into
   * an `apache-arrow` Table.
   */
  async queryArrow(query: QueryInput, signal?: AbortSignal): Promise<ArrowTable> {
    const decoder = await getArrowDecoder();
    const res = await this.queryRaw(query, undefined, signal);
    return decoder.tableFromIPC(new Uint8Array(await res.arrayBuffer()));
  }

  /**
   * Runs a query and yields Arrow `RecordBatch`es as they arrive, without
   * buffering the whole result in memory. Each batch's rows are available via
   * `batch.toArray()`.
   */
  async *queryStream(query: QueryInput, signal?: AbortSignal): AsyncGenerator<ArrowRecordBatch> {
    const decoder = await getArrowDecoder();
    const res = await this.queryRaw(query, undefined, signal);
    const reader = await decoder.readStream(responseByteStream(res));
    for await (const batch of reader) yield batch;
  }

  /**
   * Runs a query asking the server for CSV output and parses it into row objects
   * (all values are strings).
   */
  async queryCsv(
    query: QueryInput,
    signal?: AbortSignal,
  ): Promise<QueryResult<Record<string, string>>> {
    const res = await this.queryRaw(query, "csv", signal);
    const queryId = res.headers.get(QUERY_ID_HEADER);
    return { rows: parseCsv(await res.text()), queryId };
  }

  /**
   * Runs a query asking the server to materialize `format`, and returns the raw
   * `Response`. Use this to stream large results, write to a file, or handle any
   * output format (including the default compressed Arrow stream when `format`
   * is omitted) yourself.
   */
  queryRaw(query: QueryInput, format?: OutputFormat, signal?: AbortSignal): Promise<Response> {
    const output = format === undefined ? undefined : { format };
    return this.http.fetchRaw("POST", "/api/query", { json: buildBody(query, output), signal });
  }

  /**
   * Validates a query body without executing it (`POST /api/parse-query`).
   * Returns true when the payload is well-formed.
   */
  async parseQuery(query: QueryInput): Promise<boolean> {
    try {
      await this.http.fetchRaw("POST", "/api/parse-query", { json: buildBody(query) });
      return true;
    } catch {
      return false;
    }
  }

  /** Returns the planner's explanation of a query without running it. */
  explainQuery<T = unknown>(query: QueryInput): Promise<T> {
    return this.http.fetchJson<T>("POST", "/api/explain-query", { json: buildBody(query) });
  }

  /** Fetches recorded metrics for a previously executed query by its id. */
  queryMetrics(queryId: string): Promise<QueryMetricsView> {
    return this.http.fetchJson<QueryMetricsView>(
      "GET",
      `/api/query/metrics/${encodeURIComponent(queryId)}`,
    );
  }

  // -- tables -----------------------------------------------------------------

  /** Lists registered table names (`GET /api/tables`). */
  tables(): Promise<string[]> {
    return this.http.fetchJson<string[]>("GET", "/api/tables");
  }

  /** Lists registered tables together with their Arrow schemas. */
  tablesWithSchema<T = unknown[]>(): Promise<T> {
    return this.http.fetchJson<T>("GET", "/api/tables-with-schema");
  }

  /** Gets the Arrow schema of a table (`GET /api/table-schema`). */
  tableSchema<T = unknown>(tableName: string): Promise<T> {
    return this.http.fetchJson<T>("GET", "/api/table-schema", { query: { table_name: tableName } });
  }

  /** Gets a table's configuration (`GET /api/table-config`). */
  tableConfig<T = unknown>(tableName: string): Promise<T> {
    return this.http.fetchJson<T>("GET", "/api/table-config", { query: { table_name: tableName } });
  }

  /** Gets the default table (`GET /api/default-table`). */
  defaultTable<T = unknown>(): Promise<T> {
    return this.http.fetchJson<T>("GET", "/api/default-table");
  }

  /** Gets the default table's schema (`GET /api/default-table-schema`). */
  defaultTableSchema<T = unknown>(): Promise<T> {
    return this.http.fetchJson<T>("GET", "/api/default-table-schema");
  }

  // -- datasets ---------------------------------------------------------------

  /** Lists datasets with format metadata (`GET /api/list-datasets`). */
  datasets<T = unknown[]>(opts: { pattern?: string; limit?: number } = {}): Promise<T> {
    return this.http.fetchJson<T>("GET", "/api/list-datasets", {
      query: { pattern: opts.pattern, limit: opts.limit },
    });
  }

  /** Gets the schema of a single dataset file (`GET /api/dataset-schema`). */
  datasetSchema<T = unknown>(file: string): Promise<T> {
    return this.http.fetchJson<T>("GET", "/api/dataset-schema", { query: { file } });
  }

  /** Counts the total number of datasets (`GET /api/total-datasets`). */
  totalDatasets(): Promise<number> {
    return this.http.fetchJson<number>("GET", "/api/total-datasets");
  }

  // -- functions & info -------------------------------------------------------

  /** Lists available scalar/aggregate functions (`GET /api/functions`). */
  functions<T = unknown[]>(): Promise<T> {
    return this.http.fetchJson<T>("GET", "/api/functions");
  }

  /** Lists available table-valued functions (`GET /api/table-functions`). */
  tableFunctions<T = unknown[]>(): Promise<T> {
    return this.http.fetchJson<T>("GET", "/api/table-functions");
  }

  /** Returns runtime system information — version, host, resources (`GET /api/info`). */
  info<T = unknown>(): Promise<T> {
    return this.http.fetchJson<T>("GET", "/api/info");
  }

  /** Liveness probe (`GET /api/health`). Resolves when the server returns 200. */
  async health(): Promise<boolean> {
    try {
      await this.http.fetchRaw("GET", "/api/health");
      return true;
    } catch {
      return false;
    }
  }
}

/** Builds the `/api/query` request body from any accepted query input. */
function buildBody(query: QueryInput, output?: Output): Record<string, unknown> {
  const body: Record<string, unknown> = typeof query === "string" ? { sql: query } : { ...query };
  if (output !== undefined) body.output = output;
  return body;
}
