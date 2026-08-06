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
  CatalogsView,
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
   * The decode path. Defaults to `"arrow"`, decoding Beacon's native (zstd)
   * Arrow IPC stream. Set `"csv"` to instead request CSV output and parse it
   * (all values become strings, and the result's `table` is absent).
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
    const { batches } = await this.queryBatches(query, signal);
    for await (const batch of batches) yield batch;
  }

  /**
   * Runs a query and yields each arriving chunk as an `apache-arrow` Table,
   * without buffering the whole result in memory.
   *
   * This is the stream of {@link queryStream} with every `RecordBatch` wrapped in
   * a single-batch Table. Use it when the consumer takes a Table — `getChild`,
   * `schema`, {@link rowsFromTable} — instead of a batch.
   */
  async *queryArrowTableStream(
    query: QueryInput,
    signal?: AbortSignal,
  ): AsyncGenerator<ArrowTable> {
    const decoder = await getArrowDecoder();
    const { batches } = await this.queryBatches(query, signal);
    for await (const batch of batches) yield decoder.tableFromBatches([batch]);
  }

  /**
   * Opens a streaming query, returning the server-assigned query id together
   * with an async iterable of Arrow `RecordBatch`es as they arrive.
   *
   * Unlike {@link queryStream}, this also surfaces the `x-beacon-query-id` header
   * (available before the first batch). Combined with an `AbortSignal` it lets a
   * caller render results progressively and stop early — e.g. after a preview
   * limit — without downloading the full result.
   */
  async queryBatches(
    query: QueryInput,
    signal?: AbortSignal,
  ): Promise<{ queryId: string | null; batches: AsyncIterable<ArrowRecordBatch> }> {
    const decoder = await getArrowDecoder();
    const res = await this.queryRaw(query, undefined, signal);
    const queryId = res.headers.get(QUERY_ID_HEADER);
    const batches = await decoder.readStream(responseByteStream(res));
    return { queryId, batches };
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
  explainQuery<T = unknown>(query: QueryInput, signal?: AbortSignal): Promise<T> {
    return this.http.fetchJson<T>("POST", "/api/explain-query", { json: buildBody(query), signal });
  }

  /**
   * Runs a query and returns its physical plan annotated with execution metrics
   * (`EXPLAIN ANALYZE`), as a PostgreSQL-style JSON plan. Unlike `explainQuery`,
   * this executes the query to collect actual row counts and timings. Pass a
   * `signal` to cancel the (potentially long-running) execution.
   */
  explainAnalyzeQuery<T = unknown>(query: QueryInput, signal?: AbortSignal): Promise<T> {
    return this.http.fetchJson<T>("POST", "/api/explain-analyze-query", {
      json: buildBody(query),
      signal,
    });
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

  /**
   * Lists every catalog, schema, and table visible to the caller
   * (`GET /api/catalogs`).
   *
   * Where {@link tables} covers only the default schema, this also covers
   * beacon's `system` schema, `information_schema`, and any attached remote
   * catalog.
   */
  catalogs(): Promise<CatalogsView> {
    return this.http.fetchJson<CatalogsView>("GET", "/api/catalogs");
  }

  /**
   * Lists registered tables together with their Arrow schemas.
   *
   * Returns *every* column of every table, so it is heavy on an instance with
   * wide tables (a beacon table can carry 100K+ columns) — fetch one table's
   * schema with {@link tableSchema} when that is all you need.
   */
  tablesWithSchema<T = unknown[]>(): Promise<T> {
    return this.http.fetchJson<T>("GET", "/api/tables-with-schema");
  }

  /**
   * Gets the Arrow schema of a table (`GET /api/table-schema`).
   *
   * The body is the Arrow schema as Arrow serializes it: `{ fields: [{ name,
   * data_type, nullable, metadata }], metadata }`, where `data_type` is a string
   * for a simple type and a single-key object for a parameterized one
   * (`{ "Timestamp": ["Microsecond", null] }`).
   *
   * The table resolves in the default catalog and schema unless `in` names
   * another one — pass `{ catalog, schema }` for a table outside it (e.g. one
   * in `information_schema` or an attached remote catalog).
   */
  tableSchema<T = unknown>(
    tableName: string,
    in_: { catalog?: string; schema?: string } = {},
  ): Promise<T> {
    return this.http.fetchJson<T>("GET", "/api/table-schema", {
      query: { table_name: tableName, catalog: in_.catalog, schema: in_.schema },
    });
  }

  /**
   * @deprecated Table configuration is no longer served: a table's stored
   * definition is engine bookkeeping, not an API contract. The endpoint is still
   * routed (admin-only) and answers `{ message }` explaining as much. Use
   * `tableSchema()` for columns and `SHOW EXTENSIONS FOR <table>` for extensions.
   */
  tableConfig<T = unknown>(tableName: string): Promise<T> {
    return this.http.fetchJson<T>("GET", "/api/admin/table-config", {
      query: { table_name: tableName },
    });
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

  /**
   * Gets the schema of a single dataset file (`GET /api/dataset-schema`), in the
   * same Arrow-native shape as {@link tableSchema}.
   */
  datasetSchema<T = unknown>(file: string): Promise<T> {
    return this.http.fetchJson<T>("GET", "/api/dataset-schema", { query: { file } });
  }

  /** Counts the total number of datasets (`GET /api/total-datasets`). */
  totalDatasets(): Promise<number> {
    return this.http.fetchJson<number>("GET", "/api/total-datasets");
  }

  // -- functions & info -------------------------------------------------------

  /**
   * Lists the scalar, aggregate, and window functions available in queries
   * (`GET /api/functions`).
   *
   * Table-valued functions (`read_parquet`, `read_netcdf`, …) are not included:
   * DataFusion does not catalog them, so the server has nothing to list them
   * from. See the docs for the table-function reference.
   */
  functions<T = unknown[]>(): Promise<T> {
    return this.http.fetchJson<T>("GET", "/api/functions");
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
