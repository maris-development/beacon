/**
 * Hand-written ergonomic types for the Beacon query DSL and metadata responses.
 *
 * These mirror the structures in `beacon-core` (`query::Query`, `QueryBody`,
 * `Select`, `Filter`, `Output`). For the exhaustive, always-up-to-date schema
 * generated from the running server's OpenAPI document, run `npm run codegen`
 * and import from `./generated/schema`.
 */

/** A scalar literal usable in a `select` or `filter`. */
export type Literal = string | number | boolean | null;

/**
 * A single projected item in a structured query's `select`.
 *
 * - a bare column name: `"temperature"`
 * - a column with an alias: `{ column: "temp", alias: "t" }`
 * - a function call: `{ function: "avg", args: ["temp"], alias: "mean" }`
 * - a literal: `{ value: 0, alias: "zero" }`
 */
export type Select =
  | string
  | { column: string; alias?: string }
  | { function: string; args: Select[]; alias?: string }
  | { value: Literal; alias?: string };

/** A value usable in a comparison filter (number, string, or ISO timestamp). */
export type FilterValue = number | string;

/**
 * A row filter expression, matching `beacon-core`'s `Filter` enum. Comparison
 * predicates target a `column` and are matched by their fields; `and`/`or`
 * combine sub-filters; `is_null`/`is_not_null` are tagged. (There is no `not`
 * combinator — use `neq`/`is_not_null`.) The trailing index signature keeps the
 * type open for shapes not enumerated here (e.g. the GeoJSON predicate).
 */
export type Filter =
  | ComparisonFilter
  | { is_null: { column: string } }
  | { is_not_null: { column: string } }
  | { and: Filter[] }
  | { or: Filter[] }
  | Record<string, unknown>;

/** A leaf comparison predicate over a single `column`. */
export interface ComparisonFilter {
  column: string;
  eq?: FilterValue;
  neq?: FilterValue;
  gt?: FilterValue;
  gt_eq?: FilterValue;
  lt?: FilterValue;
  lt_eq?: FilterValue;
}

/**
 * A data source: either a registered table name (a bare string) or a single-key
 * object keyed by file format, e.g. `{ netcdf: { paths: ["a.nc"] } }` or
 * `{ csv: { paths: [...], delimiter: ";" } }`. Supported formats: `csv`,
 * `parquet`, `arrow`, `netcdf`, `odv`, `tiff`, `zarr`, `bbf`.
 */
export type From = string | Record<string, unknown>;

/**
 * Sort specification for a single column. Serializes as an externally-tagged
 * enum: `{ "Asc": "col" }` or `{ "Desc": "col" }`.
 */
export type Sort = { Asc: string } | { Desc: string };

/** Distinct-on specification. */
export interface Distinct {
  on: Select[];
  select: Select[];
}

/**
 * A materialized output format, matching `beacon-core`'s `OutputFormat` enum.
 *
 * Omitting `output` entirely yields the default zstd-compressed Arrow IPC
 * stream (decoded by `query()` / `queryArrow()`). Note there is **no JSON
 * output format**. The parameterized formats are single-key objects.
 */
export type OutputFormat =
  | SimpleOutputFormat
  | { ndnetcdf: { dimension_columns: string[] } }
  | { nd_netcdf: { dimension_columns: string[] } }
  | { ndzarr: { dimension_columns: string[] } }
  | { nd_zarr: { dimension_columns: string[] } }
  | { geoparquet: GeoParquetOptions }
  | { odv: Record<string, unknown> };

/**
 * Output formats expressed as a bare string. `arrow` is an alias for `ipc`.
 *
 * `zarr` returns a zip archive: a zarr store is a directory, and a single HTTP
 * response can only carry one file.
 */
export type SimpleOutputFormat =
  | "csv"
  | "ipc"
  | "arrow"
  | "parquet"
  | "netcdf"
  | "zarr";

export interface GeoParquetOptions {
  longitude_column?: string | null;
  latitude_column?: string | null;
}

export interface Output {
  format: OutputFormat;
}

/** The fields of a structured (non-SQL) query. */
export interface QueryBody {
  /** Columns, functions, or literals to project. */
  select: Select[];
  /** Row filter to apply. */
  filter?: Filter;
  /** Data source to read from. Defaults to the runtime's default table. */
  from?: From;
  /** Ordering to apply to the result. */
  sort_by?: Sort[];
  /** Distinct-on specification. */
  distinct?: Distinct;
  /** Number of rows to skip. */
  offset?: number;
  /** Maximum number of rows to return. */
  limit?: number;
}

/** A raw SQL query body. */
export interface SqlQuery {
  sql: string;
  output?: Output;
}

/** A structured JSON query body (DSL fields at the top level). */
export interface StructuredQuery extends QueryBody {
  output?: Output;
}

/**
 * Anything accepted by the query helpers: a raw SQL string, a `{ sql }` object,
 * or a structured DSL query. An `output` format may be attached to the object
 * forms; the high-level helpers set it for you.
 */
export type QueryInput = string | SqlQuery | StructuredQuery;

/** A decoded result row from `query()` (from the Arrow stream, or CSV). */
export type Row = Record<string, unknown>;

/** Planner/execution metrics returned by `GET /api/query/metrics/{id}`. */
export interface QueryMetricsView {
  input_rows: number;
  input_bytes: number;
  result_num_rows: number;
  result_size_in_bytes: number;
  [key: string]: unknown;
}

/** A registered crawler definition. Shape mirrors the admin crawler payload. */
export type Crawler = Record<string, unknown>;

/** A request to create an external table via the admin API. */
export type ExternalTableSpec = Record<string, unknown>;
