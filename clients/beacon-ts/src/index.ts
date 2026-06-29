/** Public API for the Beacon TypeScript SDK. */

export { BeaconClient } from "./client.js";
export type { QueryResult, QueryOptions } from "./client.js";
export { AdminClient } from "./admin.js";
export type {
  UploadResult,
  UploadProgress,
  UploadOptions,
  PrivilegeTarget,
  AuthUser,
  AuthRule,
  AuthRole,
} from "./admin.js";
export { Http, basicAuthHeader } from "./http.js";
export type { ClientOptions, FetchLike } from "./http.js";
export { BeaconError, ApiError, ConnectionError, TimeoutError } from "./errors.js";
export { rowsFromTable } from "./arrow.js";
export type { ArrowTable, ArrowRecordBatch } from "./arrow.js";
export { parseCsv, parseCsvRows } from "./csv.js";
export {
  QueryBuilder,
  ColumnRef,
  FilterNode,
  col,
  and,
  or,
  column,
  func,
  literal,
} from "./query-builder.js";
export type {
  Predicate,
  Fields,
  ColumnName,
  FilterValueOf,
  QueryRunner,
} from "./query-builder.js";
export type {
  Literal,
  FilterValue,
  Select,
  Filter,
  ComparisonFilter,
  From,
  Sort,
  Distinct,
  OutputFormat,
  SimpleOutputFormat,
  GeoParquetOptions,
  Output,
  QueryBody,
  SqlQuery,
  StructuredQuery,
  QueryInput,
  Row,
  QueryMetricsView,
  Crawler,
  ExternalTableSpec,
} from "./types.js";
