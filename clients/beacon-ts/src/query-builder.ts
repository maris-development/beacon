/**
 * A fluent, EF Core / LINQ-style builder that produces Beacon's JSON query DSL.
 *
 * JavaScript has no operator overloading, so predicates use method chaining
 * (`col("depth").gte(0)`) rather than `x.depth >= 0`. Otherwise the shape mirrors
 * EF Core's method syntax: `from(...).where(...).orderBy(...).take(...)`.
 *
 * ```ts
 * const { rows } = await beacon
 *   .from({ netcdf: { paths: ["argo.nc"] } })
 *   .select("TEMP", column("PSAL", "salinity"))
 *   .where((x) => x.depth.gte(0).and(x.depth.lte(100)))
 *   .orderByDescending("TEMP")
 *   .take(100)
 *   .execute();
 * ```
 */

import type { ArrowRecordBatch, ArrowTable } from "./arrow.js";
import type {
  Distinct,
  Filter,
  FilterValue,
  From,
  OutputFormat,
  QueryInput,
  Row,
  Select,
  Sort,
  StructuredQuery,
} from "./types.js";

/** The subset of `BeaconClient` a builder needs to execute itself. */
export interface QueryRunner {
  query<T>(query: QueryInput, options?: { format?: "arrow" | "csv"; signal?: AbortSignal }): Promise<{
    rows: T[];
    queryId: string | null;
    table?: ArrowTable;
  }>;
  queryArrow(query: QueryInput, signal?: AbortSignal): Promise<ArrowTable>;
  queryStream(query: QueryInput, signal?: AbortSignal): AsyncGenerator<ArrowRecordBatch>;
  queryCsv(
    query: QueryInput,
    signal?: AbortSignal,
  ): Promise<{ rows: Record<string, string>[]; queryId: string | null }>;
}

// -- filter expression builder ------------------------------------------------

/** A composable filter expression. Combine with `.and()` / `.or()`. */
export class FilterNode {
  constructor(readonly filter: Filter) {}

  /** Logical AND of this node and the others. */
  and(...others: FilterNode[]): FilterNode {
    return new FilterNode({ and: [this.filter, ...others.map((o) => o.filter)] });
  }

  /** Logical OR of this node and the others. */
  or(...others: FilterNode[]): FilterNode {
    return new FilterNode({ or: [this.filter, ...others.map((o) => o.filter)] });
  }

  /** The underlying DSL filter object. */
  build(): Filter {
    return this.filter;
  }
}

/**
 * A column reference; call a comparison method to produce a `FilterNode`.
 *
 * Generic over the column's value type `V` so that, with a typed row, the
 * comparison argument is checked (`x.TEMP.gte(0)` ok, `x.TEMP.gte("x")` errors).
 * Untyped columns default to any `FilterValue` (number or string).
 */
export class ColumnRef<V extends FilterValue = FilterValue> {
  constructor(private readonly name: string) {}

  eq(value: V): FilterNode {
    return new FilterNode({ column: this.name, eq: value });
  }
  neq(value: V): FilterNode {
    return new FilterNode({ column: this.name, neq: value });
  }
  gt(value: V): FilterNode {
    return new FilterNode({ column: this.name, gt: value });
  }
  gte(value: V): FilterNode {
    return new FilterNode({ column: this.name, gt_eq: value });
  }
  lt(value: V): FilterNode {
    return new FilterNode({ column: this.name, lt: value });
  }
  lte(value: V): FilterNode {
    return new FilterNode({ column: this.name, lt_eq: value });
  }
  /** Inclusive range: `min <= column <= max`. */
  between(min: V, max: V): FilterNode {
    return new FilterNode({ column: this.name, gt_eq: min, lt_eq: max });
  }
  isNull(): FilterNode {
    return new FilterNode({ is_null: { column: this.name } });
  }
  isNotNull(): FilterNode {
    return new FilterNode({ is_not_null: { column: this.name } });
  }
}

/** Starts a filter predicate for `name`, e.g. `col("depth").gte(0)`. */
export function col(name: string): ColumnRef {
  return new ColumnRef(name);
}

/** Logical AND of every node. */
export function and(...nodes: FilterNode[]): FilterNode {
  return new FilterNode({ and: nodes.map((n) => n.filter) });
}

/** Logical OR of every node. */
export function or(...nodes: FilterNode[]): FilterNode {
  return new FilterNode({ or: nodes.map((n) => n.filter) });
}

/** Maps a column's declared type to the value type its comparisons accept. */
export type FilterValueOf<X> = X extends number ? number : X extends string ? string : FilterValue;

/**
 * The entity passed to a `where(x => ...)` predicate: every property access
 * (`x.depth`) yields a `ColumnRef` typed by that column's value type.
 *
 * When the builder is given a row type (e.g. `beacon.from<Ctd>(...)`), this maps
 * to exactly that type's columns, so editors complete column names, typos are
 * type errors, and comparison values are checked against each column's type.
 * With the default untyped row (`Row = Record<string, unknown>`), `keyof` is
 * `string`, so any column name is accepted (no completion) and comparisons take
 * any `FilterValue` — the escape hatch for ad-hoc/aliased columns.
 */
export type Fields<T> = { [K in keyof T & string]: ColumnRef<FilterValueOf<T[K]>> };

/** A `where` argument: a node, a raw filter, or `x => node`. */
export type Predicate<T> = FilterNode | Filter | ((x: Fields<T>) => FilterNode | Filter);

function fieldsProxy<T>(): Fields<T> {
  return new Proxy(
    {},
    { get: (_t, prop) => new ColumnRef(String(prop)) },
  ) as Fields<T>;
}

function resolvePredicate<T>(pred: Predicate<T>): Filter {
  const value = typeof pred === "function" ? pred(fieldsProxy<T>()) : pred;
  return value instanceof FilterNode ? value.build() : value;
}

// -- select helpers -----------------------------------------------------------

/** A column projection, optionally aliased: `column("TEMP", "temperature")`. */
export function column(name: string, alias?: string): Select {
  return alias === undefined ? name : { column: name, alias };
}

/** A function projection: `func("avg", ["TEMP"], "mean")`. */
export function func(name: string, args: Array<string | Select>, alias?: string): Select {
  return { function: name, args, alias };
}

/** A literal projection: `literal(0, "zero")`. */
export function literal(value: string | number | boolean | null, alias?: string): Select {
  return { value, alias };
}

/**
 * A column-name string for projection/ordering. When the builder has a row type,
 * editors complete `T`'s keys, but any string is still accepted (for `"*"`,
 * aliases, and expressions). The `string & {}` arm preserves completion of the
 * key literals while widening the type to all strings.
 */
export type ColumnName<T> = (keyof T & string) | (string & {});

// -- query builder ------------------------------------------------------------

/** Fluent builder for a structured (JSON DSL) query. */
export class QueryBuilder<T = Row> {
  private _select: Select[] = [];
  private _filters: Filter[] = [];
  private _from?: From;
  private _sort: Sort[] = [];
  private _distinct?: Distinct;
  private _offset?: number;
  private _limit?: number;
  private _output?: OutputFormat;

  constructor(
    private readonly client?: QueryRunner,
    from?: From,
  ) {
    this._from = from;
  }

  // -- source --
  /** Sets the data source (a table name or a `{ format: { paths } }` object). */
  from(source: From): this {
    this._from = source;
    return this;
  }
  /** Reads from a registered table by name. */
  fromTable(name: string): this {
    this._from = name;
    return this;
  }
  fromParquet(paths: string | string[]): this {
    this._from = { parquet: { paths: toArray(paths) } };
    return this;
  }
  fromCsv(paths: string | string[], delimiter?: string): this {
    this._from = { csv: { paths: toArray(paths), delimiter } };
    return this;
  }
  fromArrow(paths: string | string[]): this {
    this._from = { arrow: { paths: toArray(paths) } };
    return this;
  }
  fromNetcdf(paths: string | string[]): this {
    this._from = { netcdf: { paths: toArray(paths) } };
    return this;
  }
  fromOdv(paths: string | string[]): this {
    this._from = { odv: { paths: toArray(paths) } };
    return this;
  }
  fromZarr(paths: string | string[]): this {
    this._from = { zarr: { paths: toArray(paths) } };
    return this;
  }
  fromTiff(paths: string | string[]): this {
    this._from = { tiff: { paths: toArray(paths) } };
    return this;
  }
  fromBbf(paths: string | string[]): this {
    this._from = { bbf: { paths: toArray(paths) } };
    return this;
  }

  // -- projection --
  /** Adds projected items (column names, `column()`, `func()`, `literal()`). */
  select(...items: Array<ColumnName<T> | Select>): this {
    this._select.push(...items);
    return this;
  }

  /** Adds a DISTINCT clause keyed by `on`, projecting `select` (defaults to `on`). */
  distinct(on: Array<ColumnName<T> | Select>, select?: Array<ColumnName<T> | Select>): this {
    this._distinct = { on, select: select ?? on };
    return this;
  }

  // -- filtering --
  /** Adds a filter. Multiple `where` calls are combined with AND. */
  where(predicate: Predicate<T>): this {
    this._filters.push(resolvePredicate(predicate));
    return this;
  }

  // -- ordering --
  orderBy(column: ColumnName<T>): this {
    this._sort.push({ Asc: column });
    return this;
  }
  orderByDescending(column: ColumnName<T>): this {
    this._sort.push({ Desc: column });
    return this;
  }
  /** Alias of `orderBy`, for chaining additional sort keys. */
  thenBy(column: ColumnName<T>): this {
    return this.orderBy(column);
  }
  /** Alias of `orderByDescending`, for chaining additional sort keys. */
  thenByDescending(column: ColumnName<T>): this {
    return this.orderByDescending(column);
  }

  // -- paging --
  /** Skips the first `n` rows (DSL `offset`). */
  skip(n: number): this {
    this._offset = n;
    return this;
  }
  /** Limits the result to `n` rows (DSL `limit`). */
  take(n: number): this {
    this._limit = n;
    return this;
  }

  // -- output --
  /** Sets a materialized output format (for `raw()` downloads). */
  output(format: OutputFormat): this {
    this._output = format;
    return this;
  }

  // -- build --
  /** Builds the JSON DSL query object. */
  build(): StructuredQuery {
    const query: StructuredQuery = { select: this._select };
    const filter = combineFilters(this._filters);
    if (filter) query.filter = filter;
    if (this._from !== undefined) query.from = this._from;
    if (this._sort.length) query.sort_by = this._sort;
    if (this._distinct) query.distinct = this._distinct;
    if (this._offset !== undefined) query.offset = this._offset;
    if (this._limit !== undefined) query.limit = this._limit;
    if (this._output !== undefined) query.output = { format: this._output };
    return query;
  }

  /** Alias of `build()` so `JSON.stringify(builder)` emits the DSL. */
  toJSON(): StructuredQuery {
    return this.build();
  }

  // -- execution (require a bound client) --
  /** Executes and returns rows plus metadata. */
  execute(options?: { format?: "arrow" | "csv"; signal?: AbortSignal }) {
    return this.runner().query<T>(this.build(), options);
  }
  /** Executes and returns just the row array (EF `ToList()`-style). */
  async toArray(signal?: AbortSignal): Promise<T[]> {
    const { rows } = await this.runner().query<T>(this.build(), { signal });
    return rows;
  }
  /** Executes and returns the first row, or `null` if there are none. */
  async first(signal?: AbortSignal): Promise<T | null> {
    const rows = await this.take(1).toArray(signal);
    return rows[0] ?? null;
  }
  /** Executes and decodes the result into an Arrow Table. */
  toArrow(signal?: AbortSignal): Promise<ArrowTable> {
    return this.runner().queryArrow(this.build(), signal);
  }
  /** Executes and streams Arrow RecordBatches. */
  stream(signal?: AbortSignal): AsyncGenerator<ArrowRecordBatch> {
    return this.runner().queryStream(this.build(), signal);
  }
  /** Executes asking for CSV output and returns parsed string rows. */
  toCsv(signal?: AbortSignal) {
    return this.runner().queryCsv(this.build(), signal);
  }

  private runner(): QueryRunner {
    if (!this.client) {
      throw new Error("This QueryBuilder is not bound to a client; use build() to get the DSL.");
    }
    return this.client;
  }
}

function combineFilters(filters: Filter[]): Filter | undefined {
  if (filters.length === 0) return undefined;
  if (filters.length === 1) return filters[0];
  return { and: filters };
}

function toArray(value: string | string[]): string[] {
  return Array.isArray(value) ? value : [value];
}
