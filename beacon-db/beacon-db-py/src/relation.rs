//! The lazy relation: a query you compose without running it.
//!
//! A `Relation` is a **deferred SQL builder** (plan §4.3, approach A). Relational methods
//! (`filter`, `project`, `aggregate`, `order`, `limit`, `join`, …) each return a *new* relation
//! carrying an accumulated `SELECT`; nothing touches the engine until a terminal method
//! (`fetchall`, `arrow`, `df`, …) runs it. Building a relation over a billion-row table costs
//! nothing.
//!
//! # Why a clause builder rather than blind subquery nesting
//!
//! The naive approach — wrap the previous SQL in `SELECT * FROM (<prev>) WHERE …` for every
//! operation — is *wrong* for the single most common chain, `.order(...).limit(n)`:
//!
//! ```sql
//! SELECT * FROM (SELECT * FROM t ORDER BY n DESC) LIMIT 10   -- order is NOT guaranteed
//! ```
//!
//! An inner `ORDER BY` under an outer query without its own `ORDER BY` is unspecified by the SQL
//! standard. So this builder keeps `ORDER BY` and `LIMIT` in the *same* select whenever it can,
//! and only wraps the accumulated select into a subquery when a new clause would otherwise
//! change an existing one's meaning (a second `LIMIT`, a `WHERE` after a `LIMIT`, a projection
//! after a projection). The result is that `.order("n desc").limit(10)` renders as one select
//! with both clauses — a correct top-10.
//!
//! # Materialization
//!
//! A relation caches its result the first time a terminal method runs it, so `fetchone()` has a
//! stable cursor and a following `arrow()` sees the same rows. Re-running means building a fresh
//! relation. (True streaming — running without buffering — is a later milestone.)

use std::sync::Arc;

use std::path::PathBuf;

use beacon_arrow_odv::writer::{ColumnInfo, OdvOptions};
use beacon_core::embedded::Database;
use beacon_core::query::output::OutputFormat;
use beacon_core::AuthIdentity;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyList, PyTuple};

use crate::errors::{map_engine_error, programming_error};
use crate::exec::{run_sql, run_to_file, schema_of};
use crate::result::{import_or_hint, ResultSet};

/// What a [`Select`] reads from.
#[derive(Clone)]
enum Source {
    /// A complete SQL statement from `con.sql(...)`. Rendered **verbatim** while the select has
    /// no added clauses, so the statement's own `ORDER BY`/`LIMIT` survive — wrapping it in
    /// `SELECT * FROM (...)` would let the optimizer legally drop an inner ordering. Wrapped as
    /// a subquery once a relational method is chained onto it.
    Query(String),
    /// A `FROM`-clause fragment: a table name, a table-function call (`read_parquet(...)`), or a
    /// subquery already written with its alias.
    From(String),
}

/// The accumulated `SELECT`. Each field maps to one clause; `render` assembles them.
#[derive(Clone)]
struct Select {
    projection: String,
    distinct: bool,
    source: Source,
    filters: Vec<String>,
    group_by: Option<String>,
    order_by: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
}

impl Select {
    /// A `SELECT *` over a `FROM`-clause fragment.
    fn from_fragment(fragment: String) -> Self {
        Self::new(Source::From(fragment))
    }

    /// A relation that *is* a complete SQL statement (from `con.sql(...)`).
    fn from_query(query: String) -> Self {
        Self::new(Source::Query(query))
    }

    fn new(source: Source) -> Self {
        Self {
            projection: "*".to_string(),
            distinct: false,
            source,
            filters: Vec::new(),
            group_by: None,
            order_by: None,
            limit: None,
            offset: None,
        }
    }

    /// Whether a projection has already been chosen (i.e. this is no longer `SELECT *`).
    fn has_projection(&self) -> bool {
        self.projection != "*" || self.distinct || self.group_by.is_some()
    }

    /// Whether any row-shaping clause that a later operation could reinterpret is present.
    fn has_tail(&self) -> bool {
        self.order_by.is_some() || self.limit.is_some() || self.offset.is_some()
    }

    /// Whether no clause has been added, so a [`Source::Query`] can run verbatim.
    fn is_pristine(&self) -> bool {
        self.projection == "*"
            && !self.distinct
            && self.filters.is_empty()
            && self.group_by.is_none()
            && !self.has_tail()
    }

    /// The `FROM`-clause text for this source.
    fn from_clause(&self) -> String {
        match &self.source {
            Source::From(fragment) => fragment.clone(),
            Source::Query(query) => format!("({query}) AS _rel"),
        }
    }

    fn render(&self) -> String {
        // A bare `con.sql(q)` runs `q` unchanged, so its own ORDER BY/LIMIT are preserved.
        if let Source::Query(query) = &self.source {
            if self.is_pristine() {
                return query.clone();
            }
        }

        let mut sql = String::from("SELECT ");
        if self.distinct {
            sql.push_str("DISTINCT ");
        }
        sql.push_str(&self.projection);
        sql.push_str(" FROM ");
        sql.push_str(&self.from_clause());
        if !self.filters.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&self.filters.join(" AND "));
        }
        if let Some(group_by) = &self.group_by {
            sql.push_str(" GROUP BY ");
            sql.push_str(group_by);
        }
        if let Some(order_by) = &self.order_by {
            sql.push_str(" ORDER BY ");
            sql.push_str(order_by);
        }
        if let Some(limit) = self.limit {
            sql.push_str(&format!(" LIMIT {limit}"));
        }
        if let Some(offset) = self.offset {
            sql.push_str(&format!(" OFFSET {offset}"));
        }
        sql
    }

    /// Turns the current select into the `FROM` of a fresh `SELECT *`, so a new clause starts
    /// from a clean slate. This is the "wrap" half of fold-or-wrap.
    fn wrapped(&self) -> Self {
        Self::from_fragment(format!("({}) AS _rel", self.render()))
    }
}

/// A lazily-composed query over an open [`Database`].
#[pyclass(module = "beacondb", name = "Relation")]
pub struct Relation {
    database: Arc<Database>,
    identity: AuthIdentity,
    select: Select,
    /// The result of the first terminal call, reused by later ones so the fetch cursor is
    /// stable and columnar reads see the same rows.
    materialized: Option<ResultSet>,
}

impl Relation {
    /// A relation reading from `from` — a table name, a table-function call (`read_parquet(...)`),
    /// or a parenthesized subquery.
    pub fn over(database: Arc<Database>, identity: AuthIdentity, from: String) -> Self {
        Self {
            database,
            identity,
            select: Select::from_fragment(from),
            materialized: None,
        }
    }

    /// A relation that is a complete SQL statement (from `con.sql(...)`).
    ///
    /// The statement is run verbatim until a relational method is chained onto it, so
    /// `con.sql("… ORDER BY …").fetchall()` keeps its ordering (wrapping it in a subquery up
    /// front would let the optimizer drop that inner `ORDER BY`).
    pub fn from_query(database: Arc<Database>, identity: AuthIdentity, query: &str) -> Self {
        Self {
            database,
            identity,
            select: Select::from_query(query.to_string()),
            materialized: None,
        }
    }

    /// A new relation sharing the database/identity, carrying `select`.
    fn with_select(&self, select: Select) -> Self {
        Self {
            database: self.database.clone(),
            identity: self.identity.clone(),
            select,
            materialized: None,
        }
    }

    /// The rendered SQL.
    fn sql_text(&self) -> String {
        self.select.render()
    }

    /// Runs the relation once and caches the result.
    fn materialize(&mut self, py: Python<'_>) -> PyResult<&mut ResultSet> {
        if self.materialized.is_none() {
            let result = run_sql(py, &self.database, &self.identity, self.sql_text())?;
            self.materialized = Some(result);
        }
        Ok(self.materialized.as_mut().expect("just materialized"))
    }

    /// Wraps the current select as a subquery and starts a fresh `SELECT *` over it. Used by
    /// aggregate/scalar/join/set operations that always need a clean scope.
    fn derived_from_subquery(&self) -> Select {
        Select::from_fragment(format!("({}) AS _rel", self.sql_text()))
    }
}

#[pymethods]
impl Relation {
    // ---- relational, lazy, chainable ---------------------------------------------------

    /// Keeps rows matching a SQL boolean expression: `filter("depth <= 100")`.
    fn filter(&self, condition: &str) -> Self {
        // A WHERE evaluated in the same select as a projection would see pre-projection columns,
        // not the filtered-relation columns the caller means — so fold only over a bare
        // `SELECT * ... [WHERE ...]`, and wrap otherwise.
        let mut select = if self.select.has_projection() || self.select.has_tail() {
            self.select.wrapped()
        } else {
            self.select.clone()
        };
        select.filters.push(format!("({condition})"));
        self.with_select(select)
    }

    /// Projects a SQL select list: `project("user_id, count(*) AS n")`.
    #[pyo3(text_signature = "(self, expressions)")]
    fn project(&self, expressions: &str) -> Self {
        let mut select = if self.select.has_projection() || self.select.has_tail() {
            self.select.wrapped()
        } else {
            self.select.clone()
        };
        select.projection = expressions.to_string();
        self.with_select(select)
    }

    /// Alias of [`Self::project`], under the conventional name `select`.
    fn select(&self, expressions: &str) -> Self {
        self.project(expressions)
    }

    /// Groups and aggregates: `aggregate("user_id, count(*) AS n", "user_id")`. An empty
    /// `groups` aggregates the whole relation.
    #[pyo3(signature = (expressions, groups=""))]
    fn aggregate(&self, expressions: &str, groups: &str) -> Self {
        let mut select = if self.select.has_projection() || self.select.has_tail() {
            self.select.wrapped()
        } else {
            self.select.clone()
        };
        select.projection = expressions.to_string();
        if !groups.trim().is_empty() {
            select.group_by = Some(groups.to_string());
        }
        self.with_select(select)
    }

    /// Orders rows: `order("n desc")`.
    fn order(&self, expressions: &str) -> Self {
        // Fold onto the current select unless it already has an ordering or a limit — keeping
        // ORDER BY and a following LIMIT in one select is the whole point (see the module docs).
        let mut select = if self.select.has_tail() {
            self.select.wrapped()
        } else {
            self.select.clone()
        };
        select.order_by = Some(expressions.to_string());
        self.with_select(select)
    }

    /// Alias of [`Self::order`], under the conventional name `sort`.
    fn sort(&self, expressions: &str) -> Self {
        self.order(expressions)
    }

    /// Limits the row count, optionally skipping `offset` rows first.
    #[pyo3(signature = (n, offset=0))]
    fn limit(&self, n: i64, offset: i64) -> Self {
        let mut select = if self.select.limit.is_some() || self.select.offset.is_some() {
            self.select.wrapped()
        } else {
            self.select.clone()
        };
        select.limit = Some(n);
        if offset > 0 {
            select.offset = Some(offset);
        }
        self.with_select(select)
    }

    /// Removes duplicate rows.
    fn distinct(&self) -> Self {
        // DISTINCT applies to the projection; if a limit/order is already in place it must run
        // after DISTINCT, so wrap. A repeated DISTINCT also wraps.
        let mut select = if self.select.distinct || self.select.has_tail() {
            self.select.wrapped()
        } else {
            self.select.clone()
        };
        select.distinct = true;
        self.with_select(select)
    }

    /// Joins another relation: `join(other, "l.id = r.id", how="inner")`.
    ///
    /// `how` is one of `inner`, `left`, `right`, `full`, `cross`. The left relation is aliased
    /// `_l` and the right `_r`, so write the condition in those terms.
    #[pyo3(signature = (other, on=None, how="inner"))]
    fn join(&self, other: &Relation, on: Option<&str>, how: &str) -> PyResult<Self> {
        let keyword = match how.to_ascii_lowercase().as_str() {
            "inner" => "INNER JOIN",
            "left" => "LEFT JOIN",
            "right" => "RIGHT JOIN",
            "full" | "outer" | "full outer" => "FULL JOIN",
            "cross" => "CROSS JOIN",
            other => {
                return Err(crate::errors::programming_error(format!(
                    "unknown join type `{other}`; use inner, left, right, full, or cross"
                )))
            }
        };
        let from = if keyword == "CROSS JOIN" {
            format!(
                "({}) AS _l CROSS JOIN ({}) AS _r",
                self.sql_text(),
                other.sql_text()
            )
        } else {
            let condition = on.ok_or_else(|| {
                crate::errors::programming_error(format!(
                    "a {how} join needs an `on` condition (e.g. \"_l.id = _r.id\")"
                ))
            })?;
            format!(
                "({}) AS _l {keyword} ({}) AS _r ON {condition}",
                self.sql_text(),
                other.sql_text()
            )
        };
        Ok(self.with_select(Select::from_fragment(from)))
    }

    /// Stacks another relation's rows, keeping duplicates.
    fn union_all(&self, other: &Relation) -> Self {
        let from = format!(
            "({} UNION ALL {}) AS _rel",
            self.sql_text(),
            other.sql_text()
        );
        self.with_select(Select::from_fragment(from))
    }

    /// Stacks another relation's rows, removing duplicates.
    fn union(&self, other: &Relation) -> Self {
        let from = format!("({} UNION {}) AS _rel", self.sql_text(), other.sql_text());
        self.with_select(Select::from_fragment(from))
    }

    /// Counts rows: a one-row relation with a single `count` column.
    fn count(&self) -> Self {
        let mut select = self.derived_from_subquery();
        select.projection = "count(*) AS \"count\"".to_string();
        self.with_select(select)
    }

    /// Aggregates a column with `sum`. One-row relation with a single `sum` column.
    fn sum(&self, column: &str) -> Self {
        self.scalar_aggregate("sum", column)
    }

    fn min(&self, column: &str) -> Self {
        self.scalar_aggregate("min", column)
    }

    fn max(&self, column: &str) -> Self {
        self.scalar_aggregate("max", column)
    }

    /// Aggregates a column with the mean (`avg`).
    fn mean(&self, column: &str) -> Self {
        self.scalar_aggregate("avg", column)
    }

    /// Runs SQL that refers to this relation by `name`, via a CTE — no view is registered.
    ///
    /// `rel.query("t", "SELECT * FROM t WHERE x > 3")` renders
    /// `WITH t AS (<rel>) SELECT * FROM t WHERE x > 3`. `sql` must be a single statement.
    fn query(&self, name: &str, sql: &str) -> Self {
        let cte = format!("WITH {} AS ({}) {}", quote_ident(name), self.sql_text(), sql);
        Self::over(
            self.database.clone(),
            self.identity.clone(),
            format!("({cte}) AS _rel"),
        )
    }

    // ---- terminal: materialize ---------------------------------------------------------

    fn fetchone<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyTuple>>> {
        self.materialize(py)?.fetchone(py)
    }

    #[pyo3(signature = (size=1))]
    fn fetchmany<'py>(&mut self, py: Python<'py>, size: usize) -> PyResult<Bound<'py, PyList>> {
        self.materialize(py)?.fetchmany(py, size)
    }

    fn fetchall<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        self.materialize(py)?.fetchall(py)
    }

    /// The Arrow C Data Interface stream — how any columnar consumer reads this relation.
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &mut self,
        py: Python<'py>,
        requested_schema: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let _ = requested_schema;
        self.materialize(py)?.arrow_c_stream(py)
    }

    /// The relation as a `pyarrow.Table`.
    fn arrow<'py>(slf: Bound<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        slf.borrow_mut().materialize(py)?;
        let pyarrow = import_or_hint(py, "pyarrow", "beacondb[arrow]")?;
        pyarrow.call_method1("table", (slf,))
    }

    /// Alias of [`Self::arrow`].
    fn fetch_arrow_table<'py>(slf: Bound<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        Self::arrow(slf)
    }

    /// Alias of [`Self::arrow`].
    fn to_arrow_table<'py>(slf: Bound<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        Self::arrow(slf)
    }

    /// The relation as a `pandas.DataFrame`.
    fn df<'py>(slf: Bound<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        import_or_hint(py, "pandas", "beacondb[pandas]")?;
        Self::arrow(slf)?.call_method0("to_pandas")
    }

    /// Aliases of [`Self::df`].
    fn to_df<'py>(slf: Bound<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        Self::df(slf)
    }

    fn fetchdf<'py>(slf: Bound<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        Self::df(slf)
    }

    /// The relation as a `polars.DataFrame`.
    fn pl<'py>(slf: Bound<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        slf.borrow_mut().materialize(py)?;
        let polars = import_or_hint(py, "polars", "beacondb[polars]")?;
        polars.call_method1("DataFrame", (slf,))
    }

    /// A streaming `pyarrow.RecordBatchReader` over the relation.
    ///
    /// Batches are pulled from the engine on demand as the reader is iterated (the GIL is released
    /// during each pull), so a query over a huge file never lands in memory all at once — unlike
    /// `.arrow()`/`.df()`, which collect. `batch_size` re-chunks to roughly that many rows per
    /// batch; omit it for the engine's native batches (most efficient). Each call runs the query
    /// afresh.
    #[pyo3(signature = (batch_size=None))]
    fn record_batch<'py>(
        &self,
        py: Python<'py>,
        batch_size: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        crate::stream::record_batch_reader(
            py,
            &self.database,
            &self.identity,
            self.sql_text(),
            batch_size,
        )
    }

    /// Alias of [`Self::record_batch`], under the conventional name `fetch_record_batch`.
    #[pyo3(signature = (batch_size=None))]
    fn fetch_record_batch<'py>(
        &self,
        py: Python<'py>,
        batch_size: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.record_batch(py, batch_size)
    }

    /// Alias of [`Self::record_batch`].
    #[pyo3(signature = (batch_size=None))]
    fn fetch_arrow_reader<'py>(
        &self,
        py: Python<'py>,
        batch_size: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.record_batch(py, batch_size)
    }

    // ---- terminal: file sinks ----------------------------------------------------------

    /// Writes the relation to a Parquet file.
    fn to_parquet(&self, py: Python<'_>, path: &str) -> PyResult<()> {
        self.write(py, OutputFormat::Parquet, path)
    }

    /// Writes the relation to a CSV file.
    fn to_csv(&self, py: Python<'_>, path: &str) -> PyResult<()> {
        self.write(py, OutputFormat::Csv, path)
    }

    /// Writes the relation to an Arrow IPC (Feather) file.
    fn to_arrow_ipc(&self, py: Python<'_>, path: &str) -> PyResult<()> {
        self.write(py, OutputFormat::Ipc, path)
    }

    /// Alias of [`Self::to_arrow_ipc`].
    fn to_ipc(&self, py: Python<'_>, path: &str) -> PyResult<()> {
        self.write(py, OutputFormat::Ipc, path)
    }

    /// Writes the relation to a flat (record-oriented) NetCDF-4 file.
    fn to_netcdf(&self, py: Python<'_>, path: &str) -> PyResult<()> {
        self.write(py, OutputFormat::NetCDF, path)
    }

    /// Writes the relation to an HDF5 file. A NetCDF-4 file *is* HDF5, so this is the flat
    /// NetCDF writer under an HDF5-friendly name — the output is a valid HDF5 file that h5py and
    /// `read_hdf5` read back. For a gridded/multi-dimensional layout, use [`Self::to_nd_netcdf`].
    fn to_hdf5(&self, py: Python<'_>, path: &str) -> PyResult<()> {
        self.write(py, OutputFormat::NetCDF, path)
    }

    /// Writes the relation to a multi-dimensional NetCDF-4 file, using `dimensions` as the grid
    /// axes. `dimensions` must not be empty — that is what distinguishes this from
    /// [`Self::to_netcdf`].
    fn to_nd_netcdf(&self, py: Python<'_>, path: &str, dimensions: Vec<String>) -> PyResult<()> {
        if dimensions.is_empty() {
            return Err(programming_error(
                "to_nd_netcdf needs at least one dimension column; use to_netcdf for a flat file",
            ));
        }
        self.write(
            py,
            OutputFormat::NdNetCDF {
                dimension_columns: dimensions,
            },
            path,
        )
    }

    /// Writes the relation to a GeoParquet file, encoding a geometry column from the named
    /// longitude/latitude columns. With neither named, beacon auto-detects lon/lat columns.
    #[pyo3(signature = (path, longitude=None, latitude=None))]
    fn to_geoparquet(
        &self,
        py: Python<'_>,
        path: &str,
        longitude: Option<String>,
        latitude: Option<String>,
    ) -> PyResult<()> {
        self.write(
            py,
            OutputFormat::GeoParquet {
                longitude_column: longitude,
                latitude_column: latitude,
            },
            path,
        )
    }

    /// Writes the relation to an Ocean Data View (ODV) archive (a `.zip`).
    ///
    /// The ODV column layout is inferred from the result schema: columns whose names match ODV's
    /// standard headers become the longitude/latitude/depth/time/key columns, and the rest are
    /// classified as data columns (those with a `<col>_qf`/`<col>_qc` quality-flag companion) or
    /// metadata columns. Most data does not use ODV's exact header names, so pass the column names
    /// to map them — e.g. `to_odv("out.zip", longitude="LONGITUDE", latitude="LATITUDE",
    /// depth="PRES", time="JULD", key="platform")`. A named column that the schema does not contain
    /// makes the engine error clearly at write time.
    #[pyo3(signature = (path, *, longitude=None, latitude=None, depth=None, time=None, key=None, qf_schema=None))]
    #[allow(clippy::too_many_arguments)]
    fn to_odv(
        &self,
        py: Python<'_>,
        path: &str,
        longitude: Option<String>,
        latitude: Option<String>,
        depth: Option<String>,
        time: Option<String>,
        key: Option<String>,
        qf_schema: Option<String>,
    ) -> PyResult<()> {
        let schema = schema_of(py, &self.database, &self.identity, self.sql_text())?;
        let mut options = OdvOptions::try_from_arrow_schema(schema).map_err(map_engine_error)?;

        // Each override points a special column at the user's column *and* removes it from the
        // auto-classified data/metadata columns, so a promoted column is never written twice.
        if let Some(name) = longitude {
            demote_named_column(&mut options.data_columns, &mut options.meta_columns, &name);
            options.longitude_column.column_name = name;
        }
        if let Some(name) = latitude {
            demote_named_column(&mut options.data_columns, &mut options.meta_columns, &name);
            options.latitude_column.column_name = name;
        }
        if let Some(name) = depth {
            demote_named_column(&mut options.data_columns, &mut options.meta_columns, &name);
            options.depth_column.column_name = name;
        }
        if let Some(name) = time {
            demote_named_column(&mut options.data_columns, &mut options.meta_columns, &name);
            options.time_column.column_name = name;
        }
        if let Some(name) = key {
            demote_named_column(&mut options.data_columns, &mut options.meta_columns, &name);
            options.key_column = name;
        }
        if let Some(schema_id) = qf_schema {
            options.qf_schema = schema_id;
        }

        self.write(py, OutputFormat::Odv(options), path)
    }

    // ---- metadata / introspection ------------------------------------------------------

    /// The rendered SQL. Reading this never runs anything, which makes it the first thing to
    /// print when a chain does not do what you expect.
    #[getter]
    fn sql(&self) -> String {
        self.sql_text()
    }

    /// Column names. Plans the query to read its schema (no rows) unless already materialized.
    #[getter]
    fn columns(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let schema = match &self.materialized {
            Some(result) => result.schema(),
            None => schema_of(py, &self.database, &self.identity, self.sql_text())?,
        };
        Ok(schema.fields().iter().map(|f| f.name().to_string()).collect())
    }

    /// Column type names, alongside [`Self::columns`].
    #[getter]
    fn types(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let schema = match &self.materialized {
            Some(result) => result.schema(),
            None => schema_of(py, &self.database, &self.identity, self.sql_text())?,
        };
        Ok(schema
            .fields()
            .iter()
            .map(|f| f.data_type().to_string())
            .collect())
    }

    /// `(rows, columns)`. **Reading `rows` runs a `COUNT(*)`** over the relation — not free.
    #[getter]
    fn shape(&mut self, py: Python<'_>) -> PyResult<(i64, usize)> {
        let columns = self.columns(py)?.len();
        let rows = self.row_count(py)?;
        Ok((rows, columns))
    }

    /// The relation's row count (a `COUNT(*)`).
    fn __len__(&mut self, py: Python<'_>) -> PyResult<usize> {
        Ok(self.row_count(py)? as usize)
    }

    /// The logical and physical plans, as DataFusion prints them. With `analyze=True`, runs the
    /// query and annotates the physical plan with per-operator runtime metrics (rows, time, bytes)
    /// — DataFusion's `EXPLAIN ANALYZE`.
    #[pyo3(signature = (analyze=false))]
    fn explain(&self, py: Python<'_>, analyze: bool) -> PyResult<String> {
        let keyword = if analyze { "EXPLAIN ANALYZE" } else { "EXPLAIN" };
        let result = run_sql(
            py,
            &self.database,
            &self.identity,
            format!("{keyword} {}", self.sql_text()),
        )?;
        Ok(format_batches(&result))
    }

    /// Prints the first `limit` rows as a table (the conventional `show`). Returns nothing.
    #[pyo3(signature = (limit=10))]
    fn show(&self, py: Python<'_>, limit: i64) -> PyResult<()> {
        let result = run_sql(
            py,
            &self.database,
            &self.identity,
            format!("SELECT * FROM ({}) AS _rel LIMIT {limit}", self.sql_text()),
        )?;
        let rendered = format_batches(&result);
        py.import("builtins")?.call_method1("print", (rendered,))?;
        Ok(())
    }

    // ---- DDL from a relation -----------------------------------------------------------

    /// Creates a table from the relation and returns a relation over it. Needs DDL privileges
    /// (a super-user, i.e. auth off or an admin session).
    fn create(&self, py: Python<'_>, name: &str) -> PyResult<Self> {
        run_sql(
            py,
            &self.database,
            &self.identity,
            format!("CREATE TABLE {} AS {}", quote_ident(name), self.sql_text()),
        )?;
        Ok(Self::over(
            self.database.clone(),
            self.identity.clone(),
            quote_ident(name),
        ))
    }

    /// Creates a view from the relation and returns a relation over it.
    fn create_view(&self, py: Python<'_>, name: &str) -> PyResult<Self> {
        run_sql(
            py,
            &self.database,
            &self.identity,
            format!("CREATE VIEW {} AS {}", quote_ident(name), self.sql_text()),
        )?;
        Ok(Self::over(
            self.database.clone(),
            self.identity.clone(),
            quote_ident(name),
        ))
    }

    fn __repr__(&self) -> String {
        format!("<beacondb.Relation sql={:?}>", self.sql_text())
    }
}

impl Relation {
    /// Runs the relation and writes it to `path` in `format`.
    fn write(&self, py: Python<'_>, format: OutputFormat, path: &str) -> PyResult<()> {
        run_to_file(
            py,
            &self.database,
            &self.identity,
            self.sql_text(),
            format,
            PathBuf::from(path),
        )
    }

    fn scalar_aggregate(&self, func: &str, column: &str) -> Self {
        let mut select = self.derived_from_subquery();
        select.projection = format!("{func}({column}) AS \"{func}\"");
        self.with_select(select)
    }

    /// Counts rows without disturbing the relation's own materialization cache.
    fn row_count(&mut self, py: Python<'_>) -> PyResult<i64> {
        if let Some(result) = &self.materialized {
            return Ok(result.total_rows() as i64);
        }
        let result = run_sql(
            py,
            &self.database,
            &self.identity,
            format!("SELECT count(*) AS n FROM ({}) AS _rel", self.sql_text()),
        )?;
        Ok(count_scalar(&result))
    }
}

/// Double-quotes a SQL identifier, escaping embedded quotes, so a name with spaces or a stray
/// keyword can't break the surrounding statement.
pub(crate) fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Drops any auto-classified ODV data/metadata column with this name (case-insensitive), used
/// when the column is promoted to a special role (longitude, key, …) so it is not emitted twice.
fn demote_named_column(data: &mut Vec<ColumnInfo>, meta: &mut Vec<ColumnInfo>, name: &str) {
    data.retain(|column| !column.column_name.eq_ignore_ascii_case(name));
    meta.retain(|column| !column.column_name.eq_ignore_ascii_case(name));
}

/// Pretty-prints a result's batches the way the CLI does.
fn format_batches(result: &ResultSet) -> String {
    match arrow::util::pretty::pretty_format_batches(result.batches()) {
        Ok(table) => table.to_string(),
        Err(err) => format!("<could not format result: {err}>"),
    }
}

/// Reads a single `count(*)` value out of a one-row, one-column result.
fn count_scalar(result: &ResultSet) -> i64 {
    result
        .batches()
        .first()
        .filter(|batch| batch.num_rows() > 0)
        .and_then(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .map(|array| array.value(0))
        })
        .unwrap_or(0)
}
