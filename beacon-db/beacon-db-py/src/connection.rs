//! `connect()` and the `Connection` object.
//!
//! A connection is an open [`Database`] plus one [`AuthIdentity`] and one result slot. The
//! database is shared (`Arc`) and the identity is not, which is what lets several connections
//! with different identities exist over a single container file — the file allows exactly one
//! handle per process, so per-connection identity is the only way to have more than one.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex, OnceLock, Weak};

use arrow::array::{Array, AsArray};

use beacon_core::embedded::{AuthMode, Database, DbPath, OpenOptions};
use beacon_core::query::{InnerQuery, Query};
use beacon_core::{AuthIdentity, Credential};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use crate::errors::{
    interface_error, map_engine_error, not_supported, programming_error, OperationalError,
};
use crate::exec::{run_query_collected, run_sql};
use crate::relation::{quote_ident, Relation};
use crate::result::ResultSet;
use crate::runtime::block_on;

/// Live file-backed databases, keyed by resolved path.
///
/// [`beacon_redb_store::RedbStore`] takes an exclusive lock on the container file, so a second
/// open of the same path *within this process* must reuse the first handle rather than fail.
/// `Weak` so a database is dropped (and its lock released) once every connection to it is gone.
static OPEN_DATABASES: LazyLock<Mutex<HashMap<PathBuf, Weak<Database>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// A stable key for a path that may not exist yet.
///
/// `canonicalize` fails on a database that is about to be created, so the parent directory is
/// canonicalized (it must exist) and the file name appended. This makes `beacon.db`,
/// `./beacon.db` and an absolute path resolve to the same entry.
fn database_key(path: &Path) -> PyResult<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|e| interface_error(format!("cannot resolve the current directory: {e}")))?
            .join(path)
    };
    let parent = absolute.parent().unwrap_or(Path::new("/"));
    let file = absolute.file_name().ok_or_else(|| {
        programming_error(format!(
            "`{}` is not a database file path",
            absolute.display()
        ))
    })?;
    match parent.canonicalize() {
        Ok(dir) => Ok(dir.join(file)),
        // The parent does not exist yet; opening will fail with a clear I/O error, so key on
        // the un-canonicalized path rather than failing here with a worse message.
        Err(_) => Ok(absolute),
    }
}

/// Opens `path`, or returns the already-open database for it.
fn open_or_attach(
    py: Python<'_>,
    path: &Path,
    options: OpenOptions,
    auth_requested: bool,
) -> PyResult<Arc<Database>> {
    let key = database_key(path)?;

    {
        let registry = OPEN_DATABASES.lock().expect("database registry poisoned");
        if let Some(existing) = registry.get(&key).and_then(Weak::upgrade) {
            // The mode belongs to the open handle, not to the file, so a second connection
            // cannot quietly get different rules than it asked for.
            if existing.auth_enabled() != auth_requested {
                return Err(programming_error(format!(
                    "`{}` is already open in this process with auth {}; a connection cannot \
                     change the mode of an open database. Close the existing connection first, \
                     or open with auth={}.",
                    key.display(),
                    if existing.auth_enabled() {
                        "enabled"
                    } else {
                        "disabled"
                    },
                    if existing.auth_enabled() {
                        "True"
                    } else {
                        "False"
                    },
                )));
            }
            return Ok(existing);
        }
    }

    let path_buf = key.clone();
    let database = block_on(py, async move {
        Database::open(DbPath::File(path_buf), options).await
    })?
    .map_err(map_engine_error)?;
    let database = Arc::new(database);

    let mut registry = OPEN_DATABASES.lock().expect("database registry poisoned");
    // Re-check: another thread may have opened the same path while the GIL was released.
    if let Some(existing) = registry.get(&key).and_then(Weak::upgrade) {
        return Ok(existing);
    }
    registry.insert(key, Arc::downgrade(&database));
    Ok(database)
}

/// An open connection: a database, an identity, and the last statement's result.
#[pyclass(module = "beacondb", name = "Connection")]
pub struct Connection {
    database: Option<Arc<Database>>,
    identity: AuthIdentity,
    last_result: Option<ResultSet>,
}

impl Connection {
    fn database(&self) -> PyResult<&Arc<Database>> {
        self.database
            .as_ref()
            .ok_or_else(|| interface_error("this connection is closed"))
    }

    /// Runs a statement and collects its result, with the GIL released for the duration.
    fn run(&self, py: Python<'_>, sql: String) -> PyResult<ResultSet> {
        run_sql(py, self.database()?, &self.identity, sql)
    }

    fn last_result_mut(&mut self) -> PyResult<&mut ResultSet> {
        self.last_result
            .as_mut()
            .ok_or_else(|| interface_error("no statement has been executed on this connection yet"))
    }
}

#[pymethods]
impl Connection {
    /// Runs a statement, holding its result on the connection (the DB-API pattern), and
    /// returns the connection so `.execute(...).fetchall()` chains.
    ///
    /// `parameters` is a list/tuple bound to the statement's `?` (or `$1`) placeholders. Values
    /// are bound to the plan, never interpolated into the SQL, so this is injection-safe.
    #[pyo3(signature = (query, parameters=None))]
    fn execute<'py>(
        slf: Bound<'py, Self>,
        query: &str,
        parameters: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, Self>> {
        let py = slf.py();
        let result = {
            let conn = slf.borrow();
            let database = conn.database()?.clone();
            let (sql, params) = crate::params::prepare(query, parameters.as_ref())?;
            crate::exec::run_sql_with_params(py, &database, &conn.identity, sql, params)?
        };
        slf.borrow_mut().last_result = Some(result);
        Ok(slf)
    }

    /// Runs `query` once per parameter sequence in `seq_of_parameters` (the DB-API bulk path,
    /// usually an `INSERT`). The last statement's result is held on the connection.
    #[pyo3(signature = (query, seq_of_parameters))]
    fn executemany<'py>(
        slf: Bound<'py, Self>,
        query: &str,
        seq_of_parameters: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, Self>> {
        let py = slf.py();
        // Each element is one parameter row; iterating a list/tuple of rows.
        let rows: Vec<Bound<'py, PyAny>> = seq_of_parameters
            .try_iter()
            .map_err(|_| {
                programming_error("seq_of_parameters must be a sequence of parameter lists")
            })?
            .collect::<PyResult<Vec<_>>>()?;

        let mut last = None;
        for row in rows {
            let result = {
                let conn = slf.borrow();
                let database = conn.database()?.clone();
                let (sql, params) = crate::params::prepare(query, Some(&row))?;
                crate::exec::run_sql_with_params(py, &database, &conn.identity, sql, params)?
            };
            last = Some(result);
        }
        slf.borrow_mut().last_result = last;
        Ok(slf)
    }

    /// Builds a lazy [`Relation`] over a SQL statement. Nothing runs until a terminal method
    /// (`fetchall`, `arrow`, `df`, …) is called, so the relation can be filtered/projected/
    /// ordered further before execution.
    fn sql(&self, query: &str) -> PyResult<Relation> {
        Ok(Relation::from_query(
            self.database()?.clone(),
            self.identity.clone(),
            query,
        ))
    }

    /// Alias for [`Connection::sql`], matching DuckDB.
    fn query(&self, query: &str) -> PyResult<Relation> {
        self.sql(query)
    }

    /// Registers a pandas / pyarrow / polars frame (or any Arrow-C-stream object, or a beacondb
    /// relation) as a table queryable by `name`, and returns the connection so the call chains.
    ///
    /// By default the table is **session-only**: an in-memory copy that lives for the process and
    /// is not written into `beacon.db` (copying the file does not carry it, reopening does not
    /// see it), and re-registering the name replaces it.
    ///
    /// With `persist=True` the data is written into `beacon.db` as a managed table instead, so it
    /// survives a reopen and travels with the file. That is real DDL: it needs the connection's
    /// identity to have write privileges (a super-user — the default with auth off), and it
    /// fails if `name` already exists (drop it first rather than silently overwriting on-disk
    /// data). Needs `pyarrow` installed either way.
    #[pyo3(signature = (name, obj, *, persist=false))]
    fn register<'py>(
        slf: Bound<'py, Self>,
        name: &str,
        obj: Bound<'py, PyAny>,
        persist: bool,
    ) -> PyResult<Bound<'py, Self>> {
        let py = slf.py();
        let (schema, batches) = crate::ingest::arrow_from_py(py, &obj)?;

        let conn = slf.borrow();
        let database = conn.database()?.clone();
        let name = name.to_string();
        if persist {
            let identity = conn.identity.clone();
            block_on(py, async move {
                database.persist_batches(&name, schema, batches, identity).await
            })?
            .map_err(map_engine_error)?;
        } else {
            // A session-only registration is a synchronous catalog mutation, but it touches the
            // session context, so keep the GIL released while it runs.
            py.detach(|| database.register_batches(&name, schema, batches))
                .map_err(map_engine_error)?;
        }
        drop(conn);
        Ok(slf)
    }

    /// Removes a table registered with [`Self::register`]. Returns the connection.
    fn unregister<'py>(slf: Bound<'py, Self>, name: &str) -> PyResult<Bound<'py, Self>> {
        let py = slf.py();
        let conn = slf.borrow();
        let database = conn.database()?.clone();
        let name = name.to_string();
        py.detach(|| database.deregister_table(&name))
            .map_err(map_engine_error)?;
        drop(conn);
        Ok(slf)
    }

    /// Runs a **structured JSON query** — beacon's non-SQL query form, the same payload the HTTP
    /// API and the TS client send (`{"select": [...], "filter": {...}, "from": {...}, ...}`).
    ///
    /// Returns the materialized result (a full query spec is not composable as SQL, so unlike
    /// `sql()` this runs eagerly); the result exposes the same terminal methods — `fetchall`,
    /// `arrow`, `df`, `pl`, `__arrow_c_stream__`.
    fn json_query(&self, py: Python<'_>, spec: Bound<'_, PyAny>) -> PyResult<ResultSet> {
        // Round-trip the dict through JSON text so beacon's own serde model does the parsing —
        // no second, drifting schema on this side.
        let json = py
            .import("json")?
            .call_method1("dumps", (spec,))?
            .extract::<String>()?;
        let inner: InnerQuery = serde_json::from_str(&json).map_err(|e| {
            programming_error(format!("not a valid beacon JSON query: {e}"))
        })?;
        let query = Query {
            inner,
            output: None,
            params: Vec::new(),
        };
        run_query_collected(py, self.database()?, &self.identity, query)
    }

    /// The registered scalar/aggregate functions, as a lazy [`Relation`] over
    /// `beacon.system.functions` (name, description, return type, parameters).
    fn functions(&self) -> PyResult<Relation> {
        Ok(Relation::from_query(
            self.database()?.clone(),
            self.identity.clone(),
            "SELECT * FROM beacon.system.functions",
        ))
    }

    /// Per-query execution metrics, as a lazy [`Relation`] over `beacon.system.query_metrics`.
    /// With `query_id`, narrows to that one query.
    #[pyo3(signature = (query_id=None))]
    fn metrics(&self, query_id: Option<&str>) -> PyResult<Relation> {
        let sql = match query_id {
            // A query id is a UUID; escape it as a string literal regardless, so this can never
            // be an injection vector.
            Some(id) => format!(
                "SELECT * FROM beacon.system.query_metrics WHERE query_id = '{}'",
                id.replace('\'', "''")
            ),
            None => "SELECT * FROM beacon.system.query_metrics".to_string(),
        };
        Ok(Relation::from_query(
            self.database()?.clone(),
            self.identity.clone(),
            &sql,
        ))
    }

    /// The names of the user tables and views in the default schema.
    fn list_tables(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let result = self.run(
            py,
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = 'public' ORDER BY table_name"
                .to_string(),
        )?;
        Ok(string_column(&result))
    }

    /// Refreshes an external table or materialized view (re-lists its files / re-runs its query),
    /// then returns the connection so the call chains. This is `REFRESH <name>`.
    fn refresh<'py>(slf: Bound<'py, Self>, name: &str) -> PyResult<Bound<'py, Self>> {
        let py = slf.py();
        let sql = format!("REFRESH {}", quote_ident(name));
        slf.borrow().run(py, sql)?;
        Ok(slf)
    }

    /// Attaches another Beacon instance as a catalog named `name`, so its tables are queryable as
    /// `name.<schema>.<table>` and joins/filters/aggregates push down to it over Flight SQL.
    ///
    /// `url` is the remote's Flight SQL endpoint (`beacon://host:port`, `grpc://…`, `http(s)://…`,
    /// or a bare `host:port`); `tls=True` (or an `https://` url) uses TLS. Credentials, if the
    /// remote requires them, are either a bearer `token` or a `username`/`password` pair (not both).
    /// The remote is contacted now — its schemas and tables are enumerated — so an unreachable or
    /// unauthorized endpoint raises here, not on first query.
    #[pyo3(signature = (name, url, *, token=None, username=None, password=None, tls=false))]
    fn attach(
        &self,
        py: Python<'_>,
        name: &str,
        url: &str,
        token: Option<String>,
        username: Option<String>,
        password: Option<String>,
        tls: bool,
    ) -> PyResult<()> {
        let credential = beacon_core::embedded::RemoteCredential::from_parts(
            token, username, password,
        )
        .map_err(|e| programming_error(e.to_string()))?;
        let database = self.database()?.clone();
        let name = name.to_string();
        let url = url.to_string();
        block_on(py, async move {
            database.attach_remote(&name, &url, credential, tls).await
        })?
        .map_err(map_engine_error)
    }

    /// Detaches a catalog attached with [`Self::attach`]. Returns `True` if it was attached.
    fn detach(&self, name: &str) -> PyResult<bool> {
        self.database()?.detach(name).map_err(map_engine_error)
    }

    /// The names of the remote Beacon instances currently attached as catalogs.
    fn attached(&self) -> PyResult<Vec<String>> {
        Ok(self.database()?.attached_catalogs())
    }

    /// A lazy [`Relation`] over a catalog table or view.
    fn table(&self, name: &str) -> PyResult<Relation> {
        Ok(Relation::over(
            self.database()?.clone(),
            self.identity.clone(),
            quote_ident(name),
        ))
    }

    /// Alias of [`Self::table`], matching DuckDB.
    fn view(&self, name: &str) -> PyResult<Relation> {
        self.table(name)
    }

    /// A lazy [`Relation`] over a table-function call: `read("read_parquet", "obs/*.parquet")`.
    ///
    /// This is the general form; the named readers (`con.read_parquet(...)`, `con.read_netcdf(...)`,
    /// …) are thin wrappers over it, resolved from the catalog by [`Self::__getattr__`], so any
    /// table function beacon registers is reachable without a hand-written method here.
    ///
    /// Format options can be passed positionally or by keyword. Keyword options are matched to the
    /// reader's declared parameters (from `beacon.system.table_functions`) by name, so
    /// `read_csv(path, delimiter=';')` fills the `delimiter` slot and leaves the rest defaulted.
    /// The universal `columns=[...]` keyword projects the named columns instead of being passed to
    /// the function.
    #[pyo3(signature = (function, *args, **kwargs))]
    fn read(
        &self,
        py: Python<'_>,
        function: &str,
        args: Vec<Bound<'_, PyAny>>,
        kwargs: Option<Bound<'_, PyDict>>,
    ) -> PyResult<Relation> {
        let database = self.database()?.clone();

        // `slots[i]` is the SQL literal for positional argument `i` (`None` = not supplied yet).
        let mut slots: Vec<Option<String>> = Vec::new();
        for (index, arg) in args.iter().enumerate() {
            ensure_slot(&mut slots, index);
            slots[index] = Some(crate::exec::py_to_sql_literal(arg)?);
        }

        let mut columns: Option<Vec<String>> = None;
        if let Some(kwargs) = kwargs {
            // The reader's declared parameters, in order (first is the path/glob). Fetched lazily so
            // a purely-positional call never touches the catalog.
            let mut param_names: Option<Vec<String>> = None;
            for (key, value) in kwargs.iter() {
                let key: String = key.extract()?;
                if key == "columns" {
                    columns = Some(extract_columns(&value)?);
                    continue;
                }
                let params = match &param_names {
                    Some(params) => params,
                    None => {
                        let meta = table_function_meta(py, self)?;
                        param_names = Some(meta.get(function).cloned().unwrap_or_default());
                        param_names.as_ref().unwrap()
                    }
                };
                let index = params.iter().position(|p| p == &key).ok_or_else(|| {
                    let options: Vec<&String> = params.iter().skip(1).collect();
                    programming_error(format!(
                        "`{function}` has no option `{key}`; its options are {options:?}"
                    ))
                })?;
                ensure_slot(&mut slots, index);
                if slots[index].is_some() {
                    return Err(programming_error(format!(
                        "option `{key}` for `{function}` was given more than once"
                    )));
                }
                slots[index] = Some(crate::exec::py_to_sql_literal(&value)?);
            }
        }

        // Unfilled interior slots become NULL, which the readers treat as "use the default".
        let rendered: Vec<String> = slots
            .into_iter()
            .map(|slot| slot.unwrap_or_else(|| "NULL".to_string()))
            .collect();
        let call = format!("{function}({})", rendered.join(", "));

        let relation = match columns {
            Some(columns) if !columns.is_empty() => {
                let projected = columns
                    .iter()
                    .map(|column| quote_ident(column))
                    .collect::<Vec<_>>()
                    .join(", ");
                Relation::from_query(
                    database,
                    self.identity.clone(),
                    &format!("SELECT {projected} FROM {call}"),
                )
            }
            _ => Relation::over(database, self.identity.clone(), call),
        };
        Ok(relation)
    }

    /// The names of the table-valued functions this database exposes (`read_parquet`,
    /// `read_netcdf`, `list_datasets`, …).
    fn table_functions(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        Ok(table_function_meta(py, self)?.keys().cloned().collect())
    }

    /// Resolves an unknown attribute against the table-function catalog, so `con.read_parquet`,
    /// `con.read_netcdf`, `con.list_datasets`, and any future reader work as methods without
    /// being written out here — the catalog is the source of truth.
    fn __getattr__(slf: Bound<'_, Self>, name: String) -> PyResult<Py<PyAny>> {
        // Never resolve dunder / private probes (`__deepcopy__`, `_ipython_*`, …) against the
        // catalog: they must fail fast so `hasattr` and copy/pickle behave, and they can't be
        // table functions anyway.
        if name.starts_with('_') {
            return Err(pyo3::exceptions::PyAttributeError::new_err(name));
        }
        let py = slf.py();
        let is_table_function = {
            let conn = slf.borrow();
            table_function_meta(py, &conn)?.contains_key(&name)
        };
        if !is_table_function {
            return Err(pyo3::exceptions::PyAttributeError::new_err(name));
        }
        // `con.read_parquet(path)` becomes `con.read("read_parquet", path)`.
        let read = slf.getattr("read")?;
        let functools = py.import("functools")?;
        let partial = functools.call_method1("partial", (read, name))?;
        Ok(partial.unbind())
    }

    fn fetchone<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyTuple>>> {
        self.last_result_mut()?.fetchone(py)
    }

    #[pyo3(signature = (size=1))]
    fn fetchmany<'py>(&mut self, py: Python<'py>, size: usize) -> PyResult<Bound<'py, PyList>> {
        self.last_result_mut()?.fetchmany(py, size)
    }

    fn fetchall<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        self.last_result_mut()?.fetchall(py)
    }

    #[getter]
    fn description<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyList>>> {
        match &self.last_result {
            Some(result) => Ok(Some(result.description(py)?)),
            None => Ok(None),
        }
    }

    #[getter]
    fn rowcount(&self) -> i64 {
        match &self.last_result {
            Some(result) => result.rowcount(),
            // PEP 249: -1 when no statement has run or the count is unknown.
            None => -1,
        }
    }

    /// A second connection over the same database, with its own result slot.
    fn cursor(&self) -> PyResult<Self> {
        Ok(Self {
            database: Some(self.database()?.clone()),
            identity: self.identity.clone(),
            last_result: None,
        })
    }

    /// A connection to the same database under a different identity.
    ///
    /// Only meaningful with auth enabled: with auth disabled there is no identity to switch to,
    /// every session is already the local super-user.
    #[pyo3(signature = (username=None, password=None, token=None))]
    fn connect_as(
        &self,
        py: Python<'_>,
        username: Option<&str>,
        password: Option<&str>,
        token: Option<&str>,
    ) -> PyResult<Self> {
        let database = self.database()?.clone();
        let credential = build_credential(username, password, token)?.ok_or_else(|| {
            programming_error("connect_as() needs either username and password, or token")
        })?;
        let identity = authenticate(py, &database, &credential)?;
        Ok(Self {
            database: Some(database),
            identity,
            last_result: None,
        })
    }

    /// A connection to the same database as the anonymous principal.
    fn as_anonymous(&self) -> PyResult<Self> {
        let database = self.database()?.clone();
        if !database.auth_enabled() {
            return Err(programming_error(
                "this database was opened with auth disabled, so there is no anonymous \
                 principal: every session is the local super-user",
            ));
        }
        let identity = database
            .require_default_identity()
            .map_err(map_engine_error)?;
        Ok(Self {
            database: Some(database),
            identity,
            last_result: None,
        })
    }

    /// The resolved identity and the mode it was resolved under.
    ///
    /// Worth calling in a notebook before wondering why a statement was refused: it answers
    /// "who am I and what may I do?" in one line.
    fn whoami<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        dict.set_item("username", &self.identity.username)?;
        dict.set_item("roles", self.identity.roles.clone())?;
        dict.set_item("is_super_user", self.identity.is_super_user)?;
        dict.set_item("auth", self.database()?.auth_enabled())?;
        Ok(dict)
    }

    #[getter]
    fn auth_enabled(&self) -> PyResult<bool> {
        Ok(self.database()?.auth_enabled())
    }

    /// Closes this connection. The database (and the container file's lock) is released once
    /// every connection to it is closed or dropped.
    fn close(&mut self) {
        self.database = None;
        self.last_result = None;
    }

    /// PEP 249 `commit`. A no-op: the engine is autocommit — every statement commits on its own,
    /// and there are no multi-statement transactions to flush. Present so DB-API consumers (e.g.
    /// SQLAlchemy) that call `commit()` after each statement work unchanged.
    fn commit(&self) {}

    /// PEP 249 `rollback`. A no-op, for the same reason as [`Self::commit`]: an autocommit
    /// connection has nothing to undo. It never errors, so DB-API error-cleanup paths are safe.
    fn rollback(&self) {}

    fn __enter__(slf: Bound<'_, Self>) -> Bound<'_, Self> {
        slf
    }

    #[pyo3(signature = (exc_type=None, exc_value=None, traceback=None))]
    fn __exit__(
        &mut self,
        exc_type: Option<Py<PyAny>>,
        exc_value: Option<Py<PyAny>>,
        traceback: Option<Py<PyAny>>,
    ) -> bool {
        let _ = (exc_type, exc_value, traceback);
        self.close();
        // Never swallow the exception the body raised.
        false
    }

    fn __repr__(&self) -> String {
        match &self.database {
            None => "<beacondb.Connection closed>".to_string(),
            Some(database) => format!(
                "<beacondb.Connection user={} auth={}>",
                self.identity.username,
                if database.auth_enabled() { "on" } else { "off" }
            ),
        }
    }
}

/// The table-function names, queried once and cached for the process.
///
/// beacon registers the same built-in table functions for every runtime, so the set does not
/// vary between connections or databases — a process-global cache is correct and spares every
/// `con.read_*` attribute access a catalog round-trip. `beacon.system.table_functions` is not
/// an auth table, so this works for any identity, anonymous included.
static TABLE_FUNCTIONS: OnceLock<Arc<BTreeMap<String, Vec<String>>>> = OnceLock::new();

/// Every table function mapped to its declared parameter names, in order. The first parameter is
/// the path/glob for the readers; the rest are the format options `read(...)` accepts as keywords.
fn table_function_meta(
    py: Python<'_>,
    conn: &Connection,
) -> PyResult<Arc<BTreeMap<String, Vec<String>>>> {
    if let Some(cached) = TABLE_FUNCTIONS.get() {
        return Ok(cached.clone());
    }
    let result = conn.run(
        py,
        "SELECT function_name, parameters FROM beacon.system.table_functions".to_string(),
    )?;
    let mut functions = BTreeMap::new();
    for batch in result.batches() {
        let names = batch.column(0).as_string_opt::<i32>();
        let params = batch.column(1).as_string_opt::<i32>();
        let (Some(names), Some(params)) = (names, params) else {
            continue;
        };
        for row in 0..batch.num_rows() {
            if names.is_null(row) {
                continue;
            }
            let param_names = if params.is_null(row) {
                Vec::new()
            } else {
                parse_param_names(params.value(row))
            };
            functions.insert(names.value(row).to_string(), param_names);
        }
    }
    let arc = Arc::new(functions);
    let _ = TABLE_FUNCTIONS.set(arc.clone());
    // Whoever won the race owns the canonical set; return that one.
    Ok(TABLE_FUNCTIONS.get().cloned().unwrap_or(arc))
}

/// Pulls the ordered `name` fields out of a table function's `parameters` JSON
/// (`[{"name": "glob_paths", ...}, {"name": "delimiter", ...}]`). Anything unparseable yields no
/// names, so the function is still callable positionally — it just has no keyword options.
fn parse_param_names(json: &str) -> Vec<String> {
    serde_json::from_str::<serde_json::Value>(json)
        .ok()
        .and_then(|value| {
            value.as_array().map(|params| {
                params
                    .iter()
                    .filter_map(|param| {
                        param.get("name").and_then(|n| n.as_str()).map(String::from)
                    })
                    .collect()
            })
        })
        .unwrap_or_default()
}

/// Grows `slots` so index `index` is addressable, padding new positions with `None`.
fn ensure_slot(slots: &mut Vec<Option<String>>, index: usize) {
    if slots.len() <= index {
        slots.resize(index + 1, None);
    }
}

/// Reads the `columns=` keyword — a single column name or a list/tuple of them.
fn extract_columns(value: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    if let Ok(single) = value.extract::<String>() {
        return Ok(vec![single]);
    }
    value.extract::<Vec<String>>().map_err(|_| {
        programming_error("`columns` must be a column name or a list of column names")
    })
}

/// Collects the first column of a single-column `Utf8` result into a `Vec<String>`.
fn string_column(result: &ResultSet) -> Vec<String> {
    let mut values = Vec::new();
    for batch in result.batches() {
        if let Some(column) = batch.column(0).as_string_opt::<i32>() {
            values.extend(column.iter().flatten().map(str::to_string));
        }
    }
    values
}

fn build_credential(
    username: Option<&str>,
    password: Option<&str>,
    token: Option<&str>,
) -> PyResult<Option<Credential>> {
    if token.is_some() && (username.is_some() || password.is_some()) {
        return Err(programming_error(
            "pass either username/password or token, not both",
        ));
    }
    match (username, password, token) {
        (None, None, None) => Ok(None),
        (_, _, Some(token)) => Ok(Some(Credential::bearer(token))),
        (Some(username), Some(password), None) => Ok(Some(Credential::basic(username, password))),
        (Some(_), None, None) => Err(programming_error("username given without a password")),
        (None, Some(_), None) => Err(programming_error("password given without a username")),
    }
}

/// Resolves a credential, classifying failures by *what was attempted* rather than by
/// matching on the engine's message.
///
/// Everything reaching here is an authentication attempt, so a rejected credential is an
/// `OperationalError` — the same class psycopg raises for a bad password, and the one DB-API
/// consumers already treat as "could not establish this session". Only the mode check is the
/// caller's mistake.
fn authenticate(
    py: Python<'_>,
    database: &Arc<Database>,
    credential: &Credential,
) -> PyResult<AuthIdentity> {
    if !database.auth_enabled() {
        return Err(programming_error(
            "this database was opened with auth disabled, so credentials cannot be used: every \
             session already has full access. Reopen with auth=True to authenticate users.",
        ));
    }
    let database = database.clone();
    let credential = credential.clone();
    block_on(py, async move { database.authenticate(&credential).await })?
        .map_err(|err| OperationalError::new_err(format!("{err:#}")))
}

/// Opens a beacon database.
///
/// `database` is `":memory:"` or the path of a container file. With `auth=False` (the default)
/// the session is a local super-user and no credentials are accepted; with `auth=True` it is
/// the anonymous principal unless credentials are supplied.
#[pyfunction]
#[pyo3(signature = (
    database = ":memory:",
    *,
    read_only = false,
    auth = false,
    username = None,
    password = None,
    token = None,
    admin_username = None,
    admin_password = None,
    anonymous = true,
    datasets = None,
    batch_size = None,
    memory_limit = None,
    cpu_limit = None,
    crawlers = false,
))]
#[allow(clippy::too_many_arguments)]
pub fn connect(
    py: Python<'_>,
    database: &str,
    read_only: bool,
    auth: bool,
    username: Option<&str>,
    password: Option<&str>,
    token: Option<&str>,
    admin_username: Option<&str>,
    admin_password: Option<&str>,
    anonymous: bool,
    datasets: Option<&str>,
    batch_size: Option<usize>,
    memory_limit: Option<usize>,
    cpu_limit: Option<usize>,
    crawlers: bool,
) -> PyResult<Connection> {
    if read_only {
        // Honest refusal: `RedbStore` only opens the container exclusively today, so a
        // read-only handle would be a lie about both concurrency and writability.
        return Err(not_supported(
            "read_only=True is not implemented yet: the container file is always opened with an \
             exclusive lock",
        ));
    }

    let credential = build_credential(username, password, token)?;
    if !auth && credential.is_some() {
        return Err(programming_error(
            "credentials were supplied but auth is disabled, so they would have no effect. \
             Open with auth=True to authenticate, or drop the credentials — with auth=False \
             every session already has full access.",
        ));
    }
    if !auth && (admin_username.is_some() || admin_password.is_some()) {
        return Err(programming_error(
            "admin_username/admin_password only apply when auth=True",
        ));
    }

    let auth_mode = if auth {
        let mut mode = match (admin_username, admin_password) {
            (Some(user), Some(pass)) => AuthMode::enabled_with_admin(user, pass),
            (None, None) => AuthMode::enabled(),
            _ => {
                return Err(programming_error(
                    "admin_username and admin_password must be given together",
                ))
            }
        };
        if !anonymous {
            mode = mode.without_anonymous();
        }
        mode
    } else {
        AuthMode::Disabled
    };

    let mut options = OpenOptions::new().with_auth(auth_mode);
    options.crawlers.enable = crawlers;
    if let Some(size) = batch_size {
        options = options.with_batch_size(size);
    }
    if let Some(limit) = memory_limit {
        options = options.with_memory_limit(limit);
    }
    if let Some(limit) = cpu_limit {
        options = options.with_cpu_limit(limit);
    }
    if let Some(dir) = datasets {
        options = options.with_datasets_dir(PathBuf::from(dir));
    }

    let database = match DbPath::parse(database) {
        // In-memory databases share nothing: each connect() gets its own, as in DuckDB.
        DbPath::Memory => {
            let opened = block_on(
                py,
                async move { Database::open(DbPath::Memory, options).await },
            )?
            .map_err(map_engine_error)?;
            Arc::new(opened)
        }
        DbPath::File(path) => open_or_attach(py, &path, options, auth)?,
    };

    let identity = match credential {
        Some(credential) => authenticate(py, &database, &credential)?,
        None => database
            .require_default_identity()
            .map_err(map_engine_error)?,
    };

    Ok(Connection {
        database: Some(database),
        identity,
        last_result: None,
    })
}
