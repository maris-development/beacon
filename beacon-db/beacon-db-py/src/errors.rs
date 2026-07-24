//! The PEP 249 exception hierarchy, and the mapping from engine errors onto it.
//!
//! DB-API consumers (SQLAlchemy dialects, `pandas.read_sql`, retry wrappers) catch these
//! specific types, so the tree and the mapping are part of the public contract:
//!
//! ```text
//! Warning
//! Error
//! ├── InterfaceError          binding misuse, closed connection
//! └── DatabaseError
//!     ├── DataError           bad values, type/conversion
//!     ├── OperationalError    I/O, object store, file lock, timeout
//!     ├── IntegrityError      constraint violations
//!     ├── InternalError       engine invariant broken / caught panic
//!     ├── ProgrammingError    SQL parse/plan errors, unknown table/column, bad config
//!     │   └── NotPermittedError   authorization denial
//!     └── NotSupportedError   unimplemented parity feature
//! ```
//!
//! [`Runtime::run_query`](beacon_core::runtime::Runtime::run_query) returns `anyhow::Error`,
//! so mapping means downcasting to the underlying type where one survives, and matching on
//! the message where beacon raised a bare `anyhow::bail!`. The message match is deliberately
//! narrow and anchored on strings the engine owns; when beacon-core grows a typed error enum,
//! [`map_engine_error`] is the single place that changes.

use datafusion::error::DataFusionError;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

create_exception!(_beacondb, Warning, PyException, "PEP 249 Warning.");
create_exception!(_beacondb, Error, PyException, "Base of every beacondb error.");
create_exception!(
    _beacondb,
    InterfaceError,
    Error,
    "Misuse of the binding itself, rather than a database failure."
);
create_exception!(
    _beacondb,
    DatabaseError,
    Error,
    "Base of every error raised by the engine."
);
create_exception!(
    _beacondb,
    DataError,
    DatabaseError,
    "A value could not be represented or converted."
);
create_exception!(
    _beacondb,
    OperationalError,
    DatabaseError,
    "An I/O, object-store, locking, or resource failure outside the caller's control."
);
create_exception!(
    _beacondb,
    IntegrityError,
    DatabaseError,
    "A constraint was violated."
);
create_exception!(
    _beacondb,
    InternalError,
    DatabaseError,
    "An engine invariant was broken, or a Rust panic was caught at the FFI boundary."
);
create_exception!(
    _beacondb,
    ProgrammingError,
    DatabaseError,
    "The statement or the API call was wrong: parse/plan errors, unknown objects, bad arguments."
);
create_exception!(
    _beacondb,
    NotSupportedError,
    DatabaseError,
    "A feature beacon does not implement."
);
// Deliberately *not* named `PermissionError`: that name is a CPython builtin (an `OSError`
// subclass), and shadowing it in this namespace would mean `except PermissionError:` in user
// code silently fails to catch this, or catches it only when the import order happens to
// cooperate. It subclasses ProgrammingError so generic DB-API consumers still catch it.
create_exception!(
    _beacondb,
    NotPermittedError,
    ProgrammingError,
    "The authenticated identity is not allowed to run this statement."
);

/// Registers the exception types on the extension module.
pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = module.py();
    module.add("Warning", py.get_type::<Warning>())?;
    module.add("Error", py.get_type::<Error>())?;
    module.add("InterfaceError", py.get_type::<InterfaceError>())?;
    module.add("DatabaseError", py.get_type::<DatabaseError>())?;
    module.add("DataError", py.get_type::<DataError>())?;
    module.add("OperationalError", py.get_type::<OperationalError>())?;
    module.add("IntegrityError", py.get_type::<IntegrityError>())?;
    module.add("InternalError", py.get_type::<InternalError>())?;
    module.add("ProgrammingError", py.get_type::<ProgrammingError>())?;
    module.add("NotSupportedError", py.get_type::<NotSupportedError>())?;
    module.add("NotPermittedError", py.get_type::<NotPermittedError>())?;
    Ok(())
}

/// Maps an engine error onto the exception tree.
///
/// The whole `anyhow` chain is rendered into the message (`{:#}`) because DataFusion's text
/// carries the SQL position and the object names, which is the main debugging aid a user has.
pub fn map_engine_error(err: anyhow::Error) -> PyErr {
    let message = format!("{err:#}");

    // Authorization first: these are bare `anyhow::bail!`s from beacon's statement-plan layer,
    // so there is no type to downcast to. Both prefixes are owned by beacon-core
    // (`validate_query_plan` and `authorize_logical_plan`) and pinned by its tests.
    if message.contains("operation not permitted") || message.contains("permission denied") {
        return NotPermittedError::new_err(message);
    }

    if let Some(df_err) = err.downcast_ref::<DataFusionError>() {
        return from_datafusion(df_err, message);
    }

    // A redb/object-store failure that never became a DataFusionError — most often a second
    // process holding the container file's exclusive lock.
    if err.downcast_ref::<std::io::Error>().is_some() {
        return OperationalError::new_err(message);
    }

    DatabaseError::new_err(message)
}

fn from_datafusion(err: &DataFusionError, message: String) -> PyErr {
    match err {
        // Unwrap the wrappers first: the informative variant is always inside.
        DataFusionError::Context(_, inner) => from_datafusion(inner, message),
        DataFusionError::Diagnostic(_, inner) => from_datafusion(inner, message),
        DataFusionError::Shared(inner) => from_datafusion(inner, message),
        DataFusionError::Collection(errors) => match errors.first() {
            Some(first) => from_datafusion(first, message),
            None => InternalError::new_err(message),
        },

        DataFusionError::SQL(..)
        | DataFusionError::Plan(_)
        | DataFusionError::SchemaError(..)
        | DataFusionError::Configuration(_) => ProgrammingError::new_err(message),

        DataFusionError::ObjectStore(_)
        | DataFusionError::IoError(_)
        | DataFusionError::ResourcesExhausted(_) => OperationalError::new_err(message),

        DataFusionError::ArrowError(..) | DataFusionError::ParquetError(_) => {
            DataError::new_err(message)
        }

        DataFusionError::NotImplemented(_) | DataFusionError::Substrait(_) => {
            NotSupportedError::new_err(message)
        }

        DataFusionError::Internal(_) | DataFusionError::ExecutionJoin(_) => {
            InternalError::new_err(message)
        }

        // `Execution` covers runtime failures that are usually the statement's fault (a bad
        // cast, a missing file), and `External` is whatever a TableProvider surfaced.
        DataFusionError::Execution(_) | DataFusionError::External(_) => {
            DatabaseError::new_err(message)
        }

        _ => DatabaseError::new_err(message),
    }
}

/// Raised when the caller misuses the binding (as opposed to the database failing).
pub fn interface_error(message: impl Into<String>) -> PyErr {
    InterfaceError::new_err(message.into())
}

/// Raised when the call itself is wrong: bad arguments, wrong mode, unknown option.
pub fn programming_error(message: impl Into<String>) -> PyErr {
    ProgrammingError::new_err(message.into())
}

/// Raised for a feature that exists in the plan but not yet in the code.
pub fn not_supported(message: impl Into<String>) -> PyErr {
    NotSupportedError::new_err(message.into())
}
