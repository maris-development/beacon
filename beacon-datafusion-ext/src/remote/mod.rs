//! Federated remote-Beacon tables.
//!
//! A `RemoteTableDefinition` points at a table on another Beacon instance and is
//! served through `datafusion-federation`: the federation optimizer pushes the
//! largest federatable sub-plan (filters, projection, limit, joins, aggregates)
//! to the remote over Arrow Flight SQL via [`BeaconFlightSqlExecutor`].

mod connection;
mod definition;
mod executor;

pub use connection::RemoteConnection;
pub use definition::{BeaconRemoteSqlTable, RemoteTableDefinition, unresolved_schema};
pub use executor::BeaconFlightSqlExecutor;

use datafusion::catalog::TableProvider;
use datafusion_federation::FederatedTableProviderAdaptor;
use datafusion_federation::sql::SQLTableSource;

/// Recover a [`RemoteTableDefinition`] from a registered provider, if it is a
/// federated remote-Beacon table.
///
/// The catalog registers the bare [`FederatedTableProviderAdaptor`] (so the
/// federation optimizer recognizes it); this digs through its public `source`
/// and our [`BeaconRemoteSqlTable`] to recover the definition for persistence.
pub fn remote_table_definition(provider: &dyn TableProvider) -> Option<RemoteTableDefinition> {
    let adaptor = provider
        .as_any()
        .downcast_ref::<FederatedTableProviderAdaptor>()?;
    let source = adaptor.source.as_any().downcast_ref::<SQLTableSource>()?;
    let table = source
        .table
        .as_any()
        .downcast_ref::<BeaconRemoteSqlTable>()?;
    Some(table.definition().clone())
}
