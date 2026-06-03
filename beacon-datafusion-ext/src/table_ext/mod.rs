//! Serializable table definitions used to rebuild DataFusion table providers.
//!
//! This module contains persisted definitions for listing tables, SQL view tables,
//! and materialized views, together with helper logic that normalizes schema and
//! partition metadata during provider creation, and the event-driven self-refresh
//! wiring for external tables.
//!
//! The module is split into focused submodules:
//! - [`definition`] — the serializable [`TableDefinition`] trait.
//! - [`listing`] — shared listing/glob/schema/partition helpers and [`ExternalTableRebuild`].
//! - [`external_table`] / [`external_definition`] — the [`ExternalTable`] provider and its definition.
//! - [`view`] — SQL [`ViewTableDefinition`].
//! - [`materialized_view`] — [`MaterializedView`] and its definition.
//! - [`events`] — datasets-store event subscriptions driving self-refresh.

mod definition;
mod events;
mod external_definition;
mod external_table;
mod listing;
mod materialized_view;
mod view;

pub use definition::TableDefinition;
pub use external_definition::ExternalTableDefinition;
pub use external_table::{ExternalTable, RefreshListener};
pub use listing::ExternalTableRebuild;
pub use materialized_view::{MaterializedView, MaterializedViewDefinition};
pub use view::ViewTableDefinition;

pub(crate) use events::datasets_store_subscriptions;
pub(crate) use listing::build_listing_table;
