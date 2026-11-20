// The object store code is in `beacon-object-store`. These names keep the old paths.
pub use beacon_object_store::registry as object_store_registry;
pub use beacon_object_store::{big_page_list, secrets};

#[cfg(test)]
mod lazy_registry_tests;

pub mod analyzer_rules;
pub mod cancel;
pub mod consts;
pub mod container_counts;
pub mod expr_adapter;
pub mod fast_object;
pub mod format_ext;
pub mod format_options;
pub mod listing_factory;
pub mod located_table;
pub mod listing_url_resolver;
pub mod nd;
pub mod remote;
pub mod table_ext;
pub mod type_widening;
pub mod unique_values;
