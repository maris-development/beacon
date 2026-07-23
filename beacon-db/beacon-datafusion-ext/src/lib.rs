pub mod analyzer_rules;
pub mod consts;
pub mod file_collection;
pub mod format_ext;
pub mod listing_factory;
pub mod listing_table_factory_ext;
pub mod listing_url_resolver;
pub mod nd;
pub mod object_store_registry;
pub mod remote;
pub mod secrets;
pub mod stats_cache;
pub mod table_ext;
pub mod type_widening;
pub mod unique_values;

// The listing-URL parser (resolve a path/glob under a datasets object store into a
// `ListingTableUrl`) lives in `beacon-common`; re-exported here so beacon-core and
// other consumers reach it through this crate rather than depending on
// `beacon-common` directly. Distinct from `listing_url_resolver::parse_listing_table_url`,
// which additionally registers a scheme-carrying store on demand.
