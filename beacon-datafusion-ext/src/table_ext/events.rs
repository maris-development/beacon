//! Datasets-store event subscription glue for self-refreshing external tables.

use beacon_object_storage::event::ObjectEvent;
use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::path::Path as ObjectPath;
use tokio::sync::broadcast::Receiver;

/// The object-store URL whose events drive external-table self-refresh.
pub(crate) const DATASETS_STORE_URL: &str = "datasets://";

/// Subscribe to datasets-store events for each of a table's storage `prefixes`.
///
/// Only the datasets store emits events, so tables on any other store (e.g. the
/// `file://` store used in tests) get an empty `Vec` and rely on manual `REFRESH`.
/// Otherwise one receiver is returned per prefix; the store filters events to each
/// prefix (segment-aware), so callers do not need to filter again. A table usually
/// has a single prefix, but multiple are supported.
pub(crate) async fn datasets_store_subscriptions(
    data_store_url: &ObjectStoreUrl,
    prefixes: &[ObjectPath],
) -> Vec<Receiver<ObjectEvent>> {
    if data_store_url.as_str() != DATASETS_STORE_URL {
        return Vec::new();
    }
    let store = beacon_object_storage::get_datasets_object_store().await;
    prefixes
        .iter()
        .map(|prefix| store.subscribe_events(prefix.as_ref()))
        .collect()
}
