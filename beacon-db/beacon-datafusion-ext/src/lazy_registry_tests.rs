//! The listing URL resolver together with the lazy object store registry.

use std::sync::Arc;

use datafusion::execution::object_store::ObjectStoreRegistry;
use futures::StreamExt;
use object_store::ObjectStoreExt;
use url::Url;

use crate::listing_url_resolver::parse_listing_table_url;
use crate::object_store_registry::LazyObjectStoreRegistry;
use crate::secrets::SecretStore;

// The resolver and the lazy registry compose: resolving a schemed path
// through `parse_listing_table_url` materializes the store in the registry.
#[test]
fn resolver_materializes_store_via_lazy_registry() {
    let registry = LazyObjectStoreRegistry::new(Arc::new(SecretStore::new()));
    // No default store URL + a schemed path → the resolver drives the
    // registry to build the s3://bucket store on demand.
    let table_url = parse_listing_table_url(None, "s3://bucket/data/*.parquet", &registry).unwrap();
    assert!(
        table_url.as_str().starts_with("s3://bucket"),
        "url={}",
        table_url.as_str()
    );
    // It is now cached in the registry (fast-path hit, no rebuild).
    assert!(
        registry
            .get_store(&Url::parse("s3://bucket").unwrap())
            .is_ok()
    );
}

// End-to-end over a real local store: resolve a local glob, then use the
// store the URL points at to actually enumerate and read the files. This
// proves the resolved `ListingTableUrl` and the registered store agree.
#[tokio::test]
async fn end_to_end_local_listing_and_read() {
    let dir = tempfile::tempdir().unwrap();
    // Canonicalize so the path matches what LocalFileSystem reports (macOS
    // routes tmp through the /private symlink).
    let root = std::fs::canonicalize(dir.path()).unwrap();
    std::fs::write(root.join("a.txt"), b"hello").unwrap();
    std::fs::write(root.join("b.txt"), b"world").unwrap();
    std::fs::write(root.join("skip.bin"), b"nope").unwrap();

    let registry = LazyObjectStoreRegistry::new(Arc::new(SecretStore::new()));
    let pattern = format!("{}/*.txt", root.display());
    let table_url = parse_listing_table_url(None, &pattern, &registry).unwrap();

    // file:// scheme + the glob is preserved on the resolved URL.
    assert!(
        table_url.as_str().starts_with("file://"),
        "url={}",
        table_url.as_str()
    );
    assert_eq!(
        table_url.get_glob().as_ref().map(glob::Pattern::as_str),
        Some("*.txt")
    );

    // Fetch the backing store the URL resolves against and list the prefix.
    let store = registry
        .get_store(&Url::parse(table_url.object_store().as_str()).unwrap())
        .unwrap();
    let prefix = table_url.prefix().clone();
    let metas: Vec<_> = store
        .list(Some(&prefix))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(Result::unwrap)
        .collect();

    let txt: Vec<_> = metas
        .iter()
        .filter(|m| m.location.as_ref().ends_with(".txt"))
        .collect();
    assert_eq!(txt.len(), 2, "expected 2 .txt files, got {metas:?}");

    // And the bytes are actually readable through that store.
    let a = txt
        .iter()
        .find(|m| m.location.as_ref().ends_with("a.txt"))
        .expect("a.txt listed");
    let bytes = store.get(&a.location).await.unwrap().bytes().await.unwrap();
    assert_eq!(bytes.as_ref(), b"hello");
}
