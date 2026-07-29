use std::sync::LazyLock;

use datafusion::execution::object_store::ObjectStoreUrl;

pub const DEFAULT_DB_STORE_URL: &str = "db://";
pub static DEFAULT_DB_STORE_URL_OBJECT_URL: LazyLock<ObjectStoreUrl> =
    LazyLock::new(|| ObjectStoreUrl::parse(DEFAULT_DB_STORE_URL).unwrap());

pub const TMP_STORE_URL: &str = "tmp://";
pub static TMP_STORE_URL_OBJECT_URL: LazyLock<ObjectStoreUrl> =
    LazyLock::new(|| ObjectStoreUrl::parse(TMP_STORE_URL).unwrap());
