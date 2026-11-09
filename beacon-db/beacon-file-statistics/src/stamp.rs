use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::ObjectMeta;
use xxhash_rust::xxh3::Xxh3;

/// A stable hash of one object version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Stamp(u64);

impl Stamp {
    /// The stamp of `object` in the store at `store_url`.
    pub fn new(store_url: &ObjectStoreUrl, object: &ObjectMeta) -> Self {
        let mut hasher = Xxh3::new();
        // A length prefix on each string keeps ("ab", "c") apart from ("a", "bc").
        for part in [store_url.as_str(), object.location.as_ref()] {
            hasher.update(&(part.len() as u64).to_le_bytes());
            hasher.update(part.as_bytes());
        }
        hasher.update(&object.last_modified.timestamp_millis().to_le_bytes());
        hasher.update(&object.size.to_le_bytes());
        Self(hasher.digest())
    }

    /// The hash value.
    pub fn value(self) -> u64 {
        self.0
    }
}
