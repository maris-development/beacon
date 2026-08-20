use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use datafusion::{
    catalog::Session,
    datasource::{
        file_format::{FileFormat, FileFormatFactory},
        listing::ListingTableUrl,
    },
    object_store::ObjectMeta,
    prelude::SessionContext,
};

use crate::listing_factory::ListingFactory;

pub trait FileFormatFactoryExt: FileFormatFactory + Send + Sync {
    fn discover_datasets(
        &self,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<Vec<DatasetMetadata>>;

    /// Whether this format claims `object`, judged from that object alone.
    ///
    /// Every format decides per object: the file formats read an extension, and
    /// Zarr reads the directory holding its marker. So the default asks
    /// [`Self::discover_datasets`] about a listing of one, and a format needs to
    /// override this only if it ever wants to see its objects together.
    ///
    /// This is what lets a listing classify as it streams, instead of holding
    /// every object first. The size and timestamp come from `object` here, where
    /// `discover_datasets` leaves them for a later pass over the whole listing.
    fn classify_object(&self, object: &ObjectMeta) -> Option<DatasetMetadata> {
        let mut found = self
            .discover_datasets(std::slice::from_ref(object))
            .ok()?
            .into_iter()
            .next()?;
        if found.file_path == object.location.as_ref() {
            found.size = Some(object.size);
            found.last_modified = Some(object.last_modified);
        }
        Some(found)
    }
    fn file_format_name(&self) -> String;
    fn list_with_file_extension(&self) -> bool {
        true
    }

    /// The filename extensions this format recognizes (e.g. `["tiff", "tif"]`).
    ///
    /// DataFusion's session registry keys a format only under its canonical
    /// `get_ext()`, so resolving a format from a raw filename extension must
    /// consult this list to honor aliases. Defaults to the canonical extension;
    /// override when a format accepts more than one spelling.
    fn file_extensions(&self) -> Vec<String> {
        vec![self.get_ext()]
    }

    /// Create a [`FileFormat`] for files located at `url`.
    ///
    /// A format read *natively* — opened by local path or http(s) URL by an
    /// external reader (netCDF-c), never streamed through the object store — needs
    /// to know which [`RootStore`](crate::listing_factory::RootStore) its objects
    /// live under so it can turn each
    /// listed object into a path that reader can open. `listing` resolves that
    /// from `url` via [`ListingFactory::native_read_root`].
    ///
    /// The default ignores both and delegates to [`FileFormatFactory::create`], so
    /// object-store formats (Parquet, CSV, …) are unaffected and no location is
    /// rejected on their behalf. Only an overriding format calls
    /// `native_read_root`, so the "only local files and http/https" error is raised
    /// exactly for the formats that have that limitation.
    fn create_with_native_root(
        &self,
        state: &dyn Session,
        format_options: &HashMap<String, String>,
        _url: &ListingTableUrl,
        _listing: &ListingFactory,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        self.create(state, format_options)
    }

    /// Create a [`FileFormat`] that computes file statistics.
    ///
    /// A format answers `infer_stats` by reading the file. For parquet that is a
    /// footer; for a format built on nd arrays it is an open and a read of every
    /// coordinate array. `ListingTable::scan` infers statistics for every file
    /// it lists, and it does so during planning, so a format of the second kind
    /// makes a query over a large collection take minutes to plan and look like
    /// a hang.
    ///
    /// Those formats therefore answer `infer_stats` with
    /// `Statistics::new_unknown` unless they were built here. The file analyzer
    /// builds them here, records what it finds in the file-statistics store, and
    /// the scan prunes from that store instead. The reading happens once, in the
    /// background, rather than once per query in the planner.
    ///
    /// The default delegates, so a format whose statistics are already cheap is
    /// unaffected and keeps feeding them to the optimizer.
    fn create_for_analysis(
        &self,
        state: &dyn Session,
        format_options: &HashMap<String, String>,
        url: &ListingTableUrl,
        listing: &ListingFactory,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        self.create_with_native_root(state, format_options, url, listing)
    }

    /// A fingerprint over every part of `format` that changes the schema of one
    /// object.
    ///
    /// The fingerprint lets the cache keep the schema of a file. A schema is not a
    /// function of the file alone: `read_dimensions` changes which netCDF
    /// variables appear, so the same bytes hold more than one schema.
    ///
    /// `None` is the default, and it keeps this format out of the cache. An
    /// override claims two things:
    ///
    /// 1. `infer_schema` over `n` objects equals the
    ///    [`ArrowTypeWidening`](crate::type_widening::ArrowTypeWidening) merge of
    ///    the session over the `n` single-object results, in listing order.
    /// 2. The fingerprint covers every part that changes the schema of a file. An
    ///    omission returns a wrong schema, not a slow one, so each implementation
    ///    carries a test that two option sets give two fingerprints.
    ///
    /// Leave out a part that changes no schema, such as a reader cache or a
    /// statistics switch. Such a part costs cache hits.
    ///
    /// `None` also suits an option that this format does not separate yet. The
    /// four nd formats return `None` for a read that names dimensions.
    ///
    /// Build the value with [`SchemaOptions`]. A `Hasher` from `std` has no stable
    /// algorithm across Rust releases, so a new toolchain would retire every
    /// stored entry.
    fn schema_options_fingerprint(&self, _format: &dyn FileFormat) -> Option<u64> {
        None
    }

    /// Which listed objects this format derives a schema from, and what each
    /// one depends on.
    ///
    /// The default is one unit per object, each depending on itself, which suits
    /// every format that reads a file. Zarr and Atlas override it: they derive
    /// one schema per *store*, from a marker at its root, and the rest of the
    /// listing is that store's contents. [`units_over_stores`] builds that.
    fn schema_units(&self, objects: &[ObjectMeta]) -> Vec<SchemaUnit> {
        (0..objects.len()).map(SchemaUnit::from_file).collect()
    }
}

/// One schema a format derives, as positions in the listing it was given.
///
/// Positions rather than objects, because the common case is one unit per file
/// and cloning a listing of a hundred thousand `ObjectMeta` to say so would cost
/// more than the lookup it feeds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaUnit {
    /// The object this schema is keyed on: the file, or a store's marker.
    pub source: usize,
    /// Every *other* object the schema depends on. Empty for a format that
    /// reads a file, which is all of them but Zarr and Atlas.
    ///
    /// These decide when a cached entry stops being valid. A Zarr store's
    /// schema comes from arrays under its marker, so a changed chunk has to
    /// invalidate it even though the marker itself did not move.
    pub dependents: Vec<usize>,
}

impl SchemaUnit {
    /// A unit over one file, depending on nothing else.
    pub fn from_file(source: usize) -> Self {
        Self {
            source,
            dependents: Vec::new(),
        }
    }
}

/// Units for a format whose schema comes from a marker at a store's root.
///
/// `markers` are the objects the format will actually infer from — Zarr's
/// top-level `zarr.json` files, Atlas's markers. Each unit depends on every
/// listed object under the marker's directory, so a change anywhere in a store
/// invalidates that store's cached schema.
///
/// A marker that is not in `objects` is skipped: the caller listed `objects`, so
/// a marker from anywhere else has no position to key on.
pub fn units_over_stores(objects: &[ObjectMeta], markers: &[ObjectMeta]) -> Vec<SchemaUnit> {
    let position: HashMap<&str, usize> = objects
        .iter()
        .enumerate()
        .map(|(index, object)| (object.location.as_ref(), index))
        .collect();

    markers
        .iter()
        .filter_map(|marker| {
            let source = *position.get(marker.location.as_ref())?;
            // Everything beside the marker, under its directory. `parent` is
            // "" for a marker at the root, and every path is under that.
            let parent = marker
                .location
                .as_ref()
                .rsplit_once('/')
                .map(|(dir, _)| dir)
                .unwrap_or("");
            let dependents = objects
                .iter()
                .enumerate()
                .filter(|(index, object)| {
                    *index != source && under(object.location.as_ref(), parent)
                })
                .map(|(index, _)| index)
                .collect();
            Some(SchemaUnit { source, dependents })
        })
        .collect()
}

/// Whether `path` sits under `directory`. Segment-aware, so `argo2/x` is not
/// under `argo`.
fn under(path: &str, directory: &str) -> bool {
    if directory.is_empty() {
        return true;
    }
    path.len() > directory.len()
        && path.starts_with(directory)
        && path.as_bytes()[directory.len()] == b'/'
}

/// Builds a format's schema fingerprint, stably.
///
/// A fixed algorithm on purpose. `std`'s `DefaultHasher` is deterministic within
/// a Rust release but not across them, so using it would silently retire every
/// stored schema on a toolchain bump.
///
/// Start with the format's name, then add each option that changes a schema:
///
/// ```ignore
/// SchemaOptions::new("netcdf")
///     .strs(self.options.read_dimensions.iter().flatten())
///     .str(backend_name)
///     .finish()
/// ```
///
/// Every method writes a length before its value, so no two option lists can
/// run together into the same digest.
pub struct SchemaOptions(blake3::Hasher);

impl SchemaOptions {
    /// Start a fingerprint for `format_name`.
    ///
    /// The name is in it so two formats that happen to take the same options
    /// never share a key.
    pub fn new(format_name: &str) -> Self {
        Self(blake3::Hasher::new()).str(format_name)
    }

    pub fn str(mut self, value: &str) -> Self {
        self.0.update(&(value.len() as u64).to_be_bytes());
        self.0.update(value.as_bytes());
        self
    }

    /// A value that may be absent. Absent and empty are different.
    pub fn opt_str(self, value: Option<&str>) -> Self {
        match value {
            Some(value) => self.u64(1).str(value),
            None => self.u64(0),
        }
    }

    /// An ordered list. Order is part of it, because it can be part of the
    /// schema.
    ///
    /// No format uses this yet: the four nd formats opt out of the cache when a
    /// read names dimensions rather than key on the set. This is what they will
    /// key on when they stop opting out — see their `TODO(#367)` notes — and
    /// the reason it hashes a length before each value.
    pub fn strs<'a>(self, values: impl IntoIterator<Item = &'a str>) -> Self {
        let values: Vec<&str> = values.into_iter().collect();
        values
            .iter()
            .fold(self.u64(values.len() as u64), |options, value| {
                options.str(value)
            })
    }

    pub fn bool(self, value: bool) -> Self {
        self.u64(u64::from(value))
    }

    pub fn u64(mut self, value: u64) -> Self {
        self.0.update(&value.to_be_bytes());
        self
    }

    /// An optional count. Absent and zero are different.
    pub fn opt_u64(self, value: Option<u64>) -> Self {
        match value {
            Some(value) => self.u64(1).u64(value),
            None => self.u64(0),
        }
    }

    pub fn finish(self) -> u64 {
        let digest = self.0.finalize();
        u64::from_be_bytes(digest.as_bytes()[..8].try_into().expect("8 bytes"))
    }
}

/// Shared, late-filled handle to the [`FileFormatRegistry`], registered as a
/// session-config extension. The formats are built *after* the session (they need
/// it), so the cell is registered empty during config construction and filled once
/// the factories exist — the same pattern as the session and crawler handles.
pub type FileFormatRegistryHandle = Arc<OnceLock<FileFormatRegistry>>;

/// Create an empty registry handle to register as a session extension.
pub fn new_file_format_registry_handle() -> FileFormatRegistryHandle {
    Arc::new(OnceLock::new())
}

/// The beacon [`FileFormatFactoryExt`] answering to `key` (a format name or file
/// extension), recovered from the session's registry handle.
///
/// This is the only way back to the `Ext` trait: DataFusion's own registry hands
/// out `Arc<dyn FileFormatFactory>`, and there is no upcast from that to
/// [`FileFormatFactoryExt`] (nor any way to downcast to a concrete factory from
/// this crate, which the format crates depend on rather than the reverse).
///
/// `None` when the registry is absent or unfilled, or nothing answers to `key`.
pub fn try_file_format_factory_ext(
    session: &dyn Session,
    key: &str,
) -> Option<Arc<dyn FileFormatFactoryExt>> {
    session
        .config()
        .get_extension::<OnceLock<FileFormatRegistry>>()?
        .get()?
        .get(key)
        .cloned()
}

/// Beacon's [`FileFormatFactoryExt`] factories, keyed by the format names and file
/// extensions they answer to. Registered (via a late-filled `OnceLock`) as a
/// session-config extension so plan-time code — the external-table builder, table
/// functions — can recover format capabilities (like
/// [`FileFormatFactoryExt::create_with_native_root`]) that DataFusion's plain
/// `FileFormatFactory` registry erases once the concrete `Ext` type is gone.
#[derive(Default)]
pub struct FileFormatRegistry {
    by_key: HashMap<String, Arc<dyn FileFormatFactoryExt>>,
}

impl FileFormatRegistry {
    pub fn new(formats: &[Arc<dyn FileFormatFactoryExt>]) -> Self {
        let mut by_key = HashMap::new();
        for format in formats {
            by_key
                .entry(format.file_format_name().to_ascii_lowercase())
                .or_insert_with(|| format.clone());
            for ext in format.file_extensions() {
                by_key
                    .entry(ext.to_ascii_lowercase())
                    .or_insert_with(|| format.clone());
            }
        }
        Self { by_key }
    }

    /// The factory answering to `key` (a format name or file extension), if any.
    pub fn get(&self, key: &str) -> Option<&Arc<dyn FileFormatFactoryExt>> {
        self.by_key.get(&key.to_ascii_lowercase())
    }
}

pub fn file_format_by_ext(ext: &str, session_ctx: &SessionContext) -> Option<Arc<dyn FileFormat>> {
    let state = session_ctx.state();
    let factory = state.get_file_format_factory(ext)?;
    Some(factory.default())
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DatasetMetadata {
    pub file_path: String,
    pub format: String,
    pub can_inspect: bool,
    pub can_partial_explore: bool,
    /// Total size in bytes of the underlying object(s), when known. Filled in by
    /// `list_datasets` from the object listing; `None` for datasets whose size
    /// can't be resolved (e.g. no matching object).
    pub size: Option<u64>,
    /// Last-modified time of the underlying object(s) (RFC 3339), when known.
    pub last_modified: Option<chrono::DateTime<chrono::Utc>>,
}

impl DatasetMetadata {
    pub fn new(file_path: String, format: String) -> Self {
        Self {
            file_path,
            format,
            can_inspect: false,
            can_partial_explore: false,
            size: None,
            last_modified: None,
        }
    }
}

#[cfg(test)]
mod schema_cache_tests {
    use super::*;
    use object_store::path::Path;

    fn objects(paths: &[&str]) -> Vec<ObjectMeta> {
        paths
            .iter()
            .map(|path| ObjectMeta {
                location: Path::from(*path),
                last_modified: chrono::Utc::now(),
                size: 1,
                e_tag: None,
                version: None,
            })
            .collect()
    }

    fn marker(objects: &[ObjectMeta], path: &str) -> ObjectMeta {
        objects
            .iter()
            .find(|object| object.location.as_ref() == path)
            .expect("the marker is in the listing")
            .clone()
    }

    /// A store's schema depends on everything in it. That is what makes a
    /// changed chunk invalidate the entry, which a stamp over the marker alone
    /// would miss.
    #[test]
    fn a_store_unit_depends_on_its_whole_directory() {
        let listing = objects(&[
            "a/zarr.json",
            "a/TEMP/zarr.json",
            "a/TEMP/c/0",
            "b/zarr.json",
            "b/c/0",
        ]);
        let markers = vec![
            marker(&listing, "a/zarr.json"),
            marker(&listing, "b/zarr.json"),
        ];

        let units = units_over_stores(&listing, &markers);
        assert_eq!(units.len(), 2);
        assert_eq!(units[0].source, 0);
        assert_eq!(units[0].dependents, vec![1, 2], "everything under a/");
        assert_eq!(units[1].source, 3);
        assert_eq!(units[1].dependents, vec![4], "everything under b/");
    }

    /// A single store at the listing root takes every object with it.
    #[test]
    fn a_marker_at_the_root_covers_the_listing() {
        let listing = objects(&["zarr.json", "TEMP/zarr.json", "TEMP/c/0"]);
        let units = units_over_stores(&listing, &[marker(&listing, "zarr.json")]);
        assert_eq!(units.len(), 1);
        assert_eq!(units[0].dependents, vec![1, 2]);
    }

    /// A marker the caller did not list has no position to key on, so it is
    /// dropped rather than guessed at.
    #[test]
    fn a_marker_outside_the_listing_is_skipped() {
        let listing = objects(&["a/zarr.json"]);
        let elsewhere = objects(&["z/zarr.json"]);
        assert!(units_over_stores(&listing, &elsewhere).is_empty());
    }

    /// A prefix must end at a directory boundary. `argo2/` is not inside
    /// `argo/`, and treating it as such would tie two stores together.
    #[test]
    fn a_directory_prefix_stops_at_a_boundary() {
        assert!(under("argo/a.nc", "argo"));
        assert!(under("argo/2024/a.nc", "argo"));
        assert!(!under("argo2/a.nc", "argo"));
        assert!(!under("argo", "argo"), "a directory is not under itself");
        assert!(under("anything", ""), "the root holds everything");
    }

    /// The default is one unit per object, depending on nothing else. That is
    /// every format but Zarr and Atlas.
    #[test]
    fn the_default_unit_is_one_file() {
        let unit = SchemaUnit::from_file(3);
        assert_eq!(unit.source, 3);
        assert!(unit.dependents.is_empty());
    }

    /// The whole point of the fingerprint: two option sets that read a file
    /// differently must never share a key.
    #[test]
    fn different_options_give_different_fingerprints() {
        let base = SchemaOptions::new("netcdf").finish();
        assert_ne!(base, SchemaOptions::new("hdf5").finish());
        assert_ne!(base, SchemaOptions::new("netcdf").strs(["TIME"]).finish());
        assert_ne!(
            SchemaOptions::new("netcdf").strs(["TIME"]).finish(),
            SchemaOptions::new("netcdf").strs(["DEPTH"]).finish()
        );
        // Order counts: it decides which dimensions a variable broadcasts over.
        assert_ne!(
            SchemaOptions::new("netcdf")
                .strs(["TIME", "DEPTH"])
                .finish(),
            SchemaOptions::new("netcdf")
                .strs(["DEPTH", "TIME"])
                .finish()
        );
        // Absent is not empty, and absent is not zero.
        assert_ne!(
            SchemaOptions::new("csv").opt_str(None).finish(),
            SchemaOptions::new("csv").opt_str(Some("")).finish()
        );
        assert_ne!(
            SchemaOptions::new("csv").opt_u64(None).finish(),
            SchemaOptions::new("csv").opt_u64(Some(0)).finish()
        );
        assert_ne!(
            SchemaOptions::new("csv").bool(true).finish(),
            SchemaOptions::new("csv").bool(false).finish()
        );
    }

    /// Two option lists must not run together into one digest. Without the
    /// length prefixes, `["ab"]` and `["a", "b"]` would collide.
    #[test]
    fn values_cannot_run_together() {
        assert_ne!(
            SchemaOptions::new("f").strs(["ab"]).finish(),
            SchemaOptions::new("f").strs(["a", "b"]).finish()
        );
        assert_ne!(
            SchemaOptions::new("f").str("a").str("b").finish(),
            SchemaOptions::new("f").str("ab").finish()
        );
    }

    /// The same options give the same answer, every time. A fingerprint that
    /// drifted would retire the cache without saying so.
    #[test]
    fn the_same_options_give_the_same_fingerprint() {
        let build = || {
            SchemaOptions::new("netcdf")
                .strs(["TIME", "DEPTH"])
                .str("oxcdf")
                .bool(true)
                .u64(4096)
                .finish()
        };
        assert_eq!(build(), build());
    }
}
