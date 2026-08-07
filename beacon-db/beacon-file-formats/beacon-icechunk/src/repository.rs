//! Opening an Icechunk repository at a Beacon location, and turning one of its
//! versions into the storage a zarr group reads over.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use beacon_arrow_zarr::util::ZarrStorage;
use beacon_datafusion_ext::listing_factory::{ListingFactory, RootStore};
use datafusion::catalog::Session;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::object_store::ObjectStoreUrl;
use zarrs_icechunk::AsyncIcechunkStore;
use zarrs_icechunk::icechunk::{
    Repository, format::SnapshotId, repository::VersionInfo, storage::Storage,
};

/// Which version of a repository a table reads.
///
/// A branch tip moves as commits land; a tag or a snapshot id is fixed, so a
/// query against one gives the same answer after a later commit.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum IcechunkVersion {
    /// The tip of a branch.
    Branch(String),
    /// A named tag.
    Tag(String),
    /// A snapshot id (Crockford base32, as printed by icechunk).
    Snapshot(String),
}

/// The branch a table reads when nothing else is asked for.
pub const DEFAULT_BRANCH: &str = "main";

impl Default for IcechunkVersion {
    fn default() -> Self {
        IcechunkVersion::Branch(DEFAULT_BRANCH.to_string())
    }
}

impl IcechunkVersion {
    /// Build a version from the `branch` / `snapshot` arguments of
    /// `read_icechunk`. An empty string counts as absent.
    ///
    /// The two select different versions, so asking for both is an error rather
    /// than a silent preference for one.
    pub fn from_branch_and_snapshot(
        branch: Option<String>,
        snapshot: Option<String>,
    ) -> anyhow::Result<Self> {
        let branch = branch.filter(|s| !s.trim().is_empty());
        let snapshot = snapshot.filter(|s| !s.trim().is_empty());
        match (branch, snapshot) {
            (Some(_), Some(_)) => anyhow::bail!(
                "a branch and a snapshot select different versions of an Icechunk \
                 repository; pass one of them"
            ),
            (Some(branch), None) => Ok(IcechunkVersion::Branch(branch)),
            (None, Some(snapshot)) => Ok(IcechunkVersion::Snapshot(snapshot)),
            (None, None) => Ok(IcechunkVersion::default()),
        }
    }

    /// Read a `branch` / `tag` / `snapshot` selector out of table OPTIONS.
    ///
    /// Accepts both the raw keys and the `format.`-prefixed forms DataFusion
    /// produces for `OPTIONS` without a dot. At most one may be set.
    pub fn from_options(options: &HashMap<String, String>) -> anyhow::Result<Self> {
        let get = |key: &str| {
            options
                .get(key)
                .or_else(|| options.get(&format!("format.{key}")))
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        };

        let selected: Vec<Self> = [
            get("branch").map(IcechunkVersion::Branch),
            get("tag").map(IcechunkVersion::Tag),
            get("snapshot").map(IcechunkVersion::Snapshot),
        ]
        .into_iter()
        .flatten()
        .collect();

        match <[Self; 1]>::try_from(selected) {
            Ok([version]) => Ok(version),
            Err(selected) if selected.is_empty() => Ok(Self::default()),
            Err(_) => anyhow::bail!(
                "an Icechunk table reads one version: set at most one of \
                 'branch', 'tag' or 'snapshot'"
            ),
        }
    }

    /// Translate into icechunk's own selector.
    fn to_version_info(&self) -> anyhow::Result<VersionInfo> {
        Ok(match self {
            IcechunkVersion::Branch(branch) => VersionInfo::BranchTipRef(branch.clone()),
            IcechunkVersion::Tag(tag) => VersionInfo::TagRef(tag.clone()),
            IcechunkVersion::Snapshot(id) => {
                let id = SnapshotId::try_from(id.as_str())
                    .map_err(|e| anyhow::anyhow!("invalid Icechunk snapshot id {id:?}: {e}"))?;
                VersionInfo::SnapshotId(id)
            }
        })
    }
}

/// The object-store backend an Icechunk repository lives on.
///
/// Icechunk talks to object storage through its own `Storage` layer rather than
/// through DataFusion's object-store registry, so a Beacon location is resolved
/// to a URL first and then mapped onto the matching icechunk backend.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RepositoryBackend {
    LocalFileSystem(PathBuf),
    S3 {
        bucket: String,
        prefix: Option<String>,
    },
    Gcs {
        bucket: String,
        prefix: Option<String>,
    },
    Azure {
        account: String,
        container: String,
        prefix: Option<String>,
    },
    /// Read-only access over plain HTTP(S). Used when the datasets store is
    /// remote and Beacon addresses it by its https base — the same view
    /// netCDF-c reads through. Requests are unsigned, so this reaches a bucket
    /// that serves reads without credentials; name the repository with an
    /// explicit `s3://…` location to read a private one.
    Http { base_url: String },
}

impl std::fmt::Display for RepositoryBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let suffix = |prefix: &Option<String>| match prefix {
            Some(prefix) => format!("/{prefix}"),
            None => String::new(),
        };
        match self {
            RepositoryBackend::LocalFileSystem(path) => write!(f, "{}", path.display()),
            RepositoryBackend::S3 { bucket, prefix } => {
                write!(f, "s3://{bucket}{}", suffix(prefix))
            }
            RepositoryBackend::Gcs { bucket, prefix } => {
                write!(f, "gs://{bucket}{}", suffix(prefix))
            }
            RepositoryBackend::Azure {
                account,
                container,
                prefix,
            } => write!(f, "az://{account}/{container}{}", suffix(prefix)),
            RepositoryBackend::Http { base_url } => write!(f, "{base_url}"),
        }
    }
}

/// Split a path into its leading segment and the rest, e.g. an Azure
/// `az://account/container/prefix` into `("container", Some("prefix"))`.
fn split_first_segment(path: &str) -> (String, Option<String>) {
    let path = path.trim_matches('/');
    match path.split_once('/') {
        Some((first, rest)) => (first.to_string(), non_empty(rest)),
        None => (path.to_string(), None),
    }
}

fn non_empty(value: &str) -> Option<String> {
    let value = value.trim_matches('/');
    (!value.is_empty()).then(|| value.to_string())
}

impl RepositoryBackend {
    /// Map a resolved location URL onto an icechunk backend.
    pub fn from_listing_url(url: &ListingTableUrl) -> anyhow::Result<Self> {
        let inner = url.get_url();
        let host = || -> anyhow::Result<String> {
            inner
                .host_str()
                .map(str::to_string)
                .with_context(|| format!("Icechunk location {inner} is missing a bucket/account"))
        };
        // `prefix` is the URL path, already percent-decoded into an object path.
        let path = || url.prefix().as_ref().to_string();

        match url.scheme() {
            "file" => {
                let path = inner
                    .to_file_path()
                    .map_err(|()| anyhow::anyhow!("invalid local Icechunk location {inner}"))?;
                Ok(RepositoryBackend::LocalFileSystem(path))
            }
            "s3" | "s3a" => Ok(RepositoryBackend::S3 {
                bucket: host()?,
                prefix: non_empty(&path()),
            }),
            "gs" | "gcs" => Ok(RepositoryBackend::Gcs {
                bucket: host()?,
                prefix: non_empty(&path()),
            }),
            "az" | "abfs" | "abfss" | "azure" => {
                let (container, prefix) = split_first_segment(&path());
                anyhow::ensure!(
                    !container.is_empty(),
                    "Azure Icechunk location {inner} is missing a container"
                );
                Ok(RepositoryBackend::Azure {
                    account: host()?,
                    container,
                    prefix,
                })
            }
            other => anyhow::bail!(
                "cannot open an Icechunk repository over `{other}://`: supported \
                 backends are local files, s3, gs and az"
            ),
        }
    }

    /// Map the physical root a configured datasets store resolves to, joined
    /// with the repository's object path within that store.
    ///
    /// A configured store addresses its contents through Beacon's own URL
    /// scheme rather than a backend's, so the root — not the scheme — is what
    /// says where the bytes are.
    pub fn from_root_store(root: &RootStore, prefix: &object_store::path::Path) -> Self {
        match root {
            RootStore::FileSystem(dir) => {
                RepositoryBackend::LocalFileSystem(dir.join(prefix.as_ref()))
            }
            RootStore::HttpsStore(base) => {
                let base = base.trim_end_matches('/');
                let prefix = prefix.as_ref();
                RepositoryBackend::Http {
                    base_url: if prefix.is_empty() {
                        base.to_string()
                    } else {
                        format!("{base}/{prefix}")
                    },
                }
            }
        }
    }

    /// Build the icechunk storage this backend describes.
    ///
    /// Remote credentials are taken from the environment (`AWS_*`, `GOOGLE_*`,
    /// `AZURE_*`), the same source the rest of Beacon's object stores use.
    pub async fn build_storage(&self) -> anyhow::Result<Arc<dyn Storage + Send + Sync>> {
        use zarrs_icechunk::icechunk::storage;

        let storage = match self {
            RepositoryBackend::LocalFileSystem(path) => {
                storage::new_local_filesystem_storage(path)
                    .await
                    .with_context(|| format!("failed to open Icechunk storage at {self}"))?
            }
            RepositoryBackend::S3 { bucket, prefix } => storage::new_s3_object_store_storage(
                storage::S3Options::default(),
                bucket.clone(),
                prefix.clone(),
                Some(storage::S3Credentials::FromEnv),
                vec![],
                vec![],
            )
            .await
            .with_context(|| format!("failed to open Icechunk storage at s3://{bucket}"))?,
            RepositoryBackend::Gcs { bucket, prefix } => storage::new_gcs_storage(
                bucket.clone(),
                prefix.clone(),
                Some(storage::GcsCredentials::FromEnv),
                None,
                vec![],
                vec![],
            )
            .with_context(|| format!("failed to open Icechunk storage at gs://{bucket}"))?,
            RepositoryBackend::Azure {
                account,
                container,
                prefix,
            } => storage::new_azure_blob_storage(
                account.clone(),
                container.clone(),
                prefix.clone(),
                Some(storage::AzureCredentials::FromEnv),
                None,
            )
            .await
            .with_context(|| {
                format!("failed to open Icechunk storage at az://{account}/{container}")
            })?,
            RepositoryBackend::Http { base_url } => storage::new_http_storage(
                base_url,
                None,
                None,
            )
            .with_context(|| format!("failed to open Icechunk storage at {base_url}"))?,
        };
        Ok(storage)
    }
}

/// A Beacon location resolved to an Icechunk repository: the backend it lives
/// on, plus the object-store URL the same location maps to.
///
/// The object-store URL never carries repository bytes — those come from the
/// icechunk storage — but the scan plan still needs a registered store URL, so
/// the repository's own is used.
pub struct ResolvedLocation {
    pub backend: RepositoryBackend,
    pub object_store_url: ObjectStoreUrl,
}

/// Resolve a Beacon location (a datasets-relative path, `s3://…`, …) into an
/// Icechunk repository backend.
pub fn resolve_location(session: &dyn Session, location: &str) -> anyhow::Result<ResolvedLocation> {
    let listing_factory = session
        .config()
        .get_extension::<ListingFactory>()
        .context("an Icechunk table requires a ListingFactory extension")?;

    let url: ListingTableUrl = listing_factory
        .parse_listing_table_url(session, location)
        .with_context(|| format!("failed to resolve Icechunk location {location:?}"))?;

    // A location that names its own backend (`s3://…`, `file://…`) maps straight
    // onto it, with credentials from the environment. A location resolved
    // against a *configured* datasets store carries Beacon's own scheme
    // (`datasets://`) instead, so the physical root that store maps to is what
    // decides the backend — the same root netCDF-c reads through.
    let backend = match url.scheme() {
        "file" | "s3" | "s3a" | "gs" | "gcs" | "az" | "abfs" | "abfss" | "azure" => {
            RepositoryBackend::from_listing_url(&url)?
        }
        _ => RepositoryBackend::from_root_store(&listing_factory.native_read_root(&url)?, url.prefix()),
    };

    Ok(ResolvedLocation {
        backend,
        object_store_url: url.object_store(),
    })
}

/// Open the repository at `backend`, read only.
///
/// # Virtual chunk references
///
/// The repository is opened with **no authorized virtual chunk containers**
/// (`authorize_virtual_chunk_access` is empty). A chunk that lives inside the
/// repository reads; a virtual reference to a netCDF or HDF5 file outside it
/// fails with an icechunk authorization error naming the container. That is
/// deliberate — see the crate documentation.
pub async fn open_repository(backend: &RepositoryBackend) -> anyhow::Result<Repository> {
    let storage = backend.build_storage().await?;
    let error = match Repository::open(None, storage.clone(), HashMap::new()).await {
        Ok(repository) => return Ok(repository),
        Err(error) => error,
    };

    // Separate "there is nothing here" from "it is here but would not open". The
    // first is what pointing at a plain zarr store or a wrong path gives, and
    // icechunk's own error does not say which of the two happened.
    if let Ok(false) = Repository::exists(storage, None).await {
        anyhow::bail!(
            "no Icechunk repository at {backend}. An Icechunk repository keeps its \
             metadata in snapshots; a plain Zarr store reads with read_zarr instead."
        );
    }
    Err(anyhow::Error::new(error)
        .context(format!("failed to open Icechunk repository at {backend}")))
}

/// Whether an Icechunk repository exists at `backend`.
///
/// Tells an Icechunk repository apart from a plain zarr store: a repository
/// keeps its metadata in snapshots, so it has no `zarr.json` object to find by
/// listing.
pub async fn is_icechunk_repository(backend: &RepositoryBackend) -> anyhow::Result<bool> {
    let storage = backend.build_storage().await?;
    Repository::exists(storage, None)
        .await
        .with_context(|| format!("failed to probe for an Icechunk repository at {backend}"))
}

/// Open `version` of `repository` as the storage a zarr group reads over.
pub async fn version_storage(
    repository: &Repository,
    version: &IcechunkVersion,
) -> anyhow::Result<ZarrStorage> {
    let session = repository
        .readonly_session(&version.to_version_info()?)
        .await
        .with_context(|| format!("failed to open Icechunk version {version:?}"))?;
    Ok(ZarrStorage::new(Arc::new(AsyncIcechunkStore::new(session))))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(s: &str) -> ListingTableUrl {
        ListingTableUrl::parse(s).unwrap()
    }

    #[test]
    fn version_defaults_to_the_main_branch() {
        assert_eq!(
            IcechunkVersion::default(),
            IcechunkVersion::Branch("main".to_string())
        );
        assert_eq!(
            IcechunkVersion::from_branch_and_snapshot(None, None).unwrap(),
            IcechunkVersion::Branch("main".to_string())
        );
        // A blank argument is the same as no argument.
        assert_eq!(
            IcechunkVersion::from_branch_and_snapshot(Some("  ".into()), Some(String::new()))
                .unwrap(),
            IcechunkVersion::Branch("main".to_string())
        );
    }

    #[test]
    fn version_takes_a_branch_or_a_snapshot_but_not_both() {
        assert_eq!(
            IcechunkVersion::from_branch_and_snapshot(Some("dev".into()), None).unwrap(),
            IcechunkVersion::Branch("dev".to_string())
        );
        assert_eq!(
            IcechunkVersion::from_branch_and_snapshot(None, Some("ABC123".into())).unwrap(),
            IcechunkVersion::Snapshot("ABC123".to_string())
        );
        let err =
            IcechunkVersion::from_branch_and_snapshot(Some("dev".into()), Some("ABC123".into()))
                .unwrap_err();
        assert!(err.to_string().contains("pass one of them"), "{err}");
    }

    #[test]
    fn version_reads_options_raw_and_format_prefixed() {
        let options = HashMap::from([("branch".to_string(), "dev".to_string())]);
        assert_eq!(
            IcechunkVersion::from_options(&options).unwrap(),
            IcechunkVersion::Branch("dev".to_string())
        );

        let options = HashMap::from([("format.tag".to_string(), "v1".to_string())]);
        assert_eq!(
            IcechunkVersion::from_options(&options).unwrap(),
            IcechunkVersion::Tag("v1".to_string())
        );

        // Nothing set falls back to the tip of main.
        assert_eq!(
            IcechunkVersion::from_options(&HashMap::new()).unwrap(),
            IcechunkVersion::default()
        );

        // Two selectors is an error, not a silent preference.
        let options = HashMap::from([
            ("branch".to_string(), "dev".to_string()),
            ("snapshot".to_string(), "ABC".to_string()),
        ]);
        let err = IcechunkVersion::from_options(&options).unwrap_err();
        assert!(err.to_string().contains("at most one"), "{err}");
    }

    #[test]
    fn snapshot_ids_are_validated_when_translated() {
        // A well-formed Crockford base32 id of the right length round-trips.
        let id = SnapshotId::new([7u8; 12]).to_string();
        assert!(matches!(
            IcechunkVersion::Snapshot(id).to_version_info().unwrap(),
            VersionInfo::SnapshotId(_)
        ));
        // Anything else is rejected with the offending value in the message.
        let err = IcechunkVersion::Snapshot("not-an-id".to_string())
            .to_version_info()
            .unwrap_err();
        assert!(err.to_string().contains("not-an-id"), "{err}");
    }

    #[test]
    fn backends_are_derived_from_the_location_scheme() {
        assert_eq!(
            RepositoryBackend::from_listing_url(&url("s3://bucket/repos/argo")).unwrap(),
            RepositoryBackend::S3 {
                bucket: "bucket".to_string(),
                prefix: Some("repos/argo".to_string())
            }
        );
        // A bucket root has no prefix.
        assert_eq!(
            RepositoryBackend::from_listing_url(&url("s3://bucket/")).unwrap(),
            RepositoryBackend::S3 {
                bucket: "bucket".to_string(),
                prefix: None
            }
        );
        assert_eq!(
            RepositoryBackend::from_listing_url(&url("gs://bucket/argo")).unwrap(),
            RepositoryBackend::Gcs {
                bucket: "bucket".to_string(),
                prefix: Some("argo".to_string())
            }
        );
        // Azure spends the first path segment on the container.
        assert_eq!(
            RepositoryBackend::from_listing_url(&url("az://account/container/argo")).unwrap(),
            RepositoryBackend::Azure {
                account: "account".to_string(),
                container: "container".to_string(),
                prefix: Some("argo".to_string())
            }
        );
        assert_eq!(
            RepositoryBackend::from_listing_url(&url("file:///data/argo")).unwrap(),
            RepositoryBackend::LocalFileSystem(PathBuf::from("/data/argo"))
        );
    }

    #[test]
    fn a_backend_displays_as_the_location_it_reads() {
        assert_eq!(
            RepositoryBackend::LocalFileSystem(PathBuf::from("/srv/datasets/argo")).to_string(),
            "/srv/datasets/argo"
        );
        assert_eq!(
            RepositoryBackend::S3 {
                bucket: "bucket".to_string(),
                prefix: Some("argo/repo".to_string())
            }
            .to_string(),
            "s3://bucket/argo/repo"
        );
        // A bucket root has no trailing separator to show.
        assert_eq!(
            RepositoryBackend::S3 {
                bucket: "bucket".to_string(),
                prefix: None
            }
            .to_string(),
            "s3://bucket"
        );
        assert_eq!(
            RepositoryBackend::Azure {
                account: "account".to_string(),
                container: "container".to_string(),
                prefix: Some("argo".to_string())
            }
            .to_string(),
            "az://account/container/argo"
        );
    }

    #[test]
    fn unsupported_schemes_say_what_is_supported() {
        let err = RepositoryBackend::from_listing_url(&url("https://example.com/argo")).unwrap_err();
        assert!(err.to_string().contains("s3"), "{err}");
        // Azure without a container is not a repository location.
        assert!(RepositoryBackend::from_listing_url(&url("az://account/")).is_err());
    }

    #[test]
    fn a_configured_store_root_decides_the_backend() {
        use object_store::path::Path as ObjectPath;

        // Local datasets store: the repository is the prefix under its root.
        assert_eq!(
            RepositoryBackend::from_root_store(
                &RootStore::FileSystem(PathBuf::from("/srv/datasets")),
                &ObjectPath::from("argo/repo")
            ),
            RepositoryBackend::LocalFileSystem(PathBuf::from("/srv/datasets/argo/repo"))
        );

        // Remote datasets store: the prefix is appended to its https base.
        assert_eq!(
            RepositoryBackend::from_root_store(
                &RootStore::HttpsStore("https://s3.example.com/bucket/".to_string()),
                &ObjectPath::from("argo/repo")
            ),
            RepositoryBackend::Http {
                base_url: "https://s3.example.com/bucket/argo/repo".to_string()
            }
        );

        // The store root itself is a valid repository location.
        assert_eq!(
            RepositoryBackend::from_root_store(
                &RootStore::HttpsStore("https://s3.example.com/bucket".to_string()),
                &ObjectPath::from("")
            ),
            RepositoryBackend::Http {
                base_url: "https://s3.example.com/bucket".to_string()
            }
        );
    }

    #[test]
    fn percent_escapes_in_a_path_are_decoded() {
        assert_eq!(
            RepositoryBackend::from_listing_url(&url("s3://bucket/my%20repo")).unwrap(),
            RepositoryBackend::S3 {
                bucket: "bucket".to_string(),
                prefix: Some("my repo".to_string())
            }
        );
    }
}
