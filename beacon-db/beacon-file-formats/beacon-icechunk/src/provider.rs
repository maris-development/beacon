//! The [`IcechunkTable`] provider: one Icechunk repository version read through
//! the zarr reader.
//!
//! The repository supplies the storage; everything above it — schema inference,
//! the leaf-group walk, the `beacon-nd-array` scan and the predicate pushdown —
//! is the same code a plain zarr store goes through.

use std::sync::Arc;

use anyhow::Context;
use arrow::datatypes::SchemaRef;
use beacon_arrow_zarr::datafusion::{ZarrFormat, nd_scan_plan, reject_unsupported_options};
use beacon_arrow_zarr::reader::schema_from_group_path;
use beacon_arrow_zarr::util::{ZarrStorage, leaf_group_keys};
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::error::DataFusionError;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use zarrs::group::Group;
use zarrs_icechunk::icechunk::Repository;

use crate::definition::IcechunkTableDefinition;
use crate::repository::{
    IcechunkVersion, ResolvedLocation, open_repository, resolve_location, version_storage,
};

/// The zarr node path of a repository's root group. Icechunk keeps the whole
/// hierarchy under it.
const ROOT_GROUP: &str = "/";

/// A DataFusion table over one version of an Icechunk repository.
///
/// Read only: Icechunk commits and writes are out of scope.
pub struct IcechunkTable {
    definition: IcechunkTableDefinition,
    version: IcechunkVersion,
    repository: Arc<Repository>,
    /// The store the repository lives on. The scan never reads through it — the
    /// bytes come from the repository — but a file scan plan needs a registered
    /// object-store URL, and this is the one that describes the data's home.
    object_store_url: ObjectStoreUrl,
    /// Pinned at registration. A branch tip that gains a *new variable* after
    /// this point is not picked up until the table is re-created; new data in
    /// the variables already present is, because every scan re-reads the ref.
    schema: SchemaRef,
}

impl std::fmt::Debug for IcechunkTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcechunkTable")
            .field("location", &self.definition.location)
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

impl IcechunkTable {
    /// Open the repository named by `definition` and infer its schema.
    pub async fn try_new(
        session: &dyn Session,
        definition: IcechunkTableDefinition,
    ) -> anyhow::Result<Self> {
        let version = IcechunkVersion::from_options(&definition.options)?;
        reject_unsupported_options(&definition.options)?;

        let ResolvedLocation {
            backend,
            object_store_url,
        } = resolve_location(session, &definition.location)?;
        let repository = Arc::new(open_repository(&backend).await?);
        let storage = version_storage(&repository, &version).await?;

        let schema = schema_from_group_path(
            storage.inner(),
            ROOT_GROUP,
            &beacon_datafusion_ext::type_widening::session_widening(session),
        )
        .await
        .with_context(|| {
            format!(
                "failed to read the schema of Icechunk repository {:?}",
                definition.location
            )
        })?;

        Ok(Self {
            definition,
            version,
            repository,
            object_store_url,
            schema,
        })
    }

    /// The persisted definition this table was built from.
    pub fn definition(&self) -> &IcechunkTableDefinition {
        &self.definition
    }

    /// The version this table reads.
    pub fn version(&self) -> &IcechunkVersion {
        &self.version
    }

    /// The `zarr.json` keys of the leaf groups in `storage` — one scan
    /// partition each.
    async fn leaf_files(&self, storage: &ZarrStorage) -> datafusion::error::Result<Vec<String>> {
        let group = Group::async_open(storage.inner(), ROOT_GROUP)
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to open the root Zarr group of Icechunk repository {:?}: {e}",
                    self.definition.location
                ))
            })?;
        leaf_group_keys(&group).await.ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Failed to list the groups of Icechunk repository {:?}",
                self.definition.location
            ))
        })
    }
}

#[async_trait::async_trait]
impl TableProvider for IcechunkTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.definition.definition.as_deref()
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // Re-open the version on every scan: a branch tip moves as commits land,
        // so this is what makes a branch-backed table see the latest data. A tag
        // or snapshot resolves to the same immutable version every time.
        let storage = version_storage(&self.repository, &self.version)
            .await
            .map_err(|e| DataFusionError::External(e.into()))?;
        let files: Vec<PartitionedFile> = self
            .leaf_files(&storage)
            .await?
            .into_iter()
            .map(|key| PartitionedFile::new(key, 0))
            .collect();

        // The scan carries nd data as `beacon.nd`-encoded struct columns; the nd
        // spine above it decodes and broadcasts back to `self.schema`.
        let table_schema = TableSchema::new(
            Arc::new(beacon_datafusion_ext::nd::encoded_schema(&self.schema)),
            vec![],
        );
        let source = ZarrFormat::new()
            .with_storage(storage)
            .morsel_source(table_schema);

        let conf = FileScanConfigBuilder::new(self.object_store_url.clone(), Arc::new(source))
            .with_file_groups(vec![FileGroup::new(files)])
            .with_projection_indices(projection.cloned())?
            .with_limit(limit)
            .build();

        // Filters are not consumed here: `supports_filters_pushdown` leaves the
        // `FilterExec` in place and DataFusion's physical filter pushdown sinks
        // it into the morsel source, exactly as for a listed zarr store.
        nd_scan_plan(conf)
    }
}
