//! DataFusion integration for zarr stores.
//!
//! Mirrors the netcdf/tiff/atlas crates: a [`ZarrFormatFactory`] discovers
//! zarr stores, [`ZarrFormat`] infers the (super-typed) Arrow schema and plans
//! the scan, and [`ZarrSource`] streams each leaf group through the shared
//! `beacon-nd-array` engine with predicate pushdown.

use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use beacon_common::super_typing::super_type_schema;
use beacon_datafusion_ext::format_ext::{DatasetMetadata, FileFormatFactoryExt};
use beacon_nd_array::arrow::schema::any_dataset_to_arrow_schema;
use beacon_nd_array::dataset::resolve_read_dimensions;
use beacon_nd_array::projection::DatasetProjection;
use datafusion::{
    catalog::{Session, memory::DataSourceExec},
    common::{GetExt, Statistics},
    datasource::{
        file_format::{FileFormat, FileFormatFactory, file_compression_type::FileCompressionType},
        listing::PartitionedFile,
        physical_plan::{FileGroup, FileScanConfig, FileScanConfigBuilder, FileSource},
    },
    physical_plan::ExecutionPlan,
};
use object_store::{ObjectMeta, ObjectStore};
use zarrs::group::Group;
use zarrs_object_store::AsyncObjectStore;
use zarrs_storage::AsyncReadableListableStorageTraits;

use crate::{
    reader::dataset_from_group,
    util::{
        ZarrPath, find_partitioned_files, is_zarr_v3_metadata, recursive_groups,
        top_level_zarr_meta_v3,
    },
};

pub mod options;
pub mod sink;
pub mod source;

pub use options::ZarrOptions;
pub use sink::{ZarrNdSink, ZarrSink};
pub use source::ZarrSource;

// ─── Factory ─────────────────────────────────────────────────────────────────

#[derive(Default)]
pub struct ZarrFormatFactory {
    /// Datasets store, used only for writing: a zarr store is built on the
    /// local filesystem under its configured tmp root. `None` for read-only
    /// registrations, where `create_writer_physical_plan` is never reached.
    datasets_store: Option<Arc<beacon_object_storage::DatasetsStore>>,
    /// Write-side defaults applied to formats this factory creates.
    options: ZarrOptions,
}

impl std::fmt::Debug for ZarrFormatFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZarrFormatFactory")
            .field("options", &self.options)
            .finish()
    }
}

impl ZarrFormatFactory {
    /// A read-only factory. Writes through it fail with a clear error.
    pub fn new() -> Self {
        Self {
            datasets_store: None,
            options: ZarrOptions::default(),
        }
    }

    /// A factory that can also write, using `datasets_store`'s tmp root as the
    /// destination for the store directory.
    pub fn new_for_write(
        datasets_store: Arc<beacon_object_storage::DatasetsStore>,
        options: ZarrOptions,
    ) -> Self {
        Self {
            datasets_store: Some(datasets_store),
            options,
        }
    }

    fn format(&self, options: ZarrOptions) -> ZarrFormat {
        ZarrFormat {
            options,
            output_dir: self
                .datasets_store
                .as_ref()
                .map(|store| store.storage().tmp_dir.clone()),
        }
    }
}

impl GetExt for ZarrFormatFactory {
    fn get_ext(&self) -> String {
        "zarr".to_string()
    }
}

impl FileFormatFactory for ZarrFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        format_options: &std::collections::HashMap<String, String>,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        // Per-table overrides from `CREATE EXTERNAL TABLE ... OPTIONS (...)`.
        let comma_separated = |value: &String| -> Vec<String> {
            value
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        };

        let mut options = self.options.clone();
        if let Some(value) = format_options.get("read_dimensions") {
            options.read_dimensions = Some(comma_separated(value));
        }
        if let Some(value) = format_options.get("write_dimensions") {
            options.write_dimensions = Some(comma_separated(value));
        }
        Ok(Arc::new(self.format(options)))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(self.format(self.options.clone()))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl FileFormatFactoryExt for ZarrFormatFactory {
    fn discover_datasets(
        &self,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<Vec<DatasetMetadata>> {
        let datasets: Vec<ObjectMeta> = objects
            .iter()
            .filter(|obj| is_zarr_v3_metadata(obj))
            .cloned()
            .collect();

        let top_level_datasets = top_level_zarr_meta_v3(&datasets);
        let zarr_paths: Vec<ZarrPath> = top_level_datasets
            .into_iter()
            .filter_map(|path| match ZarrPath::new_from_object_meta(path) {
                Ok(zarr_path) => Some(zarr_path),
                Err(e) => {
                    tracing::trace!(error = %e, "skipping non-Zarr object during dataset discovery");
                    None
                }
            })
            .collect();

        let datasets: Vec<DatasetMetadata> = zarr_paths
            .into_iter()
            .map(|path| DatasetMetadata::new(path.as_zarr_json_path(), self.get_ext()))
            .collect();
        Ok(datasets)
    }

    fn file_format_name(&self) -> String {
        self.get_ext()
    }
}

// ─── Format ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
pub struct ZarrFormat {
    /// Read and write behaviour for this format.
    pub options: ZarrOptions,
    /// Local directory zarr stores are written into — the datasets store's tmp
    /// root. `None` on read-only formats, which reject writes.
    pub output_dir: Option<std::path::PathBuf>,
}

impl ZarrFormat {
    /// Build a read-only format that reads only the variables belonging to
    /// `read_dimensions` (or auto-selects a default when `None`).
    pub fn new(read_dimensions: Option<Vec<String>>) -> Self {
        Self {
            options: ZarrOptions {
                read_dimensions,
                ..Default::default()
            },
            output_dir: None,
        }
    }

    /// Explicit dimensions requested via `read_zarr(paths, ['dims'])` or a
    /// `CREATE EXTERNAL TABLE ... OPTIONS (read_dimensions '...')`.
    fn read_dimensions(&self) -> Option<Vec<String>> {
        self.options.read_dimensions.clone()
    }
}

#[async_trait::async_trait]
impl FileFormat for ZarrFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    fn get_ext(&self) -> String {
        "zarr.json".to_string()
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> datafusion::error::Result<String> {
        Ok("zarr.json".to_string())
    }

    async fn infer_schema(
        &self,
        _state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        // The listing may include non-metadata objects — chunk data files such as
        // `<array>/c/0/0/0` — when the table is created without a `zarr.json`
        // extension filter (e.g. via `read_zarr`). Select the top-level group
        // metadata files and ignore the rest rather than erroring on the first
        // chunk we encounter.
        let verified_objects = top_level_zarr_meta_v3(objects);
        if verified_objects.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(
                "No Zarr v3 metadata (zarr.json) found in the provided path(s)".to_string(),
            ));
        }
        let mut schemas = Vec::new();
        for object in verified_objects {
            let zarr_path = ZarrPath::new_from_object_meta(object.clone()).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to create ZarrPath from ObjectMeta at {}: {e}",
                    object.location
                ))
            })?;
            let zarr_store = Arc::new(AsyncObjectStore::new(store.clone()))
                as Arc<dyn AsyncReadableListableStorageTraits>;
            let group = Group::async_open(zarr_store, &zarr_path.as_zarr_path())
                .await
                .map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to open Zarr group at {}: {e}",
                        object.location
                    ))
                })?;

            let mut leaves = Vec::new();
            recursive_groups(Arc::new(group), &mut leaves)
                .await
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

            for leaf in leaves {
                let any = dataset_from_group(&leaf, None).await.map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to read Zarr group as dataset: {e}"
                    ))
                })?;

                // Apply explicit dimensions, or narrow to a broadcast-compatible
                // default so the inferred schema matches what the scan returns.
                let any = match resolve_read_dimensions(
                    &any,
                    self.read_dimensions(),
                    Some("read_zarr"),
                ) {
                    Some(dims) => any
                        .project(&DatasetProjection::new_with_dimension_projection(dims))
                        .map_err(|e| {
                            datafusion::error::DataFusionError::Execution(format!(
                                "Failed to project Zarr dataset with dimensions: {e}"
                            ))
                        })?,
                    None => any,
                };

                let schema = any_dataset_to_arrow_schema(&any).map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to derive Zarr Arrow schema: {e}"
                    ))
                })?;
                schemas.push(Arc::new(schema));
            }
        }

        if schemas.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(
                "No valid Zarr v3 groups found to infer schema".to_string(),
            ));
        }

        let super_schema = super_type_schema(&schemas).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to compute super schema for Zarr groups: {e}"
            ))
        })?;
        Ok(Arc::new(super_schema))
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> datafusion::error::Result<Statistics> {
        Ok(Statistics::new_unknown(&table_schema))
    }

    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let mut object_metas: Vec<ObjectMeta> = Vec::new();
        for group in &conf.file_groups {
            for file in group.files() {
                object_metas.push(file.object_meta.clone());
            }
        }
        let object_store = state
            .runtime_env()
            .object_store(conf.object_store_url.clone())?;

        let top_level_metas = top_level_zarr_meta_v3(&object_metas);
        let mut file_groups: Vec<FileGroup> = vec![];
        for meta in top_level_metas {
            let file = self
                .partition_zarr_group(&meta, object_store.clone())
                .await?;
            file_groups.push(file);
        }

        // The scan carries nd data as `beacon.nd`-encoded struct columns, so the
        // file source's schema is the encoded form of the logical table schema.
        // `NdSourceExec` decodes it and `NdBroadcastExec` broadcasts it back to
        // the logical schema above the scan.
        let encoded_file_schema =
            Arc::new(beacon_datafusion_ext::nd::encoded_schema(conf.file_schema()));
        let table_schema = datafusion::datasource::table_schema::TableSchema::new(
            encoded_file_schema,
            conf.table_partition_cols().clone(),
        );
        // Preserve a projection that the scan pushed down into the incoming
        // source — rebuilding the source below would otherwise drop it.
        let projection = conf.file_source().projection().cloned();
        let source = ZarrSource::new(table_schema)
            .with_read_dimensions(self.read_dimensions())
            .with_projection(projection);
        let conf = FileScanConfigBuilder::from(conf)
            .with_file_groups(file_groups)
            .with_source(Arc::new(source))
            .build();

        let data_source: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(conf);
        let nd_source =
            Arc::new(beacon_datafusion_ext::nd::exec::NdSourceExec::try_new(data_source)?);
        let broadcast = beacon_datafusion_ext::nd::exec::NdBroadcastExec::try_new(nd_source)?;
        Ok(Arc::new(broadcast))
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        conf: datafusion::datasource::physical_plan::FileSinkConfig,
        order_requirements: Option<datafusion::physical_expr::LexRequirement>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // A zarr store is a directory tree of chunk files, which `zarrs` writes
        // through a local filesystem store. Like NetCDF, we build it under the
        // configured tmp root rather than streaming to an object store.
        let output_dir = self.output_dir.clone().ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(
                "Zarr writing requires a datasets store; this format was registered read-only"
                    .to_string(),
            )
        })?;

        // `DataSinkExec` already requires a single input partition, so the
        // gridded sink sees every row without extra plan nodes.
        let sink: Arc<dyn datafusion::datasource::sink::DataSink> =
            match &self.options.write_dimensions {
                Some(dimension_columns) if !dimension_columns.is_empty() => Arc::new(
                    sink::ZarrNdSink::new(
                        conf,
                        self.options.clone(),
                        dimension_columns.clone(),
                        output_dir,
                    )?,
                ),
                _ => Arc::new(sink::ZarrSink::new(conf, self.options.clone(), output_dir)),
            };

        Ok(Arc::new(datafusion::datasource::sink::DataSinkExec::new(
            input,
            sink,
            order_requirements,
        )))
    }

    fn file_source(
        &self,
        table_schema: datafusion::datasource::table_schema::TableSchema,
    ) -> Arc<dyn FileSource> {
        Arc::new(ZarrSource::new(table_schema).with_read_dimensions(self.read_dimensions()))
    }
}

impl ZarrFormat {
    /// Expand a top-level zarr store into one [`PartitionedFile`] per leaf
    /// group, so nested sub-groups are scanned as independent partitions.
    async fn partition_zarr_group(
        &self,
        object: &ObjectMeta,
        object_store: Arc<dyn ObjectStore>,
    ) -> datafusion::error::Result<FileGroup> {
        let zarr_store = Arc::new(AsyncObjectStore::new(object_store))
            as Arc<dyn AsyncReadableListableStorageTraits>;
        let group_path = ZarrPath::new_from_object_meta(object.clone()).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to create ZarrPath from ObjectMeta at {}: {e}",
                object.location
            ))
        })?;
        let group = Group::async_open(zarr_store, &group_path.as_zarr_path())
            .await
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to open Zarr group at {}: {e}",
                    object.location
                ))
            })?;

        match find_partitioned_files(&group).await {
            Some(partition_groups) => {
                if partition_groups.is_empty() {
                    Ok(FileGroup::new(vec![PartitionedFile::new(
                        group_path.as_zarr_json_path(),
                        0,
                    )]))
                } else {
                    let mut files = Vec::new();
                    for group in partition_groups {
                        let partition_path = group.path().to_string();
                        let partitioned_file =
                            PartitionedFile::new(format!("{partition_path}/zarr.json"), 0);
                        files.push(partitioned_file);
                    }
                    Ok(FileGroup::new(files))
                }
            }
            None => Ok(FileGroup::new(vec![])),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, TimeUnit};
    use datafusion::datasource::file_format::FileFormat;
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    use datafusion::prelude::SessionContext;

    use super::{ZarrFormat, ZarrFormatFactory};

    /// Register the bundled `gridded-example.zarr` store as a DataFusion table
    /// backed by [`ZarrFormat`] + [`ListingTable`].
    async fn register_example(ctx: &SessionContext) {
        let store_dir = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_files/gridded-example.zarr/"
        );
        let table_path = ListingTableUrl::parse(format!("file://{store_dir}")).unwrap();

        let format: Arc<dyn FileFormat> = Arc::new(ZarrFormat::default());
        let listing_options = ListingOptions::new(format).with_file_extension("zarr.json");

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .infer_schema(&ctx.state())
            .await
            .unwrap();
        let table = ListingTable::try_new(config).unwrap();
        ctx.register_table("gridded", Arc::new(table)).unwrap();
    }

    /// A session with the nd projection-pushdown rule registered — the same
    /// wiring beacon-core installs, so a `SELECT`-with-computed-column plan gets
    /// the projection sunk below the broadcast.
    fn ctx_with_pushdown() -> SessionContext {
        use datafusion::execution::session_state::SessionStateBuilder;
        use datafusion::prelude::SessionConfig;

        // Single partition so row order is deterministic (the differential tests
        // compare results positionally).
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(1))
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(
                beacon_datafusion_ext::nd::NdProjectionPushdown::new(),
            ))
            .build();
        SessionContext::new_with_state(state)
    }

    /// End-to-end: with the rule registered, `SELECT lat * 2` plans with an
    /// `NdProjectionExec` *below* the `NdBroadcastExec`, and produces the same
    /// values as the unoptimized session.
    #[tokio::test]
    async fn projection_pushdown_fires_end_to_end() {
        use arrow::compute::concat_batches;
        use datafusion::physical_plan::displayable;

        let ctx = ctx_with_pushdown();
        register_example(&ctx).await;

        let df = ctx
            .sql("SELECT lat * 2 AS lat2 FROM gridded")
            .await
            .unwrap();
        let plan = df.clone().create_physical_plan().await.unwrap();
        let rendered = displayable(plan.as_ref()).indent(true).to_string();

        let broadcast = rendered.find("NdBroadcastExec");
        let projection = rendered.find("NdProjectionExec");
        let source = rendered.find("NdSourceExec");
        assert!(
            broadcast < projection && projection < source,
            "projection must be pushed below the broadcast:\n{rendered}"
        );

        // Same result as a session without the rule.
        let bare = SessionContext::new();
        register_example(&bare).await;
        let expected = bare
            .sql("SELECT lat * 2 AS lat2 FROM gridded")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let actual = df.collect().await.unwrap();

        let schema = actual[0].schema();
        assert_eq!(
            concat_batches(&schema, &actual).unwrap(),
            concat_batches(&schema, &expected).unwrap(),
        );
    }

    /// With the nd optimizer *off* (a plain session, as when
    /// `BEACON_ENABLE_ND_PIPELINE=false`), the base pipeline still works: the
    /// broadcast and source nodes are present, the projection simply stays above
    /// the broadcast, and results are correct.
    #[tokio::test]
    async fn base_pipeline_works_without_nd_optimizer() {
        use datafusion::physical_plan::displayable;

        let ctx = SessionContext::new();
        register_example(&ctx).await;

        let df = ctx
            .sql("SELECT lat * 2 AS lat2 FROM gridded")
            .await
            .unwrap();
        let plan = df.clone().create_physical_plan().await.unwrap();
        let rendered = displayable(plan.as_ref()).indent(true).to_string();

        // The base nd pipeline is always present…
        assert!(rendered.contains("NdBroadcastExec"), "{rendered}");
        assert!(rendered.contains("NdSourceExec"), "{rendered}");
        // …but without the rule the projection is not sunk below the broadcast.
        assert!(
            !rendered.contains("NdProjectionExec"),
            "projection must stay above the broadcast when the optimizer is off:\n{rendered}"
        );

        // It still executes and returns rows.
        let rows: usize = df
            .collect()
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert!(rows > 0, "base pipeline must still produce rows");
    }

    // ── nd projection pushdown: differential integration tests ───────────
    //
    // For each projection, plan it with the optimizer ON and assert the
    // projection sank below the broadcast, then execute it with the optimizer ON
    // and OFF and assert byte-identical results — using DataFusion's own
    // (post-broadcast) evaluation as the correctness oracle.

    /// Assert that `SELECT {select_exprs} FROM gridded` (a) pushes the projection
    /// below the broadcast and (b) yields identical rows with the optimizer on
    /// and off.
    async fn check_pushdown(select_exprs: &str) {
        use arrow::compute::concat_batches;
        use datafusion::physical_plan::displayable;
        use datafusion::prelude::SessionConfig;

        let shape_sql = format!("SELECT {select_exprs} FROM gridded");
        // Bounded so the differential comparison stays cheap; row order is
        // deterministic (single-file scan) and identical on both paths.
        let data_sql = format!("SELECT {select_exprs} FROM gridded LIMIT 200");

        // Optimizer ON: the projection must sink below the broadcast.
        let on = ctx_with_pushdown();
        register_example(&on).await;
        let plan = on
            .sql(&shape_sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let rendered = displayable(plan.as_ref()).indent(true).to_string();
        let broadcast = rendered.find("NdBroadcastExec");
        let projection = rendered.find("NdProjectionExec");
        let source = rendered.find("NdSourceExec");
        assert!(
            projection.is_some() && broadcast < projection && projection < source,
            "expected NdBroadcastExec → NdProjectionExec → NdSourceExec for `{shape_sql}`:\n{rendered}"
        );
        let actual = on.sql(&data_sql).await.unwrap().collect().await.unwrap();

        // Optimizer OFF: reference result (projection stays above the broadcast).
        // Same single-partition config so row order matches positionally.
        let off = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        register_example(&off).await;
        let expected = off.sql(&data_sql).await.unwrap().collect().await.unwrap();

        let schema = expected
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| actual[0].schema());
        assert_eq!(
            concat_batches(&schema, &actual).unwrap(),
            concat_batches(&schema, &expected).unwrap(),
            "results differ with/without the optimizer for `{data_sql}`"
        );
    }

    #[tokio::test]
    async fn pushdown_arithmetic_and_casts() {
        // Arithmetic with scalars, and two coordinates on different axes.
        check_pushdown("lat * 2 + 1 AS a, lon - 10 AS b, lat + lon AS s").await;
        // Casts to wider/narrower and to integer types.
        check_pushdown(
            "CAST(lat AS DOUBLE) AS a, CAST(analysed_sst AS INTEGER) AS b, \
             CAST(time AS BIGINT) AS t",
        )
        .await;
    }

    #[tokio::test]
    async fn pushdown_scalar_functions() {
        // Single-column functions on a coordinate and on a data variable.
        check_pushdown("abs(lat) AS a, floor(analysed_sst) AS b, round(analysed_sst) AS c").await;
        // Column + scalar function, and a function over two cross-axis columns.
        check_pushdown("power(lat, 2) AS p, abs(lat - lon) AS d").await;
    }

    #[tokio::test]
    async fn pushdown_booleans_and_case() {
        check_pushdown("lat > 40 AS hi, (lat > 40 AND lon > 30) AS both").await;
        check_pushdown("CASE WHEN lat > 40 THEN 1 ELSE 0 END AS c").await;
    }

    #[tokio::test]
    async fn pushdown_attributes_and_mixed() {
        // A string function over a rank-0 attribute, co-selected with a gridded
        // coordinate so the attribute broadcasts across the grid.
        check_pushdown("lat, upper(\"analysed_sst.units\") AS u").await;
        check_pushdown("lat, (\"analysed_sst.units\" = 'kelvin') AS is_k").await;
        // Mixed: passthrough columns + a computed column + an attribute function.
        check_pushdown("lat, lon, lat * 2 AS d, upper(\"analysed_sst.units\") AS u").await;
    }

    #[tokio::test]
    async fn pushdown_nested_expressions() {
        check_pushdown("CAST(round(analysed_sst) AS INTEGER) AS r").await;
        check_pushdown("abs(CAST(lat AS DOUBLE)) * 2 AS x").await;
    }

    #[tokio::test]
    async fn factory_discovers_gridded_example() {
        use beacon_datafusion_ext::format_ext::FileFormatFactoryExt;
        use object_store::{ObjectMeta, path::Path};

        let factory = ZarrFormatFactory::new();
        let objects = vec![
            ObjectMeta {
                location: Path::from("gridded-example.zarr/zarr.json"),
                last_modified: Default::default(),
                size: 0,
                e_tag: None,
                version: None,
            },
            // A nested array's metadata must NOT become its own dataset.
            ObjectMeta {
                location: Path::from("gridded-example.zarr/lat/zarr.json"),
                last_modified: Default::default(),
                size: 0,
                e_tag: None,
                version: None,
            },
        ];
        let datasets = factory.discover_datasets(&objects).unwrap();
        assert_eq!(datasets.len(), 1);
        assert!(
            datasets[0]
                .file_path
                .ends_with("gridded-example.zarr/zarr.json")
        );
        assert_eq!(datasets[0].format, "zarr");
    }

    #[tokio::test]
    async fn reads_gridded_example_through_datafusion() {
        let ctx = SessionContext::new();
        register_example(&ctx).await;

        let df = ctx
            .sql("SELECT analysed_sst, lat, lon, time FROM gridded LIMIT 10")
            .await
            .unwrap();

        // CF time must surface as a nanosecond timestamp through the plan.
        let time_field = df.schema().field_with_unqualified_name("time").unwrap();
        assert_eq!(
            time_field.data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );

        let batches = df.collect().await.unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 10, "LIMIT 10 should yield exactly 10 rows");
        assert_eq!(batches[0].num_columns(), 4);
    }

    /// An explicit `read_dimensions` projects the schema down to only the
    /// variables whose dimensions are a subset of those requested.
    #[tokio::test]
    async fn explicit_read_dimensions_limits_schema() {
        use datafusion::catalog::TableProvider;

        let ctx = SessionContext::new();
        let store_dir = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_files/gridded-example.zarr/"
        );
        let table_path = ListingTableUrl::parse(format!("file://{store_dir}")).unwrap();

        let format: Arc<dyn FileFormat> = Arc::new(ZarrFormat::new(Some(vec!["time".to_string()])));
        let listing_options = ListingOptions::new(format).with_file_extension("zarr.json");
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .infer_schema(&ctx.state())
            .await
            .unwrap();
        let table = ListingTable::try_new(config).unwrap();
        ctx.register_table("gridded_time", Arc::new(table)).unwrap();

        let provider = ctx.table_provider("gridded_time").await.unwrap();
        let names: Vec<String> = provider
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert!(names.contains(&"time".to_string()), "time present: {names:?}");
        assert!(
            !names.contains(&"analysed_sst".to_string()),
            "analysed_sst depends on lat/lon and must be excluded: {names:?}"
        );
        assert!(
            !names.contains(&"lat".to_string()),
            "lat is on a different dimension and must be excluded: {names:?}"
        );

        // The narrowed scan still executes and returns the time column.
        let rows: usize = ctx
            .sql("SELECT time FROM gridded_time LIMIT 5")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert!(rows > 0, "should read some time rows");
    }

    #[tokio::test]
    async fn schema_includes_arrays_and_attributes() {
        use datafusion::catalog::TableProvider;

        let ctx = SessionContext::new();
        register_example(&ctx).await;

        let provider = ctx.table_provider("gridded").await.unwrap();
        let schema = provider.schema();
        let dtype = |name: &str| {
            schema
                .field_with_name(name)
                .unwrap_or_else(|_| panic!("missing field '{name}' in schema"))
                .data_type()
                .clone()
        };

        // Coordinate + data variables, with CF decoding reflected in the types.
        assert_eq!(dtype("lat"), DataType::Float32);
        assert_eq!(dtype("lon"), DataType::Float32);
        // `time` is int32 "seconds since 1981-01-01" → CF time.
        assert_eq!(
            dtype("time"),
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        // `analysed_sst` is int16 with scale_factor/add_offset → decoded f64.
        assert_eq!(dtype("analysed_sst"), DataType::Float64);

        // Global (group) attributes are surfaced as ".<attr>" columns.
        assert_eq!(dtype(".Conventions"), DataType::Utf8);
        assert_eq!(dtype(".title"), DataType::Utf8);

        // Per-array attributes are surfaced as "<array>.<attr>" columns.
        assert_eq!(dtype("lat.units"), DataType::Utf8);
        assert_eq!(dtype("analysed_sst.units"), DataType::Utf8);
        assert_eq!(dtype("analysed_sst.scale_factor"), DataType::Float64);
        assert_eq!(dtype("analysed_sst.add_offset"), DataType::Float64);
    }

    #[tokio::test]
    async fn filter_pushdown_prunes_through_datafusion() {
        let ctx = SessionContext::new();
        register_example(&ctx).await;

        // An out-of-range latitude predicate prunes every chunk: the scan
        // should return no rows.
        let batches = ctx
            .sql("SELECT analysed_sst, lat FROM gridded WHERE lat > 100000")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 0, "impossible lat predicate should prune all rows");
    }

    #[tokio::test]
    async fn predicate_pushdown_selects_subset_through_datafusion() {
        use arrow::array::{Float32Array, Int64Array};

        let ctx = SessionContext::new();
        register_example(&ctx).await;

        // Discover the latitude range, then filter on its midpoint so the
        // predicate is guaranteed to keep some — but not all — rows.
        let stats = ctx
            .sql("SELECT min(lat) AS mn, max(lat) AS mx, count(*) AS n FROM gridded")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let row = &stats[0];
        let f32_at = |i: usize| {
            row.column(i)
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(0)
        };
        let (mn, mx) = (f32_at(0), f32_at(1));
        let total = row
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert!(mx > mn, "lat must span a range");
        let mid = mn + (mx - mn) / 2.0;

        let batches = ctx
            .sql(&format!("SELECT lat FROM gridded WHERE lat > {mid}"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut kept = 0i64;
        for b in &batches {
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap();
            for i in 0..col.len() {
                assert!(col.value(i) > mid, "every returned lat must satisfy the predicate");
            }
            kept += b.num_rows() as i64;
        }
        assert!(kept > 0, "midpoint predicate should keep some rows");
        assert!(kept < total, "midpoint predicate should drop some rows");
    }

    // ── nd pipeline: plan shape + variables & attributes end-to-end ──────

    /// The physical plan is the nd spine over the standard file scan:
    /// `NdBroadcastExec` → `NdSourceExec` → `DataSourceExec`, in that nesting
    /// order (parent above child in the indented render).
    #[tokio::test]
    async fn physical_plan_is_nd_spine_over_scan() {
        use datafusion::physical_plan::displayable;

        let ctx = SessionContext::new();
        register_example(&ctx).await;

        let plan = ctx
            .sql("SELECT analysed_sst FROM gridded")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let rendered = displayable(plan.as_ref()).indent(true).to_string();

        let broadcast = rendered.find("NdBroadcastExec");
        let source = rendered.find("NdSourceExec");
        let scan = rendered.find("DataSourceExec");
        assert!(
            broadcast.is_some() && source.is_some() && scan.is_some(),
            "plan must contain the nd spine over a DataSourceExec:\n{rendered}"
        );
        assert!(
            broadcast < source && source < scan,
            "expected NdBroadcastExec → NdSourceExec → DataSourceExec nesting:\n{rendered}"
        );
    }

    /// End-to-end through DataFusion: a gridded data variable comes back decoded
    /// (scale/offset applied → Float64), and its rank-0 attributes — a variable
    /// attribute (`analysed_sst.units`) and a global attribute (`.Conventions`) —
    /// ride the `beacon.nd` encoding as constant columns on every row.
    #[tokio::test]
    async fn end_to_end_reads_variable_with_attributes() {
        use arrow::array::StringArray;

        let ctx = SessionContext::new();
        register_example(&ctx).await;

        let batches = ctx
            .sql(
                r#"SELECT analysed_sst,
                          "analysed_sst.units" AS units,
                          ".Conventions"       AS conventions
                   FROM gridded LIMIT 4"#,
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 4, "LIMIT 4 should yield exactly 4 rows");

        let batch = &batches[0];
        assert_eq!(
            batch.column_by_name("analysed_sst").unwrap().data_type(),
            &DataType::Float64
        );

        let units = batch
            .column_by_name("units")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let conventions = batch
            .column_by_name("conventions")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            assert_eq!(units.value(i), "kelvin", "variable attribute must be constant");
            assert_eq!(conventions.value(i), "CF-1.4", "global attribute must be constant");
        }
    }

    /// Co-selected with a gridded variable (`lat`, which establishes the
    /// broadcast target), a rank-0 attribute is present on every grid row and
    /// has exactly one distinct value across all of them. Projecting to only the
    /// scalar attribute would collapse the grid to a single row.
    #[tokio::test]
    async fn attribute_is_single_distinct_value_across_grid() {
        use arrow::array::Int64Array;

        let ctx = SessionContext::new();
        register_example(&ctx).await;

        let batches = ctx
            .sql(
                r#"SELECT COUNT(DISTINCT "analysed_sst.units") AS distinct_units,
                          COUNT("analysed_sst.units")          AS attr_rows,
                          COUNT(lat)                           AS grid_rows
                   FROM gridded"#,
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let int = |name: &str| {
            batches[0]
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        };
        assert_eq!(int("distinct_units"), 1, "attribute must be a single constant");
        assert!(int("grid_rows") > 1, "gridded variable must define a multi-row grid");
        assert_eq!(
            int("attr_rows"),
            int("grid_rows"),
            "attribute must be broadcast (non-null) onto every grid row"
        );
    }
}

pub mod table_function;
pub use table_function::ReadZarrFunc;
