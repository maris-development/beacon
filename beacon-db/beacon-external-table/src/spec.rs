//! The resolved inputs of an external table, and the listing table they build.

use std::{collections::HashMap, sync::Arc};

use crate::ExternalTableDefinition;
use beacon_datafusion_ext::{
    format_ext::{SchemaUnit, try_file_format_factory_ext},
    type_widening::{ArrowTypeWidening, ArrowTypeWideningStrategy, LabeledSchema},
};
use datafusion::{
    arrow::datatypes::{DataType, Schema, SchemaRef},
    catalog::Session,
    common::{Constraints, Result, arrow_datafusion_err, exec_datafusion_err},
    datasource::{
        file_format::FileFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
    execution::SessionState,
    prelude::Expr,
};
use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectMeta, ObjectStore};

/// Partition columns as `(name, data type)` pairs.
pub(crate) type PartitionCols = Vec<(String, DataType)>;

/// What an external table lists and how it reads, resolved once at build time.
///
/// The spec holds no schema. The schema is in the [`ExternalTableDefinition`].
/// A refresh builds a new listing table from this spec and that definition.
#[derive(Clone, Debug)]
pub struct ExternalTableSpec {
    /// The table location. It carries the table name, which scopes the runtime list cache.
    pub url: ListingTableUrl,
    /// The format, the partition columns and the sort order.
    pub options: ListingOptions,
    pub constraints: Constraints,
    pub column_defaults: HashMap<String, Expr>,
    pub definition_sql: Option<String>,
}

impl ExternalTableSpec {
    /// Builds a listing table for `definition` and returns it with the schema to store.
    ///
    /// With `infer`, the file schema comes from the current files. Without it,
    /// the file schema is the definition schema minus the partition columns, and
    /// no file is read. The schema to store is the table schema. It stays empty
    /// when the inference found no files, so the next build infers again.
    ///
    /// # Errors
    ///
    /// Returns the inference error, or an error when the definition schema does
    /// not hold the partition columns.
    pub async fn build_table(
        &self,
        session: &dyn Session,
        definition: &ExternalTableDefinition,
        infer: bool,
    ) -> Result<(ListingTable, SchemaRef)> {
        let file_schema = if infer {
            self.infer_schema(
                session,
                &definition.type_widening,
                definition.parallel_schema_inference,
                definition.skip_invalid_files_on_inference,
            )
                .await?
        } else {
            project_out_partition_columns(&definition.schema, &definition.partition_cols)?
        };
        let table = self.listing_table(session, Arc::clone(&file_schema))?;
        let stored = if file_schema.fields().is_empty() {
            file_schema
        } else {
            datafusion::catalog::TableProvider::schema(&table)
        };
        Ok((table, stored))
    }

    /// Builds a listing table with `file_schema` over the files of the location.
    fn listing_table(&self, session: &dyn Session, file_schema: SchemaRef) -> Result<ListingTable> {
        let config = ListingTableConfig::new(self.url.clone())
            .with_listing_options(self.options.clone())
            .with_schema(file_schema);

        Ok(ListingTable::try_new(config)?
            .with_cache(
                session
                    .runtime_env()
                    .cache_manager
                    .get_file_statistic_cache(),
            )
            .with_definition(self.definition_sql.clone())
            .with_constraints(self.constraints.clone())
            .with_column_defaults(self.column_defaults.clone()))
    }

    /// Infers the file schema from the current files and merges it with `type_widening`.
    ///
    /// The format infers one schema per unit: a file, or for Zarr and Atlas a
    /// store marker. The merge takes the units in path order, so an order
    /// dependent strategy gives the same schema on each build.
    ///
    /// A location with no files gives an empty schema. A table whose files are
    /// all deleted then reports an empty schema and does not fail.
    ///
    /// # Errors
    ///
    /// Returns the inference error when the format cannot read a file, or the
    /// merge error when `type_widening` refuses a column. With
    /// `skip_invalid_files`, a file that the format cannot read is skipped, and
    /// only a location where no file is readable returns the inference error.
    async fn infer_schema(
        &self,
        session: &dyn Session,
        type_widening: &Arc<dyn ArrowTypeWideningStrategy>,
        parallel: bool,
        skip_invalid_files: bool,
    ) -> Result<SchemaRef> {
        let objects = self.list_objects(session).await?;
        if objects.is_empty() {
            tracing::warn!(url = %self.url, "no files to infer the external table schema from; using an empty schema");
            return Ok(Arc::new(Schema::empty()));
        }
        let units = self.schema_units(session, &objects);
        if units.is_empty() {
            return Ok(Arc::new(Schema::empty()));
        }
        // Owned objects: a borrow in the closure fails the `Send` check of the async trait callers.
        let sources: Vec<ObjectMeta> = units
            .iter()
            .map(|unit| objects[unit.source].clone())
            .collect();
        let parallel_session = if parallel {
            let state = session.as_any().downcast_ref::<SessionState>();
            if state.is_none() {
                tracing::debug!("parallel schema inference needs a SessionState; inferring concurrently");
            }
            state.map(|state| Arc::new(state.clone()))
        } else {
            None
        };
        let inferred = match parallel_session {
            Some(state) => self.infer_units_parallel(state, sources).await?,
            None => self.infer_units(session, sources).await?,
        };
        let schemas = if skip_invalid_files {
            readable_schemas(inferred)?
        } else {
            inferred.into_iter().collect::<Result<Vec<_>>>()?
        };
        ArrowTypeWidening::new(Arc::clone(type_widening))
            .merge_schemas(&schemas)
            .map_err(|error| arrow_datafusion_err!(error))
    }

    /// The non-empty objects at the location, in path order.
    async fn list_objects(&self, session: &dyn Session) -> Result<Vec<ObjectMeta>> {
        let store = session.runtime_env().object_store(&self.url)?;
        let mut objects: Vec<ObjectMeta> = self
            .url
            .list_all_files(session, store.as_ref(), &self.options.file_extension)
            .await?
            // An empty file holds no schema, and a format can fail on it.
            .try_filter(|object| futures::future::ready(object.size > 0))
            .try_collect()
            .await?;
        objects.sort_by(|a, b| a.location.cmp(&b.location));
        Ok(objects)
    }

    /// The objects that the format infers a schema from: each file, or each store marker.
    fn schema_units(&self, session: &dyn Session, objects: &[ObjectMeta]) -> Vec<SchemaUnit> {
        match try_file_format_factory_ext(session, &self.options.format.get_ext()) {
            Some(factory) => factory.schema_units(objects),
            None => (0..objects.len()).map(SchemaUnit::from_file).collect(),
        }
    }

    /// One schema per source, in source order, each labeled with its object path.
    ///
    /// One task polls all inferences: the reads overlap, the CPU work does not.
    async fn infer_units(
        &self,
        session: &dyn Session,
        sources: Vec<ObjectMeta>,
    ) -> Result<Vec<Result<LabeledSchema>>> {
        let store = session.runtime_env().object_store(&self.url)?;
        Ok(futures::stream::iter(sources)
            .map(|object| {
                let store = Arc::clone(&store);
                let format = Arc::clone(&self.options.format);
                async move { infer_one(session, &format, &store, object).await }
            })
            .buffered(inference_width(session))
            .collect()
            .await)
    }

    /// One schema per source, in source order, each inferred on its own Tokio task.
    ///
    /// The tasks can run on all worker threads, so the CPU work also runs in parallel.
    /// A spawned task must own its data, so the caller clones the session state once.
    async fn infer_units_parallel(
        &self,
        state: Arc<SessionState>,
        sources: Vec<ObjectMeta>,
    ) -> Result<Vec<Result<LabeledSchema>>> {
        let store = state.runtime_env().object_store(&self.url)?;
        let width = inference_width(state.as_ref());
        Ok(futures::stream::iter(sources)
            .map(|object| {
                let state = Arc::clone(&state);
                let store = Arc::clone(&store);
                let format = Arc::clone(&self.options.format);
                tokio::spawn(async move { infer_one(state.as_ref(), &format, &store, object).await })
            })
            // `buffered` keeps the source order, so the merge order does not change.
            .buffered(width)
            .map(|joined| {
                joined.unwrap_or_else(|error| {
                    Err(exec_datafusion_err!("a schema inference task failed: {error}"))
                })
            })
            .collect()
            .await)
    }
}

/// How many inferences run at the same time.
fn inference_width(session: &dyn Session) -> usize {
    session
        .config_options()
        .execution
        .meta_fetch_concurrency
        .max(1)
}

/// The schema of one source object, labeled with its path.
async fn infer_one(
    session: &dyn Session,
    format: &Arc<dyn FileFormat>,
    store: &Arc<dyn ObjectStore>,
    object: ObjectMeta,
) -> Result<LabeledSchema> {
    let schema = format
        .infer_schema(session, store, std::slice::from_ref(&object))
        .await
        .map_err(|error| error.context(format!("cannot infer the schema of '{}'", object.location)))?;
    Ok(LabeledSchema::new(schema, object.location.as_ref()))
}

/// The schemas of the readable files. Each failed file logs a warning.
///
/// # Errors
///
/// Returns the first inference error when no file is readable.
fn readable_schemas(inferred: Vec<Result<LabeledSchema>>) -> Result<Vec<LabeledSchema>> {
    let mut schemas = Vec::with_capacity(inferred.len());
    let mut first_error = None;
    let mut skipped = 0usize;
    for result in inferred {
        match result {
            Ok(schema) => schemas.push(schema),
            Err(error) => {
                tracing::warn!(%error, "skipping a file that schema inference cannot read");
                skipped += 1;
                first_error.get_or_insert(error);
            }
        }
    }
    match first_error {
        Some(error) if schemas.is_empty() => Err(error.context(format!(
            "schema inference cannot read any of the {skipped} files"
        ))),
        _ => Ok(schemas),
    }
}

/// Splits a declared schema into the file schema and the partition columns.
///
/// An empty schema returns `None`: the caller infers the file schema. Its
/// partition columns then get the DataFusion default type, `Dictionary(UInt16, Utf8)`.
pub(crate) fn resolve_schema_and_partition_cols(
    schema: &SchemaRef,
    partition_cols: &[String],
) -> Result<(Option<SchemaRef>, PartitionCols)> {
    if schema.fields().is_empty() {
        return Ok((None, dictionary_partition_cols(partition_cols)));
    }
    let typed = partition_cols_from_schema(schema, partition_cols)?;
    let file_schema = project_out_partition_columns(schema, partition_cols)?;
    Ok((Some(file_schema), typed))
}

fn dictionary_partition_cols(partition_cols: &[String]) -> PartitionCols {
    partition_cols
        .iter()
        .map(|name| {
            (
                name.clone(),
                DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
            )
        })
        .collect()
}

fn partition_cols_from_schema(
    schema: &SchemaRef,
    partition_cols: &[String],
) -> Result<PartitionCols> {
    partition_cols
        .iter()
        .map(|name| {
            schema
                .field_with_name(name)
                .map(|field| (field.name().to_owned(), field.data_type().to_owned()))
                .map_err(|e| arrow_datafusion_err!(e))
        })
        .collect()
}

fn project_out_partition_columns(
    schema: &SchemaRef,
    partition_cols: &[String],
) -> Result<SchemaRef> {
    let kept: Vec<usize> = (0..schema.fields().len())
        .filter(|&i| !partition_cols.contains(schema.field(i).name()))
        .collect();
    Ok(Arc::new(schema.project(&kept)?))
}

/// Adds the default file glob to a folder location that has no glob.
pub(crate) fn with_default_glob(
    url: ListingTableUrl,
    options: &ListingOptions,
    file_type: &str,
) -> Result<ListingTableUrl> {
    if url.is_folder() && url.get_glob().is_none() {
        return url.with_glob(&default_file_glob(options, file_type));
    }
    Ok(url)
}

/// The glob from the compressed format extension, else from the file type.
fn default_file_glob(options: &ListingOptions, file_type: &str) -> String {
    options
        .format
        .compression_type()
        .and_then(|compression| options.format.get_ext_with_compression(&compression).ok())
        .map(|ext| format!("*.{ext}"))
        .unwrap_or_else(|| fallback_file_glob(file_type))
}

/// The glob from the file type. It matches files in all subfolders.
fn fallback_file_glob(file_type: &str) -> String {
    format!("**/*.{}", file_type.to_lowercase())
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::Field;

    use super::*;

    fn schema(fields: &[(&str, DataType)]) -> SchemaRef {
        Arc::new(Schema::new(
            fields
                .iter()
                .map(|(name, data_type)| Field::new(*name, data_type.clone(), true))
                .collect::<Vec<_>>(),
        ))
    }

    fn dictionary() -> DataType {
        DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8))
    }

    #[test]
    fn an_empty_schema_defers_inference_and_dictionary_encodes_partitions() {
        let (resolved, partition_cols) =
            resolve_schema_and_partition_cols(&Arc::new(Schema::empty()), &["year".to_string()])
                .unwrap();
        assert!(resolved.is_none());
        assert_eq!(partition_cols, vec![("year".to_string(), dictionary())]);
    }

    #[test]
    fn an_explicit_schema_projects_out_partition_columns() {
        let declared = schema(&[("value", DataType::Int32), ("year", DataType::Utf8)]);
        let (resolved, partition_cols) =
            resolve_schema_and_partition_cols(&declared, &["year".to_string()]).unwrap();

        let resolved = resolved.unwrap();
        assert_eq!(resolved.fields().len(), 1);
        assert_eq!(resolved.field(0).name(), "value");
        assert_eq!(partition_cols, vec![("year".to_string(), DataType::Utf8)]);
    }

    #[test]
    fn an_explicit_schema_without_partitions_is_kept() {
        let declared = schema(&[("value", DataType::Int32), ("year", DataType::Utf8)]);
        let (resolved, partition_cols) = resolve_schema_and_partition_cols(&declared, &[]).unwrap();
        assert_eq!(resolved.unwrap().as_ref(), declared.as_ref());
        assert!(partition_cols.is_empty());
    }

    #[test]
    fn a_partition_column_missing_from_the_schema_errors() {
        let declared = schema(&[("value", DataType::Int32)]);
        assert!(resolve_schema_and_partition_cols(&declared, &["year".to_string()]).is_err());
    }

    fn csv_spec(dir: &std::path::Path, files: &[(&str, &str)]) -> ExternalTableSpec {
        for (name, content) in files {
            std::fs::write(dir.join(name), content).unwrap();
        }
        let format =
            datafusion::datasource::file_format::csv::CsvFormat::default().with_has_header(true);
        ExternalTableSpec {
            url: ListingTableUrl::parse(format!("{}/", dir.display())).unwrap(),
            options: ListingOptions::new(Arc::new(format)).with_file_extension(".csv"),
            constraints: Constraints::default(),
            column_defaults: HashMap::new(),
            definition_sql: None,
        }
    }

    async fn infer(
        files: &[(&str, &str)],
        strategy: impl ArrowTypeWideningStrategy + 'static,
    ) -> Result<SchemaRef> {
        infer_with(files, strategy, false).await
    }

    async fn infer_with(
        files: &[(&str, &str)],
        strategy: impl ArrowTypeWideningStrategy + 'static,
        parallel: bool,
    ) -> Result<SchemaRef> {
        infer_options(files, strategy, parallel, false).await
    }

    async fn infer_options(
        files: &[(&str, &str)],
        strategy: impl ArrowTypeWideningStrategy + 'static,
        parallel: bool,
        skip_invalid_files: bool,
    ) -> Result<SchemaRef> {
        let dir = tempfile::tempdir().unwrap();
        let spec = csv_spec(dir.path(), files);
        let session = datafusion::prelude::SessionContext::new().state();
        let strategy: Arc<dyn ArrowTypeWideningStrategy> = Arc::new(strategy);
        spec
            .infer_schema(&session, &strategy, parallel, skip_invalid_files)
            .await
    }

    use beacon_datafusion_ext::type_widening::DefaultArrowTypeWidening;

    #[tokio::test]
    async fn inference_widens_a_column_over_the_files() {
        let schema = infer(
            &[("a.csv", "temp\n1\n"), ("b.csv", "temp\n1.5\n")],
            DefaultArrowTypeWidening::new(),
        )
        .await
        .unwrap();
        assert_eq!(
            schema.field_with_name("temp").unwrap().data_type(),
            &DataType::Float64
        );
    }

    #[tokio::test]
    async fn the_table_strategy_decides_a_type_conflict() {
        let files = [("a.csv", "temp\n1\n"), ("b.csv", "temp\nabc\n")];

        let refused = infer(&files, DefaultArrowTypeWidening::new()).await;
        assert!(refused.is_err(), "the fail rule refuses Int64 and Utf8");

        // Path order puts `a.csv` first, so its type stays.
        let kept = infer(&files, DefaultArrowTypeWidening::keeping_first_type())
            .await
            .unwrap();
        assert_eq!(
            kept.field_with_name("temp").unwrap().data_type(),
            &DataType::Int64
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn parallel_inference_gives_the_same_schema_in_the_same_order() {
        let files: Vec<(String, String)> = (0..64)
            .map(|i| {
                let value = if i == 0 { "1".to_string() } else { format!("{i}.5") };
                (format!("f{i:02}.csv"), format!("temp\n{value}\n"))
            })
            .chain(std::iter::once(("zz.csv".to_string(), "temp\nabc\n".to_string())))
            .collect();
        let files: Vec<(&str, &str)> = files
            .iter()
            .map(|(name, content)| (name.as_str(), content.as_str()))
            .collect();

        let concurrent = infer_with(&files, DefaultArrowTypeWidening::keeping_first_type(), false)
            .await
            .unwrap();
        let parallel = infer_with(&files, DefaultArrowTypeWidening::keeping_first_type(), true)
            .await
            .unwrap();
        assert_eq!(concurrent, parallel);
        // `f00.csv` is first in path order, so `keep_first` keeps a number type.
        assert_eq!(parallel.field_with_name("temp").unwrap().data_type(), &DataType::Float64);
    }

    /// A CSV file whose rows have another field count than its header.
    const BROKEN_CSV: &str = "temp,depth\n1\n2,3,4\n";

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn inference_skips_an_unreadable_file_only_with_the_option() {
        let files = [("a.csv", "temp\n1.5\n"), ("b.csv", BROKEN_CSV)];

        let failed = infer_options(&files, DefaultArrowTypeWidening::new(), false, false).await;
        let message = failed.unwrap_err().to_string();
        assert!(message.contains("b.csv"), "the error names the file: {message}");

        for parallel in [false, true] {
            let schema = infer_options(&files, DefaultArrowTypeWidening::new(), parallel, true)
                .await
                .unwrap();
            assert_eq!(schema.fields().len(), 1, "only `a.csv` is merged");
            assert_eq!(schema.field(0).data_type(), &DataType::Float64);
        }
    }

    #[tokio::test]
    async fn inference_fails_when_no_file_is_readable() {
        let files = [("a.csv", BROKEN_CSV)];
        assert!(
            infer_options(&files, DefaultArrowTypeWidening::new(), false, true)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn a_location_without_files_gives_an_empty_schema() {
        let schema = infer(&[], DefaultArrowTypeWidening::new()).await.unwrap();
        assert!(schema.fields().is_empty());
    }

    #[test]
    fn fallback_file_glob_is_recursive_and_lowercased() {
        assert_eq!(fallback_file_glob("ATLAS"), "**/*.atlas");
        assert_eq!(fallback_file_glob("ParQuet"), "**/*.parquet");
    }
}
