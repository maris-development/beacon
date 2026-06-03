//! The persisted [`ExternalTableDefinition`] and its provider construction.

use std::collections::HashMap;
use std::sync::Arc;

use beacon_common::listing_url::parse_listing_table_url;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::{Constraints, config_datafusion_err};
use datafusion::datasource::listing::ListingOptions;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
use object_store::path::Path as ObjectPath;

use super::definition::TableDefinition;
use super::events::datasets_store_subscriptions;
use super::external_table::ExternalTable;
use super::listing::{
    ExternalTableRebuild, build_listing_table, maybe_apply_default_glob,
    resolve_schema_and_partition_cols,
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
/// Persisted configuration for an External Table.
pub struct ExternalTableDefinition {
    /// Logical table name.
    pub name: String,
    /// Data location, relative to the object-store base URL, optionally with a glob.
    pub location: String,
    /// File format identifier, such as `parquet`, `csv`, or `json`.
    pub file_type: String,
    /// Optional explicit schema; empty means infer from files.
    pub schema: SchemaRef,
    /// Optional SQL text associated with the table definition.
    pub definition: Option<String>,
    /// Partition column names encoded in folder paths.
    pub partition_cols: Vec<String>,
    /// Additional file-format specific options.
    pub options: std::collections::HashMap<String, String>,
    /// If true, creation should no-op when the target table already exists.
    pub if_not_exists: bool,
}

#[async_trait::async_trait]
#[typetag::serde(name = "listing_table")]
impl TableDefinition for ExternalTableDefinition {
    /// Builds a [`ListingTable`]-backed provider from the persisted listing-table definition.
    async fn build_provider(
        &self,
        context: Arc<SessionContext>,
        data_store_url: &ObjectStoreUrl,
    ) -> anyhow::Result<Arc<dyn TableProvider>> {
        let session_state = context.state();
        let file_format_factory = session_state
            .get_file_format_factory(self.file_type.as_str())
            .ok_or(config_datafusion_err!(
                "Unable to create table with format {}! Could not find FileFormat.",
                self.file_type
            ))?;

        let file_format = file_format_factory.create(&session_state, &self.options)?;

        let (provided_schema, table_partition_cols) =
            resolve_schema_and_partition_cols(&self.schema, &self.partition_cols)?;

        let mut listing_table_url = parse_listing_table_url(data_store_url, &self.location)?;

        let options = ListingOptions::new(file_format)
            .with_file_extension("") // file extension is not needed for listing table factory since the file format will handle it in `infer_schema` and `infer_partition_schema`
            .with_session_config_options(session_state.config())
            .with_table_partition_cols(table_partition_cols);

        options
            .validate_partitions(&session_state, &listing_table_url)
            .await?;

        // When inferring, apply the default glob now so the rebuild spec lists
        // files identically on every subsequent refresh.
        if provided_schema.is_none() {
            listing_table_url =
                maybe_apply_default_glob(listing_table_url, &options, &self.file_type)?;
        }

        let rebuild = ExternalTableRebuild {
            listing_table_url,
            options,
            provided_schema,
            constraints: Constraints::default(),
            column_defaults: HashMap::new(),
            definition_sql: self.definition.clone(),
        };

        let initial = build_listing_table(&session_state, &rebuild).await?;

        // Subscribe per storage prefix; the store filters events to each prefix.
        let prefixes: Vec<ObjectPath> = initial
            .table_paths()
            .iter()
            .map(|url| url.prefix().clone())
            .collect();
        let event_receivers = datasets_store_subscriptions(data_store_url, &prefixes).await;

        Ok(Arc::new(ExternalTable::new_self_refreshing(
            self.clone(),
            initial,
            rebuild,
            Arc::downgrade(&context),
            event_receivers,
        )))
    }

    fn table_name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::ExternalTableDefinition;
    use crate::table_ext::TableDefinition;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::prelude::SessionContext;
    use std::collections::HashMap;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    /// Creates a unique temporary directory for table fixture data.
    fn create_temp_dir(prefix: &str) -> PathBuf {
        let mut dir = std::env::temp_dir();
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        dir.push(format!("{prefix}_{}_{}", std::process::id(), ts));
        fs::create_dir_all(&dir).expect("temporary directory should be created");
        dir
    }

    /// Converts an absolute local path into a location relative to `file://` root.
    fn to_store_relative_location(path: &std::path::Path) -> String {
        path.to_string_lossy().trim_start_matches('/').to_string()
    }

    #[tokio::test]
    /// Verifies listing providers can infer schema from folder-backed files.
    async fn listing_table_definition_build_provider_infers_schema_from_folder() {
        let root = create_temp_dir("table_ext_listing_infer");
        let data_file = root.join("part-0.csv");
        fs::write(&data_file, "1\n2\n").expect("csv fixture should be written");

        let definition = ExternalTableDefinition {
            name: "t_csv".to_string(),
            location: format!("{}/", to_store_relative_location(&root)),
            file_type: "csv".to_string(),
            schema: Arc::new(Schema::empty()),
            definition: Some("SELECT value FROM t_csv".to_string()),
            partition_cols: vec![],
            options: HashMap::new(),
            if_not_exists: false,
        };

        let context = Arc::new(SessionContext::new());
        let store_url = ObjectStoreUrl::parse("file://").unwrap();

        let provider = definition
            .build_provider(context, &store_url)
            .await
            .expect("listing provider should be built from inferred schema");

        let schema = provider.schema();
        assert_eq!(schema.fields().len(), 1);
        assert!(!schema.field(0).name().is_empty());

        fs::remove_dir_all(&root).expect("temporary directory should be cleaned up");
    }

    #[tokio::test]
    /// Verifies explicit schemas and partition metadata produce a valid provider schema.
    async fn listing_table_definition_build_provider_projects_partition_columns() {
        let root = create_temp_dir("table_ext_listing_partition");
        let partitioned = root.join("year=2026");
        fs::create_dir_all(&partitioned).expect("partition folder should be created");
        fs::write(partitioned.join("part-0.csv"), "1\n")
            .expect("partition csv fixture should be written");

        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int32, true),
            Field::new("year", DataType::Utf8, true),
        ]));

        let definition = ExternalTableDefinition {
            name: "t_partitioned".to_string(),
            location: format!("{}/", to_store_relative_location(&root)),
            file_type: "csv".to_string(),
            schema,
            definition: None,
            partition_cols: vec!["year".to_string()],
            options: HashMap::new(),
            if_not_exists: false,
        };

        let context = Arc::new(SessionContext::new());
        let store_url = ObjectStoreUrl::parse("file://").unwrap();

        let provider = definition
            .build_provider(context, &store_url)
            .await
            .expect("listing provider should be built from explicit schema");

        let schema = provider.schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "value");
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(schema.field(1).name(), "year");
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);

        fs::remove_dir_all(&root).expect("temporary directory should be cleaned up");
    }

    #[tokio::test]
    /// Verifies unknown file formats fail provider construction with a clear error.
    async fn listing_table_definition_build_provider_rejects_unknown_file_type() {
        let definition = ExternalTableDefinition {
            name: "t_bad".to_string(),
            location: "tmp".to_string(),
            file_type: "not_a_real_format".to_string(),
            schema: Arc::new(Schema::empty()),
            definition: None,
            partition_cols: vec![],
            options: HashMap::new(),
            if_not_exists: false,
        };

        let context = Arc::new(SessionContext::new());
        let store_url = ObjectStoreUrl::parse("file://").unwrap();

        let err = definition
            .build_provider(context, &store_url)
            .await
            .expect_err("unknown file type should fail");
        let msg = err.to_string();

        assert!(msg.contains("Could not find FileFormat"), "error: {msg}");
    }
}
