use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::common::{
    ToDFSchema, arrow_datafusion_err, config_datafusion_err, plan_err,
};
use datafusion::datasource::listing::ListingOptions;
use datafusion::execution::SessionState;
use datafusion::logical_expr::CreateExternalTable;
use datafusion::{
    arrow::datatypes::{DataType, SchemaRef},
    catalog::{Session, TableProvider, TableProviderFactory},
};

use crate::listing_factory::ListingFactory;
use crate::table_ext::{
    ExternalTable, ExternalTableDefinition, ExternalTableRebuild, build_listing_table,
};

type PartitionCols = Vec<(String, DataType)>;

/// A `TableProviderFactory` capable of creating new `ListingTable`s
#[derive(Debug)]
pub struct ListingTableFactoryExt;

#[async_trait::async_trait]
impl TableProviderFactory for ListingTableFactoryExt {
    async fn create(
        &self,
        state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        let session_state = as_session_state(state)?;

        let file_format_factory = session_state
            .get_file_format_factory(cmd.file_type.to_lowercase().as_str())
            .ok_or(config_datafusion_err!(
                "Unable to create table with format {}! Could not find FileFormat.",
                cmd.file_type
            ))?;

        let (provided_schema, table_partition_cols) = resolve_schema_and_partition_cols(cmd)?;
        let schema_inferred = provided_schema.is_none();

        let listing_factory = session_state.config().get_extension::<ListingFactory>().expect("Listing Factory should be registered at startup. If you are seeing this error, please report it to the Beacon team.");
        let mut listing_table_url =
            listing_factory.parse_listing_table_url(state, &cmd.location)?;

        // Built *after* the URL is parsed: a natively-read format (netCDF) opens
        // files by path rather than through the object store, so it needs the root
        // store this LOCATION resolves against. For every other format the hook
        // delegates to plain `create` and the URL is ignored.
        let file_format = match crate::format_ext::try_file_format_factory_ext(
            state,
            cmd.file_type.to_lowercase().as_str(),
        ) {
            Some(factory) => factory.create_with_native_root(
                state,
                &cmd.options,
                &listing_table_url,
                &listing_factory,
            )?,
            None => file_format_factory.create(session_state, &cmd.options)?,
        };

        let options = ListingOptions::new(file_format)
            .with_file_extension("") // file extension is not needed for listing table factory since the file format will handle it in `infer_schema` and `infer_partition_schema`
            .with_session_config_options(session_state.config())
            .with_collect_stat(true)
            .with_table_partition_cols(table_partition_cols)
            .with_file_sort_order(cmd.order_exprs.clone());

        options
            .validate_partitions(session_state, &listing_table_url)
            .await?;

        // When inferring, apply the default glob now so that the rebuild spec
        // lists files identically on every subsequent refresh.
        if schema_inferred {
            listing_table_url = maybe_apply_default_glob(listing_table_url, &options, cmd)?;
        }

        let rebuild = ExternalTableRebuild {
            listing_table_url,
            options,
            provided_schema,
            constraints: cmd.constraints.clone(),
            column_defaults: cmd.column_defaults.clone(),
            definition_sql: cmd.definition.clone(),
        };

        let table = build_listing_table(session_state, &rebuild).await?;

        // Validate ORDER BY columns against the resolved schema.
        let df_schema = Arc::clone(&table.schema()).to_dfschema()?;
        let column_refs: HashSet<_> = cmd
            .order_exprs
            .iter()
            .flat_map(|sort| sort.iter())
            .flat_map(|s| s.expr.column_refs())
            .collect();
        for column in &column_refs {
            if !df_schema.has_column(column) {
                return plan_err!("Column {column} is not in schema");
            }
        }

        let definition = ExternalTableDefinition {
            definition: cmd.definition.clone(),
            name: cmd.name.to_string(),
            file_type: cmd.file_type.to_string(),
            options: cmd.options.clone(),
            location: cmd.location.clone(),
            partition_cols: cmd.table_partition_cols.clone(),
            if_not_exists: cmd.if_not_exists,
            // Persist an empty schema when it was inferred so that on reload the
            // table re-infers (and keeps re-inferring on refresh) instead of
            // pinning this snapshot.
            schema: if schema_inferred {
                Arc::new(Schema::empty())
            } else {
                table.schema()
            },
        };

        let external_table = ExternalTable::new(definition, table, rebuild);

        Ok(Arc::new(external_table))
    }
}

/// Downcast a generic DataFusion session reference into SessionState.
fn as_session_state(state: &dyn Session) -> datafusion::error::Result<&SessionState> {
    state
        .as_any()
        .downcast_ref::<SessionState>()
        .ok_or(config_datafusion_err!(
            "Expected SessionState when creating external listing table"
        ))
}

/// Resolve the user-provided schema and partition columns for CREATE EXTERNAL TABLE.
///
/// When no schema is provided, partition columns are represented as Dictionary(UInt16, Utf8)
/// to match DataFusion's listing-table defaults.
fn resolve_schema_and_partition_cols(
    cmd: &CreateExternalTable,
) -> datafusion::error::Result<(Option<SchemaRef>, PartitionCols)> {
    if cmd.schema.fields().is_empty() {
        let partition_cols = dictionary_partition_cols(&cmd.table_partition_cols);
        return Ok((None, partition_cols));
    }

    let schema: SchemaRef = Arc::clone(cmd.schema.inner());
    let partition_cols = partition_cols_from_schema(&schema, &cmd.table_partition_cols)?;
    let projected_schema = project_out_partition_columns(&schema, &cmd.table_partition_cols)?;

    Ok((Some(projected_schema), partition_cols))
}

/// Build default partition column types used when schema is inferred from files.
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

/// Resolve partition column types from an explicit schema.
fn partition_cols_from_schema(
    schema: &SchemaRef,
    partition_cols: &[String],
) -> datafusion::error::Result<PartitionCols> {
    partition_cols
        .iter()
        .map(|col| {
            schema
                .field_with_name(col)
                .map(|f| (f.name().to_owned(), f.data_type().to_owned()))
                .map_err(|e| arrow_datafusion_err!(e))
        })
        .collect()
}

/// Exclude partition columns from the file schema for partitioned external tables.
fn project_out_partition_columns(
    schema: &SchemaRef,
    partition_cols: &[String],
) -> datafusion::error::Result<SchemaRef> {
    let mut project_idx = Vec::new();
    for i in 0..schema.fields().len() {
        if !partition_cols.contains(schema.field(i).name()) {
            project_idx.push(i);
        }
    }

    Ok(Arc::new(schema.project(&project_idx)?))
}

/// Apply a default glob for folder locations when no explicit glob was provided.
fn maybe_apply_default_glob(
    mut listing_table_url: datafusion::datasource::listing::ListingTableUrl,
    options: &ListingOptions,
    cmd: &CreateExternalTable,
) -> datafusion::error::Result<datafusion::datasource::listing::ListingTableUrl> {
    if listing_table_url.is_folder() && listing_table_url.get_glob().is_none() {
        let file_glob = inferred_file_glob(options, &cmd.file_type);
        listing_table_url = listing_table_url.with_glob(file_glob.as_ref())?;
    }

    Ok(listing_table_url)
}

/// Infer a listing glob from the format extension, falling back to file_type.
fn inferred_file_glob(options: &ListingOptions, file_type: &str) -> String {
    match options.format.compression_type() {
        Some(compression) => match options.format.get_ext_with_compression(&compression) {
            Ok(ext) => format!("*.{ext}"),
            Err(_) => fallback_file_glob(file_type),
        },
        None => fallback_file_glob(file_type),
    }
}

/// Build the fallback glob for file_type values used by CREATE EXTERNAL TABLE.
fn fallback_file_glob(file_type: &str) -> String {
    format!("*.{}", file_type.to_lowercase())
}

#[cfg(test)]
mod tests {
    use super::{
        dictionary_partition_cols, fallback_file_glob, partition_cols_from_schema,
        project_out_partition_columns,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use std::sync::Arc;

    #[test]
    fn dictionary_partition_cols_uses_dictionary_utf8_type() {
        let cols = dictionary_partition_cols(&["year".to_string(), "month".to_string()]);

        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].0, "year");
        assert_eq!(
            cols[0].1,
            DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8))
        );
    }

    #[test]
    fn partition_cols_from_schema_resolves_declared_types() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int32, true),
            Field::new("year", DataType::Utf8, true),
        ]));

        let cols = partition_cols_from_schema(&schema, &["year".to_string()]).unwrap();
        assert_eq!(cols, vec![("year".to_string(), DataType::Utf8)]);
    }

    #[test]
    fn project_out_partition_columns_removes_partition_fields() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int32, true),
            Field::new("year", DataType::Utf8, true),
            Field::new("month", DataType::Utf8, true),
        ]));

        let projected =
            project_out_partition_columns(&schema, &["year".to_string(), "month".to_string()])
                .unwrap();

        assert_eq!(projected.fields().len(), 1);
        assert_eq!(projected.field(0).name(), "value");
    }

    #[test]
    fn fallback_file_glob_lowercases_file_type() {
        assert_eq!(fallback_file_glob("ATLAS"), "*.atlas");
        assert_eq!(fallback_file_glob("ParQuet"), "*.parquet");
    }

    #[test]
    fn partition_cols_from_schema_errors_on_an_undeclared_column() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            true,
        )]));

        // PARTITIONED BY a column the explicit schema never declares.
        assert!(partition_cols_from_schema(&schema, &["year".to_string()]).is_err());
    }

    #[test]
    fn project_out_partition_columns_is_a_no_op_without_partitions() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int32, true),
            Field::new("year", DataType::Utf8, true),
        ]));

        let projected = project_out_partition_columns(&schema, &[]).unwrap();
        assert_eq!(projected.as_ref(), schema.as_ref());
    }

    #[test]
    fn an_empty_schema_defers_inference_to_the_files() {
        use super::resolve_schema_and_partition_cols;

        let cmd = create_external_table(Schema::empty(), vec!["year".to_string()]);
        let (schema, partition_cols) = resolve_schema_and_partition_cols(&cmd).unwrap();

        // `None` tells the caller to infer, and partition columns fall back to
        // DataFusion's dictionary default.
        assert!(schema.is_none());
        assert_eq!(
            partition_cols[0].1,
            DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8))
        );
    }

    #[test]
    fn an_explicit_schema_is_kept_minus_its_partition_columns() {
        use super::resolve_schema_and_partition_cols;

        let declared = Schema::new(vec![
            Field::new("value", DataType::Int32, true),
            Field::new("year", DataType::Utf8, true),
        ]);
        let cmd = create_external_table(declared, vec!["year".to_string()]);
        let (schema, partition_cols) = resolve_schema_and_partition_cols(&cmd).unwrap();

        let schema = schema.expect("an explicit schema is preserved");
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "value");
        // The partition column keeps the type the user declared for it.
        assert_eq!(partition_cols, vec![("year".to_string(), DataType::Utf8)]);
    }

    /// A minimal `CREATE EXTERNAL TABLE` command carrying just the schema and
    /// partition columns the resolver looks at.
    fn create_external_table(
        schema: Schema,
        table_partition_cols: Vec<String>,
    ) -> datafusion::logical_expr::CreateExternalTable {
        use datafusion::common::{Constraints, DFSchema};

        datafusion::logical_expr::CreateExternalTable {
            schema: Arc::new(DFSchema::try_from(schema).unwrap()),
            name: "t".into(),
            location: "data/".to_string(),
            file_type: "parquet".to_string(),
            table_partition_cols,
            if_not_exists: false,
            or_replace: false,
            temporary: false,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options: Default::default(),
            constraints: Constraints::default(),
            column_defaults: Default::default(),
        }
    }
}
