//! The one path from a table description to an [`ExternalTable`].

use std::{collections::HashMap, collections::HashSet, sync::Arc};

use beacon_datafusion_ext::{
    format_ext::try_file_format_factory_ext, listing_factory::ListingFactory,
};
use datafusion::{
    catalog::{Session, TableProvider},
    common::{Constraints, Result, ToDFSchema, config_datafusion_err, plan_err},
    datasource::{file_format::FileFormat, listing::ListingOptions},
    execution::SessionState,
    logical_expr::{CreateExternalTable, SortExpr},
    prelude::Expr,
    sql::TableReference,
};

use crate::{
    ExternalTable, ExternalTableDefinition, ExternalTableSpec, RefreshPolicy, SchemaSource,
    spec::{resolve_schema_and_partition_cols, with_default_glob},
    statistics_cache::{ExternalTableStatistics, ExternalTableStatisticsProvider},
    bool_table_option,
    definition::{
        SKIP_INVALID_FILES_ON_INFERENCE_OPTION, SKIP_INVALID_FILES_ON_SCAN_OPTION,
        USE_STATISTICS_PRUNING_OPTION,
    },
    parallel_schema_inference_from_options, type_widening_from_options,
};

/// Builds an [`ExternalTable`] from a stored definition or from a
/// `CREATE EXTERNAL TABLE` statement.
///
/// The definition holds what Beacon stores. The other settings come only from
/// the SQL statement and are not stored.
#[derive(Clone)]
pub struct ExternalTableBuilder {
    definition: ExternalTableDefinition,
    order_exprs: Vec<Vec<SortExpr>>,
    constraints: Constraints,
    column_defaults: HashMap<String, Expr>,
    statistics_provider: Option<Arc<dyn ExternalTableStatisticsProvider>>,
}

impl std::fmt::Debug for ExternalTableBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExternalTableBuilder")
            .field("definition", &self.definition)
            .field("order_exprs", &self.order_exprs)
            .field("constraints", &self.constraints)
            .field("column_defaults", &self.column_defaults)
            .field("statistics_provider", &self.statistics_provider.is_some())
            .finish()
    }
}

impl ExternalTableBuilder {
    /// Starts from a stored definition.
    pub fn new(definition: ExternalTableDefinition) -> Self {
        Self {
            definition,
            order_exprs: Vec::new(),
            constraints: Constraints::default(),
            column_defaults: HashMap::new(),
            statistics_provider: None,
        }
    }

    /// Starts from a `CREATE EXTERNAL TABLE` statement.
    ///
    /// # Errors
    ///
    /// Returns an error when the type widening options name no strategy or no conflict rule.
    pub fn from_create(cmd: &CreateExternalTable) -> Result<Self> {
        let definition = ExternalTableDefinition {
            name: cmd.name.to_string(),
            location: cmd.location.clone(),
            file_type: cmd.file_type.clone(),
            schema: Arc::clone(cmd.schema.inner()),
            // A statement with no columns asks Beacon to infer them.
            schema_source: if cmd.schema.fields().is_empty() {
                SchemaSource::Inferred
            } else {
                SchemaSource::Declared
            },
            definition: cmd.definition.clone(),
            partition_cols: cmd.table_partition_cols.clone(),
            options: cmd.options.clone(),
            if_not_exists: cmd.if_not_exists,
            refresh_policy: RefreshPolicy::default(),
            type_widening: type_widening_from_options(&cmd.options)?,
            parallel_schema_inference: parallel_schema_inference_from_options(&cmd.options)?,
            skip_invalid_files_on_inference: bool_table_option(
                &cmd.options,
                SKIP_INVALID_FILES_ON_INFERENCE_OPTION,
            )?,
            skip_invalid_files_on_scan: bool_table_option(
                &cmd.options,
                SKIP_INVALID_FILES_ON_SCAN_OPTION,
            )?,
            use_listing_cache: false,
            use_statistics_pruning: bool_table_option(&cmd.options, USE_STATISTICS_PRUNING_OPTION)?,
            use_statistics_cache: false,
        };
        Ok(Self::new(definition)
            .with_order(cmd.order_exprs.clone())
            .with_constraints(cmd.constraints.clone())
            .with_column_defaults(cmd.column_defaults.clone()))
    }

    /// Sets the sort order of the files (`WITH ORDER`).
    pub fn with_order(mut self, order_exprs: Vec<Vec<SortExpr>>) -> Self {
        self.order_exprs = order_exprs;
        self
    }

    /// Sets the table constraints.
    pub fn with_constraints(mut self, constraints: Constraints) -> Self {
        self.constraints = constraints;
        self
    }

    /// Sets when Beacon lists the files again. Beacon stores the policy but does not apply it yet.
    pub fn with_refresh_policy(mut self, policy: RefreshPolicy) -> Self {
        self.definition.refresh_policy = policy;
        self
    }

    /// Sets the column default values.
    pub fn with_column_defaults(mut self, column_defaults: HashMap<String, Expr>) -> Self {
        self.column_defaults = column_defaults;
        self
    }

    /// Sets the statistics provider that prunes the files of a scan.
    ///
    /// The scan uses it only when the definition sets `use_statistics_pruning`.
    /// Without this call, the table takes the [`ExternalTableStatistics`] session extension.
    pub fn with_statistics_provider(
        mut self,
        provider: Arc<dyn ExternalTableStatisticsProvider>,
    ) -> Self {
        self.statistics_provider = Some(provider);
        self
    }

    /// The stored definition that the table will hold.
    pub fn definition(&self) -> &ExternalTableDefinition {
        &self.definition
    }

    /// Resolves the format, the location and the schema, and builds the table.
    ///
    /// # Errors
    ///
    /// Returns an error when the format is unknown, the location does not
    /// parse, the partitions do not match the files, the schema cannot be
    /// inferred, or a `WITH ORDER` column is not in the schema.
    pub async fn build(mut self, session: &dyn Session) -> Result<ExternalTable> {
        let spec = self.spec(session).await?;
        // A stored schema, declared or inferred, needs no file read.
        let infer = self.definition.schema.fields().is_empty();
        let (table, schema) = spec.build_table(session, &self.definition, infer).await?;
        self.validate_order(&table.schema())?;
        if self.definition.schema_inferred() {
            self.definition.schema_source = SchemaSource::Inferred;
        }
        self.definition.schema = schema;
        let statistics_provider = self.statistics_provider.or_else(|| {
            session
                .config()
                .get_extension::<ExternalTableStatistics>()
                .map(|extension| Arc::clone(&extension.0))
        });
        Ok(ExternalTable::new(
            self.definition,
            spec,
            table,
            statistics_provider,
        ))
    }

    /// Resolves the definition into the spec that a refresh builds from.
    async fn spec(&self, session: &dyn Session) -> Result<ExternalTableSpec> {
        let state = as_session_state(session)?;
        let definition = &self.definition;
        let file_type = definition.file_type.to_lowercase();

        let declared = !definition.schema_inferred();
        let (_, partition_cols) =
            resolve_schema_and_partition_cols(&definition.schema, &definition.partition_cols)?;

        let listing_factory = state
            .config()
            .get_extension::<ListingFactory>()
            .ok_or_else(|| {
                config_datafusion_err!("the listing factory is not registered on the session")
            })?;
        let url = listing_factory
            .parse_listing_table_url(session, &definition.location)?
            .with_table_ref(TableReference::from(definition.name.as_str()));

        let format = self.file_format(state, &file_type, &url, &listing_factory, declared)?;

        let options = ListingOptions::new(format)
            // The format finds its own files.
            .with_file_extension("")
            .with_session_config_options(state.config())
            // After the session options: prune uses the file statistics store, and per-file statistics at plan time cost minutes.
            .with_collect_stat(false)
            .with_table_partition_cols(partition_cols)
            .with_file_sort_order(self.order_exprs.clone());

        options.validate_partitions(session, &url).await?;

        // An inferred table lists with the same glob on each refresh.
        let url = if declared {
            url
        } else {
            with_default_glob(url, &options, &file_type)?
        };

        Ok(ExternalTableSpec {
            url,
            options,
            constraints: self.constraints.clone(),
            column_defaults: self.column_defaults.clone(),
            definition_sql: definition.definition.clone(),
        })
    }

    /// Creates the file format after the URL: a format that reads files by path
    /// (netCDF) needs the root store that the location resolves against.
    fn file_format(
        &self,
        state: &SessionState,
        file_type: &str,
        url: &datafusion::datasource::listing::ListingTableUrl,
        listing_factory: &ListingFactory,
        schema_declared: bool,
    ) -> Result<Arc<dyn FileFormat>> {
        let format_options = self.definition.format_options();
        if let Some(factory) = try_file_format_factory_ext(state, file_type) {
            let format =
                factory.create_with_native_root(state, &format_options, url, listing_factory)?;
            return Ok(if schema_declared {
                factory.with_declared_columns(format)
            } else {
                format
            });
        }
        let factory = state.get_file_format_factory(file_type).ok_or_else(|| {
            config_datafusion_err!(
                "Unable to create table with format {}! Could not find FileFormat.",
                self.definition.file_type
            )
        })?;
        factory.create(state, &format_options)
    }

    fn validate_order(&self, schema: &datafusion::arrow::datatypes::SchemaRef) -> Result<()> {
        let df_schema = Arc::clone(schema).to_dfschema()?;
        let columns: HashSet<_> = self
            .order_exprs
            .iter()
            .flatten()
            .flat_map(|sort| sort.expr.column_refs())
            .collect();
        for column in columns {
            if !df_schema.has_column(column) {
                return plan_err!("Column {column} is not in schema");
            }
        }
        Ok(())
    }
}

fn as_session_state(session: &dyn Session) -> Result<&SessionState> {
    session
        .as_any()
        .downcast_ref::<SessionState>()
        .ok_or_else(|| config_datafusion_err!("an external table needs a SessionState"))
}
