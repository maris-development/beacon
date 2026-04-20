//! Builders for the Flight SQL metadata result sets exposed by Beacon.

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use arrow_flight::sql::{
    metadata::{
        GetCatalogsBuilder, GetDbSchemasBuilder, GetTablesBuilder, SqlInfoData, SqlInfoDataBuilder,
    },
    SqlSupportedCaseSensitivity,
};
use arrow_flight::sql::{CommandGetDbSchemas, CommandGetSqlInfo, CommandGetTables, SqlInfo};

use crate::flight_sql::util::to_internal_status;

const DEFAULT_TABLE_TYPE: &str = "TABLE";

/// Produces metadata record batches backed by the Beacon runtime catalog.
#[derive(Clone)]
pub(super) struct FlightSqlMetadata {
    runtime: Arc<beacon_core::runtime::Runtime>,
    sql_info_data: Arc<SqlInfoData>,
}

impl FlightSqlMetadata {
    /// Creates metadata builders bound to the shared Beacon runtime.
    pub(super) fn new(runtime: Arc<beacon_core::runtime::Runtime>) -> anyhow::Result<Self> {
        Ok(Self {
            runtime,
            sql_info_data: Arc::new(build_sql_info_data()?),
        })
    }

    /// Builds the `GetCatalogs` response batch from DataFusion catalogs visible to Beacon.
    pub(super) fn build_catalogs_batch(&self) -> Result<RecordBatch, tonic::Status> {
        let mut builder = GetCatalogsBuilder::new();

        for catalog_name in self.runtime.list_sql_catalogs() {
            builder.append(catalog_name);
        }

        builder.build().map_err(to_internal_status)
    }

    /// Builds the `GetDbSchemas` response batch from the runtime catalog listing.
    pub(super) fn build_schemas_batch(
        &self,
        query: CommandGetDbSchemas,
    ) -> Result<RecordBatch, tonic::Status> {
        let mut builder = GetDbSchemasBuilder::from(query);

        for (catalog_name, schema_name) in self.runtime.list_sql_schemas() {
            builder.append(catalog_name, schema_name);
        }

        builder.build().map_err(to_internal_status)
    }

    /// Builds the `GetTables` response batch, including Arrow schemas when requested.
    pub(super) async fn build_tables_batch(
        &self,
        query: CommandGetTables,
    ) -> Result<RecordBatch, tonic::Status> {
        let mut builder = GetTablesBuilder::from(query);

        for (catalog_name, schema_name, table_name) in self.runtime.list_sql_tables() {
            let qualified_table_name = format!("{catalog_name}.{schema_name}.{table_name}");
            let table_schema = self
                .runtime
                .list_table_schema(qualified_table_name)
                .await
                // Some internal tables may not resolve to Arrow schemas through the public lookup.
                .unwrap_or_else(|| Arc::new(Schema::empty()));

            builder
                .append(
                    catalog_name,
                    schema_name,
                    table_name,
                    DEFAULT_TABLE_TYPE,
                    table_schema.as_ref(),
                )
                .map_err(to_internal_status)?;
        }

        builder.build().map_err(to_internal_status)
    }

    /// Builds the static table-type batch advertised by the Flight SQL server.
    pub(super) fn build_table_types_batch(&self) -> Result<RecordBatch, tonic::Status> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "table_type",
            DataType::Utf8,
            false,
        )]));
        let values: ArrayRef = Arc::new(StringArray::from(vec![DEFAULT_TABLE_TYPE]));

        RecordBatch::try_new(schema, vec![values]).map_err(to_internal_status)
    }

    /// Builds the SQL info response batch for the requested Flight SQL capability identifiers.
    pub(super) fn build_sql_info_batch(
        &self,
        query: CommandGetSqlInfo,
    ) -> Result<RecordBatch, tonic::Status> {
        query
            .into_builder(self.sql_info_data.as_ref())
            .build()
            .map_err(to_internal_status)
    }
}

/// Describes the Beacon Flight SQL capabilities advertised to clients.
fn build_sql_info_data() -> anyhow::Result<SqlInfoData> {
    let mut builder = SqlInfoDataBuilder::new();
    builder.append(SqlInfo::FlightSqlServerName, "Beacon Flight SQL");
    builder.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
    builder.append(SqlInfo::FlightSqlServerArrowVersion, "56");
    builder.append(SqlInfo::FlightSqlServerReadOnly, false);
    builder.append(SqlInfo::FlightSqlServerSql, true);
    builder.append(SqlInfo::SqlCatalogTerm, "catalog");
    builder.append(SqlInfo::SqlSchemaTerm, "schema");
    builder.append(SqlInfo::SqlIdentifierQuoteChar, "\"");
    builder.append(
        SqlInfo::SqlQuotedIdentifierCase,
        SqlSupportedCaseSensitivity::SqlCaseSensitivityUnknown.as_str_name(),
    );
    builder.append(SqlInfo::SqlAllTablesAreSelectable, true);
    builder.append(SqlInfo::SqlNullOrdering, 1i32);
    builder.build().map_err(Into::into)
}
