use std::{any::Any, sync::Arc};

use datafusion::{
    catalog::{SchemaProvider, TableFunction, TableFunctionImpl, TableProvider},
    prelude::{Expr, SessionContext},
    scalar::ScalarValue,
};

use crate::schema_provider::BeaconSchemaProvider;

pub mod description;
pub mod spatial_temporal;

pub fn table_extension_functions(session_ctx: Arc<SessionContext>) -> Vec<TableFunction> {
    vec![spatial_temporal::SpatialTemporalExtension::table_ext_function(session_ctx)]
}

#[typetag::serde]
pub trait TableExtension: Any + std::fmt::Debug + Send + Sync {
    fn description(&self) -> description::TableExtensionDescription;
    fn as_any(&self) -> &dyn Any;
    fn table_ext_function_name() -> &'static str
    where
        Self: Sized;
    fn table_ext_function(session_ctx: Arc<SessionContext>) -> TableFunction
    where
        Self: Sized,
    {
        let beacon_schema_provider = Arc::new(
            session_ctx
                .catalog("datafusion")
                .unwrap()
                .schema("public")
                .unwrap()
                .as_any()
                .downcast_ref::<BeaconSchemaProvider>()
                .expect("BeaconSchemaProvider not found")
                .clone(),
        );
        let table_extension_impl = TableExtensionImpl::<Self>::new(beacon_schema_provider);

        TableFunction::new(
            Self::table_ext_function_name().to_string(),
            Arc::new(table_extension_impl),
        )
    }

    fn find_extension(extensions: &[Arc<dyn TableExtension>]) -> Option<Arc<Self>>
    where
        Self: Sized,
    {
        for ext in extensions {
            if let Ok(sized) = Arc::downcast::<Self>(ext.clone()) {
                return Some(sized);
            }
        }
        None
    }
    fn extension_table(
        &self,
        origin_table: Arc<dyn TableProvider>,
    ) -> datafusion::error::Result<Arc<dyn TableProvider>>
    where
        Self: Sized;
}

#[derive(Debug)]
pub struct TableExtensionImpl<Ext: TableExtension> {
    schema_provider: Arc<BeaconSchemaProvider>,
    phantom: std::marker::PhantomData<Ext>,
}

impl<Ext: TableExtension> TableExtensionImpl<Ext> {
    pub fn new(schema_provider: Arc<BeaconSchemaProvider>) -> Self {
        Self {
            schema_provider,
            phantom: std::marker::PhantomData,
        }
    }
}

impl<Ext: TableExtension> TableFunctionImpl for TableExtensionImpl<Ext> {
    fn call(&self, args: &[Expr]) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        let table_name = if let Some(Expr::Literal(ScalarValue::Utf8(Some(name)))) = args.get(0) {
            name.clone()
        } else {
            return Err(datafusion::error::DataFusionError::Plan(
                "Table name must be a string literal".to_string(),
            ));
        };

        let table_result: datafusion::error::Result<Option<Arc<dyn TableProvider>>> =
            tokio::task::block_in_place(|| {
                let runtime_handle = tokio::runtime::Handle::current();
                runtime_handle.block_on(async {
                    let table = self.schema_provider.table(&table_name);
                    let table = table
                        .await
                        .map_err(|e| datafusion::error::DataFusionError::Plan(format!("{}", e)))?;
                    Ok(table)
                })
            });
        let table = if let Some(table) = table_result? {
            table
        } else {
            return Err(datafusion::error::DataFusionError::Plan(format!(
                "Table {} not found",
                table_name
            )));
        };

        let table_extensions = self.schema_provider.table_extensions(&table_name).ok_or(
            datafusion::error::DataFusionError::Plan(format!("Table {} not found", table_name)),
        )?;

        let extension = Ext::find_extension(&table_extensions).ok_or(
            datafusion::error::DataFusionError::Plan(format!(
                "Table extension {} not found",
                Ext::table_ext_function_name()
            )),
        )?;

        let extension_table_provider = Ext::extension_table(&extension, table)?;

        Ok(extension_table_provider)
    }
}
