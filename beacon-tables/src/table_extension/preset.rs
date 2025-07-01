use std::{any::Any, sync::Arc};

use datafusion::catalog::TableProvider;

use crate::table_extension::{description::TableExtensionDescription, TableExtension};

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct EasyCollectionExtension {
    pub preset_name: String,
    pub filter_columns: Vec<String>,
    pub data_columns: Vec<String>,
    pub metadata_columns: Vec<String>,
}

#[typetag::serde(name = "easy_collection")]
impl TableExtension for EasyCollectionExtension {
    fn description(&self) -> TableExtensionDescription {
        todo!()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_ext_function_name() -> &'static str
    where
        Self: Sized,
    {
        "as_easy_collection"
    }

    fn extension_table(
        &self,
        origin_table: Arc<dyn TableProvider>,
    ) -> datafusion::error::Result<Arc<dyn TableProvider>>
    where
        Self: Sized,
    {
        todo!()
    }
}
