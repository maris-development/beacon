#![doc = include_str!("../README.md")]

#[cfg(panic = "abort")]
compile_error!("beacon plugins need panic = \"unwind\"");

// The code of each part is in a `beacon-ffi-*` crate. This crate is the one dependency
// of a plugin, and it adds `export_plugin!`.
pub use beacon_ffi::{ABI_SYMBOL, AbiFn, BEACON_PLUGIN_SDK_ABI, BeaconPluginAbi, sdk_abi_version};
pub use beacon_ffi::{file_format, host, object_store, scalar_functions, table_type};

pub use beacon_nd_encoding as nd;
pub use datafusion;
pub use datafusion_ffi;
pub use datafusion_proto;
pub use stabby;

/// Generates the `extern "C"` symbols that Beacon loads from a plugin.
///
/// `scalar` names a `fn() -> Vec<ScalarUDF>` that returns the scalar functions.
/// `formats` names a `fn() -> Vec<Arc<dyn FormatFactory>>` that returns the file formats.
/// `tables` names a `fn() -> Vec<TableType>` that returns the table types.
/// Give one key or more, in any order.
///
/// ```ignore
/// beacon_plugin_sdk::export_plugin!(scalar = functions, formats = file_formats, tables = table_types);
/// ```
#[macro_export]
macro_rules! export_plugin {
    // Hidden arm: sets the ABI values, for mismatch tests.
    (@abi $sdk_abi:expr, $ffi_major:expr $(, $key:ident = $value:path)+ $(,)?) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn beacon_plugin_abi() -> $crate::BeaconPluginAbi {
            $crate::BeaconPluginAbi {
                sdk_abi: $sdk_abi,
                ffi_major: $ffi_major,
            }
        }

        $( $crate::export_plugin!(@symbol $key $value); )+
    };
    (@symbol scalar $scalar:path) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn beacon_plugin_scalar_functions()
        -> $crate::datafusion_ffi::util::FFI_Result<
            $crate::stabby::vec::Vec<$crate::scalar_functions::FFI_ScalarFunction>,
        > {
            $crate::scalar_functions::export_scalar_functions($scalar)
        }
    };
    (@symbol formats $formats:path) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn beacon_plugin_file_formats()
        -> $crate::datafusion_ffi::util::FFI_Result<
            $crate::stabby::vec::Vec<$crate::file_format::FFI_FileFormatFactory>,
        > {
            $crate::file_format::export_file_formats($formats)
        }
    };
    (@symbol tables $tables:path) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn beacon_plugin_table_types(
            host: $crate::host::ffi::FFI_Host,
        ) -> $crate::datafusion_ffi::util::FFI_Result<
            $crate::stabby::vec::Vec<$crate::table_type::FFI_PluginTableType>,
        > {
            $crate::table_type::export_table_types($tables, host)
        }
    };
    (@symbol $other:ident $value:path) => {
        compile_error!(concat!(
            "unknown export_plugin! key `",
            stringify!($other),
            "`: use scalar, formats or tables"
        ));
    };
    ($($key:ident = $value:path),+ $(,)?) => {
        $crate::export_plugin!(
            @abi $crate::BEACON_PLUGIN_SDK_ABI,
            $crate::datafusion_ffi::version()
            $(, $key = $value)+
        );
    };
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use async_trait::async_trait;
    use beacon_ffi_host::testing::{TestHost, foreign_table};
    use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
    use datafusion::catalog::{Session, TableProvider, TableProviderFactory};
    use datafusion::datasource::MemTable;
    use datafusion::error::Result;
    use datafusion::execution::SendableRecordBatchStream;
    use datafusion::logical_expr::{CreateExternalTable, ScalarUDF};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion_ffi::util::FFI_Result;

    use super::*;
    use crate::file_format::{FileFormatsFn, FormatFactory, FormatReader, OutputKind, ScanRequest};
    use crate::object_store::store::{ObjectMeta, ObjectStore};
    use crate::scalar_functions::{ScalarFunctionsFn, function};
    use crate::table_type::{TableType, TableTypesFn};

    fn make_functions() -> Vec<ScalarUDF> {
        let same = function("same")
            .args([DataType::Int64])
            .returns(DataType::Int64)
            .invoke_arrays(|arrays| Ok(Arc::clone(&arrays[0])));
        vec![same]
    }

    /// A reader of empty files.
    #[derive(Debug)]
    struct EmptyReader;

    #[async_trait]
    impl FormatReader for EmptyReader {
        async fn infer_schema(&self, _: Arc<dyn ObjectStore>, _: &ObjectMeta) -> Result<SchemaRef> {
            Ok(Arc::new(Schema::empty()))
        }

        async fn open(
            &self,
            _: Arc<dyn ObjectStore>,
            _: &ObjectMeta,
            request: ScanRequest,
        ) -> Result<SendableRecordBatchStream> {
            let stream = futures::stream::empty();
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                request.schema,
                stream,
            )))
        }
    }

    #[derive(Debug)]
    struct EmptyFormat;

    impl FormatFactory for EmptyFormat {
        fn name(&self) -> &str {
            "empty"
        }

        fn extensions(&self) -> Vec<String> {
            vec!["emp".to_string()]
        }

        fn output_kind(&self) -> OutputKind {
            OutputKind::Flat
        }

        fn create(&self, _: &HashMap<String, String>) -> Result<Arc<dyn FormatReader>> {
            Ok(Arc::new(EmptyReader))
        }
    }

    fn make_formats() -> Vec<Arc<dyn FormatFactory>> {
        vec![Arc::new(EmptyFormat)]
    }

    #[derive(Debug)]
    struct EmptyTables;

    #[async_trait]
    impl TableProviderFactory for EmptyTables {
        async fn create(
            &self,
            _: &dyn Session,
            _: &CreateExternalTable,
        ) -> Result<Arc<dyn TableProvider>> {
            Ok(Arc::new(MemTable::try_new(
                Arc::new(Schema::empty()),
                vec![vec![]],
            )?))
        }
    }

    fn make_tables() -> Vec<TableType> {
        vec![TableType::new("empty_table", Arc::new(EmptyTables))]
    }

    crate::export_plugin!(
        scalar = make_functions,
        formats = make_formats,
        tables = make_tables
    );

    #[test]
    fn export_plugin_generates_the_symbols() {
        let abi: AbiFn = beacon_plugin_abi;
        let functions: ScalarFunctionsFn = beacon_plugin_scalar_functions;
        let formats: FileFormatsFn = beacon_plugin_file_formats;
        let FFI_Result::Ok(formats) = formats() else {
            panic!("expected formats");
        };
        assert_eq!(formats.len(), 1);
        let tables: TableTypesFn = beacon_plugin_table_types;
        let FFI_Result::Ok(tables) = tables(foreign_table(TestHost::new())) else {
            panic!("expected table types");
        };
        assert_eq!(tables.len(), 1);
        assert_eq!(
            abi(),
            BeaconPluginAbi {
                sdk_abi: BEACON_PLUGIN_SDK_ABI,
                ffi_major: datafusion_ffi::version()
            }
        );
        let FFI_Result::Ok(functions) = functions() else {
            panic!("expected functions");
        };
        assert_eq!(functions.len(), 1);
    }
}
