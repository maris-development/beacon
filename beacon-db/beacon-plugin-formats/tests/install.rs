//! Installing plugin formats in a session: names, collisions, table functions, uninstall.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use beacon_common::table_function::BeaconTableFunctionImpl;
use beacon_datafusion_ext::format_ext::new_file_format_registry_handle;
use beacon_datafusion_ext::listing_factory::ListingFactory;
use beacon_datafusion_ext::type_widening::ArrowTypeWidening;
use beacon_plugin_formats::{
    PluginFileFormatFactory, check_formats, install_formats, options_from_args, uninstall_formats,
};
use beacon_plugin_sdk::file_format::{ArgKind, ArgSpec, FormatFactory, FormatReader, OutputKind};
use datafusion::common::DFSchema;
use datafusion::error::Result;
use datafusion::prelude::{Expr, SessionConfig, SessionContext, lit};
use datafusion::scalar::ScalarValue;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use tokio::runtime::Handle;
use url::Url;

#[path = "plugin_format/test_formats.rs"]
mod test_formats;

use test_formats::{LineText, foreign};

/// A factory with a free name and extension, and two `read_<name>` arguments.
#[derive(Debug)]
struct Named {
    name: &'static str,
    ext: &'static str,
}

impl FormatFactory for Named {
    fn name(&self) -> &str {
        self.name
    }

    fn extensions(&self) -> Vec<String> {
        vec![self.ext.to_string()]
    }

    fn output_kind(&self) -> OutputKind {
        OutputKind::Flat
    }

    fn table_function_args(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec {
                option_key: "read_dimensions".to_string(),
                kind: ArgKind::StringList,
            },
            ArgSpec {
                option_key: "skip".to_string(),
                kind: ArgKind::Bool,
            },
            ArgSpec {
                option_key: "mode".to_string(),
                kind: ArgKind::String,
            },
        ]
    }

    fn create(&self, _: &HashMap<String, String>) -> Result<Arc<dyn FormatReader>> {
        LineText::default().create(&HashMap::new())
    }
}

fn wrap(factory: impl FormatFactory) -> PluginFileFormatFactory {
    PluginFileFormatFactory::new(foreign(Arc::new(factory)))
}

fn session() -> Arc<SessionContext> {
    let config = SessionConfig::new()
        .with_extension(new_file_format_registry_handle())
        .with_extension(ArrowTypeWidening::default_extension())
        .with_extension(Arc::new(ListingFactory::dynamic()));
    let ctx = SessionContext::new_with_config(config);
    ctx.register_object_store(
        &Url::parse("memory://").unwrap(),
        Arc::new(InMemory::new()) as Arc<dyn ObjectStore>,
    );
    Arc::new(ctx)
}

fn string_list(items: &[&str]) -> Expr {
    lit(ScalarValue::List(ScalarValue::new_list_nullable(
        &items
            .iter()
            .map(|s| ScalarValue::Utf8(Some(s.to_string())))
            .collect::<Vec<_>>(),
        &arrow::datatypes::DataType::Utf8,
    )))
}

async fn run(ctx: &SessionContext, query: &str) -> Result<Vec<RecordBatch>> {
    ctx.sql(query).await?.collect().await
}

fn text(batches: &[RecordBatch]) -> String {
    pretty_format_batches(batches).unwrap().to_string()
}

/// The options of `read_aaa('f.a', <options>)`, with `options` planned from SQL.
fn options_of(options: &str) -> Result<HashMap<String, String>> {
    let specs = Named {
        name: "aaa",
        ext: "a",
    }
    .table_function_args();
    let options = SessionContext::new().parse_sql_expr(options, &DFSchema::empty())?;
    options_from_args(&[lit("f.a"), options], &specs, "read_aaa")
}

#[test]
fn struct_fields_map_to_options() {
    let options = options_of("{mode: 'fast', read_dimensions: ['x', 'y'], skip: true}").unwrap();
    assert_eq!(
        options,
        HashMap::from([
            ("read_dimensions".to_string(), "x,y".to_string()),
            ("skip".to_string(), "true".to_string()),
            ("mode".to_string(), "fast".to_string()),
        ])
    );
    let options = options_of("named_struct('skip', false)").unwrap();
    assert_eq!(
        options,
        HashMap::from([("skip".to_string(), "false".to_string())])
    );
}

#[test]
fn null_and_missing_options_give_no_option() {
    assert!(options_of("NULL").unwrap().is_empty());
    assert!(
        options_of("{read_dimensions: [], skip: NULL}")
            .unwrap()
            .is_empty()
    );
    let specs = Named {
        name: "aaa",
        ext: "a",
    }
    .table_function_args();
    assert!(
        options_from_args(&[lit("f.a")], &specs, "read_aaa")
            .unwrap()
            .is_empty()
    );
}

#[test]
fn a_wrong_option_is_an_error() {
    for (options, message) in [
        ("{skip: 'yes'}", "read_aaa option `skip` must be a boolean"),
        ("{mode: 1}", "read_aaa option `mode` must be a string"),
        (
            "{read_dimensions: [1, 2]}",
            "read_aaa option `read_dimensions` must be a list of strings",
        ),
        (
            "{skipp: true}",
            "read_aaa has no option `skipp`. Valid options: read_dimensions, skip, mode",
        ),
        (
            "true",
            "read_aaa second argument must be a struct of options",
        ),
    ] {
        let error = options_of(options).unwrap_err().to_string();
        assert!(error.contains(message), "{options}: {error}");
    }
}

#[test]
fn positional_options_are_an_error() {
    let specs = Named {
        name: "aaa",
        ext: "a",
    }
    .table_function_args();
    let args = vec![lit("f.a"), string_list(&["x"]), lit(true)];
    let error = options_from_args(&args, &specs, "read_aaa").unwrap_err();
    assert!(
        error
            .to_string()
            .contains("read_aaa takes the paths and one struct of options"),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_names_are_refused() {
    let ctx = session();
    for (name, ext) in [("Point-Text", "ptxt"), ("ptext", ".ptxt"), ("", "p")] {
        let error = check_formats(&ctx.state(), &[wrap(Named { name, ext })]).unwrap_err();
        assert!(
            error.to_string().contains("invalid file format name"),
            "{error}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_taken_key_is_refused() {
    let ctx = session();
    // `csv` is a DataFusion format.
    let error = check_formats(
        &ctx.state(),
        &[wrap(Named {
            name: "csv",
            ext: "x",
        })],
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("`csv` is already in use"),
        "{error}"
    );
    // Two formats of one plugin with the same extension.
    let error = check_formats(
        &ctx.state(),
        &[
            wrap(Named {
                name: "aaa",
                ext: "same",
            }),
            wrap(Named {
                name: "bbb",
                ext: "same",
            }),
        ],
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("`same` is already in use"),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn install_query_and_uninstall() {
    let ctx = session();
    let store = ctx
        .runtime_env()
        .object_store(
            datafusion::execution::object_store::ObjectStoreUrl::parse("memory://").unwrap(),
        )
        .unwrap();
    store
        .put(
            &Path::from("d/a.ltxt"),
            PutPayload::from_static(b"a:Int64\n1\n2\n"),
        )
        .await
        .unwrap();
    let formats = vec![wrap(LineText::default())];
    check_formats(&ctx.state(), &formats).unwrap();
    let readers = install_formats(&ctx, &formats, Handle::current()).unwrap();
    let functions: Vec<String> = readers.iter().map(|r| r.name()).collect();
    assert_eq!(functions, vec!["read_linetext".to_string()]);

    // `STORED AS` by name and by extension.
    for key in ["linetext", "ltxt"] {
        ctx.sql(&format!(
            "CREATE EXTERNAL TABLE t_{key} STORED AS {key} LOCATION 'memory:///d/'"
        ))
        .await
        .unwrap();
        let batches = ctx
            .sql(&format!("SELECT sum(a) AS s FROM t_{key}"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert!(text(&batches).contains("| 3 |"), "{}", text(&batches));
    }

    // `read_<name>`.
    let batches = ctx
        .sql("SELECT count(*) AS n FROM read_linetext('memory:///d/a.ltxt')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(text(&batches).contains("| 2 |"), "{}", text(&batches));
    let error = run(
        &ctx,
        "SELECT * FROM read_linetext('memory:///d/a.ltxt', {x: true})",
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("read_linetext takes no options"), "{error}");

    // A second install of the same names is refused.
    let error = check_formats(&ctx.state(), &formats).unwrap_err();
    assert!(error.to_string().contains("already in use"), "{error}");

    uninstall_formats(&ctx, &formats);
    let error = run(&ctx, "SELECT * FROM t_linetext")
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("file format `linetext` is not installed"),
        "{error}"
    );
    assert!(
        run(&ctx, "SELECT * FROM read_linetext('memory:///d/a.ltxt')")
            .await
            .is_err()
    );
    let error = run(
        &ctx,
        "CREATE EXTERNAL TABLE t2 STORED AS linetext LOCATION 'memory:///d/'",
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(
        error.contains("file format `linetext` is not installed"),
        "{error}"
    );

    // After uninstall, the names are free again.
    check_formats(&ctx.state(), &formats).unwrap();
}
