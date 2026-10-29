//! The plugin format wrapper: write path (`COPY ... TO`), through the FFI tables.

use std::sync::Arc;

use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::{Int64Type, UInt64Type};
use beacon_datafusion_ext::format_ext::new_file_format_registry_handle;
use beacon_datafusion_ext::listing_factory::ListingFactory;
use beacon_datafusion_ext::type_widening::ArrowTypeWidening;
use beacon_plugin_formats::{PluginFileFormatFactory, install_formats, uninstall_formats};
use beacon_plugin_sdk::file_format::SortKey;
use datafusion::error::Result;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use tokio::runtime::Handle;
use url::Url;

#[path = "plugin_format/test_formats.rs"]
mod test_formats;

use test_formats::{LineText, Seen, foreign};

struct Fixture {
    ctx: SessionContext,
    store: Arc<InMemory>,
    seen: Arc<Seen>,
    format: PluginFileFormatFactory,
}

impl Fixture {
    fn new(format: LineText) -> Self {
        Self::with_config(format, SessionConfig::new())
    }

    fn with_config(format: LineText, config: SessionConfig) -> Self {
        let config = config
            .with_extension(new_file_format_registry_handle())
            .with_extension(ArrowTypeWidening::default_extension())
            .with_extension(Arc::new(ListingFactory::dynamic()));
        let ctx = SessionContext::new_with_config(config);
        let store = Arc::new(InMemory::new());
        ctx.register_object_store(
            &Url::parse("memory://").unwrap(),
            store.clone() as Arc<dyn ObjectStore>,
        );
        let seen = format.seen.clone();
        let format = PluginFileFormatFactory::new(foreign(Arc::new(format)));
        install_formats(&ctx, std::slice::from_ref(&format), Handle::current()).unwrap();
        Self {
            ctx,
            store,
            seen,
            format,
        }
    }

    async fn sql(&self, query: &str) -> Result<Vec<RecordBatch>> {
        self.ctx.sql(query).await?.collect().await
    }

    /// The row count that a `COPY` returns.
    async fn copy(&self, query: &str) -> Result<u64> {
        let batches = self.sql(query).await?;
        Ok(batches[0].column(0).as_primitive::<UInt64Type>().value(0))
    }

    async fn error(&self, query: &str) -> String {
        match self.sql(query).await {
            Ok(_) => panic!("expected an error from: {query}"),
            Err(error) => error.to_string(),
        }
    }

    async fn text(&self, path: &str) -> String {
        let bytes = self
            .store
            .get(&Path::from(path))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    async fn objects(&self, prefix: &str) -> Vec<String> {
        let mut paths: Vec<String> = self
            .store
            .list(Some(&Path::from(prefix)))
            .map_ok(|meta| meta.location.to_string())
            .try_collect()
            .await
            .unwrap();
        paths.sort();
        paths
    }
}

fn writer() -> LineText {
    LineText {
        can_write: true,
        ..LineText::default()
    }
}

const TWO_ROWS: &str = "SELECT * FROM (VALUES (1, 'x'), (2, 'y')) AS v(a, b)";

#[tokio::test(flavor = "multi_thread")]
async fn copy_to_one_file() {
    let fx = Fixture::new(writer());
    let count = fx
        .copy(&format!(
            "COPY ({TWO_ROWS}) TO 'memory:///out/a.ltxt' STORED AS linetext"
        ))
        .await
        .unwrap();
    assert_eq!(count, 2);
    assert_eq!(fx.text("out/a.ltxt").await, "a:Int64,b:Utf8\n1,x\n2,y\n");
    let writes = fx.seen.writes.lock().unwrap();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].path, "out/a.ltxt");
    assert_eq!(writes[0].local_path, None);
}

#[tokio::test(flavor = "multi_thread")]
async fn copy_zero_rows_writes_one_file() {
    let fx = Fixture::new(writer());
    let count = fx
        .copy("COPY (SELECT 1 AS a WHERE false) TO 'memory:///out/e.ltxt' STORED AS linetext")
        .await
        .unwrap();
    assert_eq!(count, 0);
    assert_eq!(fx.seen.writes.lock().unwrap().len(), 1);
    assert_eq!(fx.objects("out").await, vec!["out/e.ltxt".to_string()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn copy_to_directory_partitioned() {
    let fx = Fixture::new(writer());
    let count = fx
        .copy(
            "COPY (SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'a')) AS v(x, p)) \
             TO 'memory:///out/' STORED AS linetext PARTITIONED BY (p)",
        )
        .await
        .unwrap();
    assert_eq!(count, 3);
    let objects = fx.objects("out").await;
    assert_eq!(objects.len(), 2, "{objects:?}");
    assert!(objects[0].starts_with("out/p=a/"), "{objects:?}");
    assert!(objects[1].starts_with("out/p=b/"), "{objects:?}");
    for write in fx.seen.writes.lock().unwrap().iter() {
        assert!(
            write.schema.field_with_name("p").is_err(),
            "{:?}",
            write.schema
        );
    }
}

/// 10 rows in 5 batches of 2, to a directory, with at least 4 files allowed.
async fn ten_rows_to_a_directory(fx: &Fixture) -> usize {
    let count = fx
        .copy(
            "COPY (SELECT value AS a FROM generate_series(1, 10)) \
             TO 'memory:///out/' STORED AS linetext",
        )
        .await
        .unwrap();
    assert_eq!(count, 10);
    fx.seen.writes.lock().unwrap().len()
}

fn small_batches() -> SessionConfig {
    let mut config = SessionConfig::new().with_batch_size(2);
    config.options_mut().execution.minimum_parallel_output_files = 4;
    config
}

#[tokio::test(flavor = "multi_thread")]
async fn split_output_spreads_rows_over_files() {
    let fx = Fixture::with_config(writer(), small_batches());
    assert!(ten_rows_to_a_directory(&fx).await > 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn split_output_false_gives_one_file() {
    let format = LineText {
        no_split: true,
        ..writer()
    };
    let fx = Fixture::with_config(format, small_batches());
    assert_eq!(ten_rows_to_a_directory(&fx).await, 1);
    let rows: usize = fx.seen.writes.lock().unwrap()[0]
        .rows
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(rows, 10);
}

#[tokio::test(flavor = "multi_thread")]
async fn single_file_output_option() {
    let fx = Fixture::new(writer());
    fx.copy(&format!(
        "COPY ({TWO_ROWS}) TO 'memory:///out/x.ltxt' STORED AS linetext \
         OPTIONS ('single_file_output' 'false')"
    ))
    .await
    .unwrap();
    let objects = fx.objects("out").await;
    assert!(!objects.is_empty());
    for object in &objects {
        assert!(object.starts_with("out/x.ltxt/"), "{objects:?}");
    }
    for options in fx.seen.options.lock().unwrap().iter() {
        assert!(!options.contains_key("single_file_output"), "{options:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn write_ordering_sorts_input() {
    let format = LineText {
        ordering: vec![SortKey {
            column: "a".to_string(),
            descending: false,
            nulls_first: false,
        }],
        ..writer()
    };
    let fx = Fixture::new(format);
    fx.copy(
        "COPY (SELECT * FROM (VALUES (3), (1), (2)) AS v(a)) \
         TO 'memory:///out/s.ltxt' STORED AS linetext",
    )
    .await
    .unwrap();
    let writes = fx.seen.writes.lock().unwrap();
    let values: Vec<i64> = writes[0]
        .rows
        .iter()
        .flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec())
        .collect();
    assert_eq!(values, vec![1, 2, 3]);
}

#[tokio::test(flavor = "multi_thread")]
async fn write_ordering_unknown_column_is_a_plan_error() {
    let format = LineText {
        ordering: vec![SortKey {
            column: "nope".to_string(),
            descending: false,
            nulls_first: false,
        }],
        ..writer()
    };
    let fx = Fixture::new(format);
    let error = fx
        .error(&format!(
            "COPY ({TWO_ROWS}) TO 'memory:///out/n.ltxt' STORED AS linetext"
        ))
        .await;
    assert!(error.contains("nope"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn cannot_write_is_a_clear_error() {
    let fx = Fixture::new(LineText::default());
    let error = fx
        .error(&format!(
            "COPY ({TWO_ROWS}) TO 'memory:///out/c.ltxt' STORED AS linetext"
        ))
        .await;
    assert!(
        error.contains("file format linetext cannot write"),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn writer_error_fails_the_copy() {
    let fx = Fixture::new(writer());
    let error = fx
        .error(&format!(
            "COPY ({TWO_ROWS}) TO 'memory:///out/f.ltxt' STORED AS linetext \
             OPTIONS ('fail_write' 'true')"
        ))
        .await;
    assert!(error.contains("write failed"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn uninstalled_format_refuses_copy() {
    let fx = Fixture::new(writer());
    uninstall_formats(&fx.ctx, std::slice::from_ref(&fx.format));
    let error = fx
        .error(&format!(
            "COPY ({TWO_ROWS}) TO 'memory:///out/u.ltxt' STORED AS linetext"
        ))
        .await;
    assert!(error.contains("is not installed"), "{error}");
}
