//! The `list_datasets` table function: format-driven discovery over the
//! datasets store, with the optional glob pattern / offset / limit arguments.
//!
//! `list_datasets([pattern[, offset[, limit]]])` is the SQL surface the HTTP
//! dataset listing endpoints are built on, so this file is what guarantees the
//! listing (and its pagination contract) keeps working.

mod common;

use common::{column_strings, runtime, scalar_i64, total_rows, write_file, TestRuntime};

/// Seeds a small dataset tree: three CSVs at different depths plus one parquet.
async fn seeded_runtime(tag: &str) -> TestRuntime {
    let rt = runtime(tag).await;
    write_file(&rt.datasets_dir().join("a.csv"), "v,name\n1,a\n");
    write_file(&rt.datasets_dir().join("sub/b.csv"), "v,name\n2,b\n");
    write_file(&rt.datasets_dir().join("sub/deep/c.csv"), "v,name\n3,c\n");
    std::fs::copy(
        parquet_fixture(),
        rt.datasets_dir().join("p.parquet"),
    )
    .expect("copy parquet fixture");
    rt
}

fn parquet_fixture() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root")
        .join("test-datasets/test_file.parquet")
}

/// Sorted file names from a `list_datasets` invocation.
async fn names(rt: &TestRuntime, args: &str) -> Vec<String> {
    let mut names = column_strings(
        &rt.sql(&format!(
            "SELECT file_name FROM list_datasets({args}) ORDER BY file_name"
        ))
        .await,
        0,
    );
    names.sort();
    names
}

#[tokio::test(flavor = "multi_thread")]
async fn no_arguments_list_every_discovered_dataset() {
    let rt = seeded_runtime("list-datasets-all").await;

    assert_eq!(
        names(&rt, "").await,
        vec!["a.csv", "p.parquet", "sub/b.csv", "sub/deep/c.csv"],
        "all four seeded datasets should be discovered"
    );

    // The full metadata shape: format, inspectability, size and mtime.
    let batches = rt
        .sql(
            "SELECT file_name, file_format, can_inspect, can_partial_explore, \
                    size, last_modified \
             FROM list_datasets() WHERE file_name = 'a.csv'",
        )
        .await;
    assert_eq!(total_rows(&batches), 1);

    let format = column_strings(&batches, 1);
    assert!(!format[0].is_empty(), "file_format should be reported");

    // Local files always have a resolvable size and mtime; the reported size is
    // the actual on-disk byte length.
    let on_disk = std::fs::metadata(rt.datasets_dir().join("a.csv"))
        .expect("a.csv exists")
        .len();
    let size = batches[0]
        .column(4)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::UInt64Array>()
        .expect("size is UInt64");
    assert_eq!(size.value(0), on_disk, "size should be the on-disk byte length");
    let last_modified = column_strings(&batches, 5);
    chrono::DateTime::parse_from_rfc3339(&last_modified[0])
        .expect("last_modified should be RFC 3339");
}

#[tokio::test(flavor = "multi_thread")]
async fn pattern_narrows_the_listing() {
    let rt = seeded_runtime("list-datasets-pattern").await;

    assert_eq!(
        names(&rt, "'sub/**'").await,
        vec!["sub/b.csv", "sub/deep/c.csv"],
        "a directory glob should list only that subtree"
    );
    assert_eq!(
        names(&rt, "'**/*.parquet'").await,
        vec!["p.parquet"],
        "an extension glob should list only that format"
    );
    assert!(
        names(&rt, "'nothing/**'").await.is_empty(),
        "a glob matching nothing should list nothing"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn offset_and_limit_paginate_the_listing() {
    let rt = seeded_runtime("list-datasets-paging").await;

    let full = column_strings(
        &rt.sql("SELECT file_name FROM list_datasets()").await,
        0,
    );
    assert_eq!(full.len(), 4);

    // `limit` caps the page size; `offset` skips from the front. Discovery order
    // is stable between calls (same store, same walk), so the first page plus
    // the remainder must reassemble the full listing.
    let first_page = column_strings(
        &rt.sql("SELECT file_name FROM list_datasets('**/*', 0, 2)")
            .await,
        0,
    );
    assert_eq!(first_page.len(), 2, "limit should cap the page");

    let rest = column_strings(
        &rt.sql("SELECT file_name FROM list_datasets('**/*', 2)").await,
        0,
    );
    assert_eq!(rest.len(), 2, "offset should skip the first page");

    let mut reassembled = first_page;
    reassembled.extend(rest);
    assert_eq!(
        reassembled, full,
        "page + remainder should reassemble the unpaginated listing"
    );

    // Paging past the end is empty, not an error. Regression: the pagination
    // used to compute `take(end - start)`, which underflows (and panics in a
    // debug build) once the offset exceeds the number of datasets.
    assert_eq!(
        total_rows(&rt.sql("SELECT * FROM list_datasets('**/*', 100)").await),
        0,
        "an offset past the end should return no rows"
    );
    assert_eq!(
        total_rows(&rt.sql("SELECT * FROM list_datasets('**/*', 100, 10)").await),
        0,
        "an offset past the end with a limit should also return no rows"
    );
}

/// The listing is a real table: it composes with WHERE / aggregates like any
/// other relation instead of being a special-cased endpoint.
#[tokio::test(flavor = "multi_thread")]
async fn listing_composes_with_sql() {
    let rt = seeded_runtime("list-datasets-compose").await;

    let csv_count = scalar_i64(
        &rt.sql("SELECT count(*) FROM list_datasets() WHERE file_name LIKE '%.csv'")
            .await,
    );
    assert_eq!(csv_count, 3, "the three CSVs should be countable through SQL");
}
