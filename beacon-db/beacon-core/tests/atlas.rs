//! Atlas collections through an assembled runtime.
//!
//! The format crate tests the reader and the scan against its own fixtures.
//! What this covers is the wiring: that a runtime registers the format and its
//! table function, that `STORED AS ATLAS` resolves and survives a restart, and
//! that dataset pruning does not change an answer.
//!
//! An Atlas collection is one write-once file, `data.atlas`, holding many
//! datasets. These tests write real ones with the real writer.

mod common;

use std::path::Path;

use beacon_arrow_atlas::atlas::{AtlasWriter, Attr, WriterConfig};
use common::{scalar_i64, total_rows, TestRuntime};
use ndarray::arr1;

/// Write a collection of `n` datasets at `dir`, named `d0..d{n-1}`.
///
/// Dataset `i` holds `temperature: Float32[4]` over the range `[10i, 10i + 3]`
/// and the attribute `platform = "p{i}"`, so a threshold predicate has an
/// answer that can be written down.
async fn write_collection(dir: &Path, n: usize) {
    std::fs::create_dir_all(dir).expect("create the collection directory");
    let writer = AtlasWriter::create_path(dir, WriterConfig::default())
        .await
        .expect("create the collection");

    for i in 0..n {
        let mut dataset = writer
            .add_dataset(&format!("d{i}"))
            .await
            .expect("add a dataset");
        dataset
            .define_array::<f32>("temperature", vec!["obs".into()], vec![4], None, None)
            .await
            .expect("define temperature");
        let base = (10 * i) as f32;
        dataset
            .write_array(
                "temperature",
                vec![0],
                arr1(&[base, base + 1.0, base + 2.0, base + 3.0])
                    .into_dyn()
                    .view(),
            )
            .await
            .expect("write temperature");
        dataset.set_attribute("platform", Attr::String(format!("p{i}")));
        dataset.finish().await.expect("finish a dataset");
    }

    writer.finish().await.expect("finish the collection");
}

/// Every value of `temperature`, sorted, as the query returned them.
async fn temperatures(rt: &TestRuntime, sql: &str) -> Vec<f32> {
    use arrow::array::Float32Array;

    let batches = rt.sql(sql).await;
    let mut values = Vec::new();
    for batch in &batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("temperature is f32");
        values.extend(column.iter().flatten());
    }
    values
}

// ── the table function ──────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn read_atlas_reads_every_dataset() {
    let rt = common::runtime("atlas-read").await;
    write_collection(&rt.datasets_dir().join("obs"), 5).await;

    let rows = total_rows(
        &rt.sql("SELECT temperature FROM read_atlas('obs/data.atlas')")
            .await,
    );
    assert_eq!(rows, 20, "five datasets of four rows");
}

/// A glob covers several collections in one call.
#[tokio::test(flavor = "multi_thread")]
async fn read_atlas_covers_a_glob_of_collections() {
    let rt = common::runtime("atlas-glob").await;
    write_collection(&rt.datasets_dir().join("obs/january"), 2).await;
    write_collection(&rt.datasets_dir().join("obs/february"), 3).await;

    let rows = total_rows(
        &rt.sql("SELECT temperature FROM read_atlas('obs/**/data.atlas')")
            .await,
    );
    assert_eq!(rows, 20, "five datasets across two collections");
}

/// The schema counterpart is registered, and reports the columns without a
/// scan. A dataset attribute is a column under a leading dot.
#[tokio::test(flavor = "multi_thread")]
async fn read_atlas_schema_reports_the_columns() {
    let rt = common::runtime("atlas-schema").await;
    write_collection(&rt.datasets_dir().join("obs"), 2).await;

    let columns = common::column_strings(
        &rt.sql("SELECT column_name FROM read_atlas_schema('obs/data.atlas') ORDER BY column_name")
            .await,
        0,
    );
    assert_eq!(columns, vec![".platform", "temperature"]);
}

// ── the external table ──────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn an_external_table_reads_a_collection() {
    let rt = common::runtime("atlas-external").await;
    write_collection(&rt.datasets_dir().join("obs"), 5).await;

    rt.sql("CREATE EXTERNAL TABLE obs STORED AS ATLAS LOCATION 'obs/data.atlas'")
        .await;

    assert_eq!(scalar_i64(&rt.sql("SELECT count(*) FROM obs").await), 20);
}

/// A table in a Beacon-native format has to rebuild at startup. Its definition
/// names the format, and recovery resolves that through the session's registry
/// — so a format registered too late leaves the table silently missing.
#[tokio::test(flavor = "multi_thread")]
async fn an_external_table_survives_a_restart() {
    let rt = common::restartable_runtime("atlas-restart", |builder| builder).await;
    write_collection(&rt.datasets_dir().join("obs"), 4).await;

    rt.sql("CREATE EXTERNAL TABLE obs STORED AS ATLAS LOCATION 'obs/data.atlas'")
        .await;
    assert_eq!(scalar_i64(&rt.sql("SELECT count(*) FROM obs").await), 16);

    let rt = rt.restart().await;
    assert_eq!(
        scalar_i64(&rt.sql("SELECT count(*) FROM obs").await),
        16,
        "the table must come back after a restart"
    );
}

/// The dimensions argument narrows what a read returns, as it does for the
/// other nd formats.
#[tokio::test(flavor = "multi_thread")]
async fn read_atlas_takes_a_dimension_list() {
    let rt = common::runtime("atlas-dimensions").await;
    write_collection(&rt.datasets_dir().join("obs"), 2).await;

    let rows = total_rows(
        &rt.sql("SELECT temperature FROM read_atlas(['obs/data.atlas'], ['obs'])")
            .await,
    );
    assert_eq!(rows, 8, "`temperature` lives on `obs`, so it survives");
}

// ── pruning, through the assembled runtime ──────────────────────────────

/// A predicate returns the same rows whether or not whole datasets were
/// skipped to find them.
#[tokio::test(flavor = "multi_thread")]
async fn pruning_does_not_change_the_answer() {
    let rt = common::runtime_with("atlas-pruning-on", |builder| {
        builder.with_atlas_config(beacon_arrow_atlas::AtlasConfig {
            use_pruning: true,
            ..Default::default()
        })
    })
    .await;
    let unpruned = common::runtime_with("atlas-pruning-off", |builder| {
        builder.with_atlas_config(beacon_arrow_atlas::AtlasConfig {
            use_pruning: false,
            ..Default::default()
        })
    })
    .await;

    for rt in [&rt, &unpruned] {
        write_collection(&rt.datasets_dir().join("obs"), 10).await;
    }

    for predicate in [
        "temperature > 45",
        "temperature < 25",
        "temperature > 1000",
        "temperature > 45 AND temperature < 75",
    ] {
        let sql = format!(
            "SELECT temperature FROM read_atlas('obs/data.atlas') \
             WHERE {predicate} ORDER BY temperature"
        );
        assert_eq!(
            temperatures(&rt, &sql).await,
            temperatures(&unpruned, &sql).await,
            "pruning changed the answer for `{predicate}`"
        );
    }
}

/// An attribute is exact in the footer, so a predicate on one reaches the right
/// dataset without reading the others.
#[tokio::test(flavor = "multi_thread")]
async fn an_attribute_predicate_selects_one_dataset() {
    let rt = common::runtime("atlas-attribute").await;
    write_collection(&rt.datasets_dir().join("obs"), 6).await;

    let values = temperatures(
        &rt,
        r#"SELECT temperature FROM read_atlas('obs/data.atlas')
           WHERE ".platform" = 'p3' ORDER BY temperature"#,
    )
    .await;
    assert_eq!(values, vec![30.0, 31.0, 32.0, 33.0]);
}
