//! Scalar index management for managed Lance tables.
//!
//! Lance supports native secondary indexes — something Iceberg/Delta lack.
//! Beacon exposes the scalar kinds useful for admin filtering: BTREE (range /
//! equality), BITMAP (low-cardinality), ZONEMAP (block pruning), BLOOMFILTER
//! (equality on high-cardinality columns), and INVERTED (full-text search on
//! string columns). Index creation scans the column once and commits a new
//! dataset version; queries then use the index automatically.
//!
//! ZONEMAP matters more here than it looks. Parquet writes row-group min/max
//! statistics automatically, and DataFusion prunes with them for free. Lance
//! file version 2.1 and later writes no such statistics at all: its pruning path
//! (`LancePushdownScanExec`) reads them via `legacy_read_page_stats`, which only
//! finds anything in pre-2.1 files. A zone map is the explicit equivalent, and
//! without one a selective filter over a Lance table decodes every row.

use lance::dataset::builder::DatasetBuilder;
use lance::index::DatasetIndexExt;
use lance_index::scalar::inverted::InvertedIndexParams;
use lance_index::scalar::ScalarIndexParams;
use lance_index::IndexType;

use crate::warehouse::LanceWarehouse;

/// Scalar index kinds exposed via `CREATE INDEX ... USING <kind>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarIndexKind {
    /// Range / equality lookups (the default).
    BTree,
    /// Low-cardinality columns (few distinct values).
    Bitmap,
    /// Min/max per block of rows: the explicit equivalent of parquet's row-group
    /// statistics. Cheap to build and small on disk, and the right default for
    /// pruning a large table by a range or equality predicate.
    ZoneMap,
    /// Equality lookups on high-cardinality columns, where a btree's key storage
    /// would be large.
    BloomFilter,
    /// Full-text search over string columns.
    Inverted,
}

impl ScalarIndexKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            ScalarIndexKind::BTree => "btree",
            ScalarIndexKind::Bitmap => "bitmap",
            ScalarIndexKind::ZoneMap => "zonemap",
            ScalarIndexKind::BloomFilter => "bloomfilter",
            ScalarIndexKind::Inverted => "inverted",
        }
    }

    fn index_type(&self) -> IndexType {
        match self {
            ScalarIndexKind::BTree => IndexType::BTree,
            ScalarIndexKind::Bitmap => IndexType::Bitmap,
            ScalarIndexKind::ZoneMap => IndexType::ZoneMap,
            ScalarIndexKind::BloomFilter => IndexType::BloomFilter,
            ScalarIndexKind::Inverted => IndexType::Inverted,
        }
    }
}

impl std::str::FromStr for ScalarIndexKind {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "btree" => Ok(ScalarIndexKind::BTree),
            "bitmap" => Ok(ScalarIndexKind::Bitmap),
            "zonemap" | "zone_map" => Ok(ScalarIndexKind::ZoneMap),
            "bloomfilter" | "bloom_filter" | "bloom" => Ok(ScalarIndexKind::BloomFilter),
            "inverted" | "fts" => Ok(ScalarIndexKind::Inverted),
            other => Err(format!(
                "unknown index type '{other}', expected one of 'btree', 'bitmap', \
                 'zonemap', 'bloomfilter', or 'inverted'"
            )),
        }
    }
}

/// A listed index on a managed Lance table.
#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub name: String,
    pub columns: Vec<String>,
}

/// Create a scalar index named `name` on `column` of the Lance table at
/// `location`. Errors if an index with that name already exists.
pub async fn create_index(
    warehouse: &LanceWarehouse,
    uri: &str,
    column: &str,
    name: &str,
    kind: ScalarIndexKind,
) -> anyhow::Result<()> {
    tracing::info!(uri = %uri, column, name, kind = kind.as_str(), "creating Lance index");

    let lock = warehouse.lock(uri);
    let _guard = lock.lock().await;

    let mut dataset = DatasetBuilder::from_uri(uri)
        .with_session(warehouse.session())
        .load()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to open Lance dataset '{uri}': {e}"))?;

    // INVERTED needs its own params type (carrying the tokenizer config); BTREE
    // and BITMAP use the generic scalar params. Passing ScalarIndexParams for an
    // inverted index makes Lance fail to deserialize ("missing field
    // base_tokenizer").
    let index_type = kind.index_type();
    let result = match kind {
        ScalarIndexKind::Inverted => {
            let params = InvertedIndexParams::default();
            dataset
                .create_index(&[column], index_type, Some(name.to_string()), &params, false)
                .await
        }
        ScalarIndexKind::BTree
        | ScalarIndexKind::Bitmap
        | ScalarIndexKind::ZoneMap
        | ScalarIndexKind::BloomFilter => {
            // Build the params from `index_type` rather than using
            // `ScalarIndexParams::default()`, which hardcodes `index_type:
            // "btree"`. Lance happens to ignore that field here (it rebuilds the
            // params from the `IndexType` argument and only reads our `params`
            // JSON), but a struct that claims "btree" while creating a zone map
            // is one refactor away from silently creating the wrong index.
            let builtin = index_type.try_into().map_err(|e| {
                anyhow::anyhow!("Lance has no builtin scalar index for {index_type:?}: {e}")
            })?;
            let params = ScalarIndexParams::for_builtin(builtin);
            dataset
                .create_index(&[column], index_type, Some(name.to_string()), &params, false)
                .await
        }
    };
    result.map_err(|e| {
        anyhow::anyhow!("Failed to create index '{name}' on column '{column}': {e}")
    })?;
    Ok(())
}

/// Drop the index named `name` from the Lance table at `uri`.
pub async fn drop_index(
    warehouse: &LanceWarehouse,
    uri: &str,
    name: &str,
) -> anyhow::Result<()> {
    tracing::info!(uri = %uri, name, "dropping Lance index");

    let lock = warehouse.lock(uri);
    let _guard = lock.lock().await;

    let mut dataset = DatasetBuilder::from_uri(uri)
        .with_session(warehouse.session())
        .load()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to open Lance dataset '{uri}': {e}"))?;

    dataset
        .drop_index(name)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to drop index '{name}': {e}"))?;
    Ok(())
}

/// List the indexes on the Lance table at `uri` (name + indexed columns).
pub async fn list_indices(
    warehouse: &LanceWarehouse,
    uri: &str,
) -> anyhow::Result<Vec<IndexInfo>> {
    let dataset = DatasetBuilder::from_uri(uri)
        .with_session(warehouse.session())
        .load()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to open Lance dataset '{uri}': {e}"))?;

    let indices = dataset
        .load_indices()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to load indices for '{uri}': {e}"))?;
    let schema = dataset.schema();

    let mut out = Vec::with_capacity(indices.len());
    for index in indices.iter() {
        let columns = index
            .fields
            .iter()
            .filter_map(|field_id| schema.field_by_id(*field_id).map(|f| f.name.clone()))
            .collect::<Vec<_>>();
        out.push(IndexInfo {
            name: index.name.clone(),
            columns,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn parses_every_kind_case_insensitively_and_trims() {
        assert_eq!(
            ScalarIndexKind::from_str("btree").unwrap(),
            ScalarIndexKind::BTree
        );
        assert_eq!(
            ScalarIndexKind::from_str("  BITMAP ").unwrap(),
            ScalarIndexKind::Bitmap
        );
        assert_eq!(
            ScalarIndexKind::from_str("Inverted").unwrap(),
            ScalarIndexKind::Inverted
        );
        // `fts` is an accepted alias for the inverted (full-text) index.
        assert_eq!(
            ScalarIndexKind::from_str("fts").unwrap(),
            ScalarIndexKind::Inverted
        );
    }

    #[test]
    fn unknown_kind_reports_the_accepted_values() {
        let err = ScalarIndexKind::from_str("hnsw").unwrap_err();
        assert!(err.contains("btree"), "{err}");
        assert!(err.contains("bitmap"), "{err}");
        assert!(err.contains("inverted"), "{err}");
    }

    /// `as_str` is the DDL/round-trip spelling; parsing it back must be stable
    /// for every kind (note: `inverted` -> "inverted", never "fts").
    #[test]
    fn as_str_round_trips_through_from_str() {
        for kind in [
            ScalarIndexKind::BTree,
            ScalarIndexKind::Bitmap,
            ScalarIndexKind::Inverted,
        ] {
            assert_eq!(ScalarIndexKind::from_str(kind.as_str()).unwrap(), kind);
        }
        assert_eq!(ScalarIndexKind::Inverted.as_str(), "inverted");
    }
}

/// Create the default indexes for a newly written table.
///
/// * **Zone map on every column.** Lance file version 2.1 and later writes no
///   per-page statistics, so an unindexed Lance scan cannot skip blocks the way
///   a parquet reader skips row groups with its min/max statistics. A zone map is
///   the explicit equivalent. Measured on a 100M-row, 105-column table: 17 zone
///   maps took 15.8s to build and added 7MB to a 22.35GB table, while making
///   filtered scans 2.9-4.4x faster.
/// * **Bloom filter on every string column.** Zone maps only help when a column's
///   values are clustered by block; for high-cardinality strings a min/max range
///   usually spans everything and prunes nothing. A bloom filter answers equality
///   probes directly instead.
///
/// Columns Lance cannot index are skipped rather than failing the whole call: an
/// index is an optimisation, and losing one must not fail the `CREATE TABLE` that
/// triggered it.
pub async fn create_default_indexes(
    warehouse: &LanceWarehouse,
    uri: &str,
    schema: &arrow::datatypes::Schema,
) -> anyhow::Result<usize> {
    let mut created = 0;
    for field in schema.fields() {
        let column = field.name();
        let mut kinds = vec![(ScalarIndexKind::ZoneMap, format!("zm_{column}"))];
        if is_string_like(field.data_type()) {
            kinds.push((ScalarIndexKind::BloomFilter, format!("bf_{column}")));
        }
        for (kind, name) in kinds {
            match create_index(warehouse, uri, column, &name, kind).await {
                Ok(()) => created += 1,
                Err(e) => {
                    tracing::debug!(
                        uri = %uri, column, kind = kind.as_str(), error = %e,
                        "skipping default index"
                    );
                }
            }
        }
    }
    tracing::info!(uri = %uri, created, columns = schema.fields().len(), "created default indexes");
    Ok(created)
}

/// String/binary columns, which get a bloom filter in addition to a zone map.
fn is_string_like(dt: &arrow::datatypes::DataType) -> bool {
    use arrow::datatypes::DataType::*;
    matches!(
        dt,
        Utf8 | LargeUtf8 | Utf8View | Binary | LargeBinary | BinaryView
    )
}
