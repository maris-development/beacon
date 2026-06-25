//! Scalar index management for managed Lance tables.
//!
//! Lance supports native secondary indexes — something Iceberg/Delta lack.
//! Beacon exposes the scalar kinds useful for admin filtering: BTREE (range /
//! equality), BITMAP (low-cardinality), and INVERTED (full-text search on string
//! columns). Index creation scans the column once and commits a new dataset
//! version; queries then use the index automatically.

use lance::dataset::builder::DatasetBuilder;
use lance::index::DatasetIndexExt;
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
    /// Full-text search over string columns.
    Inverted,
}

impl ScalarIndexKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            ScalarIndexKind::BTree => "btree",
            ScalarIndexKind::Bitmap => "bitmap",
            ScalarIndexKind::Inverted => "inverted",
        }
    }

    fn index_type(&self) -> IndexType {
        match self {
            ScalarIndexKind::BTree => IndexType::BTree,
            ScalarIndexKind::Bitmap => IndexType::Bitmap,
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
            "inverted" | "fts" => Ok(ScalarIndexKind::Inverted),
            other => Err(format!(
                "unknown index type '{other}', expected 'btree', 'bitmap', or 'inverted'"
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

    let params = ScalarIndexParams::default();
    dataset
        .create_index(&[column], kind.index_type(), Some(name.to_string()), &params, false)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create index '{name}' on column '{column}': {e}"))?;
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
