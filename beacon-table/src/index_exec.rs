//! Index types and (future) index execution for Beacon tables.
//!
//! Defines [`IndexType`], the serializable enum describing table index
//! configurations stored in the manifest. The [`IndexExec`] execution plan
//! is a placeholder for future index-building operations.

use std::sync::Arc;

use datafusion::physical_plan::{DisplayAs, ExecutionPlan};
use tokio::sync::Mutex;

use crate::manifest;

/// Describes the type of index configured on a Beacon table.
///
/// Stored in the manifest's `z_order_index` field and used to drive
/// future index-building and query-time pruning.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum IndexType {
    /// A composite index composed of multiple sub-indexes.
    Multi {
        /// The constituent index definitions.
        indexes: Vec<IndexType>,
    },
    /// A Z-order (interleaved) index over multiple columns.
    ZOrder {
        /// The columns participating in the Z-order curve.
        columns: Vec<IndexType>,
    },
    /// An index on a single named column.
    Column {
        /// Column name.
        name: String,
    },
    /// A geospatial index using geohash encoding.
    GeoHash {
        /// Name of the longitude column.
        longitude_column: String,
        /// Name of the latitude column.
        latitude_column: String,
        /// Geohash precision (number of characters).
        precision: u8,
    },
}

/// Placeholder execution plan for building or rebuilding table indexes.
///
/// **Not yet implemented** — all trait methods currently panic with `todo!()`.
#[derive(Debug)]
pub struct IndexExec {
    /// Path to the manifest JSON file.
    manifest_path: object_store::path::Path,
    /// Shared mutex for serializing mutations.
    mutation_handle: Arc<Mutex<()>>,
    /// Shared handle to the in-memory manifest.
    manifest_handle: Arc<Mutex<manifest::TableManifest>>,
    /// Manifest snapshot captured at plan creation time.
    initial_manifest: manifest::TableManifest,
}

impl IndexExec {}

impl DisplayAs for IndexExec {
    fn fmt_as(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        todo!()
    }
}

impl ExecutionPlan for IndexExec {
    fn name(&self) -> &str {
        todo!()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        todo!()
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        todo!()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
        todo!()
    }
}
