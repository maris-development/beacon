//! What one scan reads, decided once and shared by every partition.

use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use beacon_datafusion_ext::nd::logical_schema;
use beacon_datafusion_ext::type_widening::ArrowTypeWideningStrategy;
use datafusion::{common::plan_err, error::Result, physical_plan::PhysicalExpr};

/// What one scan reads.
///
/// The source builds one per partition, and the opener, the queue and the
/// view of every collection the partition opens read through the same handle.
/// Nothing here changes after planning.
#[derive(Debug)]
pub struct ScanSpec {
    /// The scan's output schema, nd-encoded. Every batch goes out under it. Its
    /// field *names* are the columns to keep, and the encoding leaves names
    /// alone.
    pub projected_schema: SchemaRef,
    /// The same schema with the encoding unwrapped. The columns, the predicate
    /// and the pruning engine are written against it.
    pub logical_schema: SchemaRef,
    /// The dimensions the scan reads, or `None` for each dataset's default.
    pub read_dimensions: Option<Vec<String>>,
    /// The predicate to prune datasets with, if any.
    pub predicate: Option<Arc<dyn PhysicalExpr>>,
    /// The rule that merged the table schema. It decides which casts read null.
    pub type_widening: Arc<dyn ArrowTypeWideningStrategy>,
}

impl ScanSpec {
    /// A scan of `projected_schema`. The logical schema is derived from it.
    ///
    /// Refuses a schema of no column, see [`require_projection`].
    pub fn new(
        projected_schema: SchemaRef,
        read_dimensions: Option<Vec<String>>,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        type_widening: Arc<dyn ArrowTypeWideningStrategy>,
    ) -> Result<Self> {
        require_projection(&projected_schema)?;
        Ok(Self {
            logical_schema: logical_schema(&projected_schema)?,
            projected_schema,
            read_dimensions,
            predicate,
            type_widening,
        })
    }
}

/// Refuse a scan that projects no column.
///
/// A dataset's row count follows the dimensions of the columns it reads. With
/// no column there is no dimension set, so the count of a dataset that holds
/// arrays on different grids has no one answer. The scan refuses rather than
/// pick one. `COUNT(*)` reaches here; `COUNT(column)` projects a column and
/// does not.
pub(crate) fn require_projection(projected_schema: &Schema) -> Result<()> {
    if projected_schema.fields().is_empty() {
        return plan_err!(
            "an atlas scan must project at least one column: a dataset's row count \
             follows the dimensions of the columns it reads, and no column names none. \
             Use COUNT(column) instead of COUNT(*)"
        );
    }
    Ok(())
}
