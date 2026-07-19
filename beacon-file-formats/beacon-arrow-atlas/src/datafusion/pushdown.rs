use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, ListArray, ListBuilder, UInt64Array};
use arrow::datatypes::{DataType, SchemaRef};
use atlas::{Atlas, Attr, StatValue};
use datafusion::common::Column;
use datafusion::common::pruning::PruningStatistics;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::scalar::ScalarValue;

/// Prune atlas datasets whose statistics cannot satisfy `predicate`.
///
/// Walks every dataset in `store`, gathers per-column statistics
/// (`view.array_stats` for array variables; the attribute value itself for
/// metadata attributes, which are scalars), and runs them through
/// DataFusion's [`PruningPredicate`] to drop datasets that cannot match.
///
/// On any error path that prevents pruning — predicate unsupported,
/// schema derivation failure on an individual dataset, etc. — the
/// function falls back to keeping all datasets so callers never lose
/// rows due to a pruning hiccup.
pub async fn pushdown_with_statistics(
    store: Arc<Atlas>,
    predicate: Arc<dyn PhysicalExpr>,
    table_schema: SchemaRef,
) -> datafusion::error::Result<Vec<String>> {
    // let dataset_names = store.list_datasets();
    // if dataset_names.is_empty() {
    //     return Ok(dataset_names);
    // }

    // let mut views = Vec::with_capacity(dataset_names.len());
    // for name in &dataset_names {
    //     let view = store.open_dataset(name).await.map_err(|e| {
    //         tracing::debug!(dataset = %name, error = %e, "failed to open atlas dataset for predicate pruning");
    //         datafusion::error::DataFusionError::Execution(format!(
    //             "Failed to open atlas dataset '{name}' for pruning: {e}"
    //         ))
    //     })?;
    //     views.push(view);
    // }

    // let pruning_predicate = match PruningPredicate::try_new(predicate, table_schema.clone()) {
    //     Ok(p) => p,
    //     Err(_) => return Ok(dataset_names),
    // };

    // let referenced = collect_columns(pruning_predicate.orig_expr());

    // let mut columns: HashMap<String, ColumnPackedStats> = HashMap::new();
    // for col in &referenced {
    //     let col_name = col.name();
    //     let Ok(field) = table_schema.field_with_name(col_name) else {
    //         continue;
    //     };
    //     let target_dtype = field.data_type().clone();
    //     let null_scalar = ScalarValue::try_from(&target_dtype).unwrap_or(ScalarValue::Null);

    //     let mut mins: Vec<ScalarValue> = Vec::with_capacity(views.len());
    //     let mut maxes: Vec<ScalarValue> = Vec::with_capacity(views.len());
    //     let mut null_counts: Vec<Option<u64>> = Vec::with_capacity(views.len());
    //     let mut row_counts: Vec<Option<u64>> = Vec::with_capacity(views.len());

    //     for view in &views {
    //         let meta = view.array_stats();
    //         if meta.arrays.contains_key(col_name) {
    //             match view.array_stats(col_name).await {
    //                 Some(stats) => {
    //                     mins.push(stat_value_to_scalar(
    //                         stats.min.as_ref(),
    //                         &target_dtype,
    //                         &null_scalar,
    //                     ));
    //                     maxes.push(stat_value_to_scalar(
    //                         stats.max.as_ref(),
    //                         &target_dtype,
    //                         &null_scalar,
    //                     ));
    //                     null_counts.push(Some(stats.null_count));
    //                     row_counts.push(Some(stats.row_count));
    //                 }
    //                 None => {
    //                     // Stats not computed (e.g. unflushed write). Treat
    //                     // as "unknown" so the predicate cannot prune this
    //                     // dataset on this column.
    //                     mins.push(null_scalar.clone());
    //                     maxes.push(null_scalar.clone());
    //                     null_counts.push(None);
    //                     row_counts.push(None);
    //                 }
    //             }
    //         } else if let Some(attr) = meta.attributes.get(col_name) {
    //             let scalar = attr_to_scalar(attr, &target_dtype, &null_scalar);
    //             mins.push(scalar.clone());
    //             maxes.push(scalar);
    //             null_counts.push(Some(0));
    //             row_counts.push(Some(1));
    //         } else {
    //             // Column is absent from this dataset — at read time the
    //             // SchemaAdapter fills it with NULLs, so the dataset's
    //             // contribution for this column really is "all null".
    //             mins.push(null_scalar.clone());
    //             maxes.push(null_scalar.clone());
    //             null_counts.push(Some(1));
    //             row_counts.push(Some(1));
    //         }
    //     }

    //     let Ok(min_arr) = ScalarValue::iter_to_array(mins) else {
    //         continue;
    //     };
    //     let Ok(max_arr) = ScalarValue::iter_to_array(maxes) else {
    //         continue;
    //     };
    //     let null_arr: ArrayRef = Arc::new(UInt64Array::from(null_counts));
    //     let row_arr: ArrayRef = Arc::new(UInt64Array::from(row_counts));

    //     columns.insert(
    //         col_name.to_string(),
    //         ColumnPackedStats {
    //             min: min_arr,
    //             max: max_arr,
    //             null_count: null_arr,
    //             row_count: row_arr,
    //         },
    //     );
    // }

    // let pruning_stats = AtlasDatasetPruningStatistics {
    //     num_datasets: views.len(),
    //     columns,
    // };
    // let mask = pruning_predicate.prune(&pruning_stats).map_err(|e| {
    //     datafusion::error::DataFusionError::Execution(format!(
    //         "Failed to prune atlas datasets: {e}"
    //     ))
    // })?;

    // Ok(dataset_names
    //     .into_iter()
    //     .zip(mask)
    //     .filter_map(|(name, keep)| keep.then_some(name))
    //     .collect())
    todo!()
}

struct ColumnPackedStats {
    min: ArrayRef,
    max: ArrayRef,
    null_count: ArrayRef,
    row_count: ArrayRef,
}

struct AtlasDatasetPruningStatistics {
    num_datasets: usize,
    columns: HashMap<String, ColumnPackedStats>,
}

impl PruningStatistics for AtlasDatasetPruningStatistics {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        self.columns.get(column.name()).map(|c| c.min.clone())
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        self.columns.get(column.name()).map(|c| c.max.clone())
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        self.columns
            .get(column.name())
            .map(|c| c.null_count.clone())
    }

    fn row_counts(&self, column: &Column) -> Option<ArrayRef> {
        self.columns.get(column.name()).map(|c| c.row_count.clone())
    }

    fn num_containers(&self) -> usize {
        self.num_datasets
    }

    fn contained(
        &self,
        _column: &Column,
        _values: &std::collections::HashSet<ScalarValue>,
    ) -> Option<BooleanArray> {
        None
    }
}

/// Convert an `atlas::StatValue` into a `ScalarValue` of `target` type.
///
/// Goes through a canonical wide form (`Int64`/`UInt64`/`Float64`/
/// `Utf8`/`Binary`/`TimestampNanosecond`) and then casts to the target.
/// Returns `null_scalar` on any conversion failure.
fn stat_value_to_scalar(
    value: Option<&StatValue>,
    target: &DataType,
    null_scalar: &ScalarValue,
) -> ScalarValue {
    let canonical = match value {
        Some(StatValue::Int(x)) => ScalarValue::Int64(Some(*x)),
        Some(StatValue::UInt(x)) => ScalarValue::UInt64(Some(*x)),
        Some(StatValue::Float(x)) => ScalarValue::Float64(Some(*x)),
        Some(StatValue::Bytes(b)) => match std::str::from_utf8(b) {
            Ok(s) => ScalarValue::Utf8(Some(s.to_string())),
            Err(_) => ScalarValue::Binary(Some(b.clone())),
        },
        Some(StatValue::TimestampNs(x)) => ScalarValue::TimestampNanosecond(Some(*x), None),
        None => return null_scalar.clone(),
    };
    canonical
        .cast_to(target)
        .unwrap_or_else(|_| null_scalar.clone())
}

/// Convert an `atlas::Attr` into a `ScalarValue` of `target` type.
fn attr_to_scalar(attr: &Attr, target: &DataType, null_scalar: &ScalarValue) -> ScalarValue {
    let canonical = match attr {
        Attr::Bool(v) => ScalarValue::Boolean(Some(*v)),
        Attr::BoolList(v) => {
            let mut builder = ListBuilder::new(arrow::array::BooleanBuilder::new());
            for x in v {
                builder.values().append_value(*x);
                builder.append(true);
            }
            ScalarValue::List(Arc::new(builder.finish()))
        }
        Attr::Int8(v) => ScalarValue::Int8(Some(*v)),
        Attr::Int16(v) => ScalarValue::Int16(Some(*v)),
        Attr::Int32(v) => ScalarValue::Int32(Some(*v)),
        Attr::Int64(v) => ScalarValue::Int64(Some(*v)),
        Attr::UInt8(v) => ScalarValue::UInt8(Some(*v)),
        Attr::UInt16(v) => ScalarValue::UInt16(Some(*v)),
        Attr::UInt32(v) => ScalarValue::UInt32(Some(*v)),
        Attr::UInt64(v) => ScalarValue::UInt64(Some(*v)),
        Attr::Float32(v) => ScalarValue::Float32(Some(*v)),
        Attr::Float64(v) => ScalarValue::Float64(Some(*v)),
        Attr::Int8List(v) => {
            let mut builder = ListBuilder::new(arrow::array::Int8Builder::new());
            for x in v {
                builder.values().append_value(*x);
                builder.append(true);
            }
            ScalarValue::List(Arc::new(builder.finish()))
        }
        Attr::Int16List(v) => {
            let mut builder = ListBuilder::new(arrow::array::Int16Builder::new());
            for x in v {
                builder.values().append_value(*x);
                builder.append(true);
            }
            ScalarValue::List(Arc::new(builder.finish()))
        }
        Attr::Int32List(v) => {
            let mut builder = ListBuilder::new(arrow::array::Int32Builder::new());
            for x in v {
                builder.values().append_value(*x);
                builder.append(true);
            }
            ScalarValue::List(Arc::new(builder.finish()))
        }
        Attr::Int64List(v) => {
            let mut builder = ListBuilder::new(arrow::array::Int64Builder::new());
            for x in v {
                builder.values().append_value(*x);
                builder.append(true);
            }
            ScalarValue::List(Arc::new(builder.finish()))
        }

        Attr::UInt8List(v) => {
            let mut builder = ListBuilder::new(arrow::array::UInt8Builder::new());
            for x in v {
                builder.values().append_value(*x);
                builder.append(true);
            }
            ScalarValue::List(Arc::new(builder.finish()))
        }

        Attr::UInt16List(v) => {
            let mut builder = ListBuilder::new(arrow::array::UInt16Builder::new());
            for x in v {
                builder.values().append_value(*x);
                builder.append(true);
            }
            ScalarValue::List(Arc::new(builder.finish()))
        }

        Attr::UInt32List(v) => {
            let mut builder = ListBuilder::new(arrow::array::UInt32Builder::new());
            for x in v {
                builder.values().append_value(*x);
                builder.append(true);
            }
            ScalarValue::List(Arc::new(builder.finish()))
        }

        Attr::UInt64List(v) => {
            let mut builder = ListBuilder::new(arrow::array::UInt64Builder::new());
            for x in v {
                builder.values().append_value(*x);
                builder.append(true);
            }
            ScalarValue::List(Arc::new(builder.finish()))
        }

        Attr::Float32List(v) => {
            let mut builder = ListBuilder::new(arrow::array::Float32Builder::new());
            for x in v {
                builder.values().append_value(*x);
                builder.append(true);
            }
            ScalarValue::List(Arc::new(builder.finish()))
        }

        Attr::Float64List(v) => {
            let mut builder = ListBuilder::new(arrow::array::Float64Builder::new());
            for x in v {
                builder.values().append_value(*x);
                builder.append(true);
            }
            ScalarValue::List(Arc::new(builder.finish()))
        }

        Attr::String(v) => ScalarValue::Utf8(Some(v.clone())),
        Attr::StringList(v) => {
            let mut builder = ListBuilder::new(arrow::array::StringBuilder::new());
            for x in v {
                builder.values().append_value(x);
                builder.append(true);
            }
            ScalarValue::List(Arc::new(builder.finish()))
        }
        Attr::Binary(v) => ScalarValue::Binary(Some(v.clone())),
        Attr::BinaryList(v) => {
            let mut builder = ListBuilder::new(arrow::array::BinaryBuilder::new());
            for x in v {
                builder.values().append_value(x);
                builder.append(true);
            }
            ScalarValue::List(Arc::new(builder.finish()))
        }

        Attr::TimestampNanoseconds(v) => ScalarValue::TimestampNanosecond(Some(*v), None),
    };
    canonical
        .cast_to(target)
        .unwrap_or_else(|_| null_scalar.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compat::atlas_view_arrow_schema;
    use crate::datafusion::cache::get_or_open_atlas;
    use crate::datafusion::test_support::{fixture_marker_object_meta, test_store};
    use beacon_common::super_typing::super_type_schema;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{self, BinaryExpr};
    use datafusion::scalar::ScalarValue;

    async fn fixture_atlas() -> (Arc<Atlas>, SchemaRef) {
        let store = test_store().await;
        let marker = fixture_marker_object_meta();
        let atlas = get_or_open_atlas(None, store, &marker)
            .await
            .expect("open atlas");

        let names = atlas.list_datasets();
        let mut schemas = Vec::with_capacity(names.len());
        for name in &names {
            let view = atlas.open_dataset(name).await.expect("open ds");
            schemas.push(Arc::new(
                atlas_view_arrow_schema(&view, None).expect("schema"),
            ));
        }
        let table_schema: SchemaRef =
            Arc::new(super_type_schema(&schemas).expect("super type schema"));
        (atlas, table_schema)
    }

    fn binary(
        left: Arc<dyn PhysicalExpr>,
        op: Operator,
        right: Arc<dyn PhysicalExpr>,
    ) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(left, op, right))
    }

    #[tokio::test]
    async fn prune_drops_dataset_when_array_range_excludes_predicate() {
        let (atlas, schema) = fixture_atlas().await;
        // winter.temperature ∈ [1, 4], summer.temperature ∈ [20, 22]
        let col = expressions::col("temperature", &schema).expect("col");
        let lit = expressions::lit(ScalarValue::Float32(Some(10.0)));
        let predicate = binary(col, Operator::Gt, lit);

        let kept = pushdown_with_statistics(atlas, predicate, schema)
            .await
            .expect("prune");
        assert_eq!(kept, vec!["summer".to_string()]);
    }

    #[tokio::test]
    async fn prune_uses_attribute_value_for_metadata_columns() {
        let (atlas, schema) = fixture_atlas().await;
        let col = expressions::col("season", &schema).expect("col");
        let lit = expressions::lit(ScalarValue::Utf8(Some("winter".into())));
        let predicate = binary(col, Operator::Eq, lit);

        let kept = pushdown_with_statistics(atlas, predicate, schema)
            .await
            .expect("prune");
        assert_eq!(kept, vec!["winter".to_string()]);
    }

    #[tokio::test]
    async fn prune_treats_missing_column_as_all_null() {
        // `cycle` exists in winter (values 10..40) but not in summer.
        // Predicate `cycle = 1` excludes winter (range [10,40] doesn't
        // contain 1) and excludes summer (all-null can't equal 1).
        let (atlas, schema) = fixture_atlas().await;
        let col = expressions::col("cycle", &schema).expect("col");
        let lit = expressions::lit(ScalarValue::Int32(Some(1)));
        let predicate = binary(col, Operator::Eq, lit);

        let kept = pushdown_with_statistics(atlas, predicate, schema)
            .await
            .expect("prune");
        assert!(kept.is_empty(), "expected both pruned, got {kept:?}");
    }

    #[tokio::test]
    async fn prune_keeps_dataset_when_missing_only_in_one() {
        // Predicate `cycle = 20` keeps winter ([10,40] could contain 20)
        // and prunes summer (no cycle column).
        let (atlas, schema) = fixture_atlas().await;
        let col = expressions::col("cycle", &schema).expect("col");
        let lit = expressions::lit(ScalarValue::Int32(Some(20)));
        let predicate = binary(col, Operator::Eq, lit);

        let kept = pushdown_with_statistics(atlas, predicate, schema)
            .await
            .expect("prune");
        assert_eq!(kept, vec!["winter".to_string()]);
    }

    #[tokio::test]
    async fn prune_returns_all_when_no_datasets_match_or_predicate_trivial() {
        // `temperature >= -1000` keeps everything.
        let (atlas, schema) = fixture_atlas().await;
        let col = expressions::col("temperature", &schema).expect("col");
        let lit = expressions::lit(ScalarValue::Float32(Some(-1000.0)));
        let predicate = binary(col, Operator::GtEq, lit);

        let mut kept = pushdown_with_statistics(atlas, predicate, schema)
            .await
            .expect("prune");
        kept.sort();
        assert_eq!(kept, vec!["summer".to_string(), "winter".to_string()]);
    }
}
