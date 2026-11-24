//! DataFusion column statistics derived from Zarr stores.
//!
//! [`generate_statistics`] is the entry point. It opens each leaf group of a
//! store as the ND scan opens it, and computes a min/max for each column of
//! `table_schema`.
//!
//! # The policy
//!
//! For one column of one leaf group, in this order:
//!
//! - The array states its range in its `actual_range` attribute. Metadata costs
//!   no chunk read, so this check is first. A coordinate that states its range
//!   is never read; a grid that states it gets a bound for free. See
//!   [`attribute_range`].
//! - A variable of rank 0 or rank 1 is small. It is a coordinate, the thing a
//!   `WHERE` clause names, so [`beacon_nd_scan::file_statistics`] reads it and
//!   takes the range.
//! - A variable of rank 2 or higher is a data grid. Report unknown. A full read
//!   costs far more than the pruning saves, so a scan costs exactly what it
//!   cost before.
//! - Text and binary have no orderable range. Report unknown.
//!
//! # Why a wrong answer here is worse than no answer
//!
//! A recorded range prunes files. Pruning drops a file the optimizer will never
//! open, so a range that is too narrow silently deletes matching rows from the
//! query answer. Every path below therefore reports unknown when it cannot
//! *prove* a bound: an unreadable group, a malformed attribute, a leaf group
//! that cannot bound a column the others can. Unknown is always legal — it only
//! means DataFusion scans what it might have skipped.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Field, Float64Type, Schema};
use beacon_nd_dataset::{Attributes, NdDataset, NdVariable};
use beacon_nd_scan::file_statistics;
use datafusion::{
    common::{ColumnStatistics, Statistics, stats::Precision},
    scalar::ScalarValue,
};
use nd_arrow_array::compute::cf::{ADD_OFFSET, SCALE_FACTOR};
use zarrs::group::Group;
use zarrs_storage::AsyncReadableListableStorageTraits;

use crate::{
    datafusion::opener::open_leaf,
    nd::decode::{BoundSide, time_bound},
    util::recursive_groups,
};

// ─── Public entry point ─────────────────────────────────────────────────────

/// Return DataFusion [`Statistics`] for the zarr store rooted at `group_path`.
///
/// `storage` is the same storage the scan reads over, so statistics and scans
/// can never disagree about a store: a listed zarr store passes its object
/// store, an Icechunk repository passes a repository session.
///
/// `read_dimensions` is the scan's narrowing, applied here too so a column that
/// the scan never returns is never measured. `skip_unbroadcastable` is the
/// scan's too: a leaf the scan skips is not measured either.
pub async fn generate_statistics(
    storage: Arc<dyn AsyncReadableListableStorageTraits>,
    group_path: &str,
    read_dimensions: Option<Vec<String>>,
    skip_unbroadcastable: bool,
    table_schema: &Schema,
) -> anyhow::Result<Statistics> {
    let group = Group::async_open(storage.clone(), group_path)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to open Zarr group at '{group_path}': {e}"))?;

    let mut leaves = Vec::new();
    recursive_groups(Arc::new(group), &mut leaves).await?;

    let names: Vec<String> = table_schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();

    let mut store_ranges = StoreRanges::default();
    for leaf in &leaves {
        // The scan open: a leaf that the scan skips is not measured.
        let Some(dataset) = open_leaf(
            storage.clone(),
            leaf.path().as_str(),
            read_dimensions.clone(),
            skip_unbroadcastable,
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read Zarr group as dataset: {e}"))?
        else {
            continue;
        };

        for name in &names {
            let Some(variable) = dataset.variable(name) else {
                // This leaf has no such column. It contributes nulls to the
                // scan, and a null widens no bound, so it is simply skipped —
                // not an unknown that would wipe out the other leaves' range.
                continue;
            };
            let range = column_range(name, variable.as_ref(), dataset.as_ref()).await;
            store_ranges.add(name, range);
        }
    }

    let mut stats = Statistics::default();
    for name in &names {
        stats = stats.add_column_statistics(store_ranges.column_statistics(name));
    }
    Ok(stats)
}

// ─── Folding leaf groups into one store ─────────────────────────────────────

/// One column's min/max, both exact.
type Range = (ScalarValue, ScalarValue);

/// The ranges of a whole store, folded leaf group by leaf group.
///
/// A store is one file to the statistics collector, so its recorded range must
/// cover every leaf group in it. Two rules keep that bound honest:
///
/// - Two known ranges widen into their union: the lower min, the higher max.
/// - One unknown poisons the column. A leaf that cannot bound a column may hold
///   anything, so no range over the rest of the store is a bound on the store.
#[derive(Default)]
struct StoreRanges {
    columns: HashMap<String, Option<Range>>,
}

impl StoreRanges {
    /// Fold one leaf group's answer for `name` into the store's.
    fn add(&mut self, name: &str, range: Option<Range>) {
        let merged = match self.columns.remove(name) {
            // The first leaf group to hold this column.
            None => range,
            Some(Some(current)) => range.and_then(|next| widen(current, next)),
            // Unknown is sticky. A leaf group that cannot bound the column may
            // hold anything, so nothing the other leaves say bounds the store.
            Some(None) => None,
        };
        self.columns.insert(name.to_string(), merged);
    }

    fn column_statistics(&self, name: &str) -> ColumnStatistics {
        to_column_statistics(self.columns.get(name).cloned().flatten())
    }
}

/// The union of two ranges, or `None` when they cannot be compared.
///
/// Incomparable means different scalar types — one leaf group storing `lat` as
/// f32 and another as f64, say. The super-typed table schema hides that, and
/// guessing which side to cast could tighten the bound, so the column goes
/// unknown.
fn widen(current: Range, next: Range) -> Option<Range> {
    let min = match current.0.partial_cmp(&next.0)? {
        std::cmp::Ordering::Greater => next.0,
        _ => current.0,
    };
    let max = match current.1.partial_cmp(&next.1)? {
        std::cmp::Ordering::Less => next.1,
        _ => current.1,
    };
    Some((min, max))
}

fn to_column_statistics(range: Option<Range>) -> ColumnStatistics {
    match range {
        Some((min, max)) => ColumnStatistics::new_unknown()
            .with_min_value(Precision::Exact(min))
            .with_max_value(Precision::Exact(max)),
        None => ColumnStatistics::new_unknown(),
    }
}

// ─── One column of one leaf group ───────────────────────────────────────────

/// The range of the variable `name` of `dataset`, or `None` when no bound can
/// be proven cheaply. The range has the type of the variable.
///
/// `name` is the array name for a variable and `"{array}.{attr}"` or
/// `".{attr}"` for an attribute column. Only an array has attributes to
/// consult; an attribute column is a rank-0 value that costs nothing to measure.
async fn column_range(
    name: &str,
    variable: &dyn NdVariable,
    dataset: &dyn NdDataset,
) -> Option<Range> {
    // The metadata path first: it states the range without touching a chunk.
    if let Some(range) = attribute_range(variable.attributes(), variable.data_type()) {
        return Some(range);
    }

    // A data grid is never read. This is what keeps the scan cost unchanged.
    if variable.dimensions().rank() > 1 {
        return None;
    }

    let schema = Schema::new(vec![Field::new(name, variable.data_type().clone(), true)]);
    let statistics = match file_statistics(dataset, &schema).await {
        Ok(statistics) => statistics,
        Err(error) => {
            tracing::warn!(column = name, %error, "no Zarr statistics for the column");
            return None;
        }
    };
    let column = statistics.column_statistics.into_iter().next()?;
    match (column.min_value, column.max_value) {
        (Precision::Exact(min), Precision::Exact(max)) => Some((min, max)),
        _ => None,
    }
}

// ─── The metadata shortcut ──────────────────────────────────────────────────

/// The bound an array's own metadata states, or `None`.
///
/// # Only `actual_range`
///
/// `actual_range` states the range of the values the array holds. That is a
/// bound, and it is the only attribute here that is one.
///
/// `valid_min` and `valid_max` are **not** used, and must not be. They state the
/// range of *valid* values, which is a different claim: a file may hold values
/// outside them, and Beacon's reader returns those values rather than masking
/// them. Treating them as a bound would prune a file that holds a matching row.
///
/// # Only undecoded arrays
///
/// CF puts `actual_range` in the units of the *stored* data, so a
/// `scale_factor`/`add_offset` array states a packed range while Beacon surfaces
/// the unpacked values. Files disagree about that in practice, and applying the
/// wrong reading of a packed range yields a wrong bound, so a packed array skips
/// this path entirely and is measured the same way as any other array — read
/// when it is rank 1, unknown when it is a grid. Nothing is lost: a rank-1 read
/// is the cheap case, and a packed grid was never going to be read.
///
/// CF time is decoded, because there the units are unambiguous: the attribute
/// and the chunk values are both offsets in the array's own `units`. Each end
/// converts with the decode of the variable (see [`time_bound`]), so a Julian
/// variable uses the Julian epoch. An end out of range gives no bound.
///
/// `data_type` is the Arrow type of the variable, and the range has that type.
fn attribute_range(attributes: &Attributes, data_type: &DataType) -> Option<Range> {
    let (low, high) = actual_range(attributes)?;

    // A packed array's attribute is in packed units. Leave it alone.
    if attributes.contains(SCALE_FACTOR) || attributes.contains(ADD_OFFSET) {
        return None;
    }

    match data_type {
        DataType::Timestamp(_, _) => Some((
            time_bound(data_type, attributes, low, BoundSide::Lower)?,
            time_bound(data_type, attributes, high, BoundSide::Upper)?,
        )),
        // The JSON text may round to an f64 inside the stored value. Step out one ulp.
        DataType::Float64 => Some((
            ScalarValue::Float64(Some(low.next_down())),
            ScalarValue::Float64(Some(high.next_up())),
        )),
        // A JSON number is an f64, so narrowing it to f32 may round the min up
        // or the max down by an ulp and tighten the bound. Step each one back
        // out. A bound one ulp too loose costs nothing; one ulp too tight drops
        // rows.
        DataType::Float32 => Some((
            ScalarValue::Float32(Some((low as f32).next_down())),
            ScalarValue::Float32(Some((high as f32).next_up())),
        )),
        // Likewise for integers: floor the min and ceil the max, so a value
        // written as 3.0000000001 cannot exclude 3.
        DataType::Int8 => int_range::<i8>(low, high, i8::MIN as f64, i8::MAX as f64)
            .map(|(a, b)| (ScalarValue::Int8(Some(a)), ScalarValue::Int8(Some(b)))),
        DataType::Int16 => int_range::<i16>(low, high, i16::MIN as f64, i16::MAX as f64)
            .map(|(a, b)| (ScalarValue::Int16(Some(a)), ScalarValue::Int16(Some(b)))),
        DataType::Int32 => int_range::<i32>(low, high, i32::MIN as f64, i32::MAX as f64)
            .map(|(a, b)| (ScalarValue::Int32(Some(a)), ScalarValue::Int32(Some(b)))),
        DataType::Int64 => int_range::<i64>(low, high, i64::MIN as f64, i64::MAX as f64)
            .map(|(a, b)| (ScalarValue::Int64(Some(a)), ScalarValue::Int64(Some(b)))),
        DataType::UInt8 => int_range::<u8>(low, high, u8::MIN as f64, u8::MAX as f64)
            .map(|(a, b)| (ScalarValue::UInt8(Some(a)), ScalarValue::UInt8(Some(b)))),
        DataType::UInt16 => int_range::<u16>(low, high, u16::MIN as f64, u16::MAX as f64)
            .map(|(a, b)| (ScalarValue::UInt16(Some(a)), ScalarValue::UInt16(Some(b)))),
        DataType::UInt32 => int_range::<u32>(low, high, u32::MIN as f64, u32::MAX as f64)
            .map(|(a, b)| (ScalarValue::UInt32(Some(a)), ScalarValue::UInt32(Some(b)))),
        DataType::UInt64 => int_range::<u64>(low, high, u64::MIN as f64, u64::MAX as f64)
            .map(|(a, b)| (ScalarValue::UInt64(Some(a)), ScalarValue::UInt64(Some(b)))),
        // A boolean range prunes nothing worth the bytes, and text and binary
        // have no numeric range at all.
        _ => None,
    }
}

/// The two finite numbers an `actual_range` attribute holds, low first.
///
/// Anything else — a missing attribute, a wrong length, a non-number, a NaN or
/// an infinity — is not a bound and yields `None`.
fn actual_range(attributes: &Attributes) -> Option<(f64, f64)> {
    // A JSON array of numbers is a `List<Float64>` of one item.
    let list = attributes.get("actual_range")?.as_list_opt::<i32>()?;
    if list.len() != 1 || list.is_null(0) {
        return None;
    }
    let items = list.value(0);
    let values = items.as_primitive_opt::<Float64Type>()?;
    if values.len() != 2 || values.null_count() != 0 {
        return None;
    }
    let (low, high) = (values.value(0), values.value(1));
    if !low.is_finite() || !high.is_finite() {
        return None;
    }
    // CF writes [min, max]; accept the reverse rather than inverting the bound.
    Some((low.min(high), low.max(high)))
}

/// The largest magnitude an f64 holds as an exact integer (2^53).
///
/// At and past it a JSON number may not round-trip: 2^53 + 1 parses to 2^53.
/// There `floor`/`ceil` stop being the safe direction and no bound is claimed.
const EXACT_INTEGER: f64 = 9_007_199_254_740_992.0;

/// Floor `low` and ceil `high` into `T`, or `None` if either falls outside it.
fn int_range<T: TryFrom<i64>>(low: f64, high: f64, min: f64, max: f64) -> Option<(T, T)> {
    let low = low.floor();
    let high = high.ceil();
    if low < min || high > max || low <= -EXACT_INTEGER || high >= EXACT_INTEGER {
        return None;
    }
    Some((
        T::try_from(low as i64).ok()?,
        T::try_from(high as i64).ok()?,
    ))
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Float64Array;
    use arrow::datatypes::TimeUnit;
    use nd_arrow_array::compute::time::timestamp_type;
    use nd_arrow_array::{Dimensions, NdArrowArray};
    use serde_json::json;

    use crate::nd::decode::Decode;
    use crate::nd::testing::attributes_of;

    /// The attributes that a Zarr array with these JSON attributes gets.
    fn attrs(pairs: &[(&str, serde_json::Value)]) -> Attributes {
        let json = pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), value.clone()))
            .collect();
        attributes_of(&json)
    }

    /// The values that the read decode gives for the stored `Float64` values.
    fn decoded(attributes: &Attributes, values: Vec<f64>) -> Vec<ScalarValue> {
        let len = values.len();
        let raw = NdArrowArray::try_new(
            Arc::new(Float64Array::from(values)),
            Dimensions::try_new([("x", len)]).unwrap(),
        )
        .unwrap()
        .with_attributes(attributes.clone());
        let out = Decode::choose(&DataType::Float64, attributes)
            .apply(raw)
            .unwrap();
        (0..len)
            .map(|i| ScalarValue::try_from_array(out.values(), i).unwrap())
            .collect()
    }

    /// The f64 range of `attributes`: the min and the max.
    fn f64_bounds(attributes: &Attributes) -> (f64, f64) {
        let range = attribute_range(attributes, &DataType::Float64).unwrap();
        let (ScalarValue::Float64(Some(min)), ScalarValue::Float64(Some(max))) = range else {
            panic!("expected an f64 range, got {range:?}");
        };
        (min, max)
    }

    #[test]
    fn actual_range_bounds_a_float_array() {
        let a = attrs(&[("actual_range", json!([-1.5, 3.5]))]);
        let (min, max) = f64_bounds(&a);
        // The bound contains the stated values and is at most one step wider.
        assert!(min <= -1.5 && min >= (-1.5f64).next_down(), "min {min}");
        assert!(max >= 3.5 && max <= 3.5f64.next_up(), "max {max}");
    }

    #[test]
    fn actual_range_is_widened_for_f32() {
        let a = attrs(&[("actual_range", json!([0.1, 0.2]))]);
        let (min, max) = attribute_range(&a, &DataType::Float32).unwrap();
        let (ScalarValue::Float32(Some(min)), ScalarValue::Float32(Some(max))) = (min, max) else {
            panic!("expected an f32 range");
        };
        // The bound must contain every f32 the JSON could have meant.
        assert!(min < 0.1f32, "min {min} must sit below 0.1");
        assert!(max > 0.2f32, "max {max} must sit above 0.2");
    }

    #[test]
    fn actual_range_floors_and_ceils_for_integers() {
        let a = attrs(&[("actual_range", json!([-3.2, 4.1]))]);
        let range = attribute_range(&a, &DataType::Int32).unwrap();
        assert_eq!(range.0, ScalarValue::Int32(Some(-4)));
        assert_eq!(range.1, ScalarValue::Int32(Some(5)));
    }

    /// 2^53 + 1 parses to the f64 2^53, so a bound at 2^53 may sit below the data.
    #[test]
    fn an_integer_actual_range_at_two_to_the_53_is_unknown() {
        let high = attrs(&[("actual_range", json!([0, 9007199254740992.0]))]);
        assert!(attribute_range(&high, &DataType::Int64).is_none());

        let low = attrs(&[("actual_range", json!([-9007199254740992.0, 0]))]);
        assert!(attribute_range(&low, &DataType::Int64).is_none());

        let exact = attrs(&[("actual_range", json!([0, 9007199254740991.0]))]);
        let range = attribute_range(&exact, &DataType::Int64).unwrap();
        assert_eq!(range.0, ScalarValue::Int64(Some(0)));
        assert_eq!(range.1, ScalarValue::Int64(Some(9_007_199_254_740_991)));
    }

    #[test]
    fn actual_range_outside_the_integer_type_is_rejected() {
        let a = attrs(&[("actual_range", json!([0, 40000]))]);
        assert!(attribute_range(&a, &DataType::Int16).is_none());
    }

    /// A CF time attribute must decode through the very conversion the reader
    /// applies to the chunk values. Asserting a hand-computed microsecond count
    /// would only test the time kernel; asserting equality with the reader's own
    /// conversion is what keeps the bound a bound.
    #[test]
    fn actual_range_decodes_cf_time_the_way_the_reader_does() {
        let a = attrs(&[
            ("actual_range", json!([0, 60])),
            ("units", json!("seconds since 1970-01-01")),
        ]);
        let range = attribute_range(&a, &timestamp_type()).unwrap();
        let expected = decoded(&a, vec![0.0, 60.0]);
        assert_eq!(
            expected[1],
            ScalarValue::TimestampMicrosecond(Some(60_000_000), None)
        );
        assert_eq!(range.0, expected[0]);
        assert_eq!(range.1, expected[1]);
    }

    /// `CfTimeUnits` alone reads the Julian reference date as Gregorian, 13 days early.
    #[test]
    fn a_julian_actual_range_uses_the_julian_epoch() {
        let a = attrs(&[
            ("actual_range", json!([0, 1])),
            ("units", json!("days since 2000-01-01")),
            ("calendar", json!("julian")),
        ]);
        let range = attribute_range(&a, &timestamp_type()).unwrap();
        let expected = decoded(&a, vec![0.0, 1.0]);
        assert_eq!(range.0, expected[0]);
        assert_eq!(range.1, expected[1]);
        // Gregorian 2000-01-01, the date the reference text names.
        let gregorian = ScalarValue::TimestampMicrosecond(Some(946_684_800_000_000), None);
        assert_ne!(range.0, gregorian);
    }

    #[test]
    fn a_nanosecond_actual_range_is_timestamp_ns() {
        let a = attrs(&[
            ("actual_range", json!([5, 9])),
            ("units", json!("nanoseconds since 1970-01-01")),
        ]);
        let nanos = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let range = attribute_range(&a, &nanos).unwrap();
        assert_eq!(range.0, ScalarValue::TimestampNanosecond(Some(5), None));
        assert_eq!(range.1, ScalarValue::TimestampNanosecond(Some(9), None));
    }

    #[test]
    fn a_time_range_out_of_range_is_unknown() {
        let a = attrs(&[
            ("actual_range", json!([0, 1e300])),
            ("units", json!("days since 1970-01-01")),
        ]);
        assert!(attribute_range(&a, &timestamp_type()).is_none());
    }

    #[test]
    fn a_time_range_out_of_range_at_the_low_end_is_unknown() {
        let a = attrs(&[
            ("actual_range", json!([-1e300, 0])),
            ("units", json!("days since 1970-01-01")),
        ]);
        assert!(attribute_range(&a, &timestamp_type()).is_none());
    }

    /// A packed time variable states its range in packed units.
    #[test]
    fn a_packed_time_variable_gives_no_shortcut() {
        let a = attrs(&[
            ("actual_range", json!([0, 1])),
            ("units", json!("days since 1970-01-01")),
            ("scale_factor", json!(0.5)),
        ]);
        let micros = DataType::Timestamp(TimeUnit::Microsecond, None);
        assert!(attribute_range(&a, &micros).is_none());
    }

    /// The pruning contract: `valid_min`/`valid_max` bound the *valid* values,
    /// not the stored ones, so they may never become a range.
    #[test]
    fn valid_min_and_valid_max_are_never_a_bound() {
        let a = attrs(&[("valid_min", json!(0.0)), ("valid_max", json!(100.0))]);
        assert!(attribute_range(&a, &DataType::Float64).is_none());
    }

    /// A packed array states a packed range. Beacon surfaces unpacked values,
    /// so the attribute is not a bound on what a query sees.
    #[test]
    fn packed_arrays_skip_the_metadata_path() {
        let a = attrs(&[
            ("actual_range", json!([-300, 4500])),
            ("scale_factor", json!(0.01)),
            ("add_offset", json!(273.15)),
        ]);
        assert!(attribute_range(&a, &DataType::Float64).is_none());
    }

    #[test]
    fn malformed_actual_range_is_ignored() {
        for value in [
            json!([1.0]),
            json!([1.0, 2.0, 3.0]),
            json!(1.0),
            json!(["a", "b"]),
            json!([]),
        ] {
            let a = attrs(&[("actual_range", value.clone())]);
            assert!(
                attribute_range(&a, &DataType::Float64).is_none(),
                "{value} must not produce a bound"
            );
        }
    }

    #[test]
    fn reversed_actual_range_is_read_low_first() {
        let a = attrs(&[("actual_range", json!([9.0, 1.0]))]);
        let (min, max) = f64_bounds(&a);
        assert!(min <= 1.0 && min >= 1.0f64.next_down(), "min {min}");
        assert!(max >= 9.0 && max <= 9.0f64.next_up(), "max {max}");
    }

    // ── folding leaf groups ──────────────────────────────────────────────

    fn f64_range(min: f64, max: f64) -> Range {
        (
            ScalarValue::Float64(Some(min)),
            ScalarValue::Float64(Some(max)),
        )
    }

    #[test]
    fn two_leaf_groups_widen_into_their_union() {
        let mut ranges = StoreRanges::default();
        ranges.add("lat", Some(f64_range(10.0, 20.0)));
        ranges.add("lat", Some(f64_range(5.0, 15.0)));
        let stats = ranges.column_statistics("lat");
        assert_eq!(
            stats.min_value,
            Precision::Exact(ScalarValue::Float64(Some(5.0)))
        );
        assert_eq!(
            stats.max_value,
            Precision::Exact(ScalarValue::Float64(Some(20.0)))
        );
    }

    /// One leaf group that cannot bound a column makes the whole store unknown:
    /// its values may lie anywhere, so no other leaf's range bounds the store.
    #[test]
    fn one_unbounded_leaf_group_poisons_the_column() {
        let mut ranges = StoreRanges::default();
        ranges.add("lat", Some(f64_range(10.0, 20.0)));
        ranges.add("lat", None);
        ranges.add("lat", Some(f64_range(0.0, 1.0)));
        let stats = ranges.column_statistics("lat");
        assert_eq!(stats.min_value, Precision::Absent);
        assert_eq!(stats.max_value, Precision::Absent);
    }

    /// A statistics error gives `None` for the leaf group, in either order.
    #[test]
    fn a_statistics_error_makes_the_column_unknown() {
        let mut ranges = StoreRanges::default();
        ranges.add("lat", Some(f64_range(10.0, 20.0)));
        ranges.add("lat", None);
        let stats = ranges.column_statistics("lat");
        assert_eq!(stats.min_value, Precision::Absent);
        assert_eq!(stats.max_value, Precision::Absent);

        let mut ranges = StoreRanges::default();
        ranges.add("lat", None);
        ranges.add("lat", Some(f64_range(10.0, 20.0)));
        let stats = ranges.column_statistics("lat");
        assert_eq!(stats.min_value, Precision::Absent);
        assert_eq!(stats.max_value, Precision::Absent);
    }

    /// Incomparable scalar types cannot be widened without guessing a cast, and
    /// a wrong guess tightens the bound.
    #[test]
    fn incomparable_types_go_unknown() {
        let mut ranges = StoreRanges::default();
        ranges.add("lat", Some(f64_range(10.0, 20.0)));
        ranges.add(
            "lat",
            Some((
                ScalarValue::Float32(Some(1.0)),
                ScalarValue::Float32(Some(2.0)),
            )),
        );
        assert_eq!(ranges.column_statistics("lat").min_value, Precision::Absent);
    }

    /// A column no leaf group reported on is unknown, not an error.
    #[test]
    fn an_unseen_column_is_unknown() {
        let ranges = StoreRanges::default();
        let stats = ranges.column_statistics("absent");
        assert_eq!(stats.min_value, Precision::Absent);
        assert_eq!(stats.max_value, Precision::Absent);
    }
}
