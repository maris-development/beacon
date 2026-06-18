//! Geometry-based encoding classification for flattened ND columns.
//!
//! When an ND variable is flattened to tabular (row-major / C-order) form, a column
//! whose source dimensions are only a subset of the batch's target dimensions is
//! *broadcast*: it is constant along the target dims it does not span and repeats in a
//! perfectly regular pattern. Because that pattern is fully determined by the broadcast
//! geometry, we can decide up-front — without ever materializing the column — whether it
//! is best represented as a flat array, a run-end-encoded (REE) array, or a dictionary
//! array.
//!
//! This module is the pure decision step only. Constructing the encoded arrays and
//! wiring the choice into the flatten pipeline live in later work (see issues #278 and
//! #279). It depends on nothing but dimension names and shapes, so it has no Arrow or
//! ndarray dependency.

/// The encoding chosen for a single flattened output column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnEncoding {
    /// A plain, fully-materialized Arrow array.
    Flat,
    /// A run-end-encoded array — ideal when the column is broadcast across the inner
    /// dimensions, producing long contiguous runs.
    RunEndEncoded,
    /// A dictionary-encoded array — ideal for low-cardinality columns that repeat
    /// cyclically (broadcast across an outer dimension) and so lack long runs.
    Dictionary,
}

/// Tunable thresholds for [`classify_column_encoding`].
///
/// These are first-pass heuristics; they are expected to be tuned against the
/// benchmarks introduced in issue #282.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EncodingPolicy {
    /// Minimum run length (in rows) before a broadcast column is worth run-end encoding.
    pub min_run_length: usize,
    /// Maximum number of distinct values before dictionary encoding stops being worth it.
    pub max_dict_cardinality: usize,
}

impl Default for EncodingPolicy {
    fn default() -> Self {
        Self {
            min_run_length: 8,
            max_dict_cardinality: 4096,
        }
    }
}

/// Classify how a flattened column should be encoded, from broadcast geometry alone.
///
/// `target_dims` is the batch's dimension list in **row-major outer→inner order**
/// (index `0` varies slowest, the last index fastest) and `target_shape` must align
/// with it element-for-element. `source_dims` are the dimensions the column actually
/// spans and must be a subset of `target_dims`.
///
/// Precondition violations (mismatched `target_dims`/`target_shape` lengths, or a
/// `source_dims` entry not present in `target_dims`) are handled defensively by falling
/// back to [`ColumnEncoding::Flat`] rather than panicking.
pub fn classify_column_encoding(
    source_dims: &[String],
    target_dims: &[String],
    target_shape: &[usize],
    policy: &EncodingPolicy,
) -> ColumnEncoding {
    // Defensive: the target dim list and shape must describe the same axes.
    if target_dims.len() != target_shape.len() {
        return ColumnEncoding::Flat;
    }

    // Defensive: every source dim must exist in the target. If not, we cannot reason
    // about the geometry, so fall back to flat.
    if source_dims.iter().any(|d| !target_dims.contains(d)) {
        return ColumnEncoding::Flat;
    }

    // A column that spans every target dimension is a plain data column — there is no
    // broadcasting to exploit. (This is also the natural fallthrough of the formula
    // below, but is clearer as an explicit guard.)
    if target_dims.iter().all(|d| source_dims.contains(d)) {
        return ColumnEncoding::Flat;
    }

    // Index of the innermost (fastest-varying) target dim that the column spans.
    // `None` means the column spans no target dim at all (a scalar/attribute broadcast
    // across the whole batch), which behaves like `m = -1`: everything is "inner".
    let innermost_source = target_dims
        .iter()
        .enumerate()
        .filter(|(_, d)| source_dims.contains(d))
        .map(|(i, _)| i)
        .next_back();

    // Run length = product of the sizes of all target dims strictly inner to the
    // innermost source dim. Those dims are necessarily absent from the source, so the
    // column's value is constant across them, giving a contiguous run of this length.
    let inner_start = innermost_source.map_or(0, |m| m + 1);
    let run_length: usize = target_shape[inner_start..].iter().product();

    // Cardinality = number of distinct values = product of the source dim sizes.
    let cardinality: usize = target_dims
        .iter()
        .enumerate()
        .filter(|(_, d)| source_dims.contains(d))
        .map(|(i, _)| target_shape[i])
        .product();

    let total_rows: usize = target_shape.iter().product();

    if run_length >= policy.min_run_length {
        ColumnEncoding::RunEndEncoded
    } else if cardinality <= policy.max_dict_cardinality && cardinality < total_rows {
        ColumnEncoding::Dictionary
    } else {
        ColumnEncoding::Flat
    }
}

/// Convenience wrapper that classifies every column of a batch sharing the same target
/// geometry. The per-column [`classify_column_encoding`] remains the primary API.
pub fn classify_encodings<'a, I>(
    columns_source_dims: I,
    target_dims: &[String],
    target_shape: &[usize],
    policy: &EncodingPolicy,
) -> Vec<ColumnEncoding>
where
    I: IntoIterator<Item = &'a [String]>,
{
    columns_source_dims
        .into_iter()
        .map(|source_dims| classify_column_encoding(source_dims, target_dims, target_shape, policy))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dims(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    fn classify(source: &[&str], target: &[&str], shape: &[usize]) -> ColumnEncoding {
        classify_column_encoding(
            &dims(source),
            &dims(target),
            shape,
            &EncodingPolicy::default(),
        )
    }

    const T: [&str; 3] = ["time", "lat", "lon"];
    const SHAPE: [usize; 3] = [10, 30, 20]; // total 6000

    #[test]
    fn outer_broadcast_dim_is_run_end_encoded() {
        // `time` repeats across lat*lon = 600 rows per value → long runs.
        assert_eq!(classify(&["time"], &T, &SHAPE), ColumnEncoding::RunEndEncoded);
    }

    #[test]
    fn inner_broadcast_dim_is_dictionary() {
        // `lon` is the innermost dim → run length 1, cardinality 20 → cyclic, low card.
        assert_eq!(classify(&["lon"], &T, &SHAPE), ColumnEncoding::Dictionary);
    }

    #[test]
    fn full_data_column_is_flat() {
        assert_eq!(
            classify(&["time", "lat", "lon"], &T, &SHAPE),
            ColumnEncoding::Flat
        );
    }

    #[test]
    fn scalar_attribute_is_run_end_encoded() {
        // No source dims → constant across the whole batch → one giant run.
        assert_eq!(classify(&[], &T, &SHAPE), ColumnEncoding::RunEndEncoded);
    }

    #[test]
    fn non_contiguous_source_uses_cardinality() {
        // `lon` innermost → run length 1; cardinality time*lon = 200 ≤ 4096 → dictionary.
        assert_eq!(
            classify(&["time", "lon"], &T, &SHAPE),
            ColumnEncoding::Dictionary
        );
    }

    #[test]
    fn middle_dim_with_large_inner_product_is_run_end_encoded() {
        // `lat` innermost source → run length = lon = 20 ≥ 8 → REE.
        assert_eq!(classify(&["lat"], &T, &SHAPE), ColumnEncoding::RunEndEncoded);
    }

    #[test]
    fn dimension_order_independent() {
        // Source dims given in a different order than the target must not matter.
        assert_eq!(
            classify(&["lon", "time"], &T, &SHAPE),
            classify(&["time", "lon"], &T, &SHAPE)
        );
    }

    #[test]
    fn run_length_just_below_threshold_is_not_ree() {
        // target [a, b] shape [n, 4]: source `a` → run length 4 (< default 8).
        // cardinality of `a` is small, so it falls to dictionary, not REE.
        let enc = classify(&["a"], &["a", "b"], &[100, 4]);
        assert_ne!(enc, ColumnEncoding::RunEndEncoded);
        assert_eq!(enc, ColumnEncoding::Dictionary);
    }

    #[test]
    fn run_length_at_threshold_is_ree() {
        // source `a` → run length = 8 == default min_run_length → REE.
        assert_eq!(
            classify(&["a"], &["a", "b"], &[100, 8]),
            ColumnEncoding::RunEndEncoded
        );
    }

    #[test]
    fn cardinality_above_dict_cap_is_flat() {
        // Innermost dim (run length 1) with cardinality 5000 > 4096 and no long runs.
        // target [outer, inner], source = inner → run length 1, cardinality 5000.
        let policy = EncodingPolicy::default();
        let enc = classify_column_encoding(
            &dims(&["inner"]),
            &dims(&["outer", "inner"]),
            &[3, 5000],
            &policy,
        );
        assert_eq!(enc, ColumnEncoding::Flat);
    }

    #[test]
    fn mismatched_target_lengths_fall_back_to_flat() {
        assert_eq!(
            classify_column_encoding(
                &dims(&["time"]),
                &dims(&["time", "lat"]),
                &[10], // too short
                &EncodingPolicy::default(),
            ),
            ColumnEncoding::Flat
        );
    }

    #[test]
    fn unknown_source_dim_falls_back_to_flat() {
        assert_eq!(
            classify(&["depth"], &T, &SHAPE), // `depth` not in target
            ColumnEncoding::Flat
        );
    }

    #[test]
    fn batch_helper_classifies_each_column() {
        let target = dims(&T);
        let source_time = dims(&["time"]);
        let source_lon = dims(&["lon"]);
        let source_full = dims(&["time", "lat", "lon"]);
        let columns: Vec<&[String]> = vec![&source_time, &source_lon, &source_full];

        let encodings = classify_encodings(
            columns.iter().copied(),
            &target,
            &SHAPE,
            &EncodingPolicy::default(),
        );

        assert_eq!(
            encodings,
            vec![
                ColumnEncoding::RunEndEncoded,
                ColumnEncoding::Dictionary,
                ColumnEncoding::Flat,
            ]
        );
    }
}
