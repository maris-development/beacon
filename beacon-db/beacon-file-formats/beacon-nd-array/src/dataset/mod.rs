pub mod any;
pub mod ragged;
pub mod variant;

pub use any::AnyDataset;
pub use ragged::RaggedDataset;
pub use variant::DatasetType;

use crate::{NdArray, NdArrayD, array::subset::ArraySubset, datatypes::NdArrayDataType};
use indexmap::IndexMap;
use std::{collections::HashSet, sync::Arc};

#[derive(Debug, Clone)]
pub struct Dataset {
    pub name: String,
    pub dimensions: IndexMap<String, usize>,
    pub arrays: IndexMap<String, Arc<dyn NdArrayD>>,
    /// The dimensions the reader invented because the file names none.
    ///
    /// A plain HDF5 file holds no dimension scale, so every axis of it lands
    /// here. A file that names its axes leaves this empty, and so does a reader
    /// that does not know. See [`Dataset::default_broadcast_dimensions`], the
    /// one place it decides anything.
    pub invented_dimensions: HashSet<String>,
}

impl Dataset {
    pub async fn new(name: String, arrays: IndexMap<String, Arc<dyn NdArrayD>>) -> Self {
        let mut dimensions = indexmap::IndexMap::new();
        for (array_name, array) in arrays.iter() {
            for (dim_name, dim_size) in array.dimensions().iter().zip(array.shape().iter()) {
                // One name, two lengths: the reader gave two axes one name, and
                // every broadcast onto the loser is then wrong. The last one
                // wins, as it always has, but say so rather than hide it.
                if let Some(held) = dimensions.get(dim_name)
                    && held != dim_size
                {
                    tracing::warn!(
                        dataset = %name,
                        array = %array_name,
                        dimension = %dim_name,
                        "dimension '{dim_name}' holds {held} elements for one array and \
                         {dim_size} for '{array_name}'; the broadcast takes {dim_size}"
                    );
                }
                dimensions.insert(dim_name.clone(), *dim_size);
            }
        }
        Self {
            name,
            dimensions,
            arrays,
            invented_dimensions: HashSet::new(),
        }
    }

    /// Record which dimensions the reader invented, because the file names
    /// none of them.
    ///
    /// Only a reader knows this. It decides how `SELECT *` picks its grid, and
    /// nothing else. See [`Dataset::default_broadcast_dimensions`].
    pub fn with_invented_dimensions(
        mut self,
        dimensions: impl IntoIterator<Item = String>,
    ) -> Self {
        self.invented_dimensions = dimensions.into_iter().collect();
        self
    }

    pub fn get_array(&self, name: &str) -> Option<&Arc<dyn NdArrayD>> {
        self.arrays.get(name)
    }

    pub fn get_array_datatype(&self, name: &str) -> Option<NdArrayDataType> {
        self.get_array(name).map(|array| array.datatype())
    }

    pub fn project_with_dimensions(&self, dimensions: &[String]) -> anyhow::Result<Dataset> {
        // Check that all the dimensions exist
        for dim in dimensions {
            if !self.dimensions.contains_key(dim) {
                anyhow::bail!("dimension '{dim}' not found in dataset");
            }
        }

        let arrays: IndexMap<String, Arc<dyn NdArrayD>> = self
            .arrays
            .iter()
            .filter(|(_, array)| {
                array
                    .dimensions()
                    .iter()
                    .all(|dim| dimensions.contains(dim))
            })
            .map(|(name, array)| (name.clone(), array.clone()))
            .collect();

        // Keep only the dimensions actually referenced by a surviving array,
        // preserving the original order. Dropping a variable can orphan a
        // dimension (e.g. narrowing away a CF-bounds variable leaves its `nv`
        // axis behind); retaining it would inflate the broadcast grid with a
        // spurious dimension. `count(*)` builds its dataset directly (it never
        // projects) so its full-grid dimensions are unaffected.
        let dimensions = self
            .dimensions
            .iter()
            .filter(|(dim, _)| arrays.values().any(|array| array.dimensions().contains(dim)))
            .map(|(dim, size)| (dim.clone(), *size))
            .collect();

        Ok(Dataset {
            name: self.name.clone(),
            dimensions,
            arrays,
            invented_dimensions: self.invented_dimensions.clone(),
        })
    }

    /// Compute a broadcast-compatible *default* dimension set for `SELECT *`
    /// style reads, used when no explicit dimensions were requested.
    ///
    /// A regular dataset combines all of its variables into a single Arrow
    /// table by broadcasting each one onto the dimensions of the
    /// highest-dimensionality variable. When variables live on mutually
    /// incompatible dimension sets (e.g. `temp(time,lat,lon)` alongside a CF
    /// bounds variable `lat_bnds(lat,nv)`) that broadcast cannot succeed.
    ///
    /// This picks the variable dimension set that, in priority order, (1) is
    /// non-empty — never a grid whose dimension sizes multiply to zero rows when a
    /// non-empty alternative exists — (2) retains the **most** variables, (3) is the
    /// native dimension set of the most variables, (4) has the largest data volume,
    /// and finally (5) was encountered first, for determinism. Projecting the dataset
    /// to the returned set is guaranteed to be broadcast-safe: the set is itself some
    /// variable's dimensions, so that variable survives and becomes the
    /// max-dimensionality array, and every other survivor has dimensions that are a
    /// subset of it.
    ///
    /// # A file that names no dimension
    ///
    /// Rule (2) reads the variable count as "which grid is this file about". A
    /// name carries meaning, so that holds for every file that names its axes.
    ///
    /// A plain HDF5 file names none, and then the count says the opposite of
    /// what it says elsewhere. An instrument writes one payload and hundreds of
    /// metadata variables around it, so the count picks a grid of the metadata:
    /// a real 1250 x 23250 file picks a 3-element axis and drops its payload
    /// altogether. Length is the only thing an unnamed axis carries, so the
    /// order becomes (1) non-empty, (2) **largest data volume**, (3) most
    /// variables retained, (4) native home of the most variables.
    ///
    /// This applies only when [`Dataset::invented_dimensions`] covers every
    /// dimension of the dataset, which is exactly a file that names none of
    /// them. One real name is enough to keep the order above.
    ///
    /// Returns `None` when no narrowing is needed — i.e. every variable already
    /// broadcasts onto the highest-dimensionality variable — so callers can
    /// skip projection entirely.
    pub fn default_broadcast_dimensions(&self) -> Option<Vec<String>> {
        // Highest-dimensionality array's dimension set.
        let max_dims = self
            .arrays
            .values()
            .max_by_key(|array| array.dimensions().len())
            .map(|array| array.dimensions())?;

        // Already broadcast-safe: every variable's dims fit inside `max_dims`.
        let needs_narrowing = self.arrays.values().any(|array| {
            !array
                .dimensions()
                .iter()
                .all(|dim| max_dims.contains(dim))
        });
        if !needs_narrowing {
            return None;
        }

        // Candidate targets: each variable's (non-empty) dimension set, deduped
        // in first-encountered order.
        let mut candidates: Vec<Vec<String>> = Vec::new();
        for array in self.arrays.values() {
            let dims = array.dimensions();
            if dims.is_empty() {
                continue;
            }
            if !candidates.iter().any(|c| c == &dims) {
                candidates.push(dims);
            }
        }

        // Only *gridded* variables (those with at least one dimension) drive the
        // choice. Scalars — including every NetCDF attribute, which is surfaced as a
        // scalar `<var>.<attr>` array — broadcast onto every candidate equally, so
        // they only inflate the score by a constant and obscure ties. Excluding them
        // makes the score mean what it says: how many data variables a grid retains.
        let score = |c: &[String]| -> usize {
            self.arrays
                .values()
                .filter(|array| {
                    let dims = array.dimensions();
                    !dims.is_empty() && dims.iter().all(|d| c.contains(d))
                })
                .count()
        };
        // First tie-break: prefer the grid that is the *native* dimension set of the
        // most variables (an exact match). On a file like an Argo profile this picks
        // the data grid (e.g. `N_PROF × N_LEVELS`, home of PRES/TEMP/PSAL) over an
        // equally scoring bookkeeping grid (e.g. `N_HISTORY × N_PROF × DATE_TIME`, home
        // of a single HISTORY variable): both retain every `N_PROF`-only variable, so
        // the count alone ties, but the data grid carries far more variables on its own.
        let exact_count = |c: &[String]| -> usize {
            self.arrays
                .values()
                .filter(|array| array.dimensions() == c)
                .count()
        };
        // Number of rows the grid materialises — the product of its dimension sizes.
        // A grid with a zero-length dimension (e.g. an unlimited `N_HISTORY` that is
        // currently empty) yields 0 rows: selecting it would broadcast the whole table
        // down to nothing. `u128` so large grids cannot overflow the product.
        let grid_rows = |c: &[String]| -> u128 {
            c.iter()
                .map(|d| self.dimensions.get(d).copied().unwrap_or(0) as u128)
                .product()
        };
        // Whether the file names any dimension at all. One real name is enough
        // to keep the variable count first; see the note on the doc above.
        let names_no_dimension = !self.invented_dimensions.is_empty()
            && self
                .dimensions
                .keys()
                .all(|dim| self.invented_dimensions.contains(dim));

        // Selection key, compared lexicographically, larger wins:
        //   1. non-empty before empty — never auto-pick a grid that yields 0 rows when
        //      a non-empty alternative exists, even if the empty grid scores higher;
        //   2. most data variables retained (score);
        //   3. native home of the most variables (exact_count);
        //   4. largest data volume (grid_rows) — a principled stand-in for the old
        //      "higher dimensionality" tie-break;
        //   5. first-encountered order (equal keys keep the earlier candidate) for
        //      determinism.
        //
        // A file that names no dimension moves the volume to the front and the
        // other two down, and keeps 1 and 5 where they are. Every rank is a
        // `u128` so one comparison serves both orders.
        let key = |candidate: &[String]| -> (bool, u128, u128, u128) {
            let rows = grid_rows(candidate);
            let score = score(candidate) as u128;
            let exact = exact_count(candidate) as u128;
            if names_no_dimension {
                (rows > 0, rows, score, exact)
            } else {
                (rows > 0, score, exact, rows)
            }
        };

        let mut best: Option<((bool, u128, u128, u128), Vec<String>)> = None;
        for candidate in candidates {
            let candidate_key = key(&candidate);
            let is_better = match &best {
                // Strictly greater key wins; equal keys keep the earlier candidate.
                Some((best_key, _)) => candidate_key > *best_key,
                None => true,
            };
            if is_better {
                best = Some((candidate_key, candidate));
            }
        }
        best.map(|(_, dims)| dims)
    }

    pub fn project(&self, indices: &[usize]) -> anyhow::Result<Dataset> {
        let mut arrays = indexmap::IndexMap::new();
        for &index in indices {
            if let Some((name, array)) = self.arrays.get_index(index) {
                arrays.insert(name.clone(), array.clone());
            } else {
                anyhow::bail!(
                    "index {index} out of bounds for dataset with {} arrays",
                    self.arrays.len()
                );
            }
        }
        Ok(Dataset {
            name: self.name.clone(),
            dimensions: self.dimensions.clone(),
            arrays,
            invented_dimensions: self.invented_dimensions.clone(),
        })
    }

    /// Detect whether this dataset follows the CF ragged-array convention.
    ///
    /// A dataset is considered **ragged** when at least one variable carries a
    /// `sample_dimension` attribute (surfaced as `"<var>.sample_dimension"` in
    /// the array map).  Otherwise it is **regular**.
    pub fn cf_dataset_type(&self) -> DatasetType {
        let is_ragged = self
            .arrays
            .keys()
            .any(|name| name.ends_with(".sample_dimension"));

        if is_ragged {
            DatasetType::CfRagged
        } else {
            DatasetType::Regular
        }
    }
}

/// Resolve the dimension projection to apply to a freshly-opened dataset for a
/// `SELECT *`-style read.
///
/// An explicit `read_dimensions` (e.g. a table-function argument or format
/// option) always wins. When none is given, fall back to the dataset's
/// auto-selected broadcast-compatible default (see
/// [`AnyDataset::default_broadcast_dimensions`]) so that combining all variables
/// into a single table cannot fail at the broadcast step for files whose
/// variables live on mutually-incompatible dimension sets.
///
/// `None` means "apply no dimension projection" — explicit and default both
/// absent (ragged datasets, or files that are already broadcast-safe).
///
/// When `log_label` is `Some(label)` and the default narrowing actually excludes
/// variables, a single `info` line prefixed with `label` names them. Pass
/// `Some(..)` only from schema inference (runs once per query) and `None` from
/// the per-file execution path to avoid log spam.
pub fn resolve_read_dimensions(
    dataset: &AnyDataset,
    read_dimensions: Option<Vec<String>>,
    log_label: Option<&str>,
) -> Option<Vec<String>> {
    if let Some(dims) = read_dimensions {
        return Some(dims);
    }

    let default_dims = dataset.default_broadcast_dimensions()?;

    if let Some(label) = log_label {
        let dropped: Vec<&String> = dataset
            .dataset()
            .arrays
            .iter()
            .filter(|(_, array)| !array.dimensions().iter().all(|d| default_dims.contains(d)))
            .map(|(name, _)| name)
            .collect();
        if !dropped.is_empty() {
            tracing::trace!(
                "{label}: SELECT * auto-selected dimensions {:?}; excluded variables {:?} \
                 have incompatible dimensions and were omitted.",
                default_dims,
                dropped
            );
        }
    }

    Some(default_dims)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: build a Dataset from a vec of (name, array) pairs.
    async fn make_dataset(name: &str, arrays: Vec<(&str, Arc<dyn NdArrayD>)>) -> Dataset {
        let map: IndexMap<String, Arc<dyn NdArrayD>> = arrays
            .into_iter()
            .map(|(n, a)| (n.to_string(), a))
            .collect();
        Dataset::new(name.to_string(), map).await
    }

    /// Build a minimal CF contiguous-ragged-array dataset with 3 casts.
    ///
    /// Layout:
    ///   - instance dim `"casts"` (size 3)
    ///   - obs dim `"z_obs"` (size 6, row sizes = [2, 1, 3])
    ///   - `station_id` : instance var, f64  [100, 200, 300]
    ///   - `z_row_size` : row-size var, i32  [2, 1, 3]
    ///   - `z_row_size.sample_dimension` : attr "z_obs"
    ///   - `depth`      : obs var, f64       [10, 20, 30, 40, 50, 60]
    ///   - `temperature`: obs var, f64       [1, 2, 3, 4, 5, 6]
    ///   - `depth.units`: var attr, String   ["m"]
    ///   - `temperature.units`: var attr     ["°C"]
    ///   - `Conventions`: global attr        ["CF-1.6"]
    async fn make_ragged_dataset() -> Dataset {
        let z_row_size = NdArray::<i32>::try_new_from_vec_in_mem(
            vec![2, 1, 3],
            vec![3],
            vec!["casts".into()],
            None,
        )
        .unwrap();

        make_ragged_dataset_with_row_size(Arc::new(z_row_size)).await
    }

    /// The same layout, with the caller supplying the row-size variable.
    ///
    /// CF fixes no width on that variable, so a writer picks any integer that
    /// holds its largest cast. The tests below build it from each one.
    async fn make_ragged_dataset_with_row_size(z_row_size: Arc<dyn NdArrayD>) -> Dataset {
        let station_id = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![100.0, 200.0, 300.0],
            vec![3],
            vec!["casts".into()],
            None,
        )
        .unwrap();

        let sample_dim = NdArray::<String>::try_new_from_vec_in_mem(
            vec!["z_obs".into()],
            vec![1],
            vec![] as Vec<String>,
            None,
        )
        .unwrap();

        let depth = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
            vec![6],
            vec!["z_obs".into()],
            None,
        )
        .unwrap();

        let temperature = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            vec![6],
            vec!["z_obs".into()],
            None,
        )
        .unwrap();

        let depth_units = NdArray::<String>::try_new_from_vec_in_mem(
            vec!["m".into()],
            vec![1],
            vec![] as Vec<String>,
            None,
        )
        .unwrap();

        let temp_units = NdArray::<String>::try_new_from_vec_in_mem(
            vec!["°C".into()],
            vec![1],
            vec![] as Vec<String>,
            None,
        )
        .unwrap();

        let conventions = NdArray::<String>::try_new_from_vec_in_mem(
            vec!["CF-1.6".into()],
            vec![1],
            vec![] as Vec<String>,
            None,
        )
        .unwrap();

        make_dataset(
            "ragged",
            vec![
                ("Conventions", Arc::new(conventions)),
                ("station_id", Arc::new(station_id)),
                ("z_row_size", z_row_size),
                ("z_row_size.sample_dimension", Arc::new(sample_dim)),
                ("depth", Arc::new(depth)),
                ("depth.units", Arc::new(depth_units)),
                ("temperature", Arc::new(temperature)),
                ("temperature.units", Arc::new(temp_units)),
            ],
        )
        .await
    }

    // ── cf_dataset_type ──────────────────────────────────────────────

    #[tokio::test]
    async fn test_cf_dataset_type_ragged() {
        let ds = make_ragged_dataset().await;
        assert!(matches!(ds.cf_dataset_type(), DatasetType::CfRagged));
    }

    #[tokio::test]
    async fn test_cf_dataset_type_regular() {
        let arr = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![1.0, 2.0],
            vec![2],
            vec!["x".into()],
            None,
        )
        .unwrap();
        let ds = make_dataset("regular", vec![("data", Arc::new(arr))]).await;
        assert!(matches!(ds.cf_dataset_type(), DatasetType::Regular));
    }

    // ── RaggedDataset::try_new ───────────────────────────────────────

    #[tokio::test]
    async fn test_try_new_basic() {
        let ds = make_ragged_dataset().await;
        let ragged = RaggedDataset::try_new(&ds).await.unwrap();

        assert_eq!(ragged.len(), 3);
        assert!(!ragged.is_empty());
        assert_eq!(ragged.instance_dimension(), "casts");
    }

    #[tokio::test]
    async fn test_classifies_variables() {
        use ragged::RaggedArray;
        let ds = make_ragged_dataset().await;
        let ragged = RaggedDataset::try_new(&ds).await.unwrap();

        // station_id should be an InstanceVariable
        assert!(matches!(
            ragged.variables.get("station_id"),
            Some(RaggedArray::InstanceVariable(_))
        ));
        // row-size var should not appear
        assert!(ragged.variables.get("z_row_size").is_none());

        // depth and temperature should be ObservationVariables
        assert!(matches!(
            ragged.variables.get("depth"),
            Some(RaggedArray::ObservationVariable(_))
        ));
        assert!(matches!(
            ragged.variables.get("temperature"),
            Some(RaggedArray::ObservationVariable(_))
        ));

        // Conventions should be an Attribute (global)
        assert!(matches!(
            ragged.variables.get("Conventions"),
            Some(RaggedArray::Attribute(_))
        ));

        // depth.units, temperature.units should be Attributes (variable)
        assert!(matches!(
            ragged.variables.get("depth.units"),
            Some(RaggedArray::Attribute(_))
        ));
        assert!(matches!(
            ragged.variables.get("temperature.units"),
            Some(RaggedArray::Attribute(_))
        ));

        // row-size var attrs should be excluded
        assert!(
            ragged
                .variables
                .get("z_row_size.sample_dimension")
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_offsets_lazy() {
        let ds = make_ragged_dataset().await;
        let ragged = RaggedDataset::try_new(&ds).await.unwrap();

        // Offsets should not be loaded yet.
        assert!(!ragged.offsets.initialized());

        // Trigger lazy load via get_cast.
        let _ = ragged.get_cast(0).await.unwrap();

        // Now offsets are loaded.
        assert!(ragged.offsets.initialized());
        let offsets = ragged.offsets.get().unwrap();
        // row sizes [2, 1, 3] → cumulative offsets [0, 2, 3, 6]
        assert_eq!(offsets.get("z_obs").unwrap(), &[0, 2, 3, 6]);
    }

    /// Every integer width must produce the same offsets. WOD writes
    /// `Latitude_row_size` as `short`, so a reader that accepts only `int`
    /// rejects the whole file.
    macro_rules! test_row_size_type {
        ($name:ident, $ty:ty) => {
            #[tokio::test]
            async fn $name() {
                let row_size = NdArray::<$ty>::try_new_from_vec_in_mem(
                    vec![2 as $ty, 1, 3],
                    vec![3],
                    vec!["casts".into()],
                    None,
                )
                .unwrap();
                let ds = make_ragged_dataset_with_row_size(Arc::new(row_size)).await;
                let ragged = RaggedDataset::try_new(&ds).await.unwrap();

                let _ = ragged.get_cast(0).await.unwrap();
                assert_eq!(
                    ragged.offsets.get().unwrap().get("z_obs").unwrap(),
                    &[0, 2, 3, 6]
                );
            }
        };
    }

    test_row_size_type!(test_row_size_i8, i8);
    test_row_size_type!(test_row_size_i16, i16);
    test_row_size_type!(test_row_size_i32, i32);
    test_row_size_type!(test_row_size_i64, i64);
    test_row_size_type!(test_row_size_u8, u8);
    test_row_size_type!(test_row_size_u16, u16);
    test_row_size_type!(test_row_size_u32, u32);
    test_row_size_type!(test_row_size_u64, u64);

    #[tokio::test]
    async fn test_row_size_rejects_a_negative_count() {
        // `as usize` would wrap -1 into a huge count and index past the obs axis.
        let row_size = NdArray::<i16>::try_new_from_vec_in_mem(
            vec![2, -1, 3],
            vec![3],
            vec!["casts".into()],
            None,
        )
        .unwrap();
        let ds = make_ragged_dataset_with_row_size(Arc::new(row_size)).await;
        let ragged = RaggedDataset::try_new(&ds).await.unwrap();

        let err = ragged.get_cast(0).await.unwrap_err();
        assert!(
            err.to_string().contains("invalid row size"),
            "expected an invalid-row-size error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_row_size_rejects_a_non_integer_type() {
        let row_size = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![2.0, 1.0, 3.0],
            vec![3],
            vec!["casts".into()],
            None,
        )
        .unwrap();
        let ds = make_ragged_dataset_with_row_size(Arc::new(row_size)).await;
        let ragged = RaggedDataset::try_new(&ds).await.unwrap();

        let err = ragged.get_cast(0).await.unwrap_err();
        assert!(
            err.to_string().contains("must hold an integer type"),
            "expected an integer-type error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_fails_on_regular_dataset() {
        let arr =
            NdArray::<f64>::try_new_from_vec_in_mem(vec![1.0], vec![1], vec!["x".into()], None)
                .unwrap();
        let ds = make_dataset("regular", vec![("val", Arc::new(arr))]).await;
        let err = RaggedDataset::try_new(&ds).await.unwrap_err();
        assert!(
            err.to_string().contains("sample_dimension"),
            "expected sample_dimension error, got: {err}"
        );
    }

    // ── get_cast ─────────────────────────────────────────────────────

    async fn read_f64(ds: &Dataset, name: &str) -> Vec<f64> {
        let arr = ds.get_array(name).unwrap();
        let typed = arr.as_any().downcast_ref::<NdArray<f64>>().unwrap();
        typed.clone_into_raw_vec().await
    }

    async fn read_string(ds: &Dataset, name: &str) -> Vec<String> {
        let arr = ds.get_array(name).unwrap();
        let typed = arr.as_any().downcast_ref::<NdArray<String>>().unwrap();
        typed.clone_into_raw_vec().await
    }

    #[tokio::test]
    async fn test_get_cast_first() {
        let ds = make_ragged_dataset().await;
        let ragged = RaggedDataset::try_new(&ds).await.unwrap();
        let cast = ragged.get_cast(0).await.unwrap();

        assert_eq!(cast.name, "casts[0]");
        assert_eq!(read_f64(&cast, "station_id").await, vec![100.0]);
        assert_eq!(read_f64(&cast, "depth").await, vec![10.0, 20.0]);
        assert_eq!(read_f64(&cast, "temperature").await, vec![1.0, 2.0]);
    }

    #[tokio::test]
    async fn test_get_cast_middle() {
        let ds = make_ragged_dataset().await;
        let ragged = RaggedDataset::try_new(&ds).await.unwrap();
        let cast = ragged.get_cast(1).await.unwrap();

        assert_eq!(cast.name, "casts[1]");
        assert_eq!(read_f64(&cast, "station_id").await, vec![200.0]);
        assert_eq!(read_f64(&cast, "depth").await, vec![30.0]);
        assert_eq!(read_f64(&cast, "temperature").await, vec![3.0]);
    }

    #[tokio::test]
    async fn test_get_cast_last() {
        let ds = make_ragged_dataset().await;
        let ragged = RaggedDataset::try_new(&ds).await.unwrap();
        let cast = ragged.get_cast(2).await.unwrap();

        assert_eq!(cast.name, "casts[2]");
        assert_eq!(read_f64(&cast, "station_id").await, vec![300.0]);
        assert_eq!(read_f64(&cast, "depth").await, vec![40.0, 50.0, 60.0]);
        assert_eq!(read_f64(&cast, "temperature").await, vec![4.0, 5.0, 6.0]);
    }

    #[tokio::test]
    async fn test_get_cast_includes_variable_attributes() {
        let ds = make_ragged_dataset().await;
        let ragged = RaggedDataset::try_new(&ds).await.unwrap();
        let cast = ragged.get_cast(0).await.unwrap();

        assert_eq!(read_string(&cast, "depth.units").await, vec!["m"]);
        assert_eq!(read_string(&cast, "temperature.units").await, vec!["°C"]);
    }

    #[tokio::test]
    async fn test_get_cast_includes_global_attributes() {
        let ds = make_ragged_dataset().await;
        let ragged = RaggedDataset::try_new(&ds).await.unwrap();
        let cast = ragged.get_cast(0).await.unwrap();

        assert_eq!(read_string(&cast, "Conventions").await, vec!["CF-1.6"]);
    }

    #[tokio::test]
    async fn test_get_cast_excludes_row_size_vars() {
        let ds = make_ragged_dataset().await;
        let ragged = RaggedDataset::try_new(&ds).await.unwrap();
        let cast = ragged.get_cast(0).await.unwrap();

        assert!(cast.get_array("z_row_size").is_none());
        assert!(cast.get_array("z_row_size.sample_dimension").is_none());
    }

    #[tokio::test]
    async fn test_get_cast_out_of_bounds() {
        let ds = make_ragged_dataset().await;
        let ragged = RaggedDataset::try_new(&ds).await.unwrap();
        let err = ragged.get_cast(3).await.unwrap_err();
        assert!(
            err.to_string().contains("out of bounds"),
            "expected out of bounds error, got: {err}"
        );
    }

    // ── iterator ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_iter_count_and_exact_size() {
        let ds = make_ragged_dataset().await;
        let ragged = RaggedDataset::try_new(&ds).await.unwrap();

        let iter = ragged.iter();
        assert_eq!(iter.len(), 3);

        let indices: Vec<usize> = ragged.iter().map(|(i, _)| i).collect();
        assert_eq!(indices, vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn test_iter_get_cast_round_trip() {
        let ds = make_ragged_dataset().await;
        let ragged = RaggedDataset::try_new(&ds).await.unwrap();

        for (i, r) in ragged.iter() {
            let cast = r.get_cast(i).await.unwrap();
            assert_eq!(cast.name, format!("casts[{i}]"));
            // Every cast should have station_id, depth, temperature
            assert!(cast.get_array("station_id").is_some());
            assert!(cast.get_array("depth").is_some());
            assert!(cast.get_array("temperature").is_some());
        }
    }

    // ── multi-obs-dimension scenario ─────────────────────────────────

    #[tokio::test]
    async fn test_multiple_observation_dimensions() {
        // Two obs dims: z_obs (row sizes [1, 2]) and t_obs (row sizes [3, 1])
        let station_id = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![10.0, 20.0],
            vec![2],
            vec!["casts".into()],
            None,
        )
        .unwrap();

        let z_row_size = NdArray::<i32>::try_new_from_vec_in_mem(
            vec![1, 2],
            vec![2],
            vec!["casts".into()],
            None,
        )
        .unwrap();

        let z_sample_dim = NdArray::<String>::try_new_from_vec_in_mem(
            vec!["z_obs".into()],
            vec![1],
            vec![] as Vec<String>,
            None,
        )
        .unwrap();

        let t_row_size = NdArray::<i32>::try_new_from_vec_in_mem(
            vec![3, 1],
            vec![2],
            vec!["casts".into()],
            None,
        )
        .unwrap();

        let t_sample_dim = NdArray::<String>::try_new_from_vec_in_mem(
            vec!["t_obs".into()],
            vec![1],
            vec![] as Vec<String>,
            None,
        )
        .unwrap();

        let depth = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![5.0, 10.0, 15.0],
            vec![3],
            vec!["z_obs".into()],
            None,
        )
        .unwrap();

        let time = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![100.0, 200.0, 300.0, 400.0],
            vec![4],
            vec!["t_obs".into()],
            None,
        )
        .unwrap();

        let ds = make_dataset(
            "multi",
            vec![
                ("station_id", Arc::new(station_id)),
                ("z_row_size", Arc::new(z_row_size)),
                ("z_row_size.sample_dimension", Arc::new(z_sample_dim)),
                ("t_row_size", Arc::new(t_row_size)),
                ("t_row_size.sample_dimension", Arc::new(t_sample_dim)),
                ("depth", Arc::new(depth)),
                ("time", Arc::new(time)),
            ],
        )
        .await;

        let ragged = RaggedDataset::try_new(&ds).await.unwrap();
        assert_eq!(ragged.len(), 2);

        // Cast 0: z_obs=[5.0], t_obs=[100, 200, 300]
        let c0 = ragged.get_cast(0).await.unwrap();
        assert_eq!(read_f64(&c0, "depth").await, vec![5.0]);
        assert_eq!(read_f64(&c0, "time").await, vec![100.0, 200.0, 300.0]);

        // Cast 1: z_obs=[10, 15], t_obs=[400]
        let c1 = ragged.get_cast(1).await.unwrap();
        assert_eq!(read_f64(&c1, "depth").await, vec![10.0, 15.0]);
        assert_eq!(read_f64(&c1, "time").await, vec![400.0]);
    }

    #[tokio::test]
    async fn test_instance_dim_not_hardcoded_to_casts() {
        // Use "profiles" instead of "casts" as the instance dimension.
        let lat = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![55.0, 66.0],
            vec![2],
            vec!["profiles".into()],
            None,
        )
        .unwrap();

        let z_row_size = NdArray::<i32>::try_new_from_vec_in_mem(
            vec![3, 2],
            vec![2],
            vec!["profiles".into()],
            None,
        )
        .unwrap();

        let sample_dim = NdArray::<String>::try_new_from_vec_in_mem(
            vec!["z_obs".into()],
            vec![1],
            vec![] as Vec<String>,
            None,
        )
        .unwrap();

        let depth = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![1.0, 2.0, 3.0, 4.0, 5.0],
            vec![5],
            vec!["z_obs".into()],
            None,
        )
        .unwrap();

        let ds = make_dataset(
            "custom_dim",
            vec![
                ("lat", Arc::new(lat)),
                ("z_row_size", Arc::new(z_row_size)),
                ("z_row_size.sample_dimension", Arc::new(sample_dim)),
                ("depth", Arc::new(depth)),
            ],
        )
        .await;

        let ragged = RaggedDataset::try_new(&ds).await.unwrap();

        assert_eq!(ragged.instance_dimension(), "profiles");
        assert_eq!(ragged.len(), 2);

        let c0 = ragged.get_cast(0).await.unwrap();
        assert_eq!(read_f64(&c0, "lat").await, vec![55.0]);
        assert_eq!(read_f64(&c0, "depth").await, vec![1.0, 2.0, 3.0]);

        let c1 = ragged.get_cast(1).await.unwrap();
        assert_eq!(read_f64(&c1, "lat").await, vec![66.0]);
        assert_eq!(read_f64(&c1, "depth").await, vec![4.0, 5.0]);
    }

    // ── AnyDataset ───────────────────────────────────────────────────

    #[tokio::test]
    async fn test_any_dataset_detects_ragged() {
        let ds = make_ragged_dataset().await;
        let any = AnyDataset::try_from_dataset(ds).await.unwrap();

        assert!(any.is_ragged());
        assert!(any.as_ragged().is_some());
        assert!(any.as_regular().is_none());
        assert_eq!(any.name(), "ragged");
    }

    #[tokio::test]
    async fn test_any_dataset_detects_regular() {
        let arr = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![1.0, 2.0],
            vec![2],
            vec!["x".into()],
            None,
        )
        .unwrap();
        let ds = make_dataset("regular", vec![("data", Arc::new(arr))]).await;
        let any = AnyDataset::try_from_dataset(ds).await.unwrap();

        assert!(!any.is_ragged());
        assert!(any.as_regular().is_some());
        assert!(any.as_ragged().is_none());
        assert_eq!(any.name(), "regular");
    }

    #[tokio::test]
    async fn test_any_dataset_get_array() {
        let ds = make_ragged_dataset().await;
        let any = AnyDataset::try_from_dataset(ds).await.unwrap();

        assert!(any.get_array("station_id").is_some());
        assert!(any.get_array("nonexistent").is_none());
    }

    #[tokio::test]
    async fn test_any_dataset_ragged_get_cast() {
        let ds = make_ragged_dataset().await;
        let any = AnyDataset::try_from_dataset(ds).await.unwrap();
        let ragged = any.as_ragged().unwrap();

        assert_eq!(ragged.len(), 3);
        let cast = ragged.get_cast(0).await.unwrap();
        assert_eq!(read_f64(&cast, "depth").await, vec![10.0, 20.0]);
    }

    // ── fields ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_fields_regular_returns_all_arrays() {
        let a = NdArray::<f64>::try_new_from_vec_in_mem(vec![1.0], vec![1], vec!["x".into()], None)
            .unwrap();
        let b = NdArray::<i32>::try_new_from_vec_in_mem(vec![42], vec![1], vec!["x".into()], None)
            .unwrap();
        let ds = make_dataset(
            "regular",
            vec![("beta", Arc::new(b)), ("alpha", Arc::new(a))],
        )
        .await;
        let any = AnyDataset::try_from_dataset(ds).await.unwrap();

        let fields = any.fields();
        assert_eq!(fields.len(), 2);
        // Sorted alphabetically.
        let keys: Vec<&String> = fields.keys().collect();
        assert_eq!(keys, vec!["alpha", "beta"]);
        assert_eq!(fields["alpha"], NdArrayDataType::F64);
        assert_eq!(fields["beta"], NdArrayDataType::I32);
    }

    #[tokio::test]
    async fn test_fields_ragged_excludes_row_size_vars() {
        let ds = make_ragged_dataset().await;
        let any = AnyDataset::try_from_dataset(ds).await.unwrap();

        let fields = any.fields();

        // Row-size var `z_row_size` and its attribute
        // `z_row_size.sample_dimension` must be absent.
        assert!(!fields.contains_key("z_row_size"));
        assert!(!fields.contains_key("z_row_size.sample_dimension"));
    }

    #[tokio::test]
    async fn test_fields_ragged_includes_instance_vars() {
        let ds = make_ragged_dataset().await;
        let any = AnyDataset::try_from_dataset(ds).await.unwrap();

        let fields = any.fields();
        assert!(fields.contains_key("station_id"));
        assert_eq!(fields["station_id"], NdArrayDataType::F64);
    }

    #[tokio::test]
    async fn test_fields_ragged_includes_variable_attrs() {
        let ds = make_ragged_dataset().await;
        let any = AnyDataset::try_from_dataset(ds).await.unwrap();

        let fields = any.fields();
        assert!(fields.contains_key("depth.units"));
        assert_eq!(fields["depth.units"], NdArrayDataType::String);
        assert!(fields.contains_key("temperature.units"));
        assert_eq!(fields["temperature.units"], NdArrayDataType::String);
    }

    #[tokio::test]
    async fn test_fields_ragged_includes_global_attrs() {
        let ds = make_ragged_dataset().await;
        let any = AnyDataset::try_from_dataset(ds).await.unwrap();

        let fields = any.fields();
        assert!(fields.contains_key("Conventions"));
        assert_eq!(fields["Conventions"], NdArrayDataType::String);
    }

    #[tokio::test]
    async fn test_fields_ragged_sorted_by_name() {
        let ds = make_ragged_dataset().await;
        let any = AnyDataset::try_from_dataset(ds).await.unwrap();

        let fields = any.fields();
        let keys: Vec<&String> = fields.keys().collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted);
    }

    #[tokio::test]
    async fn test_fields_ragged_expected_set() {
        let ds = make_ragged_dataset().await;
        let any = AnyDataset::try_from_dataset(ds).await.unwrap();

        let fields = any.fields();
        let mut expected = vec![
            "Conventions",
            "depth",
            "depth.units",
            "station_id",
            "temperature",
            "temperature.units",
        ];
        expected.sort();
        let actual: Vec<&str> = fields.keys().map(|s| s.as_str()).collect();
        assert_eq!(actual, expected);
    }

    // ── projection ───────────────────────────────────────────────────

    #[tokio::test]
    async fn test_project_keeps_selected_variables() {
        let ds = make_ragged_dataset().await;
        let ragged = RaggedDataset::try_new(&ds).await.unwrap();

        // Variables are sorted: Conventions(0), depth(1), depth.units(2),
        // station_id(3), temperature(4), temperature.units(5)
        let projected = ragged.project(&[1, 3]).unwrap();

        let keys: Vec<&String> = projected.variables.keys().collect();
        assert_eq!(keys, vec!["depth", "station_id"]);
        // Metadata (instance_dim, n_instances) should be preserved.
        assert_eq!(projected.instance_dimension(), "casts");
        assert_eq!(projected.len(), 3);
    }

    #[tokio::test]
    async fn test_project_get_cast_still_works() {
        let ds = make_ragged_dataset().await;
        let ragged = RaggedDataset::try_new(&ds).await.unwrap();

        // Keep depth(1) and station_id(3) only.
        let projected = ragged.project(&[1, 3]).unwrap();
        let cast = projected.get_cast(0).await.unwrap();

        assert_eq!(read_f64(&cast, "station_id").await, vec![100.0]);
        assert_eq!(read_f64(&cast, "depth").await, vec![10.0, 20.0]);
        // temperature was excluded by projection.
        assert!(cast.get_array("temperature").is_none());
    }

    #[tokio::test]
    async fn test_project_out_of_bounds() {
        let ds = make_ragged_dataset().await;
        let ragged = RaggedDataset::try_new(&ds).await.unwrap();

        let err = ragged.project(&[99]).unwrap_err();
        assert!(
            err.to_string().contains("out of bounds"),
            "expected out of bounds error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_project_empty_keeps_nothing() {
        let ds = make_ragged_dataset().await;
        let ragged = RaggedDataset::try_new(&ds).await.unwrap();

        let projected = ragged.project(&[]).unwrap();
        assert!(projected.variables.is_empty());
        assert_eq!(projected.len(), 3);
    }

    // ── AnyDataset projection ────────────────────────────────────────

    #[tokio::test]
    async fn test_any_project_ragged_index_projection() {
        use crate::projection::DatasetProjection;

        let ds = make_ragged_dataset().await;
        let any = AnyDataset::try_from_dataset(ds).await.unwrap();

        // Variables sorted: Conventions(0), depth(1), depth.units(2),
        // station_id(3), temperature(4), temperature.units(5)
        let proj = DatasetProjection::new_with_index_projection(vec![1, 3]);
        let projected = any.project(&proj).unwrap();

        assert!(projected.is_ragged());
        let ragged = projected.as_ragged().unwrap();
        let keys: Vec<&String> = ragged.variables.keys().collect();
        assert_eq!(keys, vec!["depth", "station_id"]);

        // get_cast should still work on the projected ragged.
        let cast = ragged.get_cast(0).await.unwrap();
        assert_eq!(read_f64(&cast, "depth").await, vec![10.0, 20.0]);
        assert_eq!(read_f64(&cast, "station_id").await, vec![100.0]);
        assert!(cast.get_array("temperature").is_none());
    }

    #[tokio::test]
    async fn test_any_project_ragged_dimension_projection_rejected() {
        use crate::projection::DatasetProjection;

        let ds = make_ragged_dataset().await;
        let any = AnyDataset::try_from_dataset(ds).await.unwrap();

        let proj = DatasetProjection::new_with_dimension_projection(vec!["casts".into()]);
        let err = any.project(&proj).unwrap_err();
        assert!(
            err.to_string().contains("dimension projections"),
            "expected dimension projection error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_any_project_ragged_no_projection_clones() {
        use crate::projection::DatasetProjection;

        let ds = make_ragged_dataset().await;
        let any = AnyDataset::try_from_dataset(ds).await.unwrap();

        let proj = DatasetProjection::default();
        let projected = any.project(&proj).unwrap();

        assert!(projected.is_ragged());
        let ragged = projected.as_ragged().unwrap();
        assert_eq!(ragged.len(), 3);
        assert_eq!(ragged.variables.len(), 6);
    }

    #[tokio::test]
    async fn test_any_project_regular_index_projection() {
        use crate::projection::DatasetProjection;

        let a = NdArray::<f64>::try_new_from_vec_in_mem(vec![1.0], vec![1], vec!["x".into()], None)
            .unwrap();
        let b = NdArray::<i32>::try_new_from_vec_in_mem(vec![42], vec![1], vec!["x".into()], None)
            .unwrap();
        let ds = make_dataset(
            "regular",
            vec![("alpha", Arc::new(a)), ("beta", Arc::new(b))],
        )
        .await;
        let any = AnyDataset::try_from_dataset(ds).await.unwrap();

        // Keep only index 1 ("beta").
        let proj = DatasetProjection::new_with_index_projection(vec![1]);
        let projected = any.project(&proj).unwrap();

        assert!(!projected.is_ragged());
        let ds = projected.dataset();
        assert_eq!(ds.arrays.len(), 1);
        assert!(ds.get_array("beta").is_some());
        assert!(ds.get_array("alpha").is_none());
    }

    // ── default_broadcast_dimensions ─────────────────────────────────

    /// Helper: f64 array with the given dims (shape filled with 1s).
    async fn arr(dims: &[&str]) -> Arc<dyn NdArrayD> {
        let shape: Vec<usize> = vec![1; dims.len()];
        arr_shaped(dims, &shape).await
    }

    /// Helper: f64 array with the given dims and explicit shape (sizes per dim).
    async fn arr_shaped(dims: &[&str], shape: &[usize]) -> Arc<dyn NdArrayD> {
        let len: usize = shape.iter().product();
        let dim_names: Vec<String> = dims.iter().map(|d| d.to_string()).collect();
        Arc::new(
            NdArray::<f64>::try_new_from_vec_in_mem(vec![0.0; len], shape.to_vec(), dim_names, None)
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn test_default_dims_already_safe_returns_none() {
        // 2D var plus a 1D subset — already broadcast-safe.
        let ds = make_dataset(
            "safe",
            vec![("grid", arr(&["x", "y"]).await), ("scale", arr(&["y"]).await)],
        )
        .await;
        assert_eq!(ds.default_broadcast_dimensions(), None);
    }

    #[tokio::test]
    async fn test_default_dims_picks_group_with_most_variables() {
        // `temp(x,y,z)` is the highest-dimensionality variable; a CF-bounds-style
        // `bnds(y,nv)` has an extra `nv` dim. Several coords sit on (x,y,z) subsets.
        let ds = make_dataset(
            "incompatible",
            vec![
                ("temp", arr(&["x", "y", "z"]).await),
                ("bnds", arr(&["y", "nv"]).await),
                ("x", arr(&["x"]).await),
                ("y", arr(&["y"]).await),
                ("z", arr(&["z"]).await),
            ],
        )
        .await;
        assert_eq!(
            ds.default_broadcast_dimensions(),
            Some(vec!["x".to_string(), "y".to_string(), "z".to_string()])
        );
    }

    #[tokio::test]
    async fn test_default_dims_tie_prefers_larger_volume() {
        // Two incompatible groups retaining the same number of variables and each
        // the native grid of exactly one variable → the tie falls to data volume.
        // `big` (10×10 = 100 rows) must win over `small` (2×2 = 4 rows) even though
        // it is listed second, proving volume — not order — decides.
        let ds = make_dataset(
            "tie",
            vec![
                ("small", arr_shaped(&["c", "d"], &[2, 2]).await),
                ("big", arr_shaped(&["a", "b"], &[10, 10]).await),
            ],
        )
        .await;
        assert_eq!(
            ds.default_broadcast_dimensions(),
            Some(vec!["a".to_string(), "b".to_string()])
        );
    }

    /// An instrument file names no dimension, and holds one payload among many
    /// small metadata variables. The count picks the metadata; the volume picks
    /// the payload.
    ///
    /// The shape is that of a real DAS file: `data(1250, 23250)` in one group,
    /// three metadata variables on a 3-long axis in another.
    #[tokio::test]
    async fn test_default_dims_of_an_unnamed_file_follow_the_volume() {
        let arrays = vec![
            ("data", arr_shaped(&["len_1250", "len_23250"], &[1250, 23250]).await),
            ("meta_a", arr_shaped(&["len_3"], &[3]).await),
            ("meta_b", arr_shaped(&["len_3"], &[3]).await),
            ("meta_c", arr_shaped(&["len_3"], &[3]).await),
        ];

        // Named: the count wins, as it always has, and the payload is dropped.
        let named = make_dataset("named", arrays.clone()).await;
        assert_eq!(
            named.default_broadcast_dimensions(),
            Some(vec!["len_3".to_string()])
        );

        // Invented: the volume wins, and the payload survives.
        let unnamed = make_dataset("unnamed", arrays)
            .await
            .with_invented_dimensions(
                ["len_1250", "len_23250", "len_3"].map(String::from),
            );
        assert_eq!(
            unnamed.default_broadcast_dimensions(),
            Some(vec!["len_1250".to_string(), "len_23250".to_string()])
        );
    }

    /// One real name is enough to keep the count first. A file that names some
    /// axes and not others is a file with a convention, so it keeps the rule
    /// every other file follows.
    #[tokio::test]
    async fn test_one_named_dimension_keeps_the_variable_count_first() {
        let ds = make_dataset(
            "mixed",
            vec![
                ("data", arr_shaped(&["time", "len_23250"], &[1250, 23250]).await),
                ("meta_a", arr_shaped(&["len_3"], &[3]).await),
                ("meta_b", arr_shaped(&["len_3"], &[3]).await),
                ("meta_c", arr_shaped(&["len_3"], &[3]).await),
            ],
        )
        .await
        .with_invented_dimensions(["len_23250", "len_3"].map(String::from));

        assert_eq!(
            ds.default_broadcast_dimensions(),
            Some(vec!["len_3".to_string()])
        );
    }

    /// An empty grid never wins, whatever names the file gives.
    #[tokio::test]
    async fn test_an_unnamed_file_still_skips_an_empty_grid() {
        let ds = make_dataset(
            "unnamed-empty",
            vec![
                // The largest grid by shape holds no rows at all.
                ("growing", arr_shaped(&["len_0", "len_9"], &[0, 9]).await),
                ("payload", arr_shaped(&["len_4"], &[4]).await),
            ],
        )
        .await
        .with_invented_dimensions(["len_0", "len_9", "len_4"].map(String::from));

        assert_eq!(
            ds.default_broadcast_dimensions(),
            Some(vec!["len_4".to_string()])
        );
    }

    #[tokio::test]
    async fn test_default_dims_skips_empty_grid_for_nonempty_alternative() {
        // The `hist × prof` grid is the native home of MORE variables (3 vs 1) but
        // `hist` is a currently-empty unlimited dimension (size 0), so projecting to
        // it would broadcast the table down to zero rows. The non-empty `prof × lev`
        // grid must win despite its lower variable count.
        let ds = make_dataset(
            "empty-history",
            vec![
                ("history_a", arr_shaped(&["hist", "prof"], &[0, 5]).await),
                ("history_b", arr_shaped(&["hist", "prof"], &[0, 5]).await),
                ("history_c", arr_shaped(&["hist", "prof"], &[0, 5]).await),
                ("pres", arr_shaped(&["prof", "lev"], &[5, 3]).await),
            ],
        )
        .await;
        assert_eq!(
            ds.default_broadcast_dimensions(),
            Some(vec!["prof".to_string(), "lev".to_string()])
        );
    }

    #[tokio::test]
    async fn test_default_dims_tie_on_count_prefers_native_data_grid() {
        // Mirrors an Argo profile file: a `prof × levels` data grid (many
        // variables) and a `hist × prof × time` bookkeeping grid (one variable)
        // both retain every scalar and `prof`-only variable, so they tie on
        // total count. The data grid — native home of the most variables — must
        // win, even though the bookkeeping grid is higher-dimensional.
        //
        // Counts are balanced so the two grids tie on total retained variables:
        //   data grid `prof × levels`     : 1 shared `prof` + 3 exact          = 4
        //   bookkeeping `hist × prof × time`: 1 shared `prof` + 1 `time`
        //                                     + 1 `hist × prof` + 1 exact       = 4
        // Exact-match count (3 vs 1) is the deciding tie-break.
        let ds = make_dataset(
            "argo-like",
            vec![
                // 3 measurement variables share the `prof × levels` data grid.
                ("pres", arr(&["prof", "levels"]).await),
                ("temp", arr(&["prof", "levels"]).await),
                ("psal", arr(&["prof", "levels"]).await),
                // Bookkeeping grid: 1 exact var + subset vars that pad its count.
                ("history_date", arr(&["hist", "prof", "time"]).await),
                ("history_institution", arr(&["hist", "prof"]).await),
                ("date_creation", arr(&["time"]).await),
                // A shared `prof`-only variable broadcasts onto either grid.
                ("juld", arr(&["prof"]).await),
            ],
        )
        .await;
        assert_eq!(
            ds.default_broadcast_dimensions(),
            Some(vec!["prof".to_string(), "levels".to_string()])
        );
    }

    #[tokio::test]
    async fn test_default_dims_scalars_and_attributes_do_not_sway_choice() {
        // Scalar entries (here standing in for NetCDF attributes like `temp.units`)
        // broadcast onto every grid and must not tip the score. The data grid wins
        // on gridded-variable count regardless of how many scalars surround it.
        let scalar = || async {
            NdArray::<f64>::try_new_from_vec_in_mem(vec![1.0], vec![1], vec![] as Vec<String>, None)
                .map(|a| Arc::new(a) as Arc<dyn NdArrayD>)
                .unwrap()
        };
        let ds = make_dataset(
            "with-attrs",
            vec![
                ("temp", arr(&["prof", "levels"]).await),
                ("psal", arr(&["prof", "levels"]).await),
                ("history_date", arr(&["hist", "prof", "time"]).await),
                ("temp.units", scalar().await),
                ("psal.units", scalar().await),
                (".Conventions", scalar().await),
                (".title", scalar().await),
            ],
        )
        .await;
        assert_eq!(
            ds.default_broadcast_dimensions(),
            Some(vec!["prof".to_string(), "levels".to_string()])
        );
    }

    #[tokio::test]
    async fn test_default_dims_full_tie_is_deterministic_first_encountered() {
        // Equal variable count and equal dimensionality → first-encountered wins.
        let ds = make_dataset(
            "fulltie",
            vec![("first", arr(&["a", "b"]).await), ("second", arr(&["c", "d"]).await)],
        )
        .await;
        assert_eq!(
            ds.default_broadcast_dimensions(),
            Some(vec!["a".to_string(), "b".to_string()])
        );
    }

    #[tokio::test]
    async fn test_default_dims_all_scalar_returns_none() {
        let scalar = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![1.0],
            vec![1],
            vec![] as Vec<String>,
            None,
        )
        .unwrap();
        let ds = make_dataset("scalars", vec![("attr", Arc::new(scalar))]).await;
        assert_eq!(ds.default_broadcast_dimensions(), None);
    }

    #[tokio::test]
    async fn test_default_dims_empty_dataset_returns_none() {
        let ds = make_dataset("empty", vec![]).await;
        assert_eq!(ds.default_broadcast_dimensions(), None);
    }

    #[tokio::test]
    async fn test_any_default_dims_ragged_returns_none() {
        let ds = make_ragged_dataset().await;
        let any = AnyDataset::try_from_dataset(ds).await.unwrap();
        assert_eq!(any.default_broadcast_dimensions(), None);
    }

    // ── project_with_dimensions ──────────────────────────────────────

    #[tokio::test]
    async fn test_project_with_dimensions_drops_incompatible_variables() {
        // Narrowing to the `temp` grid must drop the CF-bounds variable that
        // lives on the extra `nv` axis.
        let ds = make_dataset(
            "bounds",
            vec![
                ("temp", arr_shaped(&["x", "y"], &[2, 3]).await),
                ("y_bnds", arr_shaped(&["y", "nv"], &[3, 2]).await),
                ("y", arr_shaped(&["y"], &[3]).await),
            ],
        )
        .await;

        let projected = ds
            .project_with_dimensions(&["x".to_string(), "y".to_string()])
            .unwrap();

        let names: Vec<&String> = projected.arrays.keys().collect();
        assert_eq!(names, vec!["temp", "y"]);
    }

    #[tokio::test]
    async fn test_project_with_dimensions_prunes_orphaned_dimensions() {
        // `nv` survives in `self.dimensions` only through the dropped bounds
        // variable; keeping it would inflate the broadcast grid by a factor of
        // its size and multiply the row count.
        let ds = make_dataset(
            "bounds",
            vec![
                ("temp", arr_shaped(&["x", "y"], &[2, 3]).await),
                ("y_bnds", arr_shaped(&["y", "nv"], &[3, 2]).await),
            ],
        )
        .await;
        assert!(ds.dimensions.contains_key("nv"));

        let projected = ds
            .project_with_dimensions(&["x".to_string(), "y".to_string()])
            .unwrap();

        assert_eq!(
            projected.dimensions.keys().collect::<Vec<_>>(),
            vec!["x", "y"]
        );
    }

    #[tokio::test]
    async fn test_project_with_dimensions_keeps_original_dimension_order() {
        // The retained dimensions follow the dataset's own order, not the order
        // in which they were requested.
        let ds = make_dataset(
            "ordered",
            vec![("temp", arr_shaped(&["x", "y", "z"], &[2, 3, 4]).await)],
        )
        .await;

        let projected = ds
            .project_with_dimensions(&["z".to_string(), "x".to_string(), "y".to_string()])
            .unwrap();

        assert_eq!(
            projected.dimensions.keys().collect::<Vec<_>>(),
            vec!["x", "y", "z"]
        );
    }

    #[tokio::test]
    async fn test_project_with_dimensions_rejects_unknown_dimension() {
        let ds = make_dataset("d", vec![("temp", arr(&["x"]).await)]).await;
        let err = ds
            .project_with_dimensions(&["nope".to_string()])
            .unwrap_err();
        assert!(
            err.to_string().contains("not found in dataset"),
            "expected unknown dimension error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_dataset_project_index_out_of_bounds() {
        let ds = make_dataset("d", vec![("temp", arr(&["x"]).await)]).await;
        let err = ds.project(&[5]).unwrap_err();
        assert!(
            err.to_string().contains("out of bounds"),
            "expected out of bounds error, got: {err}"
        );
    }

    // ── resolve_read_dimensions ──────────────────────────────────────

    #[tokio::test]
    async fn test_resolve_read_dimensions_explicit_wins_over_default() {
        // The dataset needs narrowing, but an explicit request must be honoured
        // verbatim — even when it differs from the auto-selected default.
        let ds = make_dataset(
            "incompatible",
            vec![
                ("temp", arr(&["x", "y", "z"]).await),
                ("bnds", arr(&["y", "nv"]).await),
            ],
        )
        .await;
        let any = AnyDataset::try_from_dataset(ds).await.unwrap();

        let resolved =
            resolve_read_dimensions(&any, Some(vec!["y".to_string(), "nv".to_string()]), None);
        assert_eq!(
            resolved,
            Some(vec!["y".to_string(), "nv".to_string()])
        );
    }

    #[tokio::test]
    async fn test_resolve_read_dimensions_explicit_empty_is_still_explicit() {
        // An empty explicit list means "project to no dimensions", which is not
        // the same as "no projection requested".
        let ds = make_dataset(
            "incompatible",
            vec![
                ("temp", arr(&["x", "y", "z"]).await),
                ("bnds", arr(&["y", "nv"]).await),
            ],
        )
        .await;
        let any = AnyDataset::try_from_dataset(ds).await.unwrap();

        assert_eq!(resolve_read_dimensions(&any, Some(vec![]), None), Some(vec![]));
    }

    #[tokio::test]
    async fn test_resolve_read_dimensions_falls_back_to_default() {
        let ds = make_dataset(
            "incompatible",
            vec![
                ("temp", arr(&["x", "y", "z"]).await),
                ("bnds", arr(&["y", "nv"]).await),
            ],
        )
        .await;
        let any = AnyDataset::try_from_dataset(ds).await.unwrap();

        assert_eq!(
            resolve_read_dimensions(&any, None, None),
            Some(vec!["x".to_string(), "y".to_string(), "z".to_string()])
        );
    }

    #[tokio::test]
    async fn test_resolve_read_dimensions_none_when_already_broadcast_safe() {
        let ds = make_dataset(
            "safe",
            vec![("grid", arr(&["x", "y"]).await), ("scale", arr(&["y"]).await)],
        )
        .await;
        let any = AnyDataset::try_from_dataset(ds).await.unwrap();

        assert_eq!(resolve_read_dimensions(&any, None, None), None);
    }

    #[tokio::test]
    async fn test_resolve_read_dimensions_none_for_ragged() {
        let ds = make_ragged_dataset().await;
        let any = AnyDataset::try_from_dataset(ds).await.unwrap();

        assert_eq!(resolve_read_dimensions(&any, None, None), None);
        // An explicit request is still passed through untouched; rejecting it is
        // `AnyDataset::project`'s job, not this function's.
        assert_eq!(
            resolve_read_dimensions(&any, Some(vec!["casts".to_string()]), None),
            Some(vec!["casts".to_string()])
        );
    }

    #[tokio::test]
    async fn test_resolve_read_dimensions_log_label_does_not_change_the_result() {
        let ds = make_dataset(
            "incompatible",
            vec![
                ("temp", arr(&["x", "y", "z"]).await),
                ("bnds", arr(&["y", "nv"]).await),
            ],
        )
        .await;
        let any = AnyDataset::try_from_dataset(ds).await.unwrap();

        assert_eq!(
            resolve_read_dimensions(&any, None, Some("netcdf")),
            resolve_read_dimensions(&any, None, None)
        );
    }

    #[tokio::test]
    async fn test_resolve_read_dimensions_result_is_projectable() {
        // The contract that matters downstream: whatever this returns can be fed
        // straight into `project_with_dimensions` without erroring, and the
        // result is broadcast-safe (every survivor's dims fit the max-rank one).
        let ds = make_dataset(
            "argo-like",
            vec![
                ("pres", arr_shaped(&["prof", "levels"], &[5, 3]).await),
                ("temp", arr_shaped(&["prof", "levels"], &[5, 3]).await),
                ("history", arr_shaped(&["hist", "prof"], &[2, 5]).await),
                ("juld", arr_shaped(&["prof"], &[5]).await),
            ],
        )
        .await;
        let any = AnyDataset::try_from_dataset(ds.clone()).await.unwrap();

        let dims = resolve_read_dimensions(&any, None, None).expect("narrowing expected");
        let projected = ds.project_with_dimensions(&dims).unwrap();

        let max_dims = projected
            .arrays
            .values()
            .max_by_key(|a| a.dimensions().len())
            .unwrap()
            .dimensions();
        assert!(
            projected
                .arrays
                .values()
                .all(|a| a.dimensions().iter().all(|d| max_dims.contains(d))),
            "projected dataset is not broadcast-safe: {:?}",
            projected.dimensions
        );
        assert!(projected.arrays.contains_key("pres"));
        assert!(!projected.arrays.contains_key("history"));
    }

    #[tokio::test]
    async fn test_any_default_dims_regular_delegates() {
        let ds = make_dataset(
            "incompatible",
            vec![
                ("temp", arr(&["x", "y", "z"]).await),
                ("bnds", arr(&["y", "nv"]).await),
            ],
        )
        .await;
        let any = AnyDataset::try_from_dataset(ds).await.unwrap();
        assert_eq!(
            any.default_broadcast_dimensions(),
            Some(vec!["x".to_string(), "y".to_string(), "z".to_string()])
        );
    }
}
