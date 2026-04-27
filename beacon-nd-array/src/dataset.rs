use crate::{NdArray, NdArrayD, array::subset::ArraySubset, datatypes::NdArrayDataType};
use indexmap::IndexMap;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::OnceCell;

#[derive(Debug, Clone)]
pub struct Dataset {
    pub name: String,
    pub dimensions: IndexMap<String, usize>,
    pub arrays: IndexMap<String, Arc<dyn NdArrayD>>,
}

impl Dataset {
    pub async fn new(name: String, arrays: IndexMap<String, Arc<dyn NdArrayD>>) -> Self {
        let mut dimensions = indexmap::IndexMap::new();
        for array in arrays.values() {
            for (dim_name, dim_size) in array.dimensions().iter().zip(array.shape().iter()) {
                dimensions.insert(dim_name.clone(), *dim_size);
            }
        }
        Self {
            name,
            dimensions,
            arrays,
        }
    }

    pub fn get_array_names(&self) -> Vec<String> {
        self.arrays.keys().cloned().collect()
    }

    pub fn get_array(&self, name: &str) -> Option<&Arc<dyn NdArrayD>> {
        self.arrays.get(name)
    }

    pub fn get_array_datatype(&self, name: &str) -> Option<NdArrayDataType> {
        self.get_array(name).map(|array| array.datatype())
    }

    /// Detect whether this dataset follows the CF ragged-array convention.
    ///
    /// A dataset is considered **ragged** when at least one variable carries a
    /// `sample_dimension` attribute (surfaced as `"<var>.sample_dimension"` in
    /// the array map).  Otherwise it is **regular**.
    pub fn cf_dataset_type(&self) -> CfDatasetType {
        let is_ragged = self
            .arrays
            .keys()
            .any(|name| name.ends_with(".sample_dimension"));

        if is_ragged {
            CfDatasetType::Ragged
        } else {
            CfDatasetType::Regular
        }
    }
}

/// A view over a [`Dataset`] that follows the CF contiguous ragged-array
/// convention.
///
/// In a ragged dataset the *instance* dimension (e.g. `casts`) indexes
/// profiles/casts, while one or more *observation* dimensions (e.g.
/// `z_obs`, `Temperature_obs`) hold the flattened per-cast data.  The
/// link between the two is established by *row-size* variables that
/// carry a `sample_dimension` attribute.
///
/// Construction via [`try_new`](Self::try_new) eagerly discovers row-size
/// variables, determines the instance dimension, and classifies all
/// variables. Only the cumulative offset tables (which require reading
/// the row-size data arrays) are computed lazily on first
/// [`get_cast`](Self::get_cast) call.
///
/// Row-size variables and their attributes are consumed internally to
/// build offset tables and are **not** included in per-cast datasets.
/// All other attributes (global and per-variable) are preserved and
/// included in every extracted cast.
pub struct RaggedDataset {
    dataset: Dataset,
    instance_dim: String,
    n_instances: usize,
    /// obs_dim name → row-size variable name, kept for lazy offset computation.
    row_size_vars: HashMap<String, String>,
    /// Cumulative offsets per observation dimension, computed lazily.
    offsets: OnceCell<HashMap<String, Vec<usize>>>,
    /// Observation-dimension → variable names on that dimension.
    pub(crate) obs_variables: HashMap<String, Vec<String>>,
    /// Variable names on the instance dimension (excludes row-size vars).
    pub(crate) instance_variables: Vec<String>,
    /// Global attributes (dimensionless, no `.` in name).
    pub(crate) global_attributes: IndexMap<String, Arc<dyn NdArrayD>>,
    /// Per-variable attributes (`"var.attr"` keys, excluding row-size var attrs).
    pub(crate) variable_attributes: IndexMap<String, Arc<dyn NdArrayD>>,
}

impl RaggedDataset {
    /// Build a `RaggedDataset` from a [`Dataset`] that follows the CF
    /// contiguous ragged-array convention.
    ///
    /// This eagerly discovers row-size variables, determines the instance
    /// dimension, and classifies all variables. The cumulative offset
    /// tables are **not** built until the first [`get_cast`](Self::get_cast)
    /// call.
    ///
    /// # Errors
    ///
    /// Returns an error if no `sample_dimension` attribute is found or
    /// the instance dimension cannot be determined.
    pub async fn try_new(dataset: Dataset) -> anyhow::Result<Self> {
        // ── 1. Discover row-size variables via *.sample_dimension attrs ──
        let mut row_size_vars: HashMap<String, String> = HashMap::new(); // obs_dim → rs_var

        for key in dataset.arrays.keys() {
            if let Some(var_name) = key.strip_suffix(".sample_dimension") {
                let attr_array = dataset
                    .get_array(key)
                    .ok_or_else(|| anyhow::anyhow!("missing array for {key}"))?;
                let typed = attr_array
                    .as_any()
                    .downcast_ref::<NdArray<String>>()
                    .ok_or_else(|| anyhow::anyhow!("{key} is not a String array"))?;
                let values = typed.clone_into_raw_vec().await;
                let obs_dim = values
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("{key} is empty"))?;
                row_size_vars.insert(obs_dim, var_name.to_string());
            }
        }

        if row_size_vars.is_empty() {
            anyhow::bail!("no sample_dimension attributes found — not a ragged dataset");
        }

        // ── 2. Determine the instance dimension ─────────────────────────
        let first_rs_var = row_size_vars.values().next().unwrap();
        let instance_dim = dataset
            .get_array(first_rs_var)
            .ok_or_else(|| anyhow::anyhow!("row-size variable {first_rs_var} not in dataset"))?
            .dimensions()
            .first()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("{first_rs_var} has no dimensions"))?;

        let n_instances = *dataset
            .dimensions
            .get(&instance_dim)
            .ok_or_else(|| anyhow::anyhow!("instance dimension {instance_dim} not in dataset"))?;

        // ── 3. Classify variables and collect attributes ─────────────────
        let obs_dim_set: std::collections::HashSet<&str> =
            row_size_vars.keys().map(String::as_str).collect();
        let row_size_var_set: std::collections::HashSet<&str> =
            row_size_vars.values().map(String::as_str).collect();

        let mut obs_variables: HashMap<String, Vec<String>> = HashMap::new();
        let mut instance_variables: Vec<String> = Vec::new();
        let mut global_attributes: IndexMap<String, Arc<dyn NdArrayD>> = IndexMap::new();
        let mut variable_attributes: IndexMap<String, Arc<dyn NdArrayD>> = IndexMap::new();

        for (name, array) in &dataset.arrays {
            if name.contains('.') {
                let var_part = name.split('.').next().unwrap_or("");
                if !row_size_var_set.contains(var_part) {
                    variable_attributes.insert(name.clone(), array.clone());
                }
                continue;
            }

            if row_size_var_set.contains(name.as_str()) {
                continue;
            }

            let dims = array.dimensions();
            if dims.is_empty() {
                global_attributes.insert(name.clone(), array.clone());
                continue;
            }

            let leading = &dims[0];
            if obs_dim_set.contains(leading.as_str()) {
                obs_variables
                    .entry(leading.clone())
                    .or_default()
                    .push(name.clone());
            } else if leading == &instance_dim {
                instance_variables.push(name.clone());
            }
        }

        Ok(Self {
            dataset,
            instance_dim,
            n_instances,
            row_size_vars,
            offsets: OnceCell::new(),
            obs_variables,
            instance_variables,
            global_attributes,
            variable_attributes,
        })
    }

    /// Lazily compute and cache the cumulative offset tables.
    async fn offsets(&self) -> anyhow::Result<&HashMap<String, Vec<usize>>> {
        self.offsets
            .get_or_try_init(|| self.compute_offsets())
            .await
    }

    async fn compute_offsets(&self) -> anyhow::Result<HashMap<String, Vec<usize>>> {
        let mut offsets: HashMap<String, Vec<usize>> = HashMap::new();

        for (obs_dim, rs_var) in &self.row_size_vars {
            let arr = self
                .dataset
                .get_array(rs_var)
                .ok_or_else(|| anyhow::anyhow!("row-size variable {rs_var} not in dataset"))?;
            let typed = arr
                .as_any()
                .downcast_ref::<NdArray<i32>>()
                .ok_or_else(|| anyhow::anyhow!("{rs_var} is not an i32 array"))?;
            let row_sizes = typed.clone_into_raw_vec().await;

            let mut cum = Vec::with_capacity(self.n_instances + 1);
            cum.push(0usize);
            for &sz in &row_sizes {
                cum.push(cum.last().unwrap() + sz as usize);
            }
            offsets.insert(obs_dim.clone(), cum);
        }

        Ok(offsets)
    }

    /// The number of casts (instances) in the dataset.
    pub fn len(&self) -> usize {
        self.n_instances
    }

    /// Returns `true` when the dataset contains no casts.
    pub fn is_empty(&self) -> bool {
        self.n_instances == 0
    }

    /// The name of the instance dimension (e.g. `"casts"`).
    pub fn instance_dimension(&self) -> &str {
        &self.instance_dim
    }

    /// A reference to the underlying [`Dataset`].
    pub fn dataset(&self) -> &Dataset {
        &self.dataset
    }

    /// Extract the sub-dataset for a single cast by zero-based index.
    ///
    /// On the first call this lazily reads the row-size arrays to build
    /// cumulative offset tables. Subsequent calls reuse the cached offsets.
    ///
    /// The returned [`Dataset`] contains:
    /// - Instance variables subsetted to a single element.
    /// - Observation variables subsetted by the row-size offsets.
    /// - Variable attributes for every included variable.
    /// - All global attributes.
    ///
    /// Row-size variables and their attributes are **not** included.
    pub async fn get_cast(&self, index: usize) -> anyhow::Result<Dataset> {
        if index >= self.n_instances {
            anyhow::bail!(
                "cast index {index} out of bounds for {} instances",
                self.n_instances
            );
        }

        let offsets = self.offsets().await?;
        let mut arrays: IndexMap<String, Arc<dyn NdArrayD>> = IndexMap::new();

        // ── Instance variables: single element at `index` ────────────────
        for var_name in &self.instance_variables {
            if let Some(array) = self.dataset.get_array(var_name) {
                let mut start = vec![0usize; array.shape().len()];
                let mut shape = array.shape();
                start[0] = index;
                shape[0] = 1;
                let sub = array.subset(ArraySubset::new(start, shape)).await?;
                arrays.insert(var_name.clone(), sub);
            }
        }

        // ── Observation variables: slice by cumulative offsets ────────────
        for (obs_dim, var_names) in &self.obs_variables {
            let cum = &offsets[obs_dim];
            let obs_start = cum[index];
            let obs_len = cum[index + 1] - obs_start;

            for var_name in var_names {
                if let Some(array) = self.dataset.get_array(var_name) {
                    let mut start = vec![0usize; array.shape().len()];
                    let mut shape = array.shape();
                    start[0] = obs_start;
                    shape[0] = obs_len;
                    let sub = array.subset(ArraySubset::new(start, shape)).await?;
                    arrays.insert(var_name.clone(), sub);
                }
            }
        }

        // ── Variable attributes (for included variables only) ────────────
        for (attr_key, attr_val) in &self.variable_attributes {
            let var_part = attr_key.split('.').next().unwrap_or("");
            if arrays.contains_key(var_part) {
                arrays.insert(attr_key.clone(), attr_val.clone());
            }
        }

        // ── Global attributes ────────────────────────────────────────────
        for (attr_key, attr_val) in &self.global_attributes {
            arrays.insert(attr_key.clone(), attr_val.clone());
        }

        arrays.sort_keys();

        let name = format!("{}[{index}]", self.dataset.name);
        Ok(Dataset::new(name, arrays).await)
    }

    /// Return an iterator that yields each cast index paired with a
    /// reference to this `RaggedDataset`, so the caller can `.await`
    /// [`get_cast`](Self::get_cast) at their own pace.
    pub fn iter(&self) -> RaggedDatasetIter<'_> {
        RaggedDatasetIter {
            ragged: self,
            n_instances: self.n_instances,
            index: 0,
        }
    }
}

impl std::fmt::Debug for RaggedDataset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaggedDataset")
            .field("dataset", &self.dataset)
            .field("instance_dim", &self.instance_dim)
            .field("n_instances", &self.n_instances)
            .field("offsets_loaded", &self.offsets.initialized())
            .finish()
    }
}

impl Clone for RaggedDataset {
    fn clone(&self) -> Self {
        let cell = OnceCell::new();
        if let Some(offsets) = self.offsets.get() {
            let _ = cell.set(offsets.clone());
        }
        Self {
            dataset: self.dataset.clone(),
            instance_dim: self.instance_dim.clone(),
            n_instances: self.n_instances,
            row_size_vars: self.row_size_vars.clone(),
            offsets: cell,
            obs_variables: self.obs_variables.clone(),
            instance_variables: self.instance_variables.clone(),
            global_attributes: self.global_attributes.clone(),
            variable_attributes: self.variable_attributes.clone(),
        }
    }
}

/// An iterator over the casts in a [`RaggedDataset`].
pub struct RaggedDatasetIter<'a> {
    ragged: &'a RaggedDataset,
    n_instances: usize,
    index: usize,
}

impl<'a> Iterator for RaggedDatasetIter<'a> {
    type Item = (usize, &'a RaggedDataset);

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.n_instances {
            let i = self.index;
            self.index += 1;
            Some((i, self.ragged))
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.n_instances - self.index;
        (remaining, Some(remaining))
    }
}

impl<'a> ExactSizeIterator for RaggedDatasetIter<'a> {}

/// A dataset that may be either regular or ragged (CF contiguous
/// ragged-array convention).
///
/// Use [`AnyDataset::try_from_dataset`] to automatically detect the
/// convention from `sample_dimension` attributes and build the
/// appropriate variant.
pub enum AnyDataset {
    Regular(Dataset),
    Ragged(RaggedDataset),
}

impl AnyDataset {
    /// Inspect a [`Dataset`] and wrap it as the appropriate variant.
    ///
    /// If the dataset contains any `sample_dimension` attributes it is
    /// treated as ragged; otherwise as regular.
    pub async fn try_from_dataset(dataset: Dataset) -> anyhow::Result<Self> {
        match dataset.cf_dataset_type() {
            CfDatasetType::Ragged => {
                let ragged = RaggedDataset::try_new(dataset).await?;
                Ok(Self::Ragged(ragged))
            }
            CfDatasetType::Regular => Ok(Self::Regular(dataset)),
        }
    }

    /// The name of the underlying dataset.
    pub fn name(&self) -> &str {
        match self {
            Self::Regular(ds) => &ds.name,
            Self::Ragged(r) => &r.dataset().name,
        }
    }

    /// A reference to the underlying [`Dataset`].
    pub fn dataset(&self) -> &Dataset {
        match self {
            Self::Regular(ds) => ds,
            Self::Ragged(r) => r.dataset(),
        }
    }

    /// Look up an array by name in the underlying dataset.
    pub fn get_array(&self, name: &str) -> Option<&Arc<dyn NdArrayD>> {
        self.dataset().get_array(name)
    }

    /// Whether this is a ragged dataset.
    pub fn is_ragged(&self) -> bool {
        matches!(self, Self::Ragged(_))
    }

    /// If this is a ragged dataset, return a reference to the
    /// [`RaggedDataset`].
    pub fn as_ragged(&self) -> Option<&RaggedDataset> {
        match self {
            Self::Ragged(r) => Some(r),
            Self::Regular(_) => None,
        }
    }

    /// If this is a regular dataset, return a reference to the
    /// [`Dataset`].
    pub fn as_regular(&self) -> Option<&Dataset> {
        match self {
            Self::Regular(ds) => Some(ds),
            Self::Ragged(_) => None,
        }
    }

    /// Return the names and datatypes of every variable exposed by this dataset,
    /// sorted alphabetically by name.
    ///
    /// The two variants behave differently:
    ///
    /// - **Regular** – returns *all* arrays in the dataset.
    /// - **Ragged** – returns only the variables that would appear in an
    ///   extracted cast: instance variables, observation variables (via
    ///   variable-attribute look-ups), and global attributes. Internal
    ///   row-size arrays and their `sample_dimension` attributes are
    ///   excluded.
    pub fn fields(&self) -> indexmap::IndexMap<String, NdArrayDataType> {
        match &self {
            AnyDataset::Regular(dataset) => {
                let mut fields: indexmap::IndexMap<String, NdArrayDataType> = dataset
                    .arrays
                    .iter()
                    .map(|(name, array)| (name.clone(), array.datatype()))
                    .collect();
                fields.sort_keys();
                fields
            }
            AnyDataset::Ragged(ragged_dataset) => {
                let dataset = ragged_dataset.dataset();
                let mut fields = indexmap::IndexMap::new();
                for variable_name in ragged_dataset.instance_variables.iter() {
                    if let Some(array) = dataset.get_array(variable_name) {
                        fields.insert(variable_name.clone(), array.datatype());
                    }
                }
                for attr_name in ragged_dataset.variable_attributes.keys() {
                    if let Some(array) = dataset.get_array(attr_name) {
                        fields.insert(attr_name.clone(), array.datatype());
                    }
                }
                for global_attr_name in ragged_dataset.global_attributes.keys() {
                    if let Some(array) = dataset.get_array(global_attr_name) {
                        fields.insert(global_attr_name.clone(), array.datatype());
                    }
                }
                // Sort the fields by name to ensure deterministic order.
                fields.sort_keys();
                fields
            }
        }
    }
}

pub enum CfDatasetType {
    Ragged,
    Regular,
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
        let station_id = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![100.0, 200.0, 300.0],
            vec![3],
            vec!["casts".into()],
            None,
        )
        .unwrap();

        let z_row_size = NdArray::<i32>::try_new_from_vec_in_mem(
            vec![2, 1, 3],
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
                ("z_row_size", Arc::new(z_row_size)),
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
        assert!(matches!(ds.cf_dataset_type(), CfDatasetType::Ragged));
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
        assert!(matches!(ds.cf_dataset_type(), CfDatasetType::Regular));
    }

    // ── RaggedDataset::try_new ───────────────────────────────────────

    #[tokio::test]
    async fn test_try_new_basic() {
        let ds = make_ragged_dataset().await;
        let ragged = RaggedDataset::try_new(ds).await.unwrap();

        assert_eq!(ragged.len(), 3);
        assert!(!ragged.is_empty());
        assert_eq!(ragged.instance_dimension(), "casts");
        assert_eq!(ragged.dataset().name, "ragged");
    }

    #[tokio::test]
    async fn test_classifies_variables() {
        let ds = make_ragged_dataset().await;
        let ragged = RaggedDataset::try_new(ds).await.unwrap();

        // instance_variables should contain station_id only
        assert!(
            ragged
                .instance_variables
                .contains(&"station_id".to_string())
        );
        assert!(
            !ragged
                .instance_variables
                .contains(&"z_row_size".to_string())
        );

        // obs_variables should contain depth and temperature under "z_obs"
        let obs = ragged.obs_variables.get("z_obs").unwrap();
        assert!(obs.contains(&"depth".to_string()));
        assert!(obs.contains(&"temperature".to_string()));

        // global_attributes should contain Conventions
        assert!(ragged.global_attributes.contains_key("Conventions"));

        // variable_attributes should contain depth.units, temperature.units
        assert!(ragged.variable_attributes.contains_key("depth.units"));
        assert!(ragged.variable_attributes.contains_key("temperature.units"));

        // row-size var attrs should be excluded
        assert!(
            !ragged
                .variable_attributes
                .contains_key("z_row_size.sample_dimension")
        );
    }

    #[tokio::test]
    async fn test_offsets_lazy() {
        let ds = make_ragged_dataset().await;
        let ragged = RaggedDataset::try_new(ds).await.unwrap();

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

    #[tokio::test]
    async fn test_fails_on_regular_dataset() {
        let arr =
            NdArray::<f64>::try_new_from_vec_in_mem(vec![1.0], vec![1], vec!["x".into()], None)
                .unwrap();
        let ds = make_dataset("regular", vec![("val", Arc::new(arr))]).await;
        let err = RaggedDataset::try_new(ds).await.unwrap_err();
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
        let ragged = RaggedDataset::try_new(ds).await.unwrap();
        let cast = ragged.get_cast(0).await.unwrap();

        assert_eq!(cast.name, "ragged[0]");
        assert_eq!(read_f64(&cast, "station_id").await, vec![100.0]);
        assert_eq!(read_f64(&cast, "depth").await, vec![10.0, 20.0]);
        assert_eq!(read_f64(&cast, "temperature").await, vec![1.0, 2.0]);
    }

    #[tokio::test]
    async fn test_get_cast_middle() {
        let ds = make_ragged_dataset().await;
        let ragged = RaggedDataset::try_new(ds).await.unwrap();
        let cast = ragged.get_cast(1).await.unwrap();

        assert_eq!(cast.name, "ragged[1]");
        assert_eq!(read_f64(&cast, "station_id").await, vec![200.0]);
        assert_eq!(read_f64(&cast, "depth").await, vec![30.0]);
        assert_eq!(read_f64(&cast, "temperature").await, vec![3.0]);
    }

    #[tokio::test]
    async fn test_get_cast_last() {
        let ds = make_ragged_dataset().await;
        let ragged = RaggedDataset::try_new(ds).await.unwrap();
        let cast = ragged.get_cast(2).await.unwrap();

        assert_eq!(cast.name, "ragged[2]");
        assert_eq!(read_f64(&cast, "station_id").await, vec![300.0]);
        assert_eq!(read_f64(&cast, "depth").await, vec![40.0, 50.0, 60.0]);
        assert_eq!(read_f64(&cast, "temperature").await, vec![4.0, 5.0, 6.0]);
    }

    #[tokio::test]
    async fn test_get_cast_includes_variable_attributes() {
        let ds = make_ragged_dataset().await;
        let ragged = RaggedDataset::try_new(ds).await.unwrap();
        let cast = ragged.get_cast(0).await.unwrap();

        assert_eq!(read_string(&cast, "depth.units").await, vec!["m"]);
        assert_eq!(read_string(&cast, "temperature.units").await, vec!["°C"]);
    }

    #[tokio::test]
    async fn test_get_cast_includes_global_attributes() {
        let ds = make_ragged_dataset().await;
        let ragged = RaggedDataset::try_new(ds).await.unwrap();
        let cast = ragged.get_cast(0).await.unwrap();

        assert_eq!(read_string(&cast, "Conventions").await, vec!["CF-1.6"]);
    }

    #[tokio::test]
    async fn test_get_cast_excludes_row_size_vars() {
        let ds = make_ragged_dataset().await;
        let ragged = RaggedDataset::try_new(ds).await.unwrap();
        let cast = ragged.get_cast(0).await.unwrap();

        assert!(cast.get_array("z_row_size").is_none());
        assert!(cast.get_array("z_row_size.sample_dimension").is_none());
    }

    #[tokio::test]
    async fn test_get_cast_out_of_bounds() {
        let ds = make_ragged_dataset().await;
        let ragged = RaggedDataset::try_new(ds).await.unwrap();
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
        let ragged = RaggedDataset::try_new(ds).await.unwrap();

        let iter = ragged.iter();
        assert_eq!(iter.len(), 3);

        let indices: Vec<usize> = ragged.iter().map(|(i, _)| i).collect();
        assert_eq!(indices, vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn test_iter_get_cast_round_trip() {
        let ds = make_ragged_dataset().await;
        let ragged = RaggedDataset::try_new(ds).await.unwrap();

        for (i, r) in ragged.iter() {
            let cast = r.get_cast(i).await.unwrap();
            assert_eq!(cast.name, format!("ragged[{i}]"));
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

        let ragged = RaggedDataset::try_new(ds).await.unwrap();
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

        let ragged = RaggedDataset::try_new(ds).await.unwrap();

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
            "depth.units",
            "station_id",
            "temperature.units",
        ];
        expected.sort();
        let actual: Vec<&str> = fields.keys().map(|s| s.as_str()).collect();
        assert_eq!(actual, expected);
    }
}
