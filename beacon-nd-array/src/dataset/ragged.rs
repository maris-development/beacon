use std::{collections::HashMap, sync::Arc};

use indexmap::IndexMap;
use tokio::sync::OnceCell;

use crate::{NdArray, NdArrayD, array::subset::ArraySubset, dataset::Dataset};

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
    instance_dim: String,
    n_instances: usize,
    /// obs_dim name → row-size variable name, kept for lazy offset computation.
    row_size_vars: HashMap<String, String>,
    /// Row-size arrays extracted from the original dataset, keyed by obs_dim.
    /// Kept so that cumulative offsets can be computed lazily.
    row_size_arrays: HashMap<String, Arc<dyn NdArrayD>>,
    /// Cumulative offsets per observation dimension, computed lazily.
    pub(crate) offsets: OnceCell<HashMap<String, Vec<usize>>>,
    /// All classified variables (instance, observation, attributes).
    /// Row-size variables and their attributes are excluded.
    pub(crate) variables: IndexMap<String, RaggedArray>,
}

#[derive(Debug, Clone)]
pub enum RaggedArray {
    InstanceVariable(Arc<dyn NdArrayD>),
    ObservationVariable(Arc<dyn NdArrayD>),
    Attribute(Arc<dyn NdArrayD>),
}

impl RaggedArray {
    /// Return a reference to the inner array regardless of variant.
    pub fn array(&self) -> &Arc<dyn NdArrayD> {
        match self {
            RaggedArray::InstanceVariable(a) => a,
            RaggedArray::ObservationVariable(a) => a,
            RaggedArray::Attribute(a) => a,
        }
    }
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
    pub async fn try_new(dataset: &Dataset) -> anyhow::Result<Self> {
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

        // ── 3. Extract row-size arrays for lazy offset computation ───────
        let mut row_size_arrays: HashMap<String, Arc<dyn NdArrayD>> = HashMap::new();
        for (obs_dim, rs_var) in &row_size_vars {
            let arr = dataset
                .get_array(rs_var)
                .ok_or_else(|| anyhow::anyhow!("row-size variable {rs_var} not in dataset"))?;
            row_size_arrays.insert(obs_dim.clone(), arr.clone());
        }

        // ── 4. Classify variables into the variables map ─────────────────
        let obs_dim_set: std::collections::HashSet<&str> =
            row_size_vars.keys().map(String::as_str).collect();
        let row_size_var_set: std::collections::HashSet<&str> =
            row_size_vars.values().map(String::as_str).collect();

        let mut variables: IndexMap<String, RaggedArray> = IndexMap::new();

        for (name, array) in &dataset.arrays {
            // Skip row-size variable attributes (e.g. "z_row_size.sample_dimension").
            if name.contains('.') {
                let var_part = name.split('.').next().unwrap_or("");
                if row_size_var_set.contains(var_part) {
                    continue;
                }
                variables.insert(name.clone(), RaggedArray::Attribute(array.clone()));
                continue;
            }

            // Skip the row-size variables themselves.
            if row_size_var_set.contains(name.as_str()) {
                continue;
            }

            let dims = array.dimensions();
            if dims.is_empty() {
                // Dimensionless → global attribute.
                variables.insert(name.clone(), RaggedArray::Attribute(array.clone()));
                continue;
            }

            let leading = &dims[0];
            if obs_dim_set.contains(leading.as_str()) {
                variables.insert(
                    name.clone(),
                    RaggedArray::ObservationVariable(array.clone()),
                );
            } else if leading == &instance_dim {
                variables.insert(name.clone(), RaggedArray::InstanceVariable(array.clone()));
            }
        }

        // Sort by key for deterministic order.
        variables.sort_keys();

        Ok(Self {
            instance_dim,
            n_instances,
            row_size_vars,
            row_size_arrays,
            offsets: OnceCell::new(),
            variables,
        })
    }

    pub fn project(&self, projection: &[usize]) -> anyhow::Result<Self> {
        let mut arrays_kept = indexmap::IndexMap::new();
        for &i in projection {
            match self.variables.get_index(i) {
                Some((name, array)) => {
                    arrays_kept.insert(name.clone(), array.clone());
                }
                None => {
                    anyhow::bail!(
                        "projection index {i} out of bounds for {} variables",
                        self.variables.len()
                    );
                }
            }
        }

        Ok(Self {
            variables: arrays_kept,
            ..self.clone()
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

        for (obs_dim, arr) in &self.row_size_arrays {
            let typed = arr.as_any().downcast_ref::<NdArray<i32>>().ok_or_else(|| {
                let rs_var = &self.row_size_vars[obs_dim];
                anyhow::anyhow!("{rs_var} is not an i32 array")
            })?;
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

    /// Look up a variable by name, returning a reference to the inner array.
    pub fn get_array(&self, name: &str) -> Option<&Arc<dyn NdArrayD>> {
        self.variables.get(name).map(|v| v.array())
    }

    /// Return the observation dimension names (keys of `row_size_vars`).
    pub fn observation_dimensions(&self) -> impl Iterator<Item = &str> {
        self.row_size_vars.keys().map(String::as_str)
    }

    /// Iterate over cast indices paired with a reference to `self`.
    pub fn iter(&self) -> RaggedIter<'_> {
        RaggedIter {
            ragged: self,
            index: 0,
        }
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

        // ── First pass: instance and observation variables ────────────────
        for (name, var) in &self.variables {
            match var {
                RaggedArray::InstanceVariable(array) => {
                    let mut start = vec![0usize; array.shape().len()];
                    let mut shape = array.shape();
                    start[0] = index;
                    shape[0] = 1;
                    let sub = array.subset(ArraySubset::new(start, shape)).await?;
                    arrays.insert(name.clone(), sub);
                }
                RaggedArray::ObservationVariable(array) => {
                    let obs_dim = array
                        .dimensions()
                        .first()
                        .cloned()
                        .expect("observation variable must have dimensions");
                    let cum = &offsets[&obs_dim];
                    let obs_start = cum[index];
                    let obs_len = cum[index + 1] - obs_start;

                    let mut start = vec![0usize; array.shape().len()];
                    let mut shape = array.shape();
                    start[0] = obs_start;
                    shape[0] = obs_len;
                    let sub = array.subset(ArraySubset::new(start, shape)).await?;
                    arrays.insert(name.clone(), sub);
                }
                RaggedArray::Attribute(_) => {
                    // Handled in second pass.
                }
            }
        }

        // ── Second pass: attributes ──────────────────────────────────────
        for (name, var) in &self.variables {
            if let RaggedArray::Attribute(array) = var
                && let Some(var_part) = name.split('.').next()
            {
                if name.contains('.') {
                    // Variable attribute — include only if parent variable is present.
                    if arrays.contains_key(var_part) {
                        arrays.insert(name.clone(), array.clone());
                    }
                } else {
                    // Global attribute (dimensionless, no dot).
                    arrays.insert(name.clone(), array.clone());
                }
            }
        }

        let name = format!("{}[{index}]", self.instance_dim);
        Ok(Dataset::new(name, arrays).await)
    }
}

/// Iterator over cast indices, yielding `(index, &RaggedDataset)`.
pub struct RaggedIter<'a> {
    ragged: &'a RaggedDataset,
    index: usize,
}

impl<'a> Iterator for RaggedIter<'a> {
    type Item = (usize, &'a RaggedDataset);

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.ragged.n_instances {
            let i = self.index;
            self.index += 1;
            Some((i, self.ragged))
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.ragged.n_instances - self.index;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for RaggedIter<'_> {}

impl std::fmt::Debug for RaggedDataset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaggedDataset")
            .field("instance_dim", &self.instance_dim)
            .field("n_instances", &self.n_instances)
            .field("variables", &self.variables.keys().collect::<Vec<_>>())
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
            instance_dim: self.instance_dim.clone(),
            n_instances: self.n_instances,
            row_size_vars: self.row_size_vars.clone(),
            row_size_arrays: self.row_size_arrays.clone(),
            offsets: cell,
            variables: self.variables.clone(),
        }
    }
}
