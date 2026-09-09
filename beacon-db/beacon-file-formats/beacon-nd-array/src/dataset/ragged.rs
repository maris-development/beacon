use std::{
    any::Any,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use arrow::record_batch::RecordBatch;
use beacon_datafusion_ext::nd::{Dimension, Dimensions, NdArrowArray, NdRecordBatch};
use indexmap::IndexMap;
use tokio::sync::OnceCell;

use num_traits::ToPrimitive;

use crate::{
    NdArray, NdArrayD,
    array::subset::ArraySubset,
    arrow::batch::{
        generate_chunk_subsets, ragged_batch_to_record_batch, ragged_record_batch_schema,
    },
    dataset::{Dataset, source::DatasetSource},
    datatypes::{NdArrayDataType, NdArrayType},
};

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

/// Read a row-size variable of a known integer type as a list of counts.
///
/// `to_usize` rejects a negative or oversized value and returns an error. A
/// plain `as` cast wraps such a value into a huge count instead, which then
/// builds offsets that run past the end of the observation dimension.
async fn read_row_sizes<T>(arr: &Arc<dyn NdArrayD>, rs_var: &str) -> anyhow::Result<Vec<usize>>
where
    T: NdArrayType + ToPrimitive + std::fmt::Display,
{
    let typed = arr.as_any().downcast_ref::<NdArray<T>>().ok_or_else(|| {
        anyhow::anyhow!(
            "row-size variable {rs_var} reports {:?}, but does not hold that type",
            arr.datatype()
        )
    })?;

    typed
        .clone_into_raw_vec()
        .await
        .into_iter()
        .map(|sz| {
            sz.to_usize().ok_or_else(|| {
                anyhow::anyhow!("row-size variable {rs_var} holds an invalid row size: {sz}")
            })
        })
        .collect()
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
            tracing::debug!(
                dataset = %dataset.name,
                "no sample_dimension attributes found — not a ragged dataset"
            );
            anyhow::bail!("no sample_dimension attributes found — not a ragged dataset");
        }
        tracing::debug!(
            dataset = %dataset.name,
            row_size_vars = row_size_vars.len(),
            "detected CF ragged-array dataset"
        );

        // ── 2. Determine the instance dimension ─────────────────────────
        // Safe: `row_size_vars` is non-empty (checked above).
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
            // Global attributes use the leading-dot convention (e.g. ".Conventions").
            if name.starts_with('.') {
                variables.insert(name.clone(), RaggedArray::Attribute(array.clone()));
                continue;
            }

            // Variable attributes are "var.attr". Skip those belonging to a
            // row-size variable (e.g. "z_row_size.sample_dimension").
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
            let rs_var = &self.row_size_vars[obs_dim];

            // CF fixes no width on a row-size variable, so a writer picks whatever
            // integer holds its largest cast. WOD ships `short`, other producers
            // ship `int` or `int64`. Read every signed and unsigned width through
            // the same path.
            let row_sizes: Vec<usize> = match arr.datatype() {
                NdArrayDataType::I8 => read_row_sizes::<i8>(arr, rs_var).await?,
                NdArrayDataType::I16 => read_row_sizes::<i16>(arr, rs_var).await?,
                NdArrayDataType::I32 => read_row_sizes::<i32>(arr, rs_var).await?,
                NdArrayDataType::I64 => read_row_sizes::<i64>(arr, rs_var).await?,
                NdArrayDataType::U8 => read_row_sizes::<u8>(arr, rs_var).await?,
                NdArrayDataType::U16 => read_row_sizes::<u16>(arr, rs_var).await?,
                NdArrayDataType::U32 => read_row_sizes::<u32>(arr, rs_var).await?,
                NdArrayDataType::U64 => read_row_sizes::<u64>(arr, rs_var).await?,
                other => anyhow::bail!(
                    "row-size variable {rs_var} holds {other:?}; it must hold an integer type"
                ),
            };

            let mut cum = Vec::with_capacity(self.n_instances + 1);
            cum.push(0usize);
            for sz in row_sizes {
                cum.push(cum.last().unwrap() + sz);
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

    pub fn get_ragged_array(&self, name: &str) -> Option<&RaggedArray> {
        self.variables.get(name)
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
                    let obs_dim = array.dimensions().first().cloned().ok_or_else(|| {
                        anyhow::anyhow!(
                            "observation variable {name} must have at least one dimension"
                        )
                    })?;
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
            if let RaggedArray::Attribute(array) = var {
                // A variable attribute is "var.attr" (an interior dot, not
                // leading). Leading-dot globals and dimensionless attributes
                // are global and always included.
                if !name.starts_with('.') && name.contains('.') {
                    let var_part = name.split('.').next().unwrap_or("");
                    // Variable attribute — include only if parent variable is present.
                    if arrays.contains_key(var_part) {
                        arrays.insert(name.clone(), array.clone());
                    }
                } else {
                    // Global attribute (leading-dot or dimensionless).
                    arrays.insert(name.clone(), array.clone());
                }
            }
        }

        let name = format!("{}[{index}]", self.instance_dim);
        Ok(Dataset::new(name, arrays).await)
    }

    /// Extract the sub-dataset for a contiguous range of casts
    /// `[start..end)` in a single I/O pass per array.
    ///
    /// This is more efficient than calling [`get_cast`](Self::get_cast)
    /// repeatedly because observation variables are read once for the
    /// entire range rather than once per cast.
    ///
    /// The returned [`Dataset`] contains:
    /// - Instance variables subsetted to `[start..end, ...]`.
    /// - Observation variables subsetted to the concatenated rows of all
    ///   casts in the range.
    /// - Variable attributes for every included variable.
    /// - All global attributes.
    ///
    /// Row-size variables and their attributes are **not** included.
    pub async fn get_casts_range(&self, start: usize, end: usize) -> anyhow::Result<Dataset> {
        if start >= end || end > self.n_instances {
            anyhow::bail!(
                "cast range [{start}..{end}) out of bounds for {} instances",
                self.n_instances
            );
        }

        let offsets = self.offsets().await?;
        let mut arrays: IndexMap<String, Arc<dyn NdArrayD>> = IndexMap::new();

        // ── First pass: instance and observation variables ────────────────
        for (name, var) in &self.variables {
            match var {
                RaggedArray::InstanceVariable(array) => {
                    let mut sub_start = vec![0usize; array.shape().len()];
                    let mut sub_shape = array.shape();
                    sub_start[0] = start;
                    sub_shape[0] = end - start;
                    let sub = array.subset(ArraySubset::new(sub_start, sub_shape)).await?;
                    arrays.insert(name.clone(), sub);
                }
                RaggedArray::ObservationVariable(array) => {
                    let obs_dim = array.dimensions().first().cloned().ok_or_else(|| {
                        anyhow::anyhow!(
                            "observation variable {name} must have at least one dimension"
                        )
                    })?;
                    let cum = &offsets[&obs_dim];
                    let obs_start = cum[start];
                    let obs_len = cum[end] - obs_start;

                    let mut sub_start = vec![0usize; array.shape().len()];
                    let mut sub_shape = array.shape();
                    sub_start[0] = obs_start;
                    sub_shape[0] = obs_len;
                    let sub = array.subset(ArraySubset::new(sub_start, sub_shape)).await?;
                    arrays.insert(name.clone(), sub);
                }
                RaggedArray::Attribute(_) => {
                    // Handled in second pass.
                }
            }
        }

        // ── Second pass: attributes ──────────────────────────────────────
        for (name, var) in &self.variables {
            if let RaggedArray::Attribute(array) = var {
                // A variable attribute is "var.attr" (an interior dot, not
                // leading). Leading-dot globals and dimensionless attributes
                // are global and always included.
                if !name.starts_with('.') && name.contains('.') {
                    let var_part = name.split('.').next().unwrap_or("");
                    // Variable attribute — include only if parent variable is present.
                    if arrays.contains_key(var_part) {
                        arrays.insert(name.clone(), array.clone());
                    }
                } else {
                    // Global attribute (leading-dot or dimensionless).
                    arrays.insert(name.clone(), array.clone());
                }
            }
        }

        let name = format!("{}[{start}..{end}]", self.instance_dim);
        Ok(Dataset::new(name, arrays).await)
    }

    /// Return the cached cumulative offsets per observation dimension.
    ///
    /// Each entry maps an obs dimension name to a vec of length
    /// `n_instances + 1` where `offsets[i]` is the starting row of
    /// cast `i` and `offsets[n_instances]` is the total row count.
    ///
    /// Lazily initialises offsets on first call.
    pub async fn cumulative_offsets(&self) -> anyhow::Result<&HashMap<String, Vec<usize>>> {
        self.offsets().await
    }
}

impl RaggedDataset {
    /// The chunk size the file stores the instance dimension in.
    ///
    /// The smallest chunk any instance variable or row-size variable reports
    /// on that axis, so a chunk of casts lies inside one stored chunk of each
    /// of them. An array with no chunk layout reports its whole axis, so a
    /// file with no chunking at all is one chunk of every cast.
    fn instance_chunk(&self) -> usize {
        let instance_arrays = self.variables.values().filter_map(|var| match var {
            RaggedArray::InstanceVariable(array) => Some(array),
            _ => None,
        });
        instance_arrays
            .chain(self.row_size_arrays.values())
            .filter_map(|array| array.chunk_shape().first().copied())
            .filter(|&chunk| chunk > 0)
            .min()
            .unwrap_or(self.n_instances)
            .max(1)
    }
}

#[async_trait::async_trait]
impl DatasetSource for RaggedDataset {
    /// Every chunk of the instance dimension, in order, as an [`ArraySubset`]
    /// over that one axis.
    ///
    /// The cut follows the stored chunking of the instance dimension, see
    /// [`RaggedDataset::instance_chunk`]. A boundary chunk shrinks to fit.
    /// The observation rows of a chunk follow from the offsets when it is
    /// read. A dataset with no casts has no chunks.
    fn chunks(&self) -> Vec<Arc<dyn Any + Send + Sync>> {
        generate_chunk_subsets(&[self.n_instances], &[self.instance_chunk()])
            .into_iter()
            .map(|subset| Arc::new(subset) as Arc<dyn Any + Send + Sync>)
            .collect()
    }

    /// Read one chunk of casts into an [`NdRecordBatch`].
    ///
    /// `chunk` is one of [`DatasetSource::chunks`]. The casts are read in one
    /// pass per array, and their rows come out flat: an instance value repeats
    /// for every observation row of its cast, and an attribute for every row.
    /// The batch then sits on one synthetic `row` axis, on which every column
    /// is full rank, the layout the flat nd encoding gives a ragged batch.
    async fn poll_next(
        &self,
        chunk: Arc<dyn Any + Send + Sync>,
    ) -> anyhow::Result<Option<NdRecordBatch>> {
        let subset = chunk
            .downcast_ref::<ArraySubset>()
            .ok_or_else(|| anyhow::anyhow!("chunk is not an ArraySubset"))?;
        let (Some(&start), Some(&len)) = (subset.start.first(), subset.shape.first()) else {
            anyhow::bail!("a ragged chunk spans the instance dimension; this one spans no axis");
        };
        let range = start..start + len;

        let casts = self.get_casts_range(range.start, range.end).await?;
        let schema = ragged_record_batch_schema(self);
        let obs_dims: HashSet<String> = self.observation_dimensions().map(String::from).collect();
        let offsets = self.cumulative_offsets().await?;
        let flat =
            ragged_batch_to_record_batch(&casts, &schema, &obs_dims, offsets, &range).await?;
        Ok(Some(flat_batch_to_nd(&flat)?))
    }
}

/// A flat batch as an nd batch on one synthetic `row` axis.
///
/// Every column is full rank on that axis, so the broadcast above the scan is
/// the identity. This is the layout `encode_flat_batch_as_nd` gives a ragged
/// batch, built here without the encoding.
fn flat_batch_to_nd(batch: &RecordBatch) -> anyhow::Result<NdRecordBatch> {
    let rows = batch.num_rows();
    let row_dim = || Dimensions::try_new(vec![Dimension::new("row", rows)]);
    let columns = batch
        .columns()
        .iter()
        .map(|column| NdArrowArray::try_new(column.clone(), row_dim()?))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(NdRecordBatch::try_new(batch.schema(), columns, row_dim()?)?)
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

#[cfg(test)]
mod source_tests {
    use super::*;
    use crate::array::backend::{ArrayBackend, mem::InMemoryArrayBackend};
    use arrow::array::{Array, Float64Array, StringArray};
    use ndarray::ArrayD;

    /// An in-memory array that reports the chunk shape a test hands it.
    #[derive(Debug)]
    struct Chunked {
        inner: InMemoryArrayBackend<i32>,
        chunk_shape: Vec<usize>,
    }

    #[async_trait::async_trait]
    impl ArrayBackend<i32> for Chunked {
        fn len(&self) -> usize {
            self.inner.len()
        }
        fn shape(&self) -> Vec<usize> {
            self.inner.shape()
        }
        fn chunk_shape(&self) -> Vec<usize> {
            self.chunk_shape.clone()
        }
        fn dimensions(&self) -> Vec<String> {
            self.inner.dimensions()
        }
        async fn read_subset(&self, subset: ArraySubset) -> anyhow::Result<ArrayD<i32>> {
            self.inner.read_subset(subset).await
        }
    }

    fn f64s(values: Vec<f64>, dim: &str) -> Arc<dyn NdArrayD> {
        let len = values.len();
        Arc::new(
            NdArray::<f64>::try_new_from_vec_in_mem(values, vec![len], vec![dim.to_string()], None)
                .unwrap(),
        )
    }

    fn text(value: &str) -> Arc<dyn NdArrayD> {
        Arc::new(
            NdArray::<String>::try_new_from_vec_in_mem(
                vec![value.to_string()],
                vec![],
                vec![] as Vec<String>,
                None,
            )
            .unwrap(),
        )
    }

    /// Three casts of two, one and three observations, the row-size variable
    /// stored in chunks of `chunk` casts.
    async fn ragged(chunk: usize) -> RaggedDataset {
        let sizes = vec![2, 1, 3];
        let inner = InMemoryArrayBackend::new(
            ArrayD::from_shape_vec(vec![3], sizes).unwrap(),
            vec![3],
            vec!["casts".to_string()],
            None,
        );
        let row_size: Arc<dyn NdArrayD> = Arc::new(
            NdArray::new_with_backend(Chunked {
                inner,
                chunk_shape: vec![chunk],
            })
            .unwrap(),
        );

        let mut arrays: IndexMap<String, Arc<dyn NdArrayD>> = IndexMap::new();
        arrays.insert("row_size".to_string(), row_size);
        arrays.insert("row_size.sample_dimension".to_string(), text("obs"));
        arrays.insert(
            "station".to_string(),
            f64s(vec![100.0, 200.0, 300.0], "casts"),
        );
        arrays.insert(
            "depth".to_string(),
            f64s(vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0], "obs"),
        );
        arrays.insert(".title".to_string(), text("casts"));

        let dataset = Dataset::new("ragged".to_string(), arrays).await;
        RaggedDataset::try_new(&dataset).await.unwrap()
    }

    fn subsets(ragged: &RaggedDataset) -> Vec<(usize, usize)> {
        ragged
            .chunks()
            .iter()
            .map(|chunk| {
                let subset = chunk.downcast_ref::<ArraySubset>().unwrap();
                (subset.start[0], subset.shape[0])
            })
            .collect()
    }

    #[tokio::test]
    async fn chunks_follow_the_stored_chunking_of_the_instance_dimension() {
        assert_eq!(subsets(&ragged(2).await), vec![(0, 2), (2, 1)]);
        assert_eq!(subsets(&ragged(1).await), vec![(0, 1), (1, 1), (2, 1)]);
        assert_eq!(
            subsets(&ragged(3).await),
            vec![(0, 3)],
            "no chunking is one chunk of every cast"
        );
    }

    /// Each chunk comes out flat on a `row` axis, and the chunks together are
    /// every observation row once, in cast order.
    #[tokio::test]
    async fn polling_every_chunk_reads_every_row_once() {
        let ragged = ragged(2).await;
        let mut station = Vec::new();
        let mut depth = Vec::new();
        let mut rows = Vec::new();
        for chunk in ragged.chunks() {
            let nd = ragged.poll_next(chunk).await.unwrap().unwrap();
            assert_eq!(nd.target().rank(), 1, "one synthetic row axis");
            assert!(
                nd.columns().iter().all(|column| column.dims().rank() == 1),
                "every column is full rank on it"
            );
            rows.push(nd.num_rows());

            let batch = nd.materialize().unwrap();
            let get = |name: &str| batch.column_by_name(name).unwrap().clone();
            station.extend(
                get("station")
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied(),
            );
            depth.extend(
                get("depth")
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied(),
            );
            let title = get(".title");
            let title = title.as_any().downcast_ref::<StringArray>().unwrap();
            assert!((0..title.len()).all(|row| title.value(row) == "casts"));
        }

        assert_eq!(rows, vec![3, 3], "casts 0 and 1, then cast 2");
        assert_eq!(station, vec![100.0, 100.0, 200.0, 300.0, 300.0, 300.0]);
        assert_eq!(depth, vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0]);
    }

    #[tokio::test]
    async fn a_chunk_of_another_type_is_an_error() {
        let ragged = ragged(3).await;
        let err = ragged.poll_next(Arc::new(42usize)).await.unwrap_err();
        assert!(err.to_string().contains("not an ArraySubset"), "{err}");
    }
}
