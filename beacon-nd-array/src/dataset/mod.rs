pub mod any;
pub mod ragged;
pub mod variant;

pub use any::AnyDataset;
pub use ragged::RaggedDataset;
pub use variant::DatasetType;

use crate::{NdArray, NdArrayD, datatypes::NdArrayDataType};
use indexmap::IndexMap;
use std::sync::Arc;

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

    pub fn project_with_dimensions(&self, dimensions: &[String]) -> anyhow::Result<Dataset> {
        // Check that all the dimensions exist
        for dim in dimensions {
            if !self.dimensions.contains_key(dim) {
                anyhow::bail!("dimension '{dim}' not found in dataset");
            }
        }

        let arrays = self
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
        Ok(Dataset {
            name: self.name.clone(),
            dimensions: self.dimensions.clone(),
            arrays,
        })
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
}
