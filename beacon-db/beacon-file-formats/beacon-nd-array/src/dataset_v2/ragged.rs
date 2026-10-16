use std::sync::Arc;

use indexmap::IndexMap;

use crate::{
    NdArray, NdArrayD, dataset::ragged::read_any_row_sizes, dataset_v2::Dataset,
    datatypes::NdArrayDataType,
};

/// A [`Dataset`] that follows the CF contiguous ragged-array convention.
///
/// A row-size array holds the observation count of each cast along the
/// instance dimension. Its `sample_dimension` attribute names the observation
/// dimension it splits.
pub struct RaggedDataset {
    pub inner: Dataset,
    /// The dimension that indexes the casts.
    pub instance_dimension: String,
    /// The row-size array name for each observation dimension.
    pub row_size_arrays: IndexMap<String, String>,
    /// The row-size array name for each ragged array.
    pub ragged_arrays: IndexMap<String, String>,
    /// All other arrays, except the row-size arrays and their attributes.
    pub instance_arrays: Vec<String>,
}

impl RaggedDataset {
    pub async fn try_new(inner: Dataset) -> anyhow::Result<RaggedDataset> {
        // A `<var>.sample_dimension` attribute marks `<var>` as a row-size array.
        let mut row_size_arrays = IndexMap::new();
        for (key, attribute) in &inner.arrays {
            let Some(row_size_name) = key.strip_suffix(".sample_dimension") else {
                continue;
            };
            let observation_dimension = attribute
                .as_any()
                .downcast_ref::<NdArray<String>>()
                .ok_or_else(|| anyhow::anyhow!("attribute '{key}' is not a string"))?
                .clone_into_raw_vec()
                .await
                .into_iter()
                .next()
                .ok_or_else(|| anyhow::anyhow!("attribute '{key}' is empty"))?;
            if let Some(other) =
                row_size_arrays.insert(observation_dimension.clone(), row_size_name.to_string())
            {
                anyhow::bail!(
                    "row-size arrays '{other}' and '{row_size_name}' both split dimension '{observation_dimension}'"
                );
            }
        }
        if row_size_arrays.is_empty() {
            anyhow::bail!(
                "dataset '{}' has no sample_dimension attribute, so it is not ragged",
                inner.name
            );
        }

        // Each row-size array is a 1-D integer array on the one instance dimension.
        let mut instance_dimension: Option<String> = None;
        for row_size_name in row_size_arrays.values() {
            let array = inner
                .arrays
                .get(row_size_name)
                .ok_or_else(|| anyhow::anyhow!("row-size array '{row_size_name}' is missing"))?;
            let dimensions = array.dimensions();
            let [dimension] = dimensions.as_slice() else {
                anyhow::bail!(
                    "row-size array '{row_size_name}' has dimensions {dimensions:?}, it must have one"
                );
            };
            if !is_integer(&array.datatype()) {
                anyhow::bail!(
                    "row-size array '{row_size_name}' holds {:?}, it must hold an integer type",
                    array.datatype()
                );
            }
            match &instance_dimension {
                Some(instance) if instance != dimension => anyhow::bail!(
                    "row-size arrays use the instance dimensions '{instance}' and '{dimension}', they must use one"
                ),
                Some(_) => {}
                None => instance_dimension = Some(dimension.clone()),
            }
        }
        let instance_dimension = instance_dimension.expect("row_size_arrays is not empty");

        // The leading observation dimension of an array selects its row-size array.
        let mut ragged_arrays = IndexMap::new();
        let mut instance_arrays = Vec::new();
        for (name, array) in &inner.arrays {
            let is_row_size_part = row_size_arrays.values().any(|row_size_name| {
                name == row_size_name
                    || name
                        .strip_prefix(row_size_name.as_str())
                        .is_some_and(|rest| rest.starts_with('.'))
            });
            if is_row_size_part {
                continue;
            }

            let dimensions = array.dimensions();
            if let Some(dim) = dimensions
                .iter()
                .skip(1)
                .find(|dim| row_size_arrays.contains_key(*dim))
            {
                anyhow::bail!(
                    "array '{name}' has observation dimension '{dim}' at a later axis, it must be the first"
                );
            }
            match dimensions
                .first()
                .and_then(|dim| row_size_arrays.get(dim))
            {
                Some(row_size_name) => {
                    ragged_arrays.insert(name.clone(), row_size_name.clone());
                }
                None => instance_arrays.push(name.clone()),
            }
        }

        Ok(RaggedDataset {
            inner,
            instance_dimension,
            row_size_arrays,
            ragged_arrays,
            instance_arrays,
        })
    }

    /// The observation offsets of each cast, for each row-size array name.
    ///
    /// Cast `i` holds the observations `offsets[i]..offsets[i + 1]`.
    pub async fn row_offsets(&self) -> anyhow::Result<IndexMap<String, Vec<usize>>> {
        let mut result = IndexMap::new();
        for (observation_dimension, row_size_name) in &self.row_size_arrays {
            let array = &self.inner.arrays[row_size_name];
            let row_sizes = read_any_row_sizes(array, row_size_name).await?;
            let mut offsets = Vec::with_capacity(row_sizes.len() + 1);
            offsets.push(0);
            for size in row_sizes {
                offsets.push(offsets[offsets.len() - 1] + size);
            }
            if let Some(&length) = self.inner.dimensions.get(observation_dimension)
                && offsets[offsets.len() - 1] > length
            {
                anyhow::bail!(
                    "row-size array '{row_size_name}' counts {} observations, dimension '{observation_dimension}' has {length}",
                    offsets[offsets.len() - 1]
                );
            }
            result.insert(row_size_name.clone(), offsets);
        }
        Ok(result)
    }
}

fn is_integer(datatype: &NdArrayDataType) -> bool {
    use NdArrayDataType::*;
    matches!(datatype, I8 | I16 | I32 | I64 | U8 | U16 | U32 | U64)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    fn array<T: crate::datatypes::NdArrayType + Clone>(
        values: Vec<T>,
        shape: &[usize],
        dims: &[&str],
    ) -> Arc<dyn NdArrayD> {
        Arc::new(
            NdArray::<T>::try_new_from_vec_in_mem(
                values,
                shape.to_vec(),
                dims.iter().map(|d| d.to_string()).collect(),
                None,
            )
            .unwrap(),
        )
    }

    fn text(value: &str) -> Arc<dyn NdArrayD> {
        array(vec![value.to_string()], &[], &[])
    }

    fn dataset(arrays: Vec<(&str, Arc<dyn NdArrayD>)>) -> Dataset {
        let mut dimensions = IndexMap::new();
        for (_, array) in &arrays {
            for (dim, size) in array.dimensions().into_iter().zip(array.shape()) {
                dimensions.insert(dim, size);
            }
        }
        Dataset {
            name: "test".to_string(),
            dimensions,
            arrays: arrays
                .into_iter()
                .map(|(name, array)| (name.to_string(), array))
                .collect(),
            invented_dimensions: HashSet::new(),
        }
    }

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|v| v.to_string()).collect()
    }

    /// Three casts with row sizes [2, 1, 3] on `z_obs` and [1, 1, 1] on `t_obs`.
    fn wod_like() -> Vec<(&'static str, Arc<dyn NdArrayD>)> {
        vec![
            (".Conventions", text("CF-1.6")),
            ("station", array(vec![1, 2, 3], &[3], &["casts"])),
            ("station.units", text("1")),
            ("z_row_size", array(vec![2i16, 1, 3], &[3], &["casts"])),
            ("z_row_size.sample_dimension", text("z_obs")),
            ("z_row_size.long_name", text("rows")),
            ("t_row_size", array(vec![1i32, 1, 1], &[3], &["casts"])),
            ("t_row_size.sample_dimension", text("t_obs")),
            ("depth", array(vec![0.0f64; 6], &[6], &["z_obs"])),
            ("depth.units", text("m")),
            ("temp", array(vec![0.0f64; 3], &[3], &["t_obs"])),
        ]
    }

    #[tokio::test]
    async fn arrays_are_classified() {
        let ragged = RaggedDataset::try_new(dataset(wod_like())).await.unwrap();
        assert_eq!(ragged.instance_dimension, "casts");
        assert_eq!(ragged.row_size_arrays["z_obs"], "z_row_size");
        assert_eq!(ragged.row_size_arrays["t_obs"], "t_row_size");
        assert_eq!(ragged.ragged_arrays["depth"], "z_row_size");
        assert_eq!(ragged.ragged_arrays["temp"], "t_row_size");
        assert_eq!(ragged.ragged_arrays.len(), 2);
        assert_eq!(
            ragged.instance_arrays,
            strings(&[".Conventions", "station", "station.units", "depth.units"])
        );
    }

    #[tokio::test]
    async fn dataset_without_sample_dimension_is_rejected() {
        let result =
            RaggedDataset::try_new(dataset(vec![("station", array(vec![1], &[1], &["casts"]))]))
                .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn missing_row_size_array_is_rejected() {
        let result =
            RaggedDataset::try_new(dataset(vec![("z_row_size.sample_dimension", text("z_obs"))]))
                .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn float_row_size_array_is_rejected() {
        let result = RaggedDataset::try_new(dataset(vec![
            ("z_row_size", array(vec![2.0f32], &[1], &["casts"])),
            ("z_row_size.sample_dimension", text("z_obs")),
        ]))
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn two_instance_dimensions_are_rejected() {
        let result = RaggedDataset::try_new(dataset(vec![
            ("z_row_size", array(vec![2], &[1], &["casts"])),
            ("z_row_size.sample_dimension", text("z_obs")),
            ("t_row_size", array(vec![1], &[1], &["profiles"])),
            ("t_row_size.sample_dimension", text("t_obs")),
        ]))
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn later_observation_axis_is_rejected() {
        let mut arrays = wod_like();
        arrays.push(("flags", array(vec![0u8; 12], &[2, 6], &["n", "z_obs"])));
        let result = RaggedDataset::try_new(dataset(arrays)).await;
        assert!(result.is_err());
    }
}
