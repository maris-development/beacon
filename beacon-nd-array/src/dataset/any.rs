use std::sync::Arc;

use crate::{
    NdArrayD,
    dataset::{Dataset, ragged::RaggedDataset, variant::DatasetType},
    datatypes::NdArrayDataType,
    projection::DatasetProjection,
};

/// A dataset that may be either regular or ragged (CF contiguous
/// ragged-array convention).
///
/// Use [`AnyDataset::try_from_dataset`] to automatically detect the
/// convention from `sample_dimension` attributes and build the
/// appropriate variant.
#[derive(Debug)]
pub enum AnyDataset {
    Regular(Dataset),
    Ragged {
        dataset: Dataset,
        ragged: RaggedDataset,
    },
}

impl Clone for AnyDataset {
    fn clone(&self) -> Self {
        match self {
            Self::Regular(ds) => Self::Regular(ds.clone()),
            Self::Ragged { dataset, ragged } => Self::Ragged {
                dataset: dataset.clone(),
                ragged: ragged.clone(),
            },
        }
    }
}

impl AnyDataset {
    /// Inspect a [`Dataset`] and wrap it as the appropriate variant.
    ///
    /// If the dataset contains any `sample_dimension` attributes it is
    /// treated as ragged; otherwise as regular.
    pub async fn try_from_dataset(dataset: Dataset) -> anyhow::Result<Self> {
        match dataset.cf_dataset_type() {
            DatasetType::CfRagged => {
                let ragged = RaggedDataset::try_new(&dataset).await?;
                Ok(Self::Ragged { dataset, ragged })
            }
            DatasetType::Regular => Ok(Self::Regular(dataset)),
        }
    }

    /// The name of the underlying dataset.
    pub fn name(&self) -> &str {
        match self {
            Self::Regular(ds) => &ds.name,
            Self::Ragged { dataset, .. } => &dataset.name,
        }
    }

    /// A reference to the underlying [`Dataset`].
    pub fn dataset(&self) -> &Dataset {
        match self {
            Self::Regular(ds) => ds,
            Self::Ragged { dataset, .. } => dataset,
        }
    }

    /// Look up an array by name in the underlying dataset.
    pub fn get_array(&self, name: &str) -> Option<&Arc<dyn NdArrayD>> {
        self.dataset().get_array(name)
    }

    /// Whether this is a ragged dataset.
    pub fn is_ragged(&self) -> bool {
        matches!(self, Self::Ragged { .. })
    }

    /// If this is a ragged dataset, return a reference to the
    /// [`RaggedDataset`].
    pub fn as_ragged(&self) -> Option<&RaggedDataset> {
        match self {
            Self::Ragged { ragged, .. } => Some(ragged),
            Self::Regular(_) => None,
        }
    }

    /// If this is a regular dataset, return a reference to the
    /// [`Dataset`].
    pub fn as_regular(&self) -> Option<&Dataset> {
        match self {
            Self::Regular(ds) => Some(ds),
            Self::Ragged { .. } => None,
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
            AnyDataset::Ragged { ragged, .. } => {
                let mut fields = indexmap::IndexMap::new();
                for (name, var) in &ragged.variables {
                    fields.insert(name.clone(), var.array().datatype());
                }
                fields.sort_keys();
                fields
            }
        }
    }

    pub fn project(&self, projection: &DatasetProjection) -> anyhow::Result<Self> {
        match self {
            Self::Regular(ds) => {
                if let Some(dimension_projection) = &projection.dimension_projection {
                    let ds = ds.project_with_dimensions(dimension_projection)?;
                    if let Some(index_projection) = &projection.index_projection {
                        let ds = ds.project(index_projection)?;
                        Ok(Self::Regular(ds))
                    } else {
                        Ok(Self::Regular(ds))
                    }
                } else if let Some(index_projection) = &projection.index_projection {
                    let ds = ds.project(index_projection)?;
                    Ok(Self::Regular(ds))
                } else {
                    Ok(Self::Regular(ds.clone()))
                }
            }
            Self::Ragged { ragged, dataset } => {
                if projection.dimension_projection.is_some() {
                    anyhow::bail!("dimension projections are not supported for ragged datasets");
                }
                if let Some(index_projection) = &projection.index_projection {
                    let projected = ragged.project(index_projection)?;
                    Ok(Self::Ragged {
                        dataset: dataset.clone(),
                        ragged: projected,
                    })
                } else {
                    Ok(Self::Ragged {
                        dataset: dataset.clone(),
                        ragged: ragged.clone(),
                    })
                }
            }
        }
    }
}
