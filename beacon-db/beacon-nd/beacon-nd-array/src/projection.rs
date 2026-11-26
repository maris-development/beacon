#[derive(Default, Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct DatasetProjection {
    pub dimension_projection: Option<Vec<String>>,
    pub index_projection: Option<Vec<usize>>,
}

impl DatasetProjection {
    pub fn new(dimension_projection: Vec<String>, index_projection: Vec<usize>) -> Self {
        Self {
            dimension_projection: Some(dimension_projection),
            index_projection: Some(index_projection),
        }
    }

    pub fn new_with_dimension_projection(dimension_projection: Vec<String>) -> Self {
        Self {
            dimension_projection: Some(dimension_projection),
            index_projection: None,
        }
    }

    pub fn new_with_index_projection(index_projection: Vec<usize>) -> Self {
        Self {
            dimension_projection: None,
            index_projection: Some(index_projection),
        }
    }

    pub fn with_index_projection(mut self, index_projection: Vec<usize>) -> Self {
        self.index_projection = Some(index_projection);
        self
    }

    pub fn with_dimension_projection(mut self, dimension_projection: Vec<String>) -> Self {
        self.dimension_projection = Some(dimension_projection);
        self
    }
}
