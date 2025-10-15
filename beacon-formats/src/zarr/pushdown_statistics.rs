#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PushDownZarrStatistics {
    pub arrays: Vec<String>,
}

impl PushDownZarrStatistics {
    pub fn has_array(&self, array_name: &str) -> bool {
        self.arrays.contains(&array_name.to_string())
    }
}
