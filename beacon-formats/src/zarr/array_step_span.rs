#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct NumericArrayStepSpan {
    pub dimension: String,
    pub column: String,
    pub start: f64,
    pub end: f64,
    pub step: f64,
}
