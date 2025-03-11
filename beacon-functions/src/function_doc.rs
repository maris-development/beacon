use datafusion::logical_expr::ScalarUDF;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FunctionDoc {
    pub function_name: String,
}

impl FunctionDoc {
    pub fn from_scalar(scalar: &ScalarUDF) -> Self {
        Self {
            function_name: scalar.name().to_string(),
        }
    }
}
