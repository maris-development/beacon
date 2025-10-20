use datafusion::logical_expr::ScalarUDF;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FunctionDoc {
    pub function_name: String,
    pub description: String,
    pub return_type: String,
    pub params: Vec<FunctionParameter>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FunctionParameter {
    name: String,
    description: String,
    data_type: String,
}

impl FunctionDoc {
    pub fn from_scalar(scalar: &ScalarUDF) -> Vec<Self> {
        let documentation = scalar.documentation();
        scalar
            .signature()
            .type_signature
            .get_example_types()
            .iter()
            .map(|data_types| {
                if let Some(doc) = documentation {
                    let function_doc = FunctionDoc {
                        function_name: scalar.name().to_string(),
                        description: doc.description.clone(),
                        return_type: "unknown".to_string(),
                        params: vec![],
                    };
                    if let Some(arguments) = doc.arguments.as_ref() {
                        let params = arguments
                            .iter()
                            .zip(data_types.iter())
                            .map(|((name, description), data_type)| FunctionParameter {
                                name: name.clone(),
                                description: description.clone(),
                                data_type: format!("{data_type}"),
                            })
                            .collect();
                        FunctionDoc {
                            params,
                            ..function_doc
                        }
                    } else {
                        // No argument documentation available so we just fill in the types
                        let params = data_types
                            .iter()
                            .enumerate()
                            .map(|(i, data_type)| FunctionParameter {
                                name: format!("arg{}", i + 1),
                                description: "No description available".to_string(),
                                data_type: format!("{data_type}"),
                            })
                            .collect();
                        FunctionDoc {
                            params,
                            ..function_doc
                        }
                    }
                } else {
                    FunctionDoc {
                        function_name: scalar.name().to_string(),
                        description: "No documentation available".to_string(),
                        return_type: "unknown".to_string(),
                        params: vec![],
                    }
                }
            })
            .collect()
    }
}
