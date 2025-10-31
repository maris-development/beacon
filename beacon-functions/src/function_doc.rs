use arrow::datatypes::DataType;
use datafusion::logical_expr::ScalarUDF;

use crate::file_formats::BeaconTableFunctionImpl;

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
    fn parse_data_type_to_string(data_type: &DataType) -> String {
        match data_type {
            DataType::List(field) => {
                format!(
                    "List<{}>",
                    Self::parse_data_type_to_string(field.data_type())
                )
            }
            DataType::Struct(fields) => {
                let field_strs: Vec<String> = fields
                    .iter()
                    .map(|f| {
                        format!(
                            "{}: {}",
                            f.name(),
                            Self::parse_data_type_to_string(f.data_type())
                        )
                    })
                    .collect();
                format!("Struct<{}>", field_strs.join(", "))
            }
            _ => format!("{}", data_type),
        }
    }

    pub fn from_beacon_table_function(func: &dyn BeaconTableFunctionImpl) -> Self {
        let mut params = vec![];
        if let Some(arguments) = func.arguments() {
            for arg in arguments {
                params.push(FunctionParameter {
                    name: arg.name().to_string(),
                    description: "No description available".to_string(),
                    data_type: Self::parse_data_type_to_string(arg.data_type()),
                });
            }
        }

        FunctionDoc {
            function_name: func.name(),
            description: "No documentation available".to_string(),
            return_type: "TABLE".to_string(),
            params,
        }
    }

    pub fn from_scalar(scalar: &ScalarUDF) -> Vec<Self> {
        let documentation = scalar.documentation();
        let signature = scalar.signature();
        if !signature.type_signature.get_example_types().is_empty() {
            signature
                .type_signature
                .get_example_types()
                .iter()
                .map(|data_types| {
                    let return_type = scalar.return_type(data_types);
                    let return_type_str = return_type
                        .map(|dt| Self::parse_data_type_to_string(&dt))
                        .unwrap_or_else(|_| "unknown".to_string());
                    let mut function_doc = FunctionDoc {
                        function_name: scalar.name().to_string(),
                        description: "No documentation available".to_string(),
                        return_type: return_type_str,
                        params: vec![],
                    };
                    if let Some(doc) = documentation {
                        function_doc.description = doc.description.clone();
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
                                    data_type: Self::parse_data_type_to_string(data_type),
                                })
                                .collect();
                            FunctionDoc {
                                params,
                                ..function_doc
                            }
                        }
                    } else {
                        function_doc
                    }
                })
                .collect()
        } else {
            let mut params = vec![];
            if let Some(arguments) = documentation.and_then(|doc| doc.arguments.as_ref()) {
                for (i, (name, description)) in arguments.iter().enumerate() {
                    params.push(FunctionParameter {
                        name: name.clone(),
                        description: description.clone(),
                        data_type: "unknown".to_string(),
                    });
                }
            }

            vec![FunctionDoc {
                function_name: scalar.name().to_string(),
                description: documentation
                    .map(|doc| doc.description.clone())
                    .unwrap_or("No documentation available".to_string()),
                return_type: "unknown".to_string(),
                params,
            }]
        }
    }
}
