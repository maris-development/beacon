use arrow::datatypes::DataType;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AtlasSchema {
    variables: Vec<VariableField>,
    global_attributes: Vec<AttributeField>,
}

impl AtlasSchema {
    pub fn empty() -> Self {
        Self {
            variables: Vec::new(),
            global_attributes: Vec::new(),
        }
    }

    pub fn add_variable(&mut self, variable: VariableField) {
        self.variables.push(variable);
    }

    pub fn add_variable_attribute(&mut self, variable_name: &str, attribute: AttributeField) {
        if let Some(variable) = self
            .variables
            .iter_mut()
            .find(|var| var.name == variable_name)
        {
            variable.attributes.push(attribute);
        }
    }

    pub fn add_global_attribute(&mut self, attribute: AttributeField) {
        self.global_attributes.push(attribute);
    }

    pub fn variables(&self) -> &Vec<VariableField> {
        &self.variables
    }

    pub fn variable_attributes(&self, variable_name: &str) -> Option<&Vec<AttributeField>> {
        self.variables
            .iter()
            .find(|var| var.name == variable_name)
            .map(|var| &var.attributes)
    }

    pub fn global_attributes(&self) -> &Vec<AttributeField> {
        &self.global_attributes
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VariableField {
    name: String,
    data_type: DataType,
    attributes: Vec<AttributeField>,
}

impl VariableField {
    pub fn new(name: &str, data_type: DataType) -> Self {
        Self {
            name: name.to_string(),
            data_type,
            attributes: Vec::new(),
        }
    }

    pub fn add_attribute(&mut self, attribute: AttributeField) {
        self.attributes.push(attribute);
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn attributes(&self) -> &Vec<AttributeField> {
        &self.attributes
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AttributeField {
    name: String,
    data_type: DataType,
}

impl AttributeField {
    pub fn new(name: &str, data_type: DataType) -> Self {
        Self {
            name: name.to_string(),
            data_type,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }
}
