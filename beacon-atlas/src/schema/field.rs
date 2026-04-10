use crate::schema::_type::AtlasDataType;

/// Represents a column in an Atlas schema, with a name and a data type.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct Field {
    pub name: String,
    pub data_type: AtlasDataType,
}

pub type FieldRef = std::sync::Arc<Field>;

impl Field {
    pub fn new(name: String, data_type: AtlasDataType) -> Self {
        Self { name, data_type }
    }
}
