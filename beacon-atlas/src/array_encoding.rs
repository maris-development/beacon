use arrow::array::ArrayRef;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ArrayEncoding {}

pub enum EncodedArray {
    RleDict(ArrayRef),
}
