use beacon_nd_arrow::array::compat_typings::ArrowTypeConversion;

use crate::{scalar::Scalar, typed_vec::TypedVec};

pub trait AtlasType: ArrowTypeConversion + Send + Sync + 'static {
    fn data_type() -> AtlasDataType;
    fn try_from_scalar(scalar: &Scalar) -> anyhow::Result<Self>
    where
        Self: Sized;
    fn try_from_archived_scalar(scalar: &rkyv::Archived<Scalar>) -> anyhow::Result<Self>
    where
        Self: Sized;
    fn try_from_vec(vec: TypedVec) -> anyhow::Result<Vec<Self>>
    where
        Self: Sized;
    fn try_from_archived_vec(vec: &rkyv::Archived<TypedVec>) -> anyhow::Result<Vec<Self>>
    where
        Self: Sized;
    fn into_vec(values: Vec<Self>) -> TypedVec
    where
        Self: Sized;
}

#[derive(
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    rkyv::Serialize,
    rkyv::Deserialize,
    rkyv::Archive,
)]
pub enum AtlasDataType {
    Bool,
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F32,
    F64,
    Timestamp,
    Binary,
    String,
}

#[derive(
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    rkyv::Serialize,
    rkyv::Deserialize,
    rkyv::Archive,
)]
#[repr(transparent)]
pub struct Timestamp(i64);

impl From<i64> for Timestamp {
    fn from(value: i64) -> Self {
        Timestamp(value)
    }
}
