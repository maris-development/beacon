use beacon_nd_arrow::datatypes::{NdArrayDataType, NdArrayType, TimestampNanosecond};

use crate::{
    scalar::{ArchivedScalar, Scalar},
    typed_vec::{ArchivedTypedVec, TypedVec},
};

pub trait AtlasType: NdArrayType + Default + Send + Sync + 'static {
    fn atlas_datatype() -> AtlasDataType;
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

macro_rules! impl_atlas_type {
    (
        $ty:ty,
        $atlas_variant:ident,
        $scalar_variant:ident,
        $typed_vec_variant:ident,
        scalar($scalar_value:ident => $from_scalar:expr),
        archived_scalar($archived_scalar_value:ident => $from_archived_scalar:expr),
        archived_vec($archived_vec_values:ident => $from_archived_vec:expr)
    ) => {
        impl AtlasType for $ty {
            fn atlas_datatype() -> AtlasDataType {
                AtlasDataType::$atlas_variant
            }

            fn try_from_scalar(scalar: &Scalar) -> anyhow::Result<Self>
            where
                Self: Sized,
            {
                match scalar {
                    Scalar::$scalar_variant($scalar_value) => Ok($from_scalar),
                    other => anyhow::bail!(
                        "Expected {} scalar but received: {:?}",
                        stringify!($ty),
                        other
                    ),
                }
            }

            fn try_from_archived_scalar(scalar: &rkyv::Archived<Scalar>) -> anyhow::Result<Self>
            where
                Self: Sized,
            {
                match scalar {
                    ArchivedScalar::$scalar_variant($archived_scalar_value) => {
                        Ok($from_archived_scalar)
                    }
                    other => anyhow::bail!(
                        "Expected archived {} scalar but received: {:?}",
                        stringify!($ty),
                        other
                    ),
                }
            }

            fn try_from_vec(vec: TypedVec) -> anyhow::Result<Vec<Self>>
            where
                Self: Sized,
            {
                match vec {
                    TypedVec::$typed_vec_variant(vals) => Ok(vals),
                    other => {
                        anyhow::bail!("Expected {} vec but received: {:?}", stringify!($ty), other)
                    }
                }
            }

            fn try_from_archived_vec(vec: &rkyv::Archived<TypedVec>) -> anyhow::Result<Vec<Self>>
            where
                Self: Sized,
            {
                match vec {
                    ArchivedTypedVec::$typed_vec_variant($archived_vec_values) => {
                        Ok($from_archived_vec)
                    }
                    other => anyhow::bail!(
                        "Expected archived {} vec but received: {:?}",
                        stringify!($ty),
                        other
                    ),
                }
            }

            fn into_vec(values: Vec<Self>) -> TypedVec
            where
                Self: Sized,
            {
                TypedVec::$typed_vec_variant(values)
            }
        }
    };
}

impl_atlas_type!(
    bool,
    Bool,
    Bool,
    Bool,
    scalar(val => *val),
    archived_scalar(val => *val),
    archived_vec(vals => vals.to_vec())
);
impl_atlas_type!(
    i8,
    I8,
    I8,
    I8,
    scalar(val => *val),
    archived_scalar(val => *val),
    archived_vec(vals => vals.to_vec())
);
impl_atlas_type!(
    i16,
    I16,
    I16,
    I16,
    scalar(val => *val),
    archived_scalar(val => val.to_native()),
    archived_vec(vals => vals.iter().map(|v| v.to_native()).collect())
);
impl_atlas_type!(
    i32,
    I32,
    I32,
    I32,
    scalar(val => *val),
    archived_scalar(val => val.to_native()),
    archived_vec(vals => vals.iter().map(|v| v.to_native()).collect())
);
impl_atlas_type!(
    i64,
    I64,
    I64,
    I64,
    scalar(val => *val),
    archived_scalar(val => val.to_native()),
    archived_vec(vals => vals.iter().map(|v| v.to_native()).collect())
);
impl_atlas_type!(
    u8,
    U8,
    U8,
    U8,
    scalar(val => *val),
    archived_scalar(val => *val),
    archived_vec(vals => vals.to_vec())
);
impl_atlas_type!(
    u16,
    U16,
    U16,
    U16,
    scalar(val => *val),
    archived_scalar(val => val.to_native()),
    archived_vec(vals => vals.iter().map(|v| v.to_native()).collect())
);
impl_atlas_type!(
    u32,
    U32,
    U32,
    U32,
    scalar(val => *val),
    archived_scalar(val => val.to_native()),
    archived_vec(vals => vals.iter().map(|v| v.to_native()).collect())
);
impl_atlas_type!(
    u64,
    U64,
    U64,
    U64,
    scalar(val => *val),
    archived_scalar(val => val.to_native()),
    archived_vec(vals => vals.iter().map(|v| v.to_native()).collect())
);
impl_atlas_type!(
    f32,
    F32,
    F32,
    F32,
    scalar(val => *val),
    archived_scalar(val => val.to_native()),
    archived_vec(vals => vals.iter().map(|v| v.to_native()).collect())
);
impl_atlas_type!(
    f64,
    F64,
    F64,
    F64,
    scalar(val => *val),
    archived_scalar(val => val.to_native()),
    archived_vec(vals => vals.iter().map(|v| v.to_native()).collect())
);
impl_atlas_type!(
    TimestampNanosecond,
    Timestamp,
    Timestamp,
    Timestamp,
    scalar(val => *val),
    archived_scalar(val => TimestampNanosecond(val.0.to_native())),
    archived_vec(vals => vals.iter().map(|v| TimestampNanosecond(v.0.to_native())).collect())
);
impl_atlas_type!(
    String,
    String,
    String,
    String,
    scalar(val => val.clone()),
    archived_scalar(val => val.as_str().to_string()),
    archived_vec(vals => vals.iter().map(|v| v.as_str().to_string()).collect())
);
impl_atlas_type!(
    Vec<u8>,
    Binary,
    Binary,
    Binary,
    scalar(val => val.clone()),
    archived_scalar(val => val.as_slice().to_vec()),
    archived_vec(vals => vals.iter().map(|v| v.as_slice().to_vec()).collect())
);

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

impl From<NdArrayDataType> for AtlasDataType {
    fn from(value: NdArrayDataType) -> Self {
        match value {
            NdArrayDataType::Bool => AtlasDataType::Bool,
            NdArrayDataType::I8 => AtlasDataType::I8,
            NdArrayDataType::I16 => AtlasDataType::I16,
            NdArrayDataType::I32 => AtlasDataType::I32,
            NdArrayDataType::I64 => AtlasDataType::I64,
            NdArrayDataType::U8 => AtlasDataType::U8,
            NdArrayDataType::U16 => AtlasDataType::U16,
            NdArrayDataType::U32 => AtlasDataType::U32,
            NdArrayDataType::U64 => AtlasDataType::U64,
            NdArrayDataType::F32 => AtlasDataType::F32,
            NdArrayDataType::F64 => AtlasDataType::F64,
            NdArrayDataType::Timestamp => AtlasDataType::Timestamp,
            NdArrayDataType::Binary => AtlasDataType::Binary,
            NdArrayDataType::String => AtlasDataType::String,
        }
    }
}

impl From<AtlasDataType> for NdArrayDataType {
    fn from(value: AtlasDataType) -> Self {
        match value {
            AtlasDataType::Bool => NdArrayDataType::Bool,
            AtlasDataType::I8 => NdArrayDataType::I8,
            AtlasDataType::I16 => NdArrayDataType::I16,
            AtlasDataType::I32 => NdArrayDataType::I32,
            AtlasDataType::I64 => NdArrayDataType::I64,
            AtlasDataType::U8 => NdArrayDataType::U8,
            AtlasDataType::U16 => NdArrayDataType::U16,
            AtlasDataType::U32 => NdArrayDataType::U32,
            AtlasDataType::U64 => NdArrayDataType::U64,
            AtlasDataType::F32 => NdArrayDataType::F32,
            AtlasDataType::F64 => NdArrayDataType::F64,
            AtlasDataType::Timestamp => NdArrayDataType::Timestamp,
            AtlasDataType::Binary => NdArrayDataType::Binary,
            AtlasDataType::String => NdArrayDataType::String,
        }
    }
}
