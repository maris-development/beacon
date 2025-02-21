use ndarray::{ArrayBase, Dim, IxDynImpl, OwnedRepr};

use crate::NcChar;

pub type NetCDFNdArrayBase<T> = ArrayBase<OwnedRepr<T>, Dim<IxDynImpl>>;

pub struct Dimension {
    name: String,
    size: usize,
}

pub struct NetCDFNdArray {
    dims: Vec<Dimension>,
    array: NetCDFNdArrayInner,
}

pub enum NetCDFNdArrayInner {
    U8(NetCDFNdArrayBase<u8>),
    U16(NetCDFNdArrayBase<u16>),
    U32(NetCDFNdArrayBase<u32>),
    U64(NetCDFNdArrayBase<u64>),
    I8(NetCDFNdArrayBase<i8>),
    I16(NetCDFNdArrayBase<i16>),
    I32(NetCDFNdArrayBase<i32>),
    I64(NetCDFNdArrayBase<i64>),
    F32(NetCDFNdArrayBase<f32>),
    F64(NetCDFNdArrayBase<f64>),
    Char(NetCDFNdArrayBase<NcChar>),
    FixedStringSize(NetCDFNdArrayBase<NcChar>),
}

impl From<NetCDFNdArrayInner> for arrow::array::ArrayRef {
    fn from(value: NetCDFNdArrayInner) -> Self {
        todo!()
    }
}
