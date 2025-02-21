use ndarray::{ArrayBase, Dim, IxDynImpl, OwnedRepr};
use netcdf::{Extents, Variable};

use crate::NcChar;

pub type NetCDFNdArrayBase<T> = ArrayBase<OwnedRepr<T>, Dim<IxDynImpl>>;

pub enum NetCDFNdArray {
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
}

pub fn read_variable(variable: &Variable) {
    let arr = ndarray::Array::from_shape_vec(vec![1, 2, 3], vec![1, 2, 3, 4, 5, 6]).unwrap();
    let nd_array = variable.get::<u8, _>(Extents::All).unwrap();
    todo!()
}
