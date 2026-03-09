use beacon_nd_arrow::array::compat_typings::ArrowTypeConversion;
use netcdf::NcTypeDescriptor;

// #[async_trait::async_trait]
// pub trait ArrowNetCDFType: NcTypeDescriptor + ArrowTypeConversion + Send + Sync + 'static {
//     async fn read_variable(
//         variable: &netcdf::Variable,
//         extents: netcdf::Extents,
//     ) -> anyhow::Result<ndarray::ArrayD<Self>>;
// }

// #[async_trait::async_trait]
// impl ArrowNetCDFType for i32 {
//     async fn read_variable(
//         variable: &netcdf::Variable,
//         extents: netcdf::Extents,
//     ) -> anyhow::Result<ndarray::ArrayD<Self>> {
//         let array = variable.get::<_, _>(extents)?;
//         Ok(array)
//     }
// }
