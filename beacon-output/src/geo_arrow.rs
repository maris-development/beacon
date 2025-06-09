// use std::sync::Arc;

// use arrow::{
//     array::{Array, Float64Array, RecordBatch},
//     compute::CastOptions,
//     util::display::FormatOptions,
// };
// use geoarrow::{
//     array::PointBuilder,
//     scalar::{Coord, InterleavedCoord},
//     ArrayBase,
// };
// use utoipa::ToSchema;

// #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
// pub struct Options {
//     #[serde(default = "default_longitude")]
//     longitude_column: String,
//     #[serde(default = "default_latitude")]
//     latitude_column: String,
// }

// fn default_longitude() -> String {
//     "longitude".to_string()
// }

// fn default_latitude() -> String {
//     "latitude".to_string()
// }

// fn convert_to_points(
//     longitude_idx: usize,
//     latitude_idx: usize,
//     record_batch: RecordBatch,
// ) -> anyhow::Result<RecordBatch> {
//     let longitude_array = cast_to_float_safe(record_batch.column(longitude_idx));
//     let latitude_array = cast_to_float_safe(record_batch.column(latitude_idx));
//     let longitude_array = longitude_array
//         .as_any()
//         .downcast_ref::<Float64Array>()
//         .unwrap();
//     let latitude_array = latitude_array
//         .as_any()
//         .downcast_ref::<Float64Array>()
//         .unwrap();

//     let mut point_builder = PointBuilder::new(geoarrow::datatypes::Dimension::XY);

//     latitude_array
//         .iter()
//         .zip(longitude_array.iter())
//         .for_each(|(lat, lon)| {
//             if let (Some(lat), Some(lon)) = (lat, lon) {
//                 let point = geo::Point::from((lat, lon));
//                 point_builder.push_point(Some(&point));
//             } else {
//                 point_builder.push_null();
//             }
//         });

//     let point_array = point_builder.finish();
//     let field = point_array.extension_field();
//     let arrow_point_array = point_array.into_array_ref();

//     let mut current_fields = record_batch.schema().fields.to_vec();
//     current_fields.push(field.clone());

//     let mut current_columns = record_batch.columns().to_vec();
//     current_columns.push(arrow_point_array);

//     let new_schema = Arc::new(arrow::datatypes::Schema::new(current_fields));
//     let new_record_batch = RecordBatch::try_new(new_schema, current_columns)?;

//     Ok(new_record_batch)
// }

// fn cast_to_float_safe(array: &dyn Array) -> Arc<dyn Array> {
//     const CAST_OPTIONS: CastOptions = CastOptions {
//         safe: true,
//         format_options: FormatOptions::new(),
//     };
//     arrow::compute::kernels::cast::cast_with_options(
//         array,
//         &arrow::datatypes::DataType::Float64,
//         &CAST_OPTIONS,
//     )
//     .unwrap_or_else(|_| panic!("Failed to cast array to Float64: {:?}", array.data_type()))
// }
