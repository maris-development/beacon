use std::{io::Cursor, sync::Arc, time::Instant};

use arrow::{array::Int32Array, datatypes::Field, record_batch::RecordBatch};
use arrow_ipc::{CompressionType, writer::IpcWriteOptions};
use arrow_schema::DataType;
use rand::{Rng, SeedableRng, rngs::StdRng};

use beacon_nd_arrow::{
    NdArrowArray,
    column::NdArrowArrayColumn,
    dimensions::{Dimension, Dimensions},
    extension,
};

fn ipc_file_bytes(batch: &RecordBatch, compression: Option<CompressionType>) -> Vec<u8> {
    let schema = batch.schema();
    let options = IpcWriteOptions::default()
        .try_with_compression(compression)
        .expect("valid compression options");

    let mut cursor = Cursor::new(Vec::<u8>::new());
    let mut writer = arrow_ipc::writer::FileWriter::try_new_with_options(cursor, &schema, options)
        .expect("create ipc file writer");

    writer.write(batch).expect("write batch");
    writer.finish().expect("finish writer");

    cursor = writer.into_inner().expect("extract ipc writer buffer");
    cursor.into_inner()
}

fn make_nd_record_batch(nrows: usize, y: usize, x: usize) -> RecordBatch {
    let dims = Dimensions::new(vec![
        Dimension::try_new("y", y).unwrap(),
        Dimension::try_new("x", x).unwrap(),
    ]);

    let mut rng = StdRng::seed_from_u64(42);
    let rows = (0..nrows)
        .map(|_| {
            let row_vals: Vec<i32> = (0..(y * x)).map(|_| rng.gen_range(200..250)).collect();
            NdArrowArray::new(Arc::new(Int32Array::from(row_vals)), dims.clone()).unwrap()
        })
        .collect::<Vec<_>>();

    let col = NdArrowArrayColumn::from_rows(rows).expect("build ND column");

    let field = extension::nd_column_field("v", DataType::Int32, true).unwrap();
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![field]));
    RecordBatch::try_new(schema, vec![col.into_array_ref()]).unwrap()
}

fn make_flat_record_batch(total_elements: usize) -> RecordBatch {
    let mut rng = StdRng::seed_from_u64(43);
    let vals: Vec<i32> = (0..total_elements)
        .map(|_| rng.gen_range(200..250))
        .collect();
    let arr = Arc::new(Int32Array::from(vals));

    let field = Field::new("v", DataType::Int32, true);
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![field]));
    RecordBatch::try_new(schema, vec![arr]).unwrap()
}

fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let nrows = args
        .get(0)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(200);
    let y = args
        .get(1)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(32);
    let x = args
        .get(2)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(32);

    let t0 = Instant::now();
    let nd_rb = make_nd_record_batch(nrows, y, x);
    let dt_nd_rb = t0.elapsed();

    let t1 = Instant::now();
    let flat_rb = make_flat_record_batch(nrows.saturating_mul(y.saturating_mul(x)));
    let dt_flat_rb = t1.elapsed();

    let nd_ipc = ipc_file_bytes(&nd_rb, Some(CompressionType::ZSTD));
    let flat_ipc = ipc_file_bytes(&flat_rb, Some(CompressionType::ZSTD));

    let elements_per_row = y.saturating_mul(x);
    let total_elements = nrows.saturating_mul(elements_per_row);

    println!("nrows: {}", nrows);
    println!("shape: {}x{}", y, x);
    println!("columns: 1");
    println!("nd rows: {}", nd_rb.num_rows());
    println!("nd elements total (nrows*y*x): {}", total_elements);
    println!("flat elements total: {}", flat_rb.num_rows());
    println!("nd ipc zstd bytes: {}", nd_ipc.len());
    println!("flat ipc zstd bytes: {}", flat_ipc.len());
    println!("build nd RecordBatch: {:?}", dt_nd_rb);
    println!("build flat RecordBatch: {:?}", dt_flat_rb);
}
