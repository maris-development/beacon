use std::{io::Cursor, sync::Arc};

use arrow::{array::Int32Array, compute::concat, datatypes::Field, record_batch::RecordBatch};
use arrow_ipc::{writer::IpcWriteOptions, CompressionType};
use arrow_schema::DataType;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::{rngs::StdRng, Rng, SeedableRng};

use beacon_nd_arrow::{
    column::NdArrowArrayColumn,
    dimensions::{Dimension, Dimensions},
    extension,
    NdArrowArray,
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

fn make_nd_column(nrows: usize, y: usize, x: usize, broadcast_inputs: bool) -> NdArrowArrayColumn {
    let target_dims = Dimensions::new(vec![
        Dimension::try_new("y", y).unwrap(),
        Dimension::try_new("x", x).unwrap(),
    ]);

    let mut rng = StdRng::seed_from_u64(42);

    let small_dims = Dimensions::new(vec![
        Dimension::try_new("y", 1).unwrap(),
        Dimension::try_new("x", x).unwrap(),
    ]);

    let rows = (0..nrows)
        .map(|i| {
            if broadcast_inputs && (i % 2 == 0) {
                let small_vals: Vec<i32> = (0..x)
                    .map(|_| rng.gen_range(i32::MIN..=i32::MAX))
                    .collect();
                NdArrowArray::new(Arc::new(Int32Array::from(small_vals)), small_dims.clone())
                    .expect("small row")
            } else {
                let full_vals: Vec<i32> = (0..(y * x))
                    .map(|_| rng.gen_range(i32::MIN..=i32::MAX))
                    .collect();
                NdArrowArray::new(
                    Arc::new(Int32Array::from(full_vals)),
                    target_dims.clone(),
                )
                .expect("full row")
            }
        })
        .collect::<Vec<_>>();

    NdArrowArrayColumn::from_rows(rows).expect("build ND column")
}

fn make_nd_record_batch(col: NdArrowArrayColumn) -> RecordBatch {
    let field = extension::nd_column_field("v", col.storage_type().clone(), true).unwrap();
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![field]));
    RecordBatch::try_new(schema, vec![col.into_array_ref()]).unwrap()
}

fn make_flat_record_batch(total_elements: usize) -> RecordBatch {
    let mut rng = StdRng::seed_from_u64(43);
    let vals: Vec<i32> = (0..total_elements)
        .map(|_| rng.gen_range(i32::MIN..=i32::MAX))
        .collect();
    let arr = Arc::new(Int32Array::from(vals));

    let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
        "v",
        DataType::Int32,
        true,
    )]));
    RecordBatch::try_new(schema, vec![arr]).unwrap()
}

fn flatten_nd_column_to_single_array(
    col: &NdArrowArrayColumn,
    target: &Dimensions,
) -> arrow::error::Result<Arc<dyn arrow::array::Array>> {
    let mut chunks = Vec::with_capacity(col.len());
    for row_idx in 0..col.len() {
        let v = col.row(row_idx).expect("row");
        let b = v.broadcast_to(target).expect("broadcast");
        chunks.push(b.values().clone());
    }

    let chunk_refs = chunks
        .iter()
        .map(|a| a.as_ref() as &dyn arrow::array::Array)
        .collect::<Vec<_>>();
    concat(&chunk_refs)
}

fn bench_flatten_and_ipc(c: &mut Criterion) {
    let mut group = c.benchmark_group("nd_flatten");

    // A couple of representative sizes (keep these reasonable; flattened expands rows).
    let cases = [
        ("rows200_32x32", 200usize, 32usize, 32usize),
        ("rows1000_16x16", 1000usize, 16usize, 16usize),
    ];

    for (name, nrows, y, x) in cases {
        let target = Dimensions::new(vec![
            Dimension::try_new("y", y).unwrap(),
            Dimension::try_new("x", x).unwrap(),
        ]);

        // Single-column ND batch where some rows are (1,x) and must be broadcast to (y,x).
        let nd_col_broadcast = make_nd_column(nrows, y, x, true);
        let nd_rb = make_nd_record_batch(nd_col_broadcast.clone());

        // Flat batch with the same total element count: nrows * y * x.
        let total_elements = nrows.saturating_mul(y.saturating_mul(x));
        let flat_rb = make_flat_record_batch(total_elements);

        // Size comparison (printed once per case). This is the main "batch size vs flattened size".
        let nd_zstd = ipc_file_bytes(&nd_rb, Some(CompressionType::ZSTD));
        let flat_zstd = ipc_file_bytes(&flat_rb, Some(CompressionType::ZSTD));

        let nd_rows = nd_rb.num_rows();
        let flat_rows_actual = flat_rb.num_rows();
        eprintln!(
            "[{name}] columns=1 nrows={nrows} shape={y}x{x} nd_rows={nd_rows} nd_elems_total={total_elements} flat_elems_total={flat_rows_actual} nd_ipc_zstd={}B flat_ipc_zstd={}B",
            nd_zstd.len(),
            flat_zstd.len(),
        );

        // Performance: broadcast-to-target + flatten to a single flat array.
        group.throughput(Throughput::Elements(total_elements as u64));
        group.bench_with_input(
            BenchmarkId::new("broadcast_flatten_to_single_array", name),
            &nd_col_broadcast,
            |b, col| {
                b.iter(|| black_box(flatten_nd_column_to_single_array(col, &target).unwrap()));
            },
        );

        // Performance: IPC encode ND batch
        let nd_bytes_len = nd_zstd.len() as u64;
        group.throughput(Throughput::Bytes(nd_bytes_len));
        group.bench_with_input(
            BenchmarkId::new("ipc_encode_nd_zstd", name),
            &nd_rb,
            |b, rb| {
                b.iter(|| black_box(ipc_file_bytes(rb, Some(CompressionType::ZSTD))));
            },
        );

        // Performance: IPC encode flattened batch
        let flat_bytes_len = flat_zstd.len() as u64;
        group.throughput(Throughput::Bytes(flat_bytes_len));
        group.bench_with_input(
            BenchmarkId::new("ipc_encode_flat_zstd", name),
            &flat_rb,
            |b, rb| {
                b.iter(|| black_box(ipc_file_bytes(rb, Some(CompressionType::ZSTD))));
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_flatten_and_ipc);
criterion_main!(benches);
