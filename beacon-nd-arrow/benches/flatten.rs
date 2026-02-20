use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Float64Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use beacon_nd_arrow::{
    NdArrowArray,
    dimensions::{Dimension, Dimensions},
};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};

#[derive(Clone)]
struct BenchmarkColumn {
    name: &'static str,
    data_type: DataType,
    array: NdArrowArray,
}

fn dim(name: &str, size: usize) -> Dimension {
    Dimension::try_new(name, size).expect("invalid benchmark dimension")
}

fn make_schema(columns: &[BenchmarkColumn]) -> Arc<Schema> {
    Arc::new(Schema::new(
        columns
            .iter()
            .map(|column| Field::new(column.name, column.data_type.clone(), true))
            .collect::<Vec<_>>(),
    ))
}

fn sliced_record_batch_from_broadcast_views(
    columns: &[BenchmarkColumn],
    schema: Arc<Schema>,
    target: &Dimensions,
    offset: usize,
    len: usize,
) -> RecordBatch {
    let arrays = columns
        .iter()
        .map(|column| {
            let view = column
                .array
                .broadcast_to(target)
                .expect("broadcast_to failed in benchmark");
            view.take(offset, len)
        })
        .collect::<Vec<ArrayRef>>();

    RecordBatch::try_new(schema, arrays).expect("failed to construct benchmark record batch")
}

fn run_case(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    case_name: &str,
    columns: Vec<BenchmarkColumn>,
    target: Dimensions,
    offset: usize,
    len: usize,
) {
    let schema = make_schema(&columns);
    let elements_per_iteration = len.saturating_mul(columns.len());
    group.throughput(Throughput::Elements(elements_per_iteration as u64));

    group.bench_with_input(
        BenchmarkId::new("record_batch_from_views", case_name),
        &len,
        |b, _| {
            b.iter(|| {
                let batch = sliced_record_batch_from_broadcast_views(
                    &columns,
                    schema.clone(),
                    &target,
                    offset,
                    len,
                );
                black_box(batch);
            });
        },
    );
}

fn bench_broadcast_shared_1d(c: &mut Criterion) {
    let mut group = c.benchmark_group("broadcast_record_batch_shared_1d");

    let n_time = 8_192usize;
    let target = Dimensions::new(vec![dim("time", n_time)]);

    let time_values = (0..n_time as i64).collect::<Vec<_>>();
    let temperature_values = (0..n_time)
        .map(|i| 8.0 + ((i % 97) as f64 * 0.05))
        .collect::<Vec<_>>();

    let columns = vec![
        BenchmarkColumn {
            name: "time",
            data_type: DataType::Int64,
            array: NdArrowArray::new(
                Arc::new(Int64Array::from(time_values)),
                Dimensions::new(vec![dim("time", n_time)]),
            )
            .unwrap(),
        },
        BenchmarkColumn {
            name: "temperature",
            data_type: DataType::Float64,
            array: NdArrowArray::new(
                Arc::new(Float64Array::from(temperature_values)),
                Dimensions::new(vec![dim("time", n_time)]),
            )
            .unwrap(),
        },
        BenchmarkColumn {
            name: "platform_name",
            data_type: DataType::Utf8,
            array: NdArrowArray::new(
                Arc::new(StringArray::from(vec!["platform-A"])),
                Dimensions::Scalar,
            )
            .unwrap(),
        },
    ];
    let offset = 1_000usize;
    let len = 2_048usize;

    run_case(
        &mut group,
        "all_1d_plus_scalar",
        columns,
        target,
        offset,
        len,
    );

    group.finish();
}

fn bench_broadcast_shared_2d(c: &mut Criterion) {
    let mut group = c.benchmark_group("broadcast_record_batch_shared_2d");

    let n_lat = 128usize;
    let n_lon = 256usize;
    let n = n_lat * n_lon;
    let target = Dimensions::new(vec![dim("latitude", n_lat), dim("longitude", n_lon)]);

    let latitude_grid = (0..n)
        .map(|index| {
            let lat_idx = index / n_lon;
            -90.0 + (lat_idx as f64 * 180.0 / (n_lat.saturating_sub(1)) as f64)
        })
        .collect::<Vec<_>>();

    let longitude_grid = (0..n)
        .map(|index| {
            let lon_idx = index % n_lon;
            -180.0 + (lon_idx as f64 * 360.0 / (n_lon.saturating_sub(1)) as f64)
        })
        .collect::<Vec<_>>();

    let temperature_grid = (0..n)
        .map(|index| {
            let lat_idx = index / n_lon;
            let lon_idx = index % n_lon;
            12.0 + (lat_idx as f64 * 0.02) + (lon_idx as f64 * 0.01)
        })
        .collect::<Vec<_>>();

    let columns = vec![
        BenchmarkColumn {
            name: "latitude",
            data_type: DataType::Float64,
            array: NdArrowArray::new(
                Arc::new(Float64Array::from(latitude_grid)),
                Dimensions::new(vec![dim("latitude", n_lat), dim("longitude", n_lon)]),
            )
            .unwrap(),
        },
        BenchmarkColumn {
            name: "longitude",
            data_type: DataType::Float64,
            array: NdArrowArray::new(
                Arc::new(Float64Array::from(longitude_grid)),
                Dimensions::new(vec![dim("latitude", n_lat), dim("longitude", n_lon)]),
            )
            .unwrap(),
        },
        BenchmarkColumn {
            name: "temperature",
            data_type: DataType::Float64,
            array: NdArrowArray::new(
                Arc::new(Float64Array::from(temperature_grid)),
                Dimensions::new(vec![dim("latitude", n_lat), dim("longitude", n_lon)]),
            )
            .unwrap(),
        },
        BenchmarkColumn {
            name: "platform_name",
            data_type: DataType::Utf8,
            array: NdArrowArray::new(
                Arc::new(StringArray::from(vec!["platform-B"])),
                Dimensions::Scalar,
            )
            .unwrap(),
        },
    ];
    let offset = 2_000usize;
    let len = 8_192usize;

    run_case(
        &mut group,
        "all_2d_plus_scalar",
        columns,
        target,
        offset,
        len,
    );

    group.finish();
}

fn bench_broadcast_shared_3d(c: &mut Criterion) {
    let mut group = c.benchmark_group("broadcast_record_batch_mixed_dims_3d_target");

    let n_time = 24usize;
    let n_lat = 64usize;
    let n_lon = 128usize;
    let n_2d = n_lat * n_lon;
    let n_3d = n_time * n_2d;

    let target = Dimensions::new(vec![
        dim("time", n_time),
        dim("latitude", n_lat),
        dim("longitude", n_lon),
    ]);

    let time_values = (0..n_time as i64).collect::<Vec<_>>();

    let latitude_2d = (0..n_2d)
        .map(|index| {
            let lat_idx = index / n_lon;
            -90.0 + (lat_idx as f64 * 180.0 / (n_lat.saturating_sub(1)) as f64)
        })
        .collect::<Vec<_>>();

    let longitude_2d = (0..n_2d)
        .map(|index| {
            let lon_idx = index % n_lon;
            -180.0 + (lon_idx as f64 * 360.0 / (n_lon.saturating_sub(1)) as f64)
        })
        .collect::<Vec<_>>();

    let temperature_3d = (0..n_3d)
        .map(|index| {
            let t = index / n_2d;
            let rem = index % n_2d;
            let lat = rem / n_lon;
            let lon = rem % n_lon;
            10.0 + (t as f64 * 0.3) + (lat as f64 * 0.02) + (lon as f64 * 0.01)
        })
        .collect::<Vec<_>>();

    let columns_mixed_1d_2d_3d = vec![
        BenchmarkColumn {
            name: "time",
            data_type: DataType::Int64,
            array: NdArrowArray::new(
                Arc::new(Int64Array::from(time_values)),
                Dimensions::new(vec![dim("time", n_time)]),
            )
            .unwrap(),
        },
        BenchmarkColumn {
            name: "latitude",
            data_type: DataType::Float64,
            array: NdArrowArray::new(
                Arc::new(Float64Array::from(latitude_2d)),
                Dimensions::new(vec![dim("latitude", n_lat), dim("longitude", n_lon)]),
            )
            .unwrap(),
        },
        BenchmarkColumn {
            name: "longitude",
            data_type: DataType::Float64,
            array: NdArrowArray::new(
                Arc::new(Float64Array::from(longitude_2d)),
                Dimensions::new(vec![dim("latitude", n_lat), dim("longitude", n_lon)]),
            )
            .unwrap(),
        },
        BenchmarkColumn {
            name: "temperature",
            data_type: DataType::Float64,
            array: NdArrowArray::new(
                Arc::new(Float64Array::from(temperature_3d)),
                Dimensions::new(vec![
                    dim("time", n_time),
                    dim("latitude", n_lat),
                    dim("longitude", n_lon),
                ]),
            )
            .unwrap(),
        },
        BenchmarkColumn {
            name: "platform_name",
            data_type: DataType::Utf8,
            array: NdArrowArray::new(
                Arc::new(StringArray::from(vec!["platform-C"])),
                Dimensions::Scalar,
            )
            .unwrap(),
        },
    ];

    let offset = 10_000usize;
    let len = 16_384usize;

    run_case(
        &mut group,
        "mixed_1d_2d_3d_plus_scalar",
        columns_mixed_1d_2d_3d,
        target.clone(),
        offset,
        len,
    );

    let temperature_time_1d = (0..n_time)
        .map(|t| 9.0 + t as f64 * 0.25)
        .collect::<Vec<_>>();
    let temperature_3d = (0..n_3d)
        .map(|index| {
            let t = index / n_2d;
            let rem = index % n_2d;
            let lat = rem / n_lon;
            let lon = rem % n_lon;
            10.0 + (t as f64 * 0.3) + (lat as f64 * 0.02) + (lon as f64 * 0.01)
        })
        .collect::<Vec<_>>();

    let columns_mixed_1d_3d = vec![
        BenchmarkColumn {
            name: "time",
            data_type: DataType::Int64,
            array: NdArrowArray::new(
                Arc::new(Int64Array::from((0..n_time as i64).collect::<Vec<_>>())),
                Dimensions::new(vec![dim("time", n_time)]),
            )
            .unwrap(),
        },
        BenchmarkColumn {
            name: "temperature_time",
            data_type: DataType::Float64,
            array: NdArrowArray::new(
                Arc::new(Float64Array::from(temperature_time_1d)),
                Dimensions::new(vec![dim("time", n_time)]),
            )
            .unwrap(),
        },
        BenchmarkColumn {
            name: "temperature_3d",
            data_type: DataType::Float64,
            array: NdArrowArray::new(
                Arc::new(Float64Array::from(temperature_3d)),
                Dimensions::new(vec![
                    dim("time", n_time),
                    dim("latitude", n_lat),
                    dim("longitude", n_lon),
                ]),
            )
            .unwrap(),
        },
        BenchmarkColumn {
            name: "platform_name",
            data_type: DataType::Utf8,
            array: NdArrowArray::new(
                Arc::new(StringArray::from(vec!["platform-D"])),
                Dimensions::Scalar,
            )
            .unwrap(),
        },
    ];

    run_case(
        &mut group,
        "mixed_1d_3d_plus_scalar",
        columns_mixed_1d_3d,
        target,
        offset,
        len,
    );

    group.finish();
}

fn criterion_benchmark(c: &mut Criterion) {
    bench_broadcast_shared_1d(c);
    bench_broadcast_shared_2d(c);
    bench_broadcast_shared_3d(c);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
