use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array, StringArray, UInt64Array};
use arrow_schema::{DataType, Field};
use beacon_binary_format::array_partition::ArrayPartitionReader;
use beacon_binary_format::collection::{CollectionReader, CollectionWriter};
use beacon_binary_format::collection_partition::{
    CollectionPartitionReadOptions, CollectionPartitionWriter, WriterOptions,
};
use beacon_binary_format::io_cache::ArrayIoCache;
use beacon_binary_format::partition_resolution::PartitionResolution;
use futures::{StreamExt, stream};
use nd_arrow_array::NdArrowArray;
use nd_arrow_array::dimensions::{Dimension, Dimensions};
use object_store::ObjectStore;
use object_store::memory::InMemory;
use object_store::path::Path;

fn scalar_int32(values: &[i32]) -> NdArrowArray {
    let array: ArrayRef = Arc::new(Int32Array::from(values.to_vec()));
    let dimension = Dimension {
        name: "dim0".to_string(),
        size: values.len(),
    };
    NdArrowArray::new(array, Dimensions::MultiDimensional(vec![dimension]))
        .expect("nd array creation")
}

#[tokio::test]
async fn write_read_and_pruning_index_round_trip() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let root = Path::from("file.bbf");

    // 1) Write a partition.
    let mut partition_writer = CollectionPartitionWriter::new(
        root.clone(),
        store.clone(),
        "p1".to_string(),
        WriterOptions {
            max_group_size: usize::MAX,
        },
    );

    let temp_field = Arc::new(Field::new("temp", DataType::Int32, true));
    let sal_field = Arc::new(Field::new("sal", DataType::Int32, true));

    partition_writer
        .write_entry(
            "entry-1",
            stream::iter(vec![
                (temp_field.clone(), scalar_int32(&[1, 2])),
                (sal_field.clone(), scalar_int32(&[7])),
            ]),
        )
        .await
        .expect("write entry 1");

    partition_writer
        .write_entry(
            "entry-2",
            stream::iter(vec![(temp_field.clone(), scalar_int32(&[3]))]),
        )
        .await
        .expect("write entry 2");

    let partition_metadata = partition_writer.finish().await.expect("finish partition");

    // 2) Persist collection metadata (bbf.json).
    let mut collection_writer = CollectionWriter::new(store.clone(), root.clone())
        .await
        .expect("collection writer");
    collection_writer
        .append_partition(partition_metadata.clone())
        .expect("append partition metadata");
    collection_writer.persist().await.expect("persist bbf.json");

    // 3) Verify pruning index sidecar exists + can be read through ArrayPartitionReader.
    let partition_path = root.child("partitions".to_string()).child("p1".to_string());

    let temp_meta = partition_metadata.arrays.get("temp").expect("temp meta");
    let pruning_hash = temp_meta
        .pruning_index_hash
        .clone()
        .expect("expected pruning index hash");
    let pruning_blob_path = partition_path.child("pruning_index.bbpi".to_string());

    // Ensure the partition-level pruning index blob is present.
    store
        .head(&pruning_blob_path)
        .await
        .expect("pruning index exists");

    // Load resolution.json and open a reader for the temp array.
    let resolution_path = partition_path.child("resolution.json".to_string());
    let resolution_bytes = store
        .get(&resolution_path)
        .await
        .expect("resolution.json")
        .bytes()
        .await
        .expect("resolution bytes");
    let resolution: PartitionResolution =
        serde_json::from_slice(&resolution_bytes).expect("decode resolution");

    let slice = *resolution.objects.get(&temp_meta.hash).expect("temp slice");
    let pruning_slice = *resolution
        .objects
        .get(&pruning_hash)
        .expect("pruning index slice");

    let blob_path = partition_path.child("partition_blob.bbb".to_string());

    let array_reader = ArrayPartitionReader::new(
        store.clone(),
        "temp".to_string(),
        blob_path,
        slice,
        Some((pruning_blob_path.clone(), pruning_slice)),
        temp_meta.clone(),
        ArrayIoCache::new(1024 * 1024),
    )
    .await
    .expect("array reader");

    let pruning_batch = array_reader
        .read_partition_pruning_index()
        .await
        .expect("read pruning index")
        .expect("pruning index present");

    assert_eq!(pruning_batch.num_columns(), 4);
    assert_eq!(pruning_batch.schema().field(0).name(), "temp:min");
    assert_eq!(pruning_batch.schema().field(1).name(), "temp:max");
    assert_eq!(pruning_batch.schema().field(2).name(), "temp:null_count");
    assert_eq!(pruning_batch.schema().field(3).name(), "temp:row_count");

    // Sanity: the row_count column should have at least one non-zero row.
    let row_count = pruning_batch
        .column(3)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("row_count array");
    assert!(row_count.iter().flatten().any(|v| v > 0));

    // 4) End-to-end read via CollectionReader / CollectionPartitionReader.
    let collection_reader =
        CollectionReader::new(store.clone(), root.clone(), ArrayIoCache::new(2))
            .await
            .expect("collection reader");

    let partition_reader = collection_reader
        .partition_reader("p1")
        .expect("partition reader");

    let scheduler = partition_reader
        .read(
            None,
            CollectionPartitionReadOptions {
                max_concurrent_reads: 2,
            },
        )
        .await
        .expect("scheduler");

    let stream = scheduler.shared_pollable_stream_ref().await;
    let batches = stream.collect::<Vec<_>>().await;

    assert_eq!(batches.len(), 2);

    // Validate entry keys came back.
    let mut keys = Vec::new();
    for batch in batches {
        let batch = batch.expect("batch ok");
        let schema = batch.schema();
        let arrays = batch.arrays();

        for (field, nd_array) in schema.fields().iter().zip(arrays.iter()) {
            if field.name() == "__entry_key" {
                let arr = nd_array
                    .as_arrow_array()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("entry key array");
                keys.push(arr.value(0).to_string());
            }
        }
    }

    keys.sort();
    assert_eq!(keys, vec!["entry-1".to_string(), "entry-2".to_string()]);
}
