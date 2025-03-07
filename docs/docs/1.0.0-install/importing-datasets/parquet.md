# Parquet

Importing Parquet files is a simple process. The Beacon API allows you to import datasets in Parquet format. The datasets can be imported using the Beacon Swagger Interface or API calls.

```json
POST /admin/api/datasets/import-parquet
{
  "path": "./datasets/sst_dataset.parquet"
}
```

# Batch Importing Parquet Datasets

You can also batch import multiple Parquet datasets at once using a glob pattern. The datasets can be imported using the Beacon Swagger Interface or API calls.

```json
POST /admin/api/datasets/import-parquet-batch
{
  "path": "./datasets/*.parquet"
}
```