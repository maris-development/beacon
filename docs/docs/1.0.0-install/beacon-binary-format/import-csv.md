# Importing CSV into Beacon Binary Format

Importing a CSV dataset is a simple process. The Beacon API allows you to import datasets in CSV format. The datasets can be imported using the Beacon Swagger Interface or API calls.

```json
POST /admin/api/datasets/import-csv
{
  "path": "./datasets/sst_dataset.csv"
}
```

## Batch Importing CSV Datasets

You can also batch import multiple CSV datasets at once using a glob pattern. The datasets can be imported using the Beacon Swagger Interface or API calls.

```json
POST /admin/api/datasets/import-csv-batch
{
  "path": "./datasets/*.csv"
}
```
