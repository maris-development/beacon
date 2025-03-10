# Importing Arrow IPC Datasets into Beacon Binary Format

Importing arrow files is a simple process. The Beacon API allows you to import datasets in arrow ipc format. The datasets can be imported using the Beacon Swagger Interface or API calls.

```json
POST /admin/api/datasets/import-arrow
{
  "path": "./datasets/sst_dataset.arrow"
}
```
