# ODV

Importing datasets from Ocean Data View (ODV) is a two-step process. First, you need to export the dataset from ODV to an ASCII text file. Then, you can import the dataset into Beacon using the Beacon Swagger Interface or API calls.

```json
POST /admin/api/datasets/import-odv
{
  "path": "./datasets/odv.txt"
}
```