# Importing NetCDF Datasets into Beacon Binary Format

The Beacon API allows you to import datasets in Beacon Binary Format straight from NetCDF files. The datasets can be imported using the Beacon Swagger Interface or API calls.

## Importing NetCDF Datasets

Use the Beacon Swagger Interface or API calls to import datasets by providing the dataset file.
An example of a dataset imported is shown below:

```http
POST /admin/api/datasets/import-netcdf
Content-Type: application/json
Authorization: Basic <base64-encoded-username-and-password>

{
  "bbf_path": "collection_name.bbf",
  "netcdf_paths": [
    "**/*.nc"
  ],
  "block_size": 1000 // Number of netcdf datasets to be processed in a single block
}
```
