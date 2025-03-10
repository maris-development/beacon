# Importing NetCDF Datasets into Beacon Binary Format

The Beacon API allows you to import datasets in NetCDF format. The datasets can be imported using the Beacon Swagger Interface or API calls.

## Importing NetCDF Datasets

Use the Beacon Swagger Interface or API calls to import datasets by providing the dataset file.
An example of a dataset imported is shown below:

```json
POST /admin/api/datasets/import-netcdf
{
  "path": "/datasets/sst_dataset.nc"
}
```

## Batch Importing NetCDF Datasets

You can also batch import multiple NetCDF datasets at once using a glob pattern. The datasets can be imported using the Beacon Swagger Interface or API calls.

```json
POST /admin/api/datasets/import-netcdf-batch
{
  "path": "/datasets/*.nc",
}
```
