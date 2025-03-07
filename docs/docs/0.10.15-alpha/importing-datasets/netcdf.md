# Importing NetCDF Datasets

The Beacon API allows you to import datasets in NetCDF format. The datasets can be imported using the Beacon Swagger Interface or API calls. The datasets can be stored locally or on network mounted storage.

## Importing NetCDF Datasets:

::: info
If you are trying to import NetCDF Files that are not stored locally but on network mounted storage, it is recommended to use the network mounted endpoint. See [Network Mounted NetCDF Driver](#importing-network-mounted-netcdf-datasets) for more information.
:::


Use the Beacon Swagger Interface or API calls to import datasets by providing the dataset file.
The identifier must always be unique but is not required.
An example of a dataset imported is shown below:

```json
POST /admin/api/datasets/import-netcdf
{
  "path": "/datasets/sst_dataset.nc",
  "identifier": 1,
}
```

You can also specify that Beacon will generate the dataset id for you by settings the dataset_id to `"incremental"`.

```json
{
  "path": "/datasets/sst_dataset.nc",
  "identifier": "incremental",
}
```

## Importing Network Mounted NetCDF Datasets

When importing NetCDF files that are stored on network mounted storage, it is recommended to use a Network Mounted NetCDF Driver. This driver is similar to the native NetCDF driver but is specifically designed to handle datasets stored on network mounted storage.

```json
POST /admin/api/datasets/insert-network-mounted-netcdf
{
  "path": "/datasets/sst_dataset.nc",
  "identifier": "incremental",
}
```