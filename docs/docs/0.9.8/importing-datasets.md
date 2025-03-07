
# Import Datasets

When you start importing datasets into Beacon, it will only need to know where to find the data parameter (eg. sea-surface-temperature) and unit (eg. celsius) for a column or variable. Once Beacon knows where to find this information, it will be able to import all of your datasets. To define where to find such information you will have to define an import driver for the specific data format of the datasets you want to import. This configuration of such a driver depends on the data format.

## Importing Datasets with Native NetCDF Drivers

::: info
If you are trying to import NetCDF Files that are not stored locally but on network mounted storage, it is recommended to use setup a [`Network Mounted NetCDF Driver`](#importing-network-mounted-netcdf-datasets) instead of a native netcdf driver.
:::

The native NetCDF driver in Beacon enables you to define the data parameter and unit using attributes of NetCDF variables using a JSON object. 
The JSON object defines  Follow these steps to import datasets using native NetCDF drivers:

An example native NetCDF driver that can import datasets for CDS SST satellite data (https://cds.climate.copernicus.eu/cdsapp#!/dataset/satellite-sea-surface-temperature?tab=overview):

#### NC Dump of a CDS SST dataset.

![nc dump of CDS sst file](https://beacon.maris.nl/grfx/sst_nc_dump_example.png)

Beacon requires only the green parts which are the attribute keys that define that data stored in that variable. The blue parts are then usuable as data parameters when querying beacon. When it is defined, Beacon will also use the same attribute key for the 'lat' and 'lon' variable.

#### Step 1: Define the Native NetCDF Driver

Create a JSON object that describes the name of the driver and the attribute keys of the NetCDF variables that define the parameter and unit. (using LZ4 compression is always recommended, can be disabled by replacing it with ```"none"``` )

```json
{
  "parameter_attribute_name": "standard_name",
  "unit_attribute_name": "units",
  "driver_name": "sea-surface-satellite",
  "compression" : "lz4"
}
```

#### Step 2: Post the Native NetCDF Driver JSON Object

Send a POST request to /admin/api/drivers/add-native-netcdf-driver with the JSON object as the request body.
This action registers the native NetCDF driver in Beacon, making it available for importing datasets.
You can find more information about the native NetCDF driver in the Beacon Swagger API reference (http://beacon-endpoint/admin/swagger/).

#### Step 3: Import the Dataset

Once the native NetCDF driver is registered, you can import datasets using the driver that you previously defined.
Use the Beacon Swagger Interface or API calls to import datasets by selecting the appropriate driver and providing the dataset file.
The dataset id must always be unique.
An example of a dataset imported using the sea-surface-satellite driver is shown below:

```json
{
  "path": "/datasets/sst_dataset.nc",
  "dataset_id": 1,
  "driver": "sea-surface-satellite"
}
```

You can also specify that Beacon will generate the dataset id for you by settings the dataset_id to `"incremental"`.

```json
{
  "path": "/datasets/sst_dataset.nc",
  "dataset_id": "incremental",
  "driver": "sea-surface-satellite"
}
```

## Importing Network Mounted NetCDF Datasets

If you have a dataset that is mounted on a network drive, you can import it using the network mounted NetCDF driver. You can define such driver using the endpoint `/admin/api/drivers/add-native-driver`.

The following example shows how to define a driver for a network mounted NetCDF dataset:

It has the following properties:

- `name`: The name of the driver.
- `compression`: The compression algorithm to use. This must be `lz4` or `none`.
- `kind` : Is an object that has the following properties:
  - `parameter_attribute_name`: The name of the attribute that contains the data parameter.
  - `unit_attribute_name`: The name of the attribute that contains the unit.
  - `type`: The type of driver. This must be `network_mounted_netcdf`.

```json
{
  "name": "sst-network-driver",
  "kind": {
    "parameter_attribute_name": "standard_name",
    "unit_attribute_name": "units",
    "type": "network_mounted_netcdf"
  },
  "compression": "lz4"
}
```
