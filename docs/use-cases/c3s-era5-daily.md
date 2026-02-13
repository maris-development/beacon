# ERA5 Daily (Zarr)

In this use case, ERA5 Daily data for 1950-2025 is retrieved from the C3S Climate Data Store, converted to Zarr, and published through Beacon.

This pipeline makes the collection directly queryable, so users can subset and download only the data they need without manual preprocessing.

The published dataset includes the following parameters:

- 2m temperature
- 2m temperature daily maximum
- 2m temperature daily minimum
- precipitation
- mean sea level pressure
- z500 geopotential

## Access

Beacon endpoint: <https://beacon-era5.maris.nl/>

Install the Python SDK:

```bash
pip install beacon-api
```

## Query example

The example below selects longitude, latitude, time, and temperature (`t2m`) for a spatial bounding box and a June 2024 time range, then returns the result as a pandas DataFrame.

```python
from beacon_api import *

client = Client("https://beacon-era5.maris.nl/")

daily_collection = client.list_tables()['daily_single_levels']

(
    daily_collection.query()
    .add_select_column('longitude')
    .add_select_column('latitude')
    .add_select_column('valid_time')
    .add_select_column('t2m')
    .add_bbox_filter('longitude','latitude', (30, 30, 40, 40)) # bounding box (lon_min, lat_min, lon_max, lat_max)
    .add_range_filter('valid_time', gt_eq="1950-06-01T00:00:00Z", lt_eq="2024-06-30T23:59:59Z")
    .to_pandas_dataframe()
)

```
