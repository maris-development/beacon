# Querying with JSON

## Sending an API request

Based on the programming language you are using, the POST request might be set-up differently. Below you can see examples for Python, cURL and JavaScript for a Beacon node that is set-up for the World Ocean Database in the context of the Blue-Cloud2026 project. This Beacon node is used internally in the project and therefore a token is required.

Depending on what kind of Beacon node you are requesting, you might thus need to provide a token in the request header. You will also need to specify a body in the request. In the section below you can find information on how this query body can be set-up.

::: code-group

```python [temperature.py]
url = "https://beacon-wod.maris.nl/api/query"
token = "" # Add your token here

headers = {
  'Authorization': f'Bearer {token}',
  'Content-Type': 'application/json',
}

response = requests.request("POST", url, headers=headers, data=body)

```

```sh [temperature.sh]
curl --location --request POST 'https://beacon-wod.maris.nl/api/query' \
--header 'Authorization: Bearer YOUR_TOKEN'
--header 'Content-Type: application/json' \
--data-raw body
```

```javascript [temperature.js]
var myHeaders = new Headers();
myHeaders.append("Authorization", "Bearer YOUR_TOKEN");
myHeaders.append("Content-Type", "application/json");

var requestOptions = {
  method: 'POST',
  headers: myHeaders,
  body: raw,
  redirect: 'follow'
};

fetch("https://beacon-wod.maris.nl/api/query", requestOptions)
  .then(response => response.text())
  .then(result => console.log(result))
  .catch(error => console.log('error', error));
```

:::

## Constructing the API query body

The query body can be set-up in JSON and must consist of the following three components:

* Query Parameters : This decided the (meta)data parameters (columns in the output file) that you want to query including some optional fields for the resulting output of that data parameter.
* Filters : This enables you to apply filters on the previously defined query parameters. Examples include min-max ranges for parameters, or even more powerful ones such as polygon filtering.
* Distinct (optionally): To query distinct values for a specific column or column combinations.
* Output : Here you specify the output format of the query.

In the sections below, each of these elements will be highlighted and details will be given on how to formulate these.

```json
{
    "query_parameters": [
        {
            "column_name": "query_parameter1",
            "alias": "query_parameter1_alias"
        }
    ],
    "filters": [
        {
            "for_query_parameter": "",
            "min": ,
            "max": 
        },
    ],
    "distinct": {
        "columns": [
            "query_parameter1_alias", 
            "query_parameter2_alias"
        ]
    },
    "output": {
      "format": "csv"
    }
}

```

### Query parameters

In this part of the body, you include the column names that you want to request and write to your output file. The columns can be of type "data" as well as "metadata". If you would like for example temperature, depth and time, you would create a query parameter for each of these. 

One single query parameter has the following schema:

```json
{
    "column_name": "sea_surface_temperature", // The data parameter you would like to query
    "alias": "temperature", // Create an alias for the output column name
}
```

#### Column names

In order to retrieve the available column names you can send a GET request to the `available-columns` endpoint. Below you can find examples for Python, cURL and JavaScript.

::: code-group

```python [temperature.py]
responseinfo = requests.get("https://beacon-{collection-name}.maris.nl/api/query/available-columns", headers = {"Authorization" : f"Bearer {Token}"}) 
params = responseinfo.json() 
```

```sh [temperature.sh]
curl --location --request GET 'https://beacon-{collection-name}.maris.nl/api/query/available-columns' \
--header "Authorization: Bearer YOUR_TOKEN"
```

:::

#### Alias

You need to select an alias for the column name for the output response. This can be the same name as the original column name.

### From

Beacon can host multiple collections and datasets at once. When no 'from' is specified, the default collection will be used. If you want to specify a different collection, you can do so by adding the `from` field to the query.

```json
{
    "from": "collection_name"
}     
```

### Filters

In order to specify the relevant filters, you need to select the parameter aliases that you want to filter on (under `for query parameter`).

Within Beacon we currently have four types of filters.

* Min, Max filter (numbers and strings)
* Eq filter (numbers and strings)
* Geo polygon filtering
* Time filtering

#### Filter Null (Fill Values)

This is relevant for data filters where you want to filter out rows that have fill values in this column. This is useful if you want to filter out rows that have missing data in this column.

```json
{
    "is_not_null": {
        "for_query_parameter": "{query_parameter_alias}"
    }
}
```

#### Min-max

This is relevant for data filters where you need to specify a minimum and maximum value, or for data filters where you need to specify a spatial boundary in the form of a bounding box (using longitude min/max and latitude min/max).

```json
{
    "for_query_parameter": "{query_parameter_alias}",
    "min": 0,
    "max": 5
}
```

```json
{
    "for_query_parameter": "{query_parameter_alias}",
    "min": "STRING_A",
    "max": "ZZZZZZZZZZZ"
}
```

#### Eq filter

If the column that you want to query on contains strings you will need to provide the filter as shown in the example below.

```json
{
    "for_query_parameter": "query_parameter_alias",
    "eq": 5
}
```

```json
{
    "for_query_parameter": "query_parameter_alias",
    "eq": "string"
}
```

#### Not eq filter

If the column that you want to query on contains strings you will need to provide the filter as shown in the example below.

```json
{
    "for_query_parameter": "query_parameter_alias",
    "neq": 5
}
```

```json
{
    "for_query_parameter": "query_parameter_alias",
    "neq": "string"
}
```

#### Geo polygon

This is relevant for data filters where you need to specify a spatial boundary in the form of a polygon.

```json
{
    "longitude_query_parameter": "{query_parameter_alias}",
    "latitude_query_parameter": "{query_parameter_alias}",
    "geometry": {
        "coordinates": [
            [
                [
                    -5.848324434183297,
                    35.27053808026095
                ],
                [
                    -2.549403432154577,
                    34.40213766838207
                ],
                ... //can be more coordinates added
            ]
        ],
        "type": "Polygon"
    }
}
```

#### Time filtering

In the example below you can see how you can query on a "time" column. This requires you to use the ISO8601 format. The time filter can be used to filter on a specific time range.

```json
{
    "for_query_parameter": "datetime_query_parameter_alias",
    "min": "2000-00-00T00:00:00",
    "max": "2001-00-00T00:00:00",
}
```

#### Combining filters using AND / OR

You can combine filters using the `and` and `or` keywords. This allows you to create more complex queries.

```json
{
    "and": [
        {
            "for_query_parameter": "query_parameter_alias",
            "min": 0,
            "max": 5
        },
        {
            "for_query_parameter": "query_parameter_alias",
            "eq": "string"
        }
    ]
}
```

```json
{
    "or": [
        {
            "for_query_parameter": "query_parameter_alias",
            "min": 0,
            "max": 5
        },
        {
            "for_query_parameter": "query_parameter_alias",
            "eq": "string"
        }
    ]
}
```

### Distinct

It is possible to query distinct values for a specific column or column combinations. This can be useful for example to get a list of all unique values for a specific column.
Because it accepts an array of columns, it is also possible to get a list of unique values for a combination of columns.

```json
    "distinct": {
        "columns": [
            "query_parameter1_alias", "query_parameter2_alias"
        ]
    },
```

In the full query it would look the following way:

```json{12-16}
{
    "query_parameters": [
        {
            "column_name": "PLATFORM",
            "alias": "Platform_code"
        },
        ...
    ],
    "filters": [
        ...
    ],
    "distinct": {
        "columns": [
            "Platform_code",
        ]
    },
    "output": {
        ...
    }
}
```

### Output format

Here you select the output format and choose between CSV, netCDF, ODV, ..

Some output formats might have different options due to them having more features such as internal compression or file layout.

We have the following output formats:

#### NetCDF

```json
{
    "format": "netcdf"
}
```

#### ODV

Creating an ODV file requires you to specify the format as shown below. The ODV format requires you to specify the
following columns: `longitude_column`, `latitude_column`, `timestamp_column`, `depth_column` and `data_columns`.
In the `data_columns` array you can specify multiple data columns that you want to include in the ODV file.
For the data columns it is possible to specify a `qf_column` that will be used as the quality flag for the data column.

You can also specify the `metadata_columns` array to include additional metadata columns in the ODV file.

The `column_name` field in the `output` should be a reference name to the column you have selected in the `query_parameters` section or the alias of that query parameter.
The `key_column` is used to identify unique profiles/timeseries or trajectories in the ODV file. This is used to determine whether a dataset is a profile or a time series.

```json
{
    "query_parameters": [
        {
            "column_name": "TIME",
            "alias": "time_ISO8601 [yyyy-MM-ddTHH:mm:ss.SSS]"
        },
        {
            "column_name": "LONGITUDE",
        },
        {
            "column_name": "DEPTH",
            "alias": "Depth [m]"
        },
        ...
    ],
    "filters": [
        ...
    ],
    "output": {
        "format": {
            "odv": {
                "longitude_column": {"column_name": "LONGITUDE"},
                "latitude_column": {"column_name": "LATITUDE"},
                "time_column": {"column_name": "time_ISO8601 [yyyy-MM-ddTHH:mm:ss.SSS]"},
                "depth_column": {
                    "column_name": "Depth [m]",
                    "qf_column": "DEPTH_QC",
                },
                "data_columns": [
                    {
                        "column_name": "TEMPERATURE",
                        "qf_column": "TEMPERATURE_QC",
                    },
                    {
                        "column_name": "SALINITY",
                        "qf_column": "SALINITY_QC",
                    },
                ],
                "metadata_columns": [
                    {
                        "column_name": "TEMPERATURE_P01",
                    },
                    {
                        "column_name": "TEMPERATURE_P06",
                    },
                    {
                        "column_name": "TEMPERATURE_L05",
                    },
                    {
                        "column_name": "TEMPERATURE_L22",
                    },
                    {
                        "column_name": "TEMPERATURE_L35",
                    },
                    {
                        "column_name": "SALINITY_P01",
                    },
                    {
                        "column_name": "SALINITY_P06",
                    },
                    {
                        "column_name": "SALINITY_L05",
                    },
                    {
                        "column_name": "SALINITY_L22",
                    },
                    {
                        "column_name": "SALINITY_L35",
                    },
                    {
                        "column_name": "DEPTH_P01",
                    },
                    {
                        "column_name": "DEPTH_P06",
                    },
                    {
                        "column_name": "PLATFORM_L06",
                    },
                    {
                        "column_name": "SOURCE_BDI",
                    },
                    {
                        "column_name": "SOURCE_BDI_DATASET_ID",
                    },
                ],
                "qf_schema": "SEADATANET",
                "key_column": "COMMON_ODV_TAG",
                # With this line we are asking the zip file to be gzip compressed
                "archiving": "zip_deflate",
            }
        }
    }
}
```

#### GeoJSON

```json
{
    "format": {
        "geojson" : {
            "longitude_column" : "{query_parameter_alias}",
            "latitude_column" : "{query_parameter_alias}"
        }
    },
}
```

#### CSV

```json
{
    "format": "csv"
}
```

#### Parquet

```json
{
    "format": "parquet"
}
```

#### Arrow IPC

```json
{
    "format": "ipc"
}
```
