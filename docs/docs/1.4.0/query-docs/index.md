# Querying

In this section, we will explore how you can query data from Beacon. Beacon provides a flexible and powerful query interface that allows you to retrieve specific data based on your requirements. By constructing a JSON query object and sending a POST request to the query endpoint (e.g. `https://beacon-{collection-name}.maris.nl/api/query`), you can retrieve data that matches your specified column_names, filters, and other criteria.

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
            "column_name": "",
            "alias": "",
            "optional": true,
            "skip_fill_values": true,
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
    "optional": true, // If the column is optional or not, if it is optional it is treated as an "OR" gate
    "skip_fill_values": true, //Skips any rows that have fill values in this column
}
```

#### Column name
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

#### Optional
You can select whether you want to the column to be optional or not, by setting it to True or False. If the column is listed as optional, it is treated as an "OR" gate, this means that the rows of data that you will retrieve do not have to contain a value or string for this column. In the case that there is no value or string given for a column, but other columns do include a value/string the column cell would be empty.

#### Skip fill values

If you want to skip any rows that have fill values in this column, you can set this to true. This is useful if you want to filter out rows that have missing data in this column.


### Filters
In order to specify the relevant filters, you need to select the parameter aliases that you want to filter on (under `for query parameter`).

Within Beacon we currently have four types of filters.
* Min, Max filter (numbers and strings)
* Eq filter (numbers and strings)
* Geo polygon filtering
* Time filtering

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
In the example below you can see how you can query on a "time" column. This requires however, that the column can be converted by Beacon into the standard datetime format.

```json
{
    "for_query_parameter": "datetime_query_parameter_alias",
    "min": "2000-00-00T00:00:00",
    "max": "2001-00-00T00:00:00",
    "cast": "timestamp"
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

All output formats also include the ability to be compressed on output by adding `"compression" : "{compression_format}"`
We currently support the following compression formats (`zstd` is recommended):

* zstd : `"compression" : "zstd"`
* gzip : `"compression" : "gzip"`
* biz2 : `"compression" : "biz2"`

We have the following output formats:

#### NetCDF

```json
{
    "format": "netcdf",
    "compression" : "zstd" // This line is optional if you want the output compressed using zstd
}
```

#### ODV

Creating an ODV file requires you to specify the format as shown below. The ODV format requires you to specify the 
following columns: `longitude_column`, `latitude_column`, `timestamp_column`, `depth_column` and `data_columns`.
In the `data_columns` array you can specify multiple data columns that you want to include in the ODV file.
For the data columns it is possible to specify a `qf_column` that will be used as the quality flag for the data column.

You can also specify the `metadata_columns` array to include additional metadata columns in the ODV file.

```json
{
"output": {
        "format": {
            "odv": {
                "longitude_column": {
                    "column_name": "Longitude [degrees_east]"
                },
                "latitude_column": {
                    "column_name": "Latitude [degrees_north]"
                },
                "timestamp_column": {
                    "data_column_name": "iso_timestamp",
                    "comment": "_"
                },
                "depth_column": {
                    "data_column_name": "Depth [m]",
                    "comment": "Codes: SDN:P01::ADEPZZ01 SDN:P06::ULAA"
                },
                "data_columns": [
                    {
                        "data_column_name": "Temperature [celsius]",
                        "comment": "Codes: SDN:P01::TEMPPR01 SDN:P06::UPAA",
                        "qf_column": "Temperature_qc"
                    },
                    {
                        "data_column_name": "Time [days since -4713-01-01T00:00:00Z]",
                        "comment": "Codes: SDN:P01::CJDY1101 SDN:P06::UTAA"
                    }
                ],
                "metadata_columns": [
                    {
                        "column_name": "EDMO_CODE"
                    },
                    {
                        "column_name": "LOCAL_CDI_ID"
                    }
                ],
                "qf_schema": "SEADATANET"
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

## Union queries

A union query is a query that combines the results of two or more queries into a single result set. This can be useful when you want to combine data from multiple sources or when you want to combine data from the same source but with different filters or query parameters.

To create a union query, you need to specify the queries that you want to combine in the `queries` field of the query body. Each query should have its own `query_parameters`, `filters`, and `output` fields.

```json
    {
        "union": [
            {
                "query_parameters": [
                    {
                        "column_name": "Temperature",
                        "alias": "Temperature"
                    },
                ],
                "filters": []
            },
            {
                "query_parameters": [
                    {
                        "column_name": "Salinity",
                        "alias": "Salinity"
                    },
                ],
                "filters": []
            }
        ],
        "output": {
            "format": "csv"
        }
    }
```
