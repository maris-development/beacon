
# Querying

In this chapter, we will explore how to query data from Beacon using a JSON query object. Beacon provides a flexible and powerful query interface that allows you to retrieve specific data based on your requirements. By constructing a JSON query object and sending a POST request to the query endpoint at `http://beacon-endpoint/api/query` , you can retrieve data that matches your specified parameters, units, filters, and other criteria.

## Example JSON Query Object for querying sea surface temperature data from CDS SST satellite data:

```json
{
    "query_parameters": [
        {
            "data_parameter": "sea_surface_temperature",
            "unit": "kelvin",
            "skip_fill_values": false,
        },
        {
            "data_parameter": "time",
            "unit": "seconds since 1981-01-01 00:00:00",
            "alias": "TEMPORAL"
        },
        {
            "data_parameter": "longitude",
            "unit": "degree_east",
        },
        {
            "data_parameter": "latitude",
            "unit": "degree_north",
        }
    ],
    "filters": [
        {
            "for_query_parameter": {
                "alias": "TEMPORAL"
            },
            "min": 940377600,
            "max": 1003536000
        }
    ],
    "output": {
        "format": "netcdf"
    }
}
```

Here we can see the example for querying sea surface temperature data from the CDS SST satellite data collection we imported previously in the 'Importing Datasets' chapter.

A query consists of 3 components.
* Query Parameters : Which data parameters in combination with unit you would like to query and additional options for the resulting output of that data parameter.
* Filters : Allow for subsetting on the previously defined query parameters. Such as min max ranges you would like to to certain query parameters. Or even more powerful ones such as Geo Polygon filtering.
* Output : Specifying the output format of the query.


## Query Parameters

Query Parameters define what you would like to query out of the data lake. For example, if you would like temperature, depth and time, you would create a query parameter for each of these. Its important to remember that a query parameter always needs a `data_parameter` which defines what type of data you would like to query and a `unit` which defines for which unit to search for. For example `data_parameter` temperature with unit `celsius` would make Beacon find all of the data that is temperature and is stored in celsius.  

A query parameter has the following schema: 

```json
{
    "data_parameter": "temperature", // The data parameter you would like to query
    "unit": "celsius", // The unit which you would like to query
    "skip_fill_values": true, // Whether to skipp fill values in the output result 
    "alias": "temperature_in_celsius" // Create an alias for the output column name
}
```

### Unit Conversions

### Dynamic Aggregate Parameters

Sometimes, the datasets inserted into Beacon might contain different data parameters because the original datasets might define for example depth using 2 different parameters. One data provider might use the parameter code `"depth_parameter_code_1"` while the second data provider might use `"depth_parameter_code_2"` while they essentially both mean the same data. This is where dynamic aggregate parameters can help. They can aggregate these 2 parameters as one single parameter on output. It allows Beacon to search for both these parameter codes as the depth parameter. This means you will get all the data for `"depth_parameter_code_1"` and `"depth_parameter_code_2"` in one single harmonized column.

```json
{
    "data_parameter": {
        "aggregate_parameter": "aggregated_depth_parameter",
        "sub_parameters": [
            "depth_parameter_code_1",
            "depth_parameter_code_2"
        ]
    },
    "unit": "meter",
    "skip_fill_values": false,
    "alias": "DEPTH"
}
```

## Query Filters

Within Beacon we currently have 3 types of filters.
* Min, Max Range filter
* Geo Polygon Filtering
* Time Filtering
* Metadata Filtering

### Min Max Range Filtering

```json
{
    "for_query_parameter": {
        "alias": "{QUERY_PARAMETER_ALIAS}" // E.g. "alias" : "DEPTH"
    },
    "min": 0,
    "max": 5
}
```


### Geo Polygon Filtering


```json
{
    "longitude_query_parameter": {
        "alias": "{LONGITUDE_QUERY_PARAMETER_ALIAS}"
    },
    "latitude_query_parameter": {
        "alias": "{LATITUDE_QUERY_PARAMETER_ALIAS}"
    },
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
                ...
            ]
        ],
        "type": "Polygon"
    }
}
```

### Time Filtering

ToDo!

### Metadata Filtering

With Beacon you will be able to filter on both metadata of a column and the global metadata of a dataset. (e.g. NetCDF Attributes for variables and global)



```json
{
    "datasets_contains": [
        {
            "key": "platform_id",
            "value": "plf_1001"
        }
    ]
}
```
Values can be either of type `string`, `[string]` or `[number]`. If you want to match on a single number, use an array with 1 single number in it.

```json
{
    "datasets_contains": [
        {
            "key": "platform_id",
            "value": [1001]
        }
    ]
}
```

## Output

Within Beacon we support various output formats. Some output format might have different options due to them having more features such as internal compression or file layout. 

All output formats also include the ability to be compressed on output by adding `"compression" : "{compression_format}"`
We currently support the following compression formats (`zstd` is recommended): 
* zstd : `"compression" : "zstd"`
* gzip : `"compression" : "gzip"`
* biz2 : `"compression" : "biz2"`

We have the following output formats:

### NetCDF

```json
{
    "format": "netcdf",
    "compression" : "zstd" // This line is optional if you want the output compressed using zstd
}
```

### GeoJSON

```json
{
    "format": {
        "geojson" : {
            "longitude_column" : "{LONGITUDE_QUERY_PARAMETER_ALIAS}",
            "latitude_column" : "{LATITUDE_QUERY_PARAMETER_ALIAS}"
        }
    },
}
```

### CSV

```json
{
    "format": "csv",
}
```

### Parquet

```json
{
    "format": "parquet",
}
```

### Arrow IPC

```json
{
    "format": "ipc",
}
```