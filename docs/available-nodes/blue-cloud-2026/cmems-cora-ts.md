# Beacon - Copernicus Marine Environment Monitoring Service (CMEMS) Cora Time Series

::: tip
Access to this Beacon node requires a token. Please contact [Paul (Maris Blue Cloud Developer)](mailto:paul@maris.nl) or [Robin (Maris Beacon Author/Developer)](mailto:robin@maris.nl) to request a token.
:::

The Copernicus Marine Environment Monitoring Service (CMEMS) CORA (CORA – In situ observations of Temperature and Salinity) collection provides comprehensive datasets of in-situ oceanographic observations. These datasets are crucial for understanding and monitoring the marine environment.

These data are collected from various sources such as Argo floats, CTDs (Conductivity, Temperature, Depth sensors), moorings, gliders, and other oceanographic platforms. The dataset includes long-term time series data essential for marine and climate studies.

::: warning
This node only provides access to the time series data. If you're interested in a combined node of profiles and timeseries, please contact us.
:::

## About the collection in Beacon

## Jupyter Notebooks

There are various notebooks available that demonstrate how to access and analyze the WOD data in Beacon. These notebooks are available in the [Beacon Blue Cloud Repository](https://github.com/maris-development/beacon-blue-cloud/tree/main/notebook-examples).

## API Queries

::: code-group

```python [temperature.py]
import requests
import json

url = "https://beacon-cora-ts.maris.nl/api/query"
token = "" # Add your token here

payload = json.dumps({
  "query_parameters": [
    {
      "column_name": "TEMP",
      "alias": "Temperature [degree_Celsius]"
    },
    {
      "column_name": "JULD",
      "alias": "Time [days since 1950-01-01 00:00:00 UTC]"
    },
    {
      "column_name": "DEPH",
      "alias": "Depth [meter]"
    },
    {
      "column_name": "LONGITUDE",
      "alias": "Longitude"
    },
    {
      "column_name": "LATITUDE",
      "alias": "Latitude"
    }
  ],
  "filters": [
    {
      "for_query_parameter": "Time [days since 1950-01-01 00:00:00 UTC]",
      "min": 21915,
      "max": 22280
    },
    {
      "for_query_parameter": "Depth [meter]",
      "min": 0,
      "max": 10
    },
    {
      "for_query_parameter": "Temperature [degree_Celsius]",
      "min": -2,
      "max": 38
    }
  ],
  "output": {
    "format": "csv"
  }
})
headers = {
  'Authorization': f'Bearer {token}',
  'Content-Type': 'application/json',
}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)

```

```bash [temperature.sh]

curl --location --request POST 'https://beacon-cora-ts.maris.nl/api/query' \
--header 'Authorization: Bearer YOUR_TOKEN' \
--header 'Content-Type: application/json' \
--data-raw '{
    "query_parameters": [
        {
            "column_name": "TEMP",
            "alias": "Temperature [degree_Celsius]"
        },
        {
            "column_name": "JULD",
            "alias": "Time [days since 1950-01-01 00:00:00 UTC]"
        },
        {
            "column_name": "DEPH",
            "alias": "Depth [meter]"
        },
        {
            "column_name": "LONGITUDE",
            "alias": "Longitude"
        },
        {
            "column_name": "LATITUDE",
            "alias": "Latitude"
        }
    ],
    "filters": [
        {
            "for_query_parameter": "Time [days since 1950-01-01 00:00:00 UTC]",
            "min": 21915,
            "max": 22280
        },
        {
            "for_query_parameter": "Depth [meter]",
            "min": 0,
            "max": 10
        },
        {
            "for_query_parameter": "Temperature [degree_Celsius]",
            "min": -2,
            "max": 38
        }
    ],
    "output": {
        "format": "csv"
    }
}'

```

:::