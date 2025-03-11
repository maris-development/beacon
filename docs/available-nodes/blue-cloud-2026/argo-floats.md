# Beacon - Argo Floats

::: tip
Access to this Beacon node requires a token. Please contact [Paul (Maris Blue Cloud Developer)](mailto:paul@maris.nl) or [Robin (Maris Beacon Author/Developer)](mailto:robin@maris.nl) to request a token.
:::

Argo floats are autonomous instruments used to collect data on the temperature, salinity, and pressure of the ocean. They drift with ocean currents and periodically change their buoyancy to move vertically through the water column, typically profiling from the surface down to 2,000 meters. After completing a profile, the floats surface and transmit their data to satellites, which then relay the information to researchers.

Each Argo float is equipped with sensors to measure key oceanographic parameters. The floats are designed to operate for several years, providing continuous data over time. The Argo program began in 2000 and has since deployed thousands of floats across the world's oceans. This extensive network of floats provides a detailed and consistent dataset, which is invaluable for understanding ocean circulation, climate variability, and marine environments.

## About the collection in Beacon

The Argo Float collection contains data from over 20,000 floats deployed worldwide. The data includes profiles of temperature, salinity, and pressure, as well as other parameters such as oxygen and chlorophyll. In total, this collection contains over 3.5 million profiles and billions of observations.

Here is an example subset of the temperature data from the Argo Float collection:

[`Jupyter Notebook`](https://github.com/maris-development/beacon-blue-cloud/blob/main/notebook-examples/Argo2%20-%20Demo%20Notebook%20-%20Bounding%20Box.ipynb)

## Jupyter Notebooks

There are various notebooks available that demonstrate how to access and analyze the Argo Float data in Beacon. These notebooks are available in the [Beacon Blue Cloud Repository](https://github.com/maris-development/beacon-blue-cloud/tree/main/notebook-examples).

## API Queries

::: code-group

```python [temperature.py]
import requests
import json

url = "https://beacon-argo.maris.nl/api/query"
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
      "column_name": "PRES",
      "alias": "Pressure [decibar]"
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
      "for_query_parameter": "Pressure [decibar]",
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

curl --location --request POST 'https://beacon-argo.maris.nl/api/query' \
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
            "column_name": "PRES",
            "alias": "Pressure [decibar]"
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
            "for_query_parameter": "Pressure [decibar]",
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
