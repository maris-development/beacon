# Beacon - World Ocean Database

::: tip
Remember that in order to get access to a Beacon node you will require a personal token. You can get yourself access via the [`Homepage`](https://beacon.maris.nl/).
:::

## About the collection in Beacon
The World Ocean Database (WOD) is a comprehensive collection of oceanographic data developed and maintained by the National Oceanographic Data Center (NODC) and the World Data Service for Oceanography. It provides a globally integrated and consistent dataset of oceanographic parameters, including temperature, salinity, oxygen, nutrients, and plankton data, among others. The data is collected from a variety of sources, such as research vessels, autonomous floats, and fixed ocean stations, covering the world's oceans from the surface to the deep sea.

The original collection is available in various formats, including NetCDF, CSV, and ASCII, and was downloaded from the [World Ocean Database](https://www.ncei.noaa.gov/products/world-ocean-database) website. The downloaded data was then stored in Beacon's data lake, where it is available for extraction/subsetting.

The WOD contains around 18 million oceanographic profiles, spanning from the late 18th century to the present day.

## Jupyter notebooks & curl script examples

There is a notebook and a curl script example available that give practical examples on how to access and analyze the WOD data with Beacon. These examples are available in the [Beacon-GitHub](https://github.com/maris-development/beacon-blue-cloud).

Below you can see an example map-plot of a subset of the temperature data between 0-5 meters depth from the WOD collection between 2000 and 2001 produced with the example [Jupyter Notebook](https://github.com/maris-development/beacon-blue-cloud/blob/main/notebook-examples/WOD%20-%20Demo%20Notebook%20-%20Bounding%20Box.ipynb):

![WOD Temperature 2000 - 2001](/wod-temp-example.png)

## API Query examples
Hier komt een uitleg over de voorbeelden die hier worden laten zien, richting python, curl scripts, bounding boxes, polygons, distinct queries, union queries, netcdf, odv etc.

### Temperature Query CSV - Bounding Box

::: code-group

```python [temperature.py]
import requests
import json

url = "https://beacon-wod.maris.nl/api/query"
token = "" # Add your token here

payload = json.dumps({
  "query_parameters": [
    {
      "column_name": "Temperature",
      "alias": "Temperature [degree_C]",
      "optional": False
    },
    {
      "column_name": "time",
      "alias": "Time [days since 1770-01-01 00:00:00 UTC]",
      "optional": False
    },
    {
      "column_name": "z",
      "alias": "Depth [meter]",
      "optional": False
    },
    {
      "column_name": "lon",
      "alias": "Latitude",
      "optional": False
    },
    {
      "column_name": "lat",
      "alias": "Longitude",
      "optional": False
    }
  ],
  "filters": [
    {
      "for_query_parameter": "Time [days since 1770-01-01 00:00:00 UTC]",
      "min": 81485,
      "max": 81744
    },
    {
      "for_query_parameter": "Depth [meter]",
      "min": 0,
      "max": 5
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


```sh [temperature.sh]
curl --location --request POST 'https://beacon-wod.maris.nl/api/query' \
--header 'Authorization: Bearer YOUR_TOKEN'
--header 'Content-Type: application/json' \
--data-raw '{
    "query_parameters": [
        {
            "column_name": "Temperature",
            "alias": "Temperature [degree_C]",
            "optional": false
        },
        {
            "column_name": "time",
            "alias": "Time [days since 1770-01-01 00:00:00 UTC]",
            "optional": false
        },
        {
            "column_name": "z",
            "alias": "Depth [meter]",
            "optional": false
        },
        {
            "column_name": "lon",
            "alias": "Latitude",
            "optional": false
        },
        {
            "column_name": "lat",
            "alias": "Longitude",
            "optional": false
        }
    ],
    "filters": [
        {
            "for_query_parameter": "Time [days since 1770-01-01 00:00:00 UTC]",
            "min": 81485,
            "max": 81744
        },
        {
            "for_query_parameter": "Depth [meter]",
            "min": 0,
            "max": 5
        }
    ],
    "output": {
        "format": "csv"
    }
}'
```

```javascript [temperature.js]
var myHeaders = new Headers();
myHeaders.append("Authorization", "Bearer YOUR_TOKEN");
myHeaders.append("Content-Type", "application/json");

var raw = JSON.stringify({
  "query_parameters": [
    {
      "column_name": "Temperature",
      "alias": "Temperature [degree_C]",
      "optional": false
    },
    {
      "column_name": "time",
      "alias": "Time [days since 1770-01-01 00:00:00 UTC]",
      "optional": false
    },
    {
      "column_name": "z",
      "alias": "Depth [meter]",
      "optional": false
    },
    {
      "column_name": "lon",
      "alias": "Latitude",
      "optional": false
    },
    {
      "column_name": "lat",
      "alias": "Longitude",
      "optional": false
    }
  ],
  "filters": [
    {
      "for_query_parameter": "Time [days since 1770-01-01 00:00:00 UTC]",
      "min": 81485,
      "max": 81744
    },
    {
      "for_query_parameter": "Depth [meter]",
      "min": 0,
      "max": 5
    }
  ],
  "output": {
    "format": "csv"
  }
});

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

### Temperature Query NetCDF - Polygon

::: code-group

```python [temperature.py]
import requests
import json

url = "https://beacon-wod.maris.nl/api/query"
token = "" # Add your token here

payload = json.dumps({
  "query_parameters": [
    {
      "column_name": "Temperature",
      "alias": "Temperature [degree_C]",
      "optional": False
    },
    {
      "column_name": "time",
      "alias": "Time [days since 1770-01-01 00:00:00 UTC]",
      "optional": False
    },
    {
      "column_name": "z",
      "alias": "Depth [meter]",
      "optional": False
    },
    {
      "column_name": "lon",
      "alias": "Latitude",
      "optional": False
    },
    {
      "column_name": "lat",
      "alias": "Longitude",
      "optional": False
    }
  ],
  "filters": [
    {
      "for_query_parameter": "Time [days since 1770-01-01 00:00:00 UTC]",
      "min": 81485,
      "max": 81744
    },
    {
      "for_query_parameter": "Depth [meter]",
      "min": 0,
      "max": 5
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


```sh [temperature.sh]
curl --location --request POST 'https://beacon-wod.maris.nl/api/query' \
--header 'Authorization: Bearer YOUR_TOKEN'
--header 'Content-Type: application/json' \
--data-raw '{
    "query_parameters": [
        {
            "column_name": "Temperature",
            "alias": "Temperature [degree_C]",
            "optional": false
        },
        {
            "column_name": "time",
            "alias": "Time [days since 1770-01-01 00:00:00 UTC]",
            "optional": false
        },
        {
            "column_name": "z",
            "alias": "Depth [meter]",
            "optional": false
        },
        {
            "column_name": "lon",
            "alias": "Latitude",
            "optional": false
        },
        {
            "column_name": "lat",
            "alias": "Longitude",
            "optional": false
        }
    ],
    "filters": [
        {
            "for_query_parameter": "Time [days since 1770-01-01 00:00:00 UTC]",
            "min": 81485,
            "max": 81744
        },
        {
            "for_query_parameter": "Depth [meter]",
            "min": 0,
            "max": 5
        }
    ],
    "output": {
        "format": "csv"
    }
}'
```

```javascript [temperature.js]
var myHeaders = new Headers();
myHeaders.append("Authorization", "Bearer YOUR_TOKEN");
myHeaders.append("Content-Type", "application/json");

var raw = JSON.stringify({
  "query_parameters": [
    {
      "column_name": "Temperature",
      "alias": "Temperature [degree_C]",
      "optional": false
    },
    {
      "column_name": "time",
      "alias": "Time [days since 1770-01-01 00:00:00 UTC]",
      "optional": false
    },
    {
      "column_name": "z",
      "alias": "Depth [meter]",
      "optional": false
    },
    {
      "column_name": "lon",
      "alias": "Latitude",
      "optional": false
    },
    {
      "column_name": "lat",
      "alias": "Longitude",
      "optional": false
    }
  ],
  "filters": [
    {
      "for_query_parameter": "Time [days since 1770-01-01 00:00:00 UTC]",
      "min": 81485,
      "max": 81744
    },
    {
      "for_query_parameter": "Depth [meter]",
      "min": 0,
      "max": 5
    }
  ],
  "output": {
    "format": "csv"
  }
});

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

### Temperature Query ODV - Bounding Box

::: code-group

```python [temperature.py]
import requests
import json

url = "https://beacon-wod.maris.nl/api/query"
token = "" # Add your token here

payload = json.dumps({
  "query_parameters": [
    {
      "column_name": "Temperature",
      "alias": "Temperature [degree_C]",
      "optional": False
    },
    {
      "column_name": "time",
      "alias": "Time [days since 1770-01-01 00:00:00 UTC]",
      "optional": False
    },
    {
      "column_name": "z",
      "alias": "Depth [meter]",
      "optional": False
    },
    {
      "column_name": "lon",
      "alias": "Latitude",
      "optional": False
    },
    {
      "column_name": "lat",
      "alias": "Longitude",
      "optional": False
    }
  ],
  "filters": [
    {
      "for_query_parameter": "Time [days since 1770-01-01 00:00:00 UTC]",
      "min": 81485,
      "max": 81744
    },
    {
      "for_query_parameter": "Depth [meter]",
      "min": 0,
      "max": 5
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


```sh [temperature.sh]
curl --location --request POST 'https://beacon-wod.maris.nl/api/query' \
--header 'Authorization: Bearer YOUR_TOKEN'
--header 'Content-Type: application/json' \
--data-raw '{
    "query_parameters": [
        {
            "column_name": "Temperature",
            "alias": "Temperature [degree_C]",
            "optional": false
        },
        {
            "column_name": "time",
            "alias": "Time [days since 1770-01-01 00:00:00 UTC]",
            "optional": false
        },
        {
            "column_name": "z",
            "alias": "Depth [meter]",
            "optional": false
        },
        {
            "column_name": "lon",
            "alias": "Latitude",
            "optional": false
        },
        {
            "column_name": "lat",
            "alias": "Longitude",
            "optional": false
        }
    ],
    "filters": [
        {
            "for_query_parameter": "Time [days since 1770-01-01 00:00:00 UTC]",
            "min": 81485,
            "max": 81744
        },
        {
            "for_query_parameter": "Depth [meter]",
            "min": 0,
            "max": 5
        }
    ],
    "output": {
        "format": "csv"
    }
}'
```

```javascript [temperature.js]
var myHeaders = new Headers();
myHeaders.append("Authorization", "Bearer YOUR_TOKEN");
myHeaders.append("Content-Type", "application/json");

var raw = JSON.stringify({
  "query_parameters": [
    {
      "column_name": "Temperature",
      "alias": "Temperature [degree_C]",
      "optional": false
    },
    {
      "column_name": "time",
      "alias": "Time [days since 1770-01-01 00:00:00 UTC]",
      "optional": false
    },
    {
      "column_name": "z",
      "alias": "Depth [meter]",
      "optional": false
    },
    {
      "column_name": "lon",
      "alias": "Latitude",
      "optional": false
    },
    {
      "column_name": "lat",
      "alias": "Longitude",
      "optional": false
    }
  ],
  "filters": [
    {
      "for_query_parameter": "Time [days since 1770-01-01 00:00:00 UTC]",
      "min": 81485,
      "max": 81744
    },
    {
      "for_query_parameter": "Depth [meter]",
      "min": 0,
      "max": 5
    }
  ],
  "output": {
    "format": "csv"
  }
});

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


### Unique/Distinct Query

::: code-group

```python [distinct.py]{25-29}
import requests
import json

url = "https://beacon-wod.maris.nl/api/query"
token = "" # Add your token here

payload = json.dumps({
  "query_parameters": [
    {
      "column_name": "Platform",
      "alias": "Platform"
    },
    {
      "column_name": "time",
      "alias": "Time"
    }
  ],
  "filters": [
    {
      "for_query_parameter": "Time",
      "min": 81485,
      "max": 81744
    }
  ],
  "distinct": {
    "columns": [
      "Platform"
    ]
  },
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

```sh [distinct.sh] {22-26}
curl --location --request POST 'https://beacon-wod.maris.nl/api/query' \
--header 'Authorization: Bearer YOUR_TOKEN' \
--header 'Content-Type: application/json' \
--data-raw '{
    "query_parameters": [
        {
            "column_name": "Platform",
            "alias": "Platform"
        },
        {
            "column_name": "time",
            "alias": "Time"
        }
    ],
    "filters": [
        {
            "for_query_parameter": "Time",
            "min": 81485,
            "max": 81744
        }
    ],
    "distinct": {
        "columns": [
            "Platform"
        ]
    },
    "output": {
        "format": "csv"
    }
}'
```

:::
