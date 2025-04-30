# Querying with SQL

Querying Beacon with SQL is done through a api call to the `/api/query` endpoint. Every programming language has a different way of making API calls. Below are some examples of how to make an API call using SQL in different programming languages.

## Querying a single dataset

::: code-group

```python [netcdf.py]
import requests
import json
import pandas as pd
import io
# Define the API endpoint
url = "http://beacon-host/api/query"

# Define the SQL query and output format
query = {
    "sql": "SELECT TEMP, \"TEMP.units\",PSAL,LONGITUDE,LATITUDE FROM read_netcdf('dataset.nc')",
    "output": {
        "format": "parquet"
    }
}

# Make the API call
response = requests.post(url, json=query)

# Check the response status code
if response.status_code == 200:
    # Parse the response parquet data
    parquet_data = response.content
    # Convert the response content to a BytesIO object
    io_buffer = io.BytesIO(parquet_data)
    # Read the Parquet data into a DataFrame
    df = pd.read_parquet(io_buffer)
    print(df)
else:
    print(f"Error: {response.status_code} - {response.text}")
```

```python [odv.py]
import requests
import json
import pandas as pd
import io
# Define the API endpoint
url = "http://beacon-host/api/query"

# Define the SQL query and output format
query = {
    "sql": "SELECT TEMP,PSAL,LONGITUDE,LATITUDE FROM read_odv_ascii('dataset.txt')",
    "output": {
        "format": "parquet"
    }
}

# Make the API call
response = requests.post(url, json=query)

# Check the response status code
if response.status_code == 200:
    # Parse the response parquet data
    parquet_data = response.content
    # Convert the response content to a BytesIO object
    io_buffer = io.BytesIO(parquet_data)
    # Read the Parquet data into a DataFrame
    df = pd.read_parquet(io_buffer)
    print(df)
else:
    print(f"Error: {response.status_code} - {response.text}")
```

```python [parquet.py]
import requests
import json
import pandas as pd
import io
# Define the API endpoint
url = "http://beacon-host/api/query"

# Define the SQL query and output format
query = {
    "sql": "SELECT TEMP,PSAL,LONGITUDE,LATITUDE FROM read_parquet('dataset.parquet')",
    "output": {
        "format": "parquet"
    }
}

# Make the API call
response = requests.post(url, json=query)

# Check the response status code
if response.status_code == 200:
    # Parse the response parquet data
    parquet_data = response.content
    # Convert the response content to a BytesIO object
    io_buffer = io.BytesIO(parquet_data)
    # Read the Parquet data into a DataFrame
    df = pd.read_parquet(io_buffer)
    print(df)
else:
    print(f"Error: {response.status_code} - {response.text}")
```

:::

## Querying multiple datasets 

::: code-group

```python [netcdf.py]
import requests
import json
import pandas as pd
import io
# Define the API endpoint
url = "http://beacon-host/api/query"

# Define the SQL query and output format
query = {
    "sql": "SELECT TEMP, \"TEMP.units\",PSAL,LONGITUDE,LATITUDE FROM read_netcdf('*.nc')",
    "output": {
        "format": "parquet"
    }
}

# Make the API call
response = requests.post(url, json=query)

# Check the response status code
if response.status_code == 200:
    # Parse the response parquet data
    parquet_data = response.content
    # Convert the response content to a BytesIO object
    io_buffer = io.BytesIO(parquet_data)
    # Read the Parquet data into a DataFrame
    df = pd.read_parquet(io_buffer)
    print(df)
else:
    print(f"Error: {response.status_code} - {response.text}")
```

```python [odv.py]
import requests
import json
import pandas as pd
import io
# Define the API endpoint
url = "http://beacon-host/api/query"

# Define the SQL query and output format
query = {
    "sql": "SELECT TEMP, \"TEMP.units\",PSAL,LONGITUDE,LATITUDE FROM read_odv_ascii('*.txt')",
    "output": {
        "format": "parquet"
    }
}

# Make the API call
response = requests.post(url, json=query)

# Check the response status code
if response.status_code == 200:
    # Parse the response parquet data
    parquet_data = response.content
    # Convert the response content to a BytesIO object
    io_buffer = io.BytesIO(parquet_data)
    # Read the Parquet data into a DataFrame
    df = pd.read_parquet(io_buffer)
    print(df)
else:
    print(f"Error: {response.status_code} - {response.text}")
```


```python [parquet.py]
import requests
import json
import pandas as pd
import io
# Define the API endpoint
url = "http://beacon-host/api/query"

# Define the SQL query and output format
query = {
    "sql": "SELECT TEMP,PSAL,LONGITUDE,LATITUDE FROM read_parquet('*.parquet')",
    "output": {
        "format": "parquet"
    }
}

# Make the API call
response = requests.post(url, json=query)

# Check the response status code
if response.status_code == 200:
    # Parse the response parquet data
    parquet_data = response.content
    # Convert the response content to a BytesIO object
    io_buffer = io.BytesIO(parquet_data)
    # Read the Parquet data into a DataFrame
    df = pd.read_parquet(io_buffer)
    print(df)
else:
    print(f"Error: {response.status_code} - {response.text}")
```

:::
