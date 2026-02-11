
# Introducing the Beacon Node for the World Ocean Database: Open Access to more than 20 Million Ocean Datasets

We are excited to announce the launch of a new Beacon node dedicated to the [World Ocean Database (WOD)](https://www.ncei.noaa.gov/products/world-ocean-database) from NOAA, publicly accessible for everyone. This initiative was initiated in the BlueCloud2026 project and is part of supporting valuable outcomes of EU projects, ongoing commitment to open science and optimising access to the world’s most important collections of marine and oceanographic in situ data.

![World Ocean Database Beacon Node](/wod-screenshot-2022-2023.png)
*World Ocean Database Temperature Data (2022-2023) accessed via Beacon using the example notebook provided at the bottom*

## What is the World Ocean Database?

The World Ocean Database is NOAA’s flagship collection of oceanographic profile data, containing over 20 million NetCDF datasets from decades of global ocean observations. These datasets are invaluable for climate research, oceanography, marine biology, and countless other fields.

## The Challenge: Making Massive Data Usable

Traditionally, accessing subsets and analyzing these subsets from a vast collection of NetCDF files has been a challenging task. Downloading, indexing, and subsetting millions of files took much time, was storage-intensive, and needed complex workflows.

## Our Solution: Beacon and its Beacon Binary Format

To solve the above issues, we leveraged the Beacon Binary Format, a high-performance, open-source format designed to store and index millions of NetCDF datasets efficiently. Instead of handling millions of small files, Beacon converts and stores all WOD NetCDFs into just a few Beacon Binary Format files.

This approach:

- **Enables instant subsetting**: Query the Beacon API and extract just the data you need, on the fly, from the entire WOD collection.
- **Reduces storage overhead**: Millions of NetCDFs are consolidated into a handful of optimized files.
- **Accelerates access**: No more waiting for downloads or batch processing—get results in seconds.

## Try It: Run Notebooks and Query Instantly

Our beacon node is now live and open for public use.

You can:

- **Run Jupyter notebooks** directly against against the API of the Beacon node
- **Query and subset** from 20+ million ocean profiles in real time
- **Integrate with your own tools** using our open APIs

Whether you’re a researcher, educator, or ocean enthusiast, this node puts the WOD ocean data at your fingertips—no special infrastructure required.

## Example: Querying the World Ocean Database

To help you get started, we've provided a [Jupyter notebook example](https://github.com/maris-development/beacon-blue-cloud/blob/main/notebook-examples/1.5.0%20(latest)/wod-global.ipynb) that demonstrates how to connect to the beacon node, list datasets, inspect schema, and run queries using the [Beacon Python SDK](https://maris-development.github.io/beacon-py/latest/using/collections/). Here's a quick overview:

- **Install the SDK:**

	```bash
	pip install beacon-api
	```
- **Connect and list datasets:**

	```python
	from beacon_api import Client
    client = Client("https://beacon-wod.maris.nl")
	```
- **Inspect schema (and available columns):**

	```python
	available_columns = client.available_columns_with_data_type()
    list(available_columns)[:50] # Display first 50 available columns
    ```

- **Run a query:**

    ```python
    # Here we build the query step by step. First we select the columns we want to retrieve, then we add the filters and finally we specify the output format.
    query_builder = client.query()

    query_builder.add_select_column("wod_unique_cast") 
    query_builder.add_select_column("Platform", alias="PLATFORM") 
    query_builder.add_select_column("Institute", alias="INSTITUTE") 
    query_builder.add_select_column("Temperature", alias="TEMPERATURE")
    query_builder.add_select_column("Temperature_WODflag", alias="TEMPERATURE_QC")
    query_builder.add_select_column("Temperature.units", alias="TEMPERATURE_UNIT") # Attributes of variables can also be selected, just use the dot notation to specify that it's an attribute of a variable, eg. "z.units" or "Temperature.units"
    query_builder.add_select_column("z", alias="DEPTH")
    query_builder.add_select_column("z.units", alias="DEPTH_UNIT") # Attributes of variables can also be selected, just use the dot notation to specify that it's an attribute of a variable, eg. "z.units" or "Temperature.units"
    query_builder.add_select_column("time", alias="TIME") 
    query_builder.add_select_column("lon", alias="LONGITUDE")
    query_builder.add_select_column("lat", alias="LATITUDE")
    query_builder.add_select_column(".featureType", alias="FEATURE_TYPE") # Global attributes can be selected as well, just use the dot notation to specify that it's a global attribute

    ## Add the filters
    query_builder.add_range_filter("TIME", "2022-01-01T00:00:00", "2023-01-01T00:00:00")  # You can adjust the date range as needed. The format is ISO 8601.
    query_builder.add_is_not_null_filter("TEMPERATURE")  # Ensure Temperature is not null
    query_builder.add_not_equals_filter("TEMPERATURE", -1e+10)  # Remove missing values as WOD netcdf files didn't specify missing value attribute which means we have to filter them out manually
    query_builder.add_equals_filter("TEMPERATURE_QC", 0.0)  # Only good quality temperature
    query_builder.add_range_filter("DEPTH", 0, 10)  # Depth range from 0 to 10 meters

    # Execute the query and get results as a Pandas DataFrame
    df = query_builder.to_pandas_dataframe()
    df
	```

For more details, see the [Beacon Python SDK documentation](https://maris-development.github.io/beacon-py/latest/using/datasets/) and the [World Ocean Database example notebooks](https://github.com/maris-development/beacon-blue-cloud/tree/main/notebook-examples).

---

### Interested in having your own data lake, installing your own Beacon node on top of your data collection, or want to contribute?

**Beacon is fully open source available under the AGPL v3 license.**

Please visit the GitHub repository or check the documentation for setup instructions and support.

Beacon is easy & fast to deploy. Within an hour (or even minutes), you can instantly subset and analyze millions of datasets, directly from the Beacon node!

- **Beacon Data Lake GitHub:** [https://github.com/maris-development/beacon/](https://github.com/maris-development/beacon/)
- **Beacon Documentation:** [https://maris-development.github.io/beacon/](https://maris-development.github.io/beacon/)

If you encounter any bugs or want to get involved, please visit the GitHub repository or check the documentation for setup instructions and support.

With these tools, you can instantly subset and analyze millions of ocean profiles directly from the beacon node—no downloads or special infrastructure required!

## Beacon supports Open (data) Science

By making this resource freely available, we hope to empower the global community to:

- Accelerate climate and ocean research
- Foster transparency and reproducibility
- Enable new discoveries and applications

We invite you to explore, experiment, and discover what’s possible with open ocean data.

**Ready to dive in?** Try out the beacon node for the World Ocean Database today and be part of the open science movement!
