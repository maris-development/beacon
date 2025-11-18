# Introduction Beacon Data Lake

In this section, we will explore how the Beacon Data Lake organizes its datasets and how you can query them. The Beacon Data Lake is designed to provide a unified interface for accessing and querying datasets, regardless of their underlying format or storage location. This allows users to work with datasets in a consistent and efficient manner.

## Data Lake Architecture

Beacon has data organized in 2 types:

- **Datasets** : Datasets are the raw data files that are stored in various formats, such as NetCDF, Parquet, and CSV. Datasets are the building blocks of the Beacon Data Lake. They can be queried directly or used to create higher-level abstractions like tables. An example of a dataset is a NetCDF file containing climate data for a specific year, such as `2020.nc`, `2021.nc`, or `2022.nc`. These datasets can be queried directly using SQL or JSON.
- **Tables** : These are structured collections of datasets that are organized in a tabular format. Tables are used to group related datasets together and provide a higher-level interface for querying and analyzing data.
  - Tables can be created from one or more datasets, and they can be queried using SQL. For example, a table named `2020_2022` could be created from the datasets `2020.nc`, `2021.nc`, and `2022.nc`. This table would allow users to query all three datasets as if they were a single entity.

::: details Open to see a technical overview of the Beacon Data Lake architecture:

```ascii

                ┌────────────────────────────────────┐
                │         BEACON DATA LAKE           │
                └────────────────────────────────────┘
                                     │
              ┌──────────────────────┴─────────────────────────┐
              │                                                │
      ┌────────────────┐                              ┌────────────────────┐
      │    DATASETS    │                              │    DATA TABLES     │
      └────────────────┘                              └────────────────────┘
              │                                                │
  ┌───────────┴─────────────────────┐                ┌─────────┴────────────────────┐
  │         SUPPORTED FORMATS       │                │                              │
  │  ┌────────┐ ┌────────┐ ┌──────┐ │                ▼                              ▼
  │  │ NETCDF │ │ PARQUET│ │ CSV  │ │         ┌─────────────────┐            ┌─────────────────┐
  │  └──┬─────┘ └──┬─────┘ └──┬───┘ │         │ LOGICAL TABLES  │            │ PHYSICAL TABLES │
  └─────│──────────│──────────│─────┘         └──────┬──────────┘            └──────┬──────────┘
        ▼          ▼          ▼                      │                              │
  ┌──────────┐ ┌──────────┐ ┌──────────┐             ▼                              ▼
  │ 2020.nc  │ │ 2021.nc  │ │ 2022.nc  │           (built from              (backed by formats:)
  └──────────┘ └──────────┘ └──────────┘            same-format
        \           |             /                 datasets)           ┌────────────┐ ┌─────────────────────┐
         \__________|____________/                                      │  PARQUET   │ │ BEACON BINARY FORMAT│
                    ▼                                                   └────────────┘ └─────────────────────┘
                    |
           ┌────────────────────┐
           │   LOGICAL TABLE    │ ◄── named collection of datasets
           │  name: "2020_2022" │
           │  paths: [          │
           │    "2020.nc",      │
           │    "2021.nc",      │
           │    "2022.nc"]      │
           └────────────────────┘

Notes:
- DATASETS support NetCDF, Parquet, BBF, and CSV.
- DATA TABLES are LOGICAL (eg. sql views).
- LOGICAL TABLES abstract same-format datasets as a collection.
```

:::

## Datasets

We support querying & sub-setting datasets from the following formats:

- Zarr (eg. `example1.zarr/zarr.json`, `example2.zarr/zarr.json`)
- NetCDF (eg. `example1.nc`, `example2.nc`)
- Parquet (eg. `example1.parquet`, `example2.parquet`)
- CSV (eg. `example1.csv`, `example2.csv`)
- ODV ASCII (eg. `example1.txt`, `example2.txt`)
- Arrow IPC (eg. `example1.arrow`, `example2.arrow`)

It also possible to query many datasets at once using glob paths (eg. `*.nc`, `*.parquet`, `*.csv`, `*/zarr.json`,etc.). This allows users to query all datasets of a specific format in a directory without having to specify each dataset individually.

## Data Tables

Data tables are structured collections of datasets that are organized in a tabular format. Tables are used to group related datasets together and provide a higher-level interface for querying and analyzing data.

These tables are named and can be queried using SQL. For example, a table named `2020_2022` could be created from the datasets `2020.nc`, `2021.nc`, and `2022.nc`. This table would allow users to query all three datasets as if they were a single entity.
