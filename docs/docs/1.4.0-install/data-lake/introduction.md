# Beacon Data Lake Introduction

The Beacon Data Lake is a powerful and flexible data storage solution designed to handle large volumes of data efficiently. 
It is built on top of the Apache Arrow ecosystem, which provides a columnar in-memory format that is optimized for analytics and uses Apache Datafusion (<https://datafusion.apache.org/>) as it OLAP engine.

- Beacon Data Lake supports directly querying (SQL & JSON) and subsetting out of datasets of the following supported data formats:

  - `Zarr`
  - `NetCDF`
  - `Parquet`
  - `CSV`
  - `ODV ASCII`
  - `Arrow IPC`
  - `Beacon Binary Format (Standard Edition only)`

- Beacon also allows you to create logical data tables (aka collections) from datasets, enabling efficient querying and analysis of large collections of datasets as logical tables similar to a virtual database table. Learn more about [Data Tables](./data-tables.md).

- Beacon will automatically handle schema merging for querying multiple `datasets` containing differing schemas, allowing you to work with heterogeneous datasets seamlessly. Learn more about [Schema Merging](./datasets-harmonization.md).

## Processing Pipeline

Beacon provides a processing pipeline that allows you to create a data lake from your datasets with minimal effort while striving to provide excellent scalability and performance.

Clients should interact with the Beacon REST API, which is built using the Axum framework. The API allows you to query datasets in various formats, including NetCDF and Parquet. The Beacon Parser is responsible for creating a DataFusion plan based on the query, which is then executed by DataFusion.
The results are returned to the client in the requested format.

The API also provides a way to query the data lake, which is built on top of the Beacon Data Providers. These providers abstract the underlying datasets and allow you to work with them in a unified way.
The Beacon Data Lake is designed to be extensible and can be easily integrated with other data sources and formats. It supports various file formats, including NetCDF, Parquet, CSV, and ODV ASCII.
The architecture of the Beacon Data Lake is designed to be modular and flexible, allowing for easy integration with other data sources and formats. The following diagram illustrates the processing pipeline:

```ascii
                          ┌──────────────────────────────┐
                          │   BEACON REST API (Axum)     │
                          └────────────┬─────────────────┘
                                       │
     Query Result (NetCDF, Parquet) ◄──┘
                                       │
                                       ▼
                          ┌──────────────────────────────┐
                          │        BEACON PARSER         │
                          │ (creates DataFusion plan)    │
                          └────────────┬─────────────────┘
                                       │
                                       ▼
                          ┌──────────────────────────────┐
                          │         DATAFUSION           │
                          │   (executes query plan)      │
                          └────────────┬─────────────────┘
                                       │
                                       ▼
                    ┌────────────────────────────────────────┐              
                    │        BEACON DATA PROVIDERS           │          
                    │ (abstractions for underlying datasets) │          
                    └────────────────┬───────────────────────┘
                                     │
                                     ▼
                         ┌────────────────────────────┐
                         │       BEACON DATA LAKE     │
                         └────────────────────────────┘

```

## Data Lake Architecture

The Beacon Data Lake is a flexible, simple and extensible architecture that allows you to work with datasets in a unified way. The following diagram illustrates the architecture of the Beacon Data Lake:

Data is stored in 2 ways:

1. **Datasets**: These are the raw data files that are stored in various formats, such as NetCDF, Parquet, and CSV. Datasets are the building blocks of the Beacon Data Lake.
2. **Data Tables**: These are logical or physical tables that are built from the datasets. Data tables can be used to query the data in a more efficient way, and they can be optimized for performance.
    - **Logical Tables**: These are named collections of datasets that are abstracted as a single table. Multiple datasets can be grouped together to form a logical table, which can be queried as a single entity. Logical tables are useful for organizing and managing datasets that share the same format or represent a collection.
    - **Physical Tables**: These are materialized views that are built from the datasets. Physical tables can be used to query the data in a more efficient way, and they can be optimized for performance.


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
- DATASETS support NetCDF, Parquet, and CSV.
- DATA TABLES are LOGICAL (eg. sql views) or PHYSICAL tables.
- LOGICAL TABLES abstract same-format datasets as a collection.
- PHYSICAL TABLES are efficient, materialized results from SQL queries allowing 
  for various optimization and chunking rules to be applied. These are backed by formats like Parquet or Beacon Binary Format.
```

## Schema Merging

