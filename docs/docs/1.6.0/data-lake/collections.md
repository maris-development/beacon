# Collections (Data Tables)

Collections (Data Tables) can be created on top of multiple datasets to make them available as a single data table which is then accessible via a single `table` name.
This is useful when you have multiple datasets that are related to each other and you want to query them together using a single collection name.

To list the available data tables, you can use the following API endpoint:

```http
GET /api/tables
```

To view the columns (and datatype) of a data table, you can use the following API endpoint:

```http
GET /api/table-schema?table_name=your_data_table_name
```

## Logical Data Tables

> [!WARNING]
> Logical data tables currently only work when combining datasets of the same file format, such as Zarr, Parquet, NetCDF, or ODV. Mixing file formats in one table is not supported.

Collections are now managed through SQL DDL rather than dedicated admin HTTP endpoints. Execute the statements through Beacon's SQL surfaces, such as the HTTP query endpoint or Flight SQL.

### Creating a logical data table

Use `CREATE EXTERNAL TABLE` to register a collection over one or more dataset paths. The location may point at a folder or an explicit glob, depending on how you want Beacon to discover files.

```sql
CREATE EXTERNAL TABLE argo
STORED AS PARQUET
LOCATION 'argo/'
```

You can also target explicit glob patterns when you want tighter control over the included datasets.

```sql
CREATE EXTERNAL TABLE argo_daily
STORED AS PARQUET
LOCATION 'argo/daily/*.parquet'
```

The location is resolved relative to Beacon's configured dataset storage root, such as `/beacon/data/datasets` in the container or the configured object-store prefix.

### Format-specific notes

- Zarr tables should point at `zarr.json` entry files, for example `datasets/*/zarr.json`.
- CSV and Arrow IPC collections are supported, but predicate pruning is less efficient than with Parquet, BBF, or suitably configured Zarr datasets.
- Beacon applies file-format specific options through the SQL DDL path, so keep collection definitions in SQL alongside the queries that depend on them.

### Atlas-backed tables

Beacon also supports Atlas-backed tables through Beacon SQL.

```sql
CREATE ATLAS TABLE sensor_data
LOCATION '/collections/sensor'
```

### Removing a table

Use SQL to remove a registered table from the catalog.

```sql
DROP TABLE argo
```

### Preset or derived tables

For derived read models, prefer SQL-native constructs such as `CREATE VIEW` on top of your registered tables so lifecycle stays in the same SQL surface as query execution.
