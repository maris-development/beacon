# Remote Tables

A **remote table** points at a table living on **another Beacon instance** and is queried over Arrow Flight SQL. Once created it behaves like any other table in your SQL — you `SELECT`, `JOIN`, and aggregate over it — but the data stays on the remote, and Beacon pushes as much work as possible down to it so only the reduced result set travels the network.

This chapter covers querying remote tables. For the DDL that creates them (`LOCATION`, `OPTIONS`, TLS), schema handling, and the full list of limitations, see the [Remote Tables (Federation) setup chapter](../data-lake/remote-tables.md).

## Querying a remote table

Assuming a remote table `remote_profiles` has been registered, it is queried exactly like a local table:

```sql
SELECT count(*), avg(temperature)
FROM remote_profiles
WHERE depth < 50 AND platform = 'argo';
```

The `WHERE` filter and the aggregate are executed **on the remote**, and only the small aggregated result is returned over the wire.

## How pushdown works

When you query a remote table, Beacon's planner federates the largest sub-plan rooted at it and sends it to the remote as SQL. Filters, projected columns, `LIMIT`, and whole aggregates push down. Confirm what gets pushed with `EXPLAIN`:

```sql
EXPLAIN SELECT count(*) FROM remote_profiles WHERE depth < 50;
```

The plan shows a federated (virtual) scan node in place of a local table scan — everything below it runs remotely.

## Joins across the same remote

Tables that live on the **same** remote instance federate together, so a join between two remote tables on that instance is pushed down and executed remotely:

```sql
SELECT p.id, m.station_name
FROM remote_profiles p
JOIN remote_stations m ON p.station_id = m.id;
```

Joins that mix a remote table with a **local** table (or with a table on a *different* remote) are executed locally: Beacon fetches the needed remote rows and joins them on this instance.

:::tip
Keep remote-pushed predicates to standard SQL comparisons. A filter or projection that uses a Beacon UDF (e.g. a geospatial `st_*` function) is only pushed down if the remote instance has the same function; otherwise Beacon falls back to local execution. See the [setup chapter's limitations](../data-lake/remote-tables.md#limitations).
:::
