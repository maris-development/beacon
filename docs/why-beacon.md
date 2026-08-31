---
title: Why Beacon
description: The problems Beacon solves. It serves large scientific archives to many users. It sends answers, not files.
---

# Why Beacon

Beacon solves one problem. You hold a large collection of scientific files. Other people need parts
of that collection.

This looks simple. It is not. The two usual answers both fail at scale.

## What people do today

### Answer 1: give out the files

You put the archive on FTP, HTTP or a bucket. Users download what they want.

This causes four problems:

- **Users take too much.** A user wants 200 profiles from a 4 TB collection. The smallest unit is
  a file. The user must download whole files.
- **Every user writes the same code.** The code lists the files. It opens each file. It skips a
  file without the variable. It joins the results. Many users write this code. Each version has
  different faults.
- **The layout becomes an interface.** Users put your paths in their code. You can no longer change
  the directory structure.
- **Access control is coarse.** The bucket is open or closed. There is no middle position.

### Answer 2: build a download portal

You put a web form in front of the archive. The form makes extracts.

This causes three problems:

- **You maintain an application.** The portal needs a job queue and a place to hold the extracts.
- **The form limits the questions.** A new question needs a new form field. A user cannot ask a
  question that you did not plan for.
- **The extracts are copies.** They become old. They use storage. Nobody knows which copy is
  current.

Beacon replaces both answers. A Beacon server answers questions. It does not send files. Users send
SQL. Beacon sends back only the rows and columns of the answer.

## What Beacon solves

**Beacon reads the true shape of observational data.** Argo floats, CTD casts, gliders and moorings
give ragged profiles. They give hundreds of thousands of small files. The schema changes from file
to file. `xarray.open_mfdataset` fails on that change. A manual loop is slow. Beacon joins the files
by column name. It reads the collection as one table. See
[Coming from xarray](/docs/2.0.0-rc5/coming-from-xarray).

**Beacon needs no conversion step.** It reads NetCDF, Zarr, Parquet, GeoParquet, CSV, ODV, HDF5,
Arrow, GeoTIFF, Delta and BBF in place. You run no import job. You keep no second copy.

**Beacon gives one set of names.** Register a glob once as a table or a view. Users then query a
name. You can move the storage later. No saved query breaks. See
[Server Setup](/docs/2.0.0-rc5/server/).

**Beacon controls access per table and per path.** A grant is `ON TABLE <name>` or
`ON PATH '<glob>'`. A deny always beats a grant. Three public collections and one private collection
need a few statements. They do not need a second bucket. See
[Access Control](/docs/2.0.0-rc5/security/access-control).

**Beacon queries other servers.** `ATTACH` a second Beacon server. Then join its tables against your
own in one statement. Neither server moves its data. Each server keeps its own access rules. See
[ATTACH](/docs/2.0.0-rc5/data-sources/attach).

## Why it fits data in a cloud bucket

Beacon fits an archive that already sits in S3. The reasons are concrete.

### It can cut egress cost

Read this section with care. The conditions decide whether you save money.

A cloud provider charges for data that leaves the cloud. It does not charge for data that moves
inside one region. So put the Beacon server in the same region as the bucket.

```text
without Beacon:  bucket ─── whole files, billed ───► every user

with Beacon:     bucket ─── free, same region ───► Beacon ─── answers, billed ───► every user
```

The first leg costs nothing. Only the answer leaves the cloud. An answer is much smaller than the
files that hold it. Three effects combine:

1. **Beacon drops whole files.** A filter on time or depth removes a file before Beacon reads its
   data. Beacon uses the statistics in an [Atlas](/docs/2.0.0-rc5/formats/atlas) collection, or the
   row group statistics in a Parquet file.
2. **Beacon reads only the columns you name.** `SELECT time, temp` reads two columns from a columnar
   file. It does not read the other 198.
3. **Beacon filters the rows.** It sends only the rows that match.

Here is an example. A 4 TB Parquet collection serves 200 users each year. Each user wants about
0.5% of the collection. The egress rate is **$0.09/GB**. Use the rate of your own provider.

| | Bytes that leave the cloud | Cost at $0.09/GB |
|---|---|---|
| Users download the files | 200 × 40 GB = 8 TB | about $720 |
| Users query Beacon in one region | 200 × 200 MB = 40 GB | about $3.60 |

::: warning Read the conditions
You save money only when all four conditions are true:

- **Beacon runs in the region of the bucket.** A server in a different region pays egress twice.
  This condition decides the result.
- **Users want subsets.** A user who wants the whole archive must get the files. Beacon saves
  nothing for that user.
- **You count the compute cost.** You exchange an egress bill for a server that runs all the time.
  A small archive costs more this way. A large archive costs less.
- **The format supports pushdown.** Parquet, Atlas, Delta and Zarr drop data early. A CSV file
  cannot. Beacon reads a CSV file from start to end. See the
  [capability matrix](/docs/2.0.0-rc5/formats/#capability-matrix).

Range reads add `GET` requests. Request charges are much lower than egress charges. They do not
change the result.
:::

### The bucket stays private

Users query Beacon. Beacon holds the credentials. A user needs no AWS key, no IAM role and no signed
URL. You can open one part of a private bucket to anonymous users. The bucket itself stays private.

Enable anonymous access. Then grant the `anonymous` user what it may read:

```sql
CREATE ROLE public_reader;
GRANT SELECT ON TABLE observations TO ROLE public_reader;
GRANT ROLE public_reader TO USER anonymous;
```

The [public World Ocean Database server](/available-nodes/available-nodes) works this way. The bucket
is private. One table is open. No account is necessary.

### Users need no cloud skills

A scientist needs a URL and SQL. A scientist does not need boto3, an endpoint, a region or a
credential chain. This removes a real barrier. It is also why teams build a portal. Beacon removes
the need for the portal.

### The storage layout stays private

A path in a query is relative to the datasets root. You set that root when the server starts. You can
restructure the bucket, rename it, or move to another provider. No user sees a change. See
[Object Storage](/docs/2.0.0-rc5/data-sources/object-storage).

## Beacon and DuckDB

People ask how Beacon compares to DuckDB in a notebook. **The two tools are not competitors. They
solve different problems.**

**DuckDB is an engine.** You put it inside your own process. It works on data that you can already
reach. It answers your own questions, on your own machine.

**Beacon is a server.** You put it in front of an archive that you hold. It answers the questions of
other people. It also decides what each person may read.

One is a library for one person. The other is infrastructure for a group. A comparison of speed or
of SQL features misses that difference.

The useful question is therefore not "which engine is faster". The useful questions are "who needs
the answer" and "where does the data live".

### Use DuckDB when the data is yours

One analyst holds tabular files that they can reach. Use DuckDB. That answer is honest. DuckDB starts
in a second. It costs nothing to operate. Its ecosystem is larger. Beacon adds nothing for that
person.

### Use Beacon when other people need your data

Four differences then matter.

#### 1. Format

DuckDB has no native reader for NetCDF, Zarr, ODV, HDF5, GeoTIFF or Atlas. Most ocean and climate
archives use those formats. To query them with DuckDB, convert them to Parquet first. That
conversion is the step you wanted to avoid. It also makes a second copy.

Beacon reads all twelve formats in place.

#### 2. Latency, not bandwidth

A notebook reads S3 across the internet. One round trip costs about 50 ms. A server in the same
region costs about 1 ms.

A collection of 200,000 small files needs at least one request for each file. At 50 ms the round
trips dominate the query time. A faster laptop does not help. A server next to the bucket removes
the distance.

#### 3. Cost for each user

DuckDB does read byte ranges. It does push filters into Parquet. It does not pull whole files. That
part is fine.

The filtered bytes still cross the paid boundary. They cross once for each user. They cross again for
each repeat of the query. A Beacon answer crosses once. See
[It can cut egress cost](#it-can-cut-egress-cost).

#### 4. Governance and shared definitions

DuckDB has no access control. Every analyst needs bucket credentials. You cannot open one collection
to the public and keep the rest private.

Each notebook also defines its own paths and views. Nobody shares them. A Beacon server holds one
catalog, one set of names and one set of grants.

### Use both

The two tools fit together, because they sit at different points in the same task. Beacon reads the
archive and makes the result small. DuckDB, pandas or xarray then does the analysis on your machine.

A good pattern looks like this. Beacon serves the collection to the group. Each person queries the
part that they need. Each person then works locally with the tool that they prefer.

| | DuckDB in a notebook | A Beacon server |
|---|---|---|
| What it is | A library in your process | A service in front of an archive |
| Who it serves | You | Your users |
| Scientific array formats | Convert them first | Reads them in place |
| Distance to the data | Internet | Same region |
| Access control | None | Per table and per path |
| Shared catalog | No | Yes |
| Operation cost | None | One server |

## When Beacon is not the answer

Beacon is the wrong tool in four cases:

- **Users want the original files.** Publish the files for archives, citation and reproduction.
  Beacon answers questions. It does not distribute records.
- **You hold one correct file on your own disk.** Open it with xarray. A server adds nothing.
- **You need interpolation, regridding or a rolling window.** Those belong to xarray. Use Beacon
  first to make the collection smaller. Then give the result to xarray.
- **You need many concurrent writes.** Beacon owns rows only in a managed table. It serves reads.
  It is not a transaction database.

## How it works

Beacon uses Rust, [Apache Arrow](https://arrow.apache.org/) and
[DataFusion](https://datafusion.apache.org/). It parses a query into a plan. It pushes filters and
column choices as deep as the format permits. It streams the result back as Arrow, Parquet, NetCDF,
CSV or ODV. See [How it works](/docs/2.0.0-rc5/how-it-works).

## Next

| | |
|---|---|
| **Try it. No setup** | [Query the public server](/docs/2.0.0-rc5/quickstart#query-the-public-node) |
| **Deploy a server** | [Quick Start](/docs/2.0.0-rc5/quickstart#deploy-a-server) · [Getting Started](/docs/2.0.0-rc5/getting-started) |
| **See the full model** | [Introduction](/docs/2.0.0-rc5/introduction) |
| **Move from Python** | [Coming from xarray](/docs/2.0.0-rc5/coming-from-xarray) |
