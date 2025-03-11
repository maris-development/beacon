# Beacon Binary Format

> [!WARNING]
> This is a Feature that requires the `Beacon Standard Edition` with an activated Token. See: [Beacon Plans](https://beacon.maris.nl/plans)

Beacon Binary Format is a multi-block based data format. It enables users to combine millions of datasets into a single file, which can be directly used in Beacon. The Beacon Binary Format is designed to be fast, efficient, and scalable, making it ideal for large-scale marine and oceanographic datasets.

Data can be appended and deleted from the Beacon Binary Format without the need to rewrite the entire file. This makes it easy to update and manage datasets without the need to re-import the data.

Beacon Binary Format has first class support for N-Dimensional arrays in combination with named dimensions and is similar to NetCDF, but with a focus on speed and efficiency. Beacon Binary Format is a thread-safe format, allowing multiple threads to read data simultaneously and efficiently. It is also similar to parquet as to how it organizes the data in blocks.

Every dataset in Beacon Binary Format is stored in a `data block` which can consists of multiple datasets. Every Beacon Binary Format can consists of thousands of data blocks, this allows it to efficiently store and manage large data collections. Every block also consists of an index which allows for efficient pruning of relevant datasets (similar to parquet).

## Ultrawide Super Typed Schema

Beacon Binary Format uses super typing to combine data schemas of multiple datasets into a single schema. Every block therefore has its own schema which is a combination of all the schemas of the datasets in the block. The file then has a super schema which is a combination of all the schemas of all the blocks. This makes it a very flexible wide data format that can store any type of data.

## Compression

Beacon Binary Format supports multiple compression algorithms such as `lz4`, `zstd`, and `snappy`. This allows for efficient storage of data and reduces the file size. By default, Beacon Binary Format uses `lz4` compression, which is fast and relatively efficient.
