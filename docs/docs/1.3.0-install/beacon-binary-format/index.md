# Beacon Binary Format

> [!WARNING]
> This is a Feature that requires the `Beacon Standard Edition` with an activated Token. See: [Beacon plans](https://beacon.maris.nl/plans)

Beacon Binary Format is a collection file format that allows users to combine multiple datasets into a single file while still maintaining the ability to access individual or multiple files inside the collection. It is designed to be fast, efficient, and scalable, making it ideal for large-scale marine and oceanographic datasets.

Its possible to store millions of datasets into a individual Beacon Binary Format file without sacrificing performance and making it immediately accessible for Beacon. Beacon can then leverage multiple threads at the same time to prune and read the file in parallel.

Beacon Binary Format has first class support for N-Dimensional arrays in combination with named dimensions and is similar to NetCDF, but with a focus on speed and efficiency. Beacon Binary Format is a thread-safe format, allowing multiple threads to read data simultaneously and efficiently. It is also similar to parquet as to how it organizes the data in blocks.

Beacon Binary Format also natively supports cloud storage solutions, making it easy to work with data stored in the cloud.
It can effectively prune irrelevant datasets in advance from the file, similar to how parquet leverages pruning.

## Multi Dataset Super Typing

Beacon Binary Format uses super typing to combine data schemas of multiple datasets into a single schema. Every Beacon Binary Format file therefore has its own schema which is a combination of all the schemas of all the datasets stored inside. This makes it a very flexible wide data format that can store any type of data safely without losing data due to type conversion.

## Compression

Beacon Binary Format supports multiple compression algorithms such as `lz4`, `zstd`. This allows for efficient storage of data and reduces the file size. By default, Beacon Binary Format uses `zstd` compression, which is fast and relatively efficient.
