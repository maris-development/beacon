# Improving NetCDF Performance

> [!WARNING]
> Beacon Binary Format requires the `Beacon Standard Edition` with an activated Token. See: [Beacon plans](https://beacon.maris.nl/plans)

NetCDF is a binary data format that is widely used in the scientific community for storing large datasets. It is a self-describing format that allows for the storage of multi-dimensional arrays and metadata. NetCDF files can be very large, and reading and writing data from these files can be slow, especially when working with large datasets. In this guide, we will discuss some tips and tricks for improving the performance of reading and writing NetCDF files.

Because NetCDF is limited to a single thread, it can be slow when working with large datasets. To improve performance, you can transform your NetCDF files into Beacon Binary Format, which is a multi-block based data format that is designed to be fast, efficient, and scalable. Beacon Binary Format allows you to combine millions of datasets into a single file, which can be directly used in Beacon. The format is thread-safe, allowing multiple threads to read data simultaneously and efficiently. Beacon Binary Format also supports multiple compression algorithms, such as `lz4`, `zstd`, and `snappy`, which allows for efficient storage of data and reduces the file size.

Beacon Binary Format can be seen as a super set of NetCDF, with a focus on speed and efficiency. It is similar to NetCDF in that it supports N-Dimensional arrays in combination with named dimensions, but it is optimized for performance and scalability. Beacon Binary Format is ideal for large-scale marine and oceanographic datasets, as it allows you to efficiently store and manage large data collections.
