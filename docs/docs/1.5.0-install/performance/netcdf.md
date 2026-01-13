# Improving `NetCDF` Performance

`NetCDF` is a binary data format that is widely used in the scientific community for storing large datasets. It is a self-describing format that allows for the storage of multi-dimensional arrays and metadata. `NetCDF` files can be very large, and reading and writing data from these files can be slow, especially when working with large datasets. In this guide, we will discuss some tips and tricks for improving the performance of reading and writing `NetCDF` files.

Because `NetCDF` is limited to a single thread, it can be slow when working with large datasets. To improve performance, you can transform your `NetCDF` files into `Beacon Binary Format`, which is a multi-block based data format that is designed to be fast, efficient, and scalable. `Beacon Binary Format` allows you to combine millions of datasets into a single file, which can be directly used in Beacon. The format is thread-safe, allowing multiple threads to read data simultaneously and efficiently. `Beacon Binary Format` also supports multiple compression algorithms, such as `lz4`, `zstd`, and `snappy`, which allows for efficient storage of data and reduces the file size.

`Beacon Binary Format` can be seen as a super set of `NetCDF`, with a focus on speed and efficiency. It is similar to `NetCDF` in that it supports N-Dimensional arrays in combination with named dimensions, but it is optimized for performance and scalability. `Beacon Binary Format` is ideal for large-scale marine and oceanographic datasets, as it allows you to efficiently store and manage large data collections.

To convert your collection of `NetCDF` files into `Beacon Binary Format`, you can use the [beacon-binary-format-toolbox](../beacon-binary-format/how-to-use.md) tool, which is part of the Beacon Data Lake ecosystem. The tool allows you to convert your `NetCDF` files into `Beacon Binary Format` with just a few commands.
You can find more information about how to do this in the [Beacon Binary Format Toolbox documentation](../beacon-binary-format/how-to-use.md).

By using `Beacon Binary Format`, you can significantly improve the performance of reading large datasets, making it easier to work with large-scale scientific data.
