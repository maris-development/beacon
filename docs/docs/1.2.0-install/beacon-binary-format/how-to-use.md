# How To Use

The easiest way to use the Beacon Binary Format is to leverage the provided command-line tools. These tools allow you to easily convert your datasets into the Beacon Binary Format and perform various operations on them.

To get started, you can download the beacon-binary-format-toolbox from the official repository.
This is a command-line tool that simplifies the process of working with the Beacon Binary Format and is supported on Windows and Linux.

## Installation

To install the `beacon-binary-format-toolbox`, follow these steps:

### Linux (Ubuntu)

1. Download the latest release from the [official repository](https://github.com/maris-development/beacon/releases).
2. Extract the downloaded archive.
3. Install the netcdf binaries: `apt install libnetcdf-dev` and `apt install netcdf-bin`.

### Windows

1. Download the latest release from the [official repository](https://github.com/maris-development/beacon/releases).
2. Extract the downloaded archive.
3. Add the toolbox to your system's PATH.
4. Install the netcdf & hdf5 binaries and add them to your system's PATH.

## Creating a Beacon Binary Format File Collection

The following command allows you to create a Beacon Binary Format file collection:

```bash
beacon-binary-format-toolbox create --glob <GLOB> -o <OUTPUT>
beacon-binary-format-toolbox create --glob "data/*.nc" -o output.bbf
```

The following flags are available to customize the creation of the file:

- `--compression <COMPRESSION>`: Set the compression type and level (default: zstd:3). Options: `zstd:[0-21]`, `lz4`, `none`
- `--pruning` : Enable the creation of a pruning index (default: true)
- `--group-size <GROUP_SIZE>`: Set the array group size before compression (default: 4MB)

Supported file formats are:

- NetCDF (.nc)
- Parquet (.parquet)
- CSV (.csv)

## Inspecting

### Listing the footer (file collection metadata)

This will return the super type schema of the file collection.

``` bash
beacon-binary-format-toolbox list-footer --file-path output.bbf
```

### Listing the stored datasets

Listing the datasets stored in the file collection. Filtering can be done using a regex expr on the file name (entry_key)

```bash
beacon-binary-format-toolbox list-datasets-regex --file-path output.bbf --pattern ".*"
```

It is also possible to just list a single column:

```bash
beacon-binary-format-toolbox list-datasets-regex --file-path output.bbf --pattern ".*" --column <COLUMN_NAME>
```

### Listing the pruning index

``` bash
beacon-binary-format-toolbox list-pruning-index --file-path output.bbf
```

Listing for specific column:

``` bash
beacon-binary-format-toolbox list-pruning-index --file-path output.bbf --column <COLUMN_NAME>
```
