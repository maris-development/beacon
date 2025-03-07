::: warning
Tracking large numbers of files might cause performance issues when using a single directory tracking as it can take a while before it starts tracking due to indexing the filesystem. It is advised to use multiple tracking sub-directories in such cases. This issue will be addressed in future releases.
::::

# Tracking Directory

The Tracking Directory feature allows Beacon to track a directory and all its subdirectories for changes. This is useful when you have a large number of files that you want to track. Beacon will automatically detect changes in the directory and its subdirectories and update the internal loaded datasets accordingly. This feature follows the `eventual consistency` model, meaning that the changes will be eventually reflected in the loaded datasets.

## Tracking Directory Configuration

To setup a tracking directory, you call the rest api endpoint `/api/datasets/track-directory` with a json body containing the configuration options. The following options are available:

- `driver_name`: The name of the driver to use for tracking the directory. This must be an existing driver that has be defined beforehand. Once a driver is being used for tracking a directory, it cannot be removed until the directory is untracked.
- `directory`: The directory to track.
- `regex`: A regex to filter the files in the directory. Only files that match the regex will be tracked.
- `include_subdirectories`: A boolean to indicate whether to track the subdirectories of the directory or not.

```json
{
  "driver_name": "seadatanet-driver",
  "directory": "/home/user/netcdf-datasets/",
  "regex": ".*\\.nc$",
  "include_subdirectories": true
}
```

## Viewing Tracked Directories

To view the tracked directories, you can call the rest api endpoint `/api/datasets/tracked-directories`. This will return a json object containing the tracked directories and their configuration options.

```json
{
  "tracked_directories": [
    {
      "driver_name": "seadatanet-driver",
      "directory": "/home/user/netcdf-datasets/",
      "regex": ".*\\.nc$",
      "include_subdirectories": true
    }
  ]
}
```

## Untracking a Directory

To untrack a directory, you can call the rest api endpoint `/api/datasets/untrack-directory` with a json body containing the directory to untrack.

```json
{
  "directory": "/home/user/netcdf-datasets/"
}
```