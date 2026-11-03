---
description: Add functions, file formats and table types to Beacon with INSTALL and UNINSTALL. Install a plugin from a file, a web server or a GitHub release.
---

# Plugins

A plugin adds features to Beacon. You do not build a new Beacon release.

A plugin can add these features:

- **Scalar functions**, in Rust or in SQL.
- **File formats**, for `read_<name>`, `STORED AS <name>` and `COPY ... TO`.
- **Table types**, for `CREATE EXTERNAL TABLE ... STORED AS <name>`.

To build a plugin, read [Write a plugin](/docs/2.1.0-rc2/guides/write-a-plugin).

```sql
INSTALL example FROM 'github:my-org/my-plugin@v0.1.0'
```

The names `my-org` and `my-plugin` are example names. Use the source of your own plugin.

```sql
SELECT example_add_one(1.5)
```

## Syntax

```sql
INSTALL <name> FROM '<source>'

UNINSTALL <name>
```

- Only a super-user can run `INSTALL` and `UNINSTALL`. Any user can use the features of a plugin.
- The name is the plugin name in the manifest. Beacon refuses a name that does not match.
- If the name is installed, Beacon refuses the `INSTALL`. Run `UNINSTALL` first.
- `UNINSTALL` deletes the plugin, its functions, its file formats and its table types.

## Sources

| Source | Meaning |
| --- | --- |
| `file:///path/plugin.tar.gz` | A `.tar.gz` file on the server. An optional `SHA256SUMS` file in the same folder checks it. |
| `file:///path/plugin/` | An unpacked plugin folder on the server. |
| `https://host/path/` | A folder URL. The folder must hold the archives and a `SHA256SUMS` file. |
| `github:<owner>/<repo>@<tag>` | The assets of a GitHub release. The release must have a `SHA256SUMS` asset. |

For a folder URL and a GitHub release, Beacon reads `SHA256SUMS`. Then it picks the archive. It
picks the native archive for this server first. Then it picks the SQL archive. Beacon checks the
checksum before it unpacks the archive.

Beacon accepts every source. Only a super-user can run `INSTALL`, so Beacon has no list of allowed
sources. Beacon refuses a source with a user name or a token in the URL, because the source text
goes to the logs. Plain `http://` works only for the local computer (`127.0.0.1`, `[::1]` or
`localhost`). It exists for tests.

## Install at startup

`BEACON_PLUGINS` is a comma list of `name=source`. At startup, Beacon installs each plugin that is
not yet installed.

```sh
BEACON_PLUGINS=example=github:my-org/my-plugin@v0.1.0
```

- A failed install logs an error. The server starts.
- Beacon retries a failed download at the next start.
- If a check fails after the download, the plugin gets the status `failed`. Beacon stores
  nothing. Beacon does not try the plugin again until `UNINSTALL` or the next start.
- If the name is installed from a different source, Beacon keeps the installed plugin and logs a
  warning. Run `UNINSTALL` to change the source.
- An entry that is not `name=source` stops the server at startup.
- A source in `BEACON_PLUGINS` cannot contain a comma.
- If you run `UNINSTALL` and the name stays in `BEACON_PLUGINS`, the next start installs the plugin
  again. Remove the name from `BEACON_PLUGINS` to keep it uninstalled.

## Storage

The plugins directory is the only store of the plugins. The server uses
`<BEACON_DATA_DIR>/plugins`. The database file `beacon.db` holds no copy of a plugin.

Each plugin has one folder, `<BEACON_DATA_DIR>/plugins/<name>/`. The folder holds the files of the
plugin and an `install.json` file. `install.json` records the version, the source, the checksum of
the archive and the install time.

- `INSTALL` checks the archive and unpacks it into a temporary folder. Then Beacon writes
  `install.json` and renames the folder to `<name>/`.
- Beacon checks the checksum at `INSTALL` only. At startup, Beacon loads each plugin folder as it is.
- A plugin stays installed after a restart.
- At startup, Beacon deletes the temporary folders of an incomplete `INSTALL` or `UNINSTALL`.
- If a folder has no valid `install.json` or manifest, the plugin gets the status `failed`.
- Beacon ignores a folder whose name is not a valid plugin name.
- `UNINSTALL` deletes the folder of the plugin.
- If you delete the folder of a plugin, Beacon does not load the plugin at the next start.
- A moved or restored `beacon.db` does not bring its plugins. Install the plugins again, or put
  them in `BEACON_PLUGINS`. Then Beacon installs them at startup.

With Docker, mount `/beacon/data/plugins` to keep the plugins when you replace the container.

Beacon loads these parts in this order at startup:

1. built-in functions, file formats and table types
2. plugins
3. user functions
4. tables and views

A user function, a view and a table can use the features of a plugin.

## Show the plugins

`beacon.system.plugins` has one row for each plugin. A plugin that failed to load also has a row.

```sql
SELECT name, version, status, error FROM beacon.system.plugins
```

| Column | Value |
| --- | --- |
| `name` | The plugin name. |
| `version` | The plugin version. |
| `kind` | `native`, `sql` or `native+sql`. |
| `status` | `loaded`, `failed` or `uninstalled`. |
| `error` | The reason, if the status is `failed`. |
| `functions` | The names of the scalar functions of the plugin. |
| `source` | The source text of the install. |
| `sha256` | The checksum of the archive. |

A failed plugin never stops the server.

## Functions

A plugin function works like a built-in function.

- A plugin SQL file holds `CREATE FUNCTION` statements only. Beacon refuses `CREATE OR REPLACE`.
- A plugin SQL function can call built-in functions and the native functions of its own plugin.
- A stored user function wins over a plugin function.
- Beacon refuses `DROP FUNCTION`, `CREATE OR REPLACE FUNCTION` and `COMMENT ON FUNCTION` for a
  plugin function. Run `UNINSTALL` instead.
- Beacon does not send a native plugin function to a remote Beacon. The local server runs it.

## File formats

A file format plugin works like a built-in format, such as Parquet or NetCDF. A format named
`example` gives these features:

```sql
-- Read files with the table function.
SELECT * FROM read_example(['obs/*.ex'])

-- Show the schema of the files.
SELECT * FROM read_example_schema(['obs/*.ex'])

-- Create a table on the files.
CREATE EXTERNAL TABLE obs STORED AS example LOCATION 'obs/'

-- Write a result, if the format has a writer.
COPY (SELECT * FROM obs) TO 'out/obs.ex' STORED AS example
```

- Beacon lists the files, merges their schemas and adds the partition columns.
- Beacon applies each filter again after the plugin reads a file.
- `COPY ... TO` works only when the format has a writer. Beacon refuses it for a format without
  a writer.
- After `UNINSTALL`, a table on the format fails with the error `file format ... is not installed`.
  Install the plugin again to read the table.

## Table types

A table type plugin adds a new kind of external table, such as a remote catalog or a web service.

```sql
CREATE EXTERNAL TABLE readings STORED AS sensors
LOCATION 'https://example.com/api' OPTIONS ('station' 'north');

SELECT * FROM readings;
INSERT INTO readings VALUES (1.5);
```

- `SELECT` reads from the plugin. `INSERT INTO` works only when the plugin accepts writes.
- `DELETE` and `UPDATE` are not available on a plugin table.
- Beacon stores the table and creates it again at each start. When the plugin is missing, Beacon
  skips the table and keeps its definition.
- Beacon stores `LOCATION` and `OPTIONS` as plain text in the table definition. Do not put a secret
  there.

## Rules and limits

- If a name collides, Beacon refuses the whole plugin. At `INSTALL`, Beacon stores nothing. At
  startup, the plugin gets the status `failed`. This rule applies to function names, aliases,
  file format names and table type names.
- A file format or table type name must be lower case `[a-z0-9_]+`. It must not be the name of a
  built-in format or table type.
- An archive has a limit of 128 MiB. The unpacked files have a limit of 512 MiB.
- The `beacon` range in the manifest must match the Beacon version. Beacon ignores the
  pre-release part. `2.1.0-rc2` reads as `2.1.0`.

## Native plugins

A native plugin is a dynamic library. It runs in the Beacon process with the permissions of the
server. Install a native plugin only from a source that you trust.

- The plugin must use the same DataFusion major version as Beacon. Beacon 2.1.0-rc2 uses 54.
- The plugin must use the same SDK ABI version as Beacon.
- A panic in a plugin call becomes a query error.
- A fault in native code stops the server.
- Each reinstall of a native plugin keeps the old library in memory until Beacon restarts.
- File formats and table types need a native plugin. A SQL plugin adds functions only.
