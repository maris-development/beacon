---
description: Beta. Write a Beacon plugin with SQL functions, native functions, file formats or table types. Pack it, test it on a local server and release it on GitHub.
---

# Write a plugin <Badge type="warning" text="beta" />

::: warning Beta. In development
Plugins are in development. The plugin format, the SQL statements and the SDK can change in a
new release without notice. Do not build production workflows on them yet. Report problems in the
[Beacon issue tracker](https://github.com/maris-development/beacon/issues).
:::

A plugin adds functions, file formats or table types to Beacon. This guide shows how to write,
pack, test and release one. To install a plugin, read [Plugins](/docs/2.1.0-rc3/sql/plugins).

To start fast, use the
[beacon-plugin-template](https://github.com/maris-development/beacon-plugin-template) repository.
It has an example of each plugin part and a release workflow. See [Plugins](/docs/2.1.0-rc3/server/plugins).

The [SDK README](https://github.com/maris-development/beacon/blob/main/beacon-db/beacon-plugin/beacon-plugin-sdk/README.md)
holds the full Rust examples and the rules for each plugin part.
The names `my-org` and `my-plugin` in this guide are example names.

## Choose a plugin type

| Type | Contents | Adds |
| --- | --- | --- |
| SQL | `CREATE FUNCTION` statements | Scalar functions |
| Native | A Rust dynamic library | Scalar functions, file formats and table types |
| Native and SQL | Both | All of the above |

A SQL plugin needs no Rust code and works on every server. A native plugin must match the
platform and the DataFusion version of the server.

## Write the manifest

The plugin folder has a `beacon-plugin.toml` file at the root.

```toml
[plugin]
name = "example"
version = "0.1.0"
beacon = ">=2.1, <3"

[native]
crate = "native"
library = "example_native"

[sql]
files = ["sql/functions.sql"]
```

| Field | Meaning |
| --- | --- |
| `plugin.name` | The plugin name. Use lowercase letters, digits and `_`. Start with a letter. |
| `plugin.version` | The plugin version, as a semver version. |
| `plugin.beacon` | The range of Beacon versions that the plugin supports. |
| `native.crate` | The folder of the Rust crate, relative to the plugin folder. |
| `native.library` | The library name, without prefix and suffix. |
| `sql.files` | The SQL files, relative to the plugin folder. |

The manifest needs the `[plugin]` table. Beacon refuses `name`, `version` and `beacon` outside
`[plugin]`. The manifest also needs `[native]`, `[sql]` or both. The `INSTALL` name must match
`plugin.name`.

Beacon compares `plugin.beacon` with its own version. It removes the pre-release part first.

## Write SQL functions

A SQL file holds `CREATE FUNCTION` statements only. Do not use `OR REPLACE`.

```sql
CREATE FUNCTION example_add_two(x DOUBLE) RETURNS DOUBLE
RETURN example_add_one(example_add_one(x));
```

A plugin SQL function can call built-in functions and the native functions of its own plugin.

## Write native code

Create a crate that builds a `cdylib`. Depend on `beacon-plugin-sdk` only.
Use a Git dependency. Set the tag to the Beacon release that the plugin targets.
The tag fixes the DataFusion version.

```toml
[lib]
name = "example_native"
crate-type = ["cdylib"]

[dependencies]
beacon-plugin-sdk = { git = "https://github.com/maris-development/beacon", tag = "v2.1.0-rc3" }
```

The SDK re-exports `datafusion`, `datafusion_ffi`, `datafusion_proto` and `stabby`. Use these
copies. Then the versions match the versions in Beacon.

`export_plugin!` exports the parts of the plugin. Give it one key for each part:

```rust
beacon_plugin_sdk::export_plugin!(
    scalar = functions,
    formats = file_formats,
    tables = table_types,
);
```

Each key names a function that returns a list. A plugin can use one, two or all three keys.

### Functions

Use the `function` helper to build a scalar function from a closure.

```rust
fn functions() -> Vec<ScalarUDF> {
    let add_one = function("example_add_one")
        .args([DataType::Float64])
        .returns(DataType::Float64)
        .invoke_arrays(|arrays| {
            let Some(values) = arrays[0].as_any().downcast_ref::<Float64Array>() else {
                return exec_err!("example_add_one expects a Float64 argument");
            };
            let result: Float64Array = values.iter().map(|v| v.map(|v| v + 1.0)).collect();
            Ok(Arc::new(result))
        });
    vec![add_one]
}
```

- The helper sets `Immutable` by default. Call `stable()` or `volatile()` to change this.
- For full control, write a DataFusion `ScalarUDFImpl` and use `ScalarUDF::from`.
- Planner hooks (`implies`, `bounds`, `propagate`) let a filter on the function prune files.

### File formats

A file format implements two traits from the `file_format` module:

- `FormatFactory` gives the name, the file extensions, the output kind and the writer flags.
- `FormatReader` reads one file in each call. It can also write one file in each call.

Beacon does the work for many files. It lists the files, merges the schemas, adds the partition
columns and splits the output of `COPY ... TO`. Do all I/O through the `ObjectStore` that Beacon
gives to the reader.

### Table types

A table type is a DataFusion `TableProviderFactory` with a name in a `TableType`. Beacon calls
`create` for each `CREATE EXTERNAL TABLE ... STORED AS <name>` and again at each start.

- The table code reads the object stores of Beacon. See [Object stores in a table type](#object-stores-in-a-table-type).
- The SDK runs the table code in a tokio runtime of the plugin.
- Do not keep the state of one server in a `static`. Several Beacon servers can share a process.

### Object stores in a table type

`Host` is the Beacon server that runs the table code. Get it from the session of `create`, `scan`
or `insert_into`. Give it the `LOCATION` text. It gives back the object store and the path.

```rust
use beacon_plugin_sdk::host::Host;

let host = Host::of(state)?;
let open = host.open(&cmd.location)?;
let bytes = open.store.get(&Path::from(open.path)).await?.bytes().await?;
```

For a location with a glob, or for a DataFusion `ListingTable`, use `listing_url`:

```rust
let url = host.listing_url(&cmd.location)?;
let store = state.runtime_env().object_store(url.object_store())?;
let files = url.list_all_files(state, store.as_ref(), "").await?;
```

To test your table code without Beacon, implement the `HostApi` trait in the test and build the
host with `Host::new`.

Beacon owns the rule for `LOCATION`. The plugin sends the text and does not parse it:

| `LOCATION` | Store | Path |
| --- | --- | --- |
| `obs/part_*.lance` | `datasets:///` | `obs/part_*.lance` |
| `datasets://obs/part_*.lance` | `datasets:///` | `obs/part_*.lance` |
| `s3://bucket/obs/a.parquet` | `s3://bucket/` | `obs/a.parquet` |
| `/data/obs/a.parquet` | none | `/data/obs/a.parquet` |

- `datasets://` is the URL of the datasets store. The plugin reads this store with the
  credentials of Beacon, also when the store is an S3 bucket.
- Each lookup reads the stores of Beacon at that moment. A store that Beacon adds or replaces later
  is visible to the plugin.
- The session of `create`, `scan` and `insert_into` holds the same stores. The task context of
  `execute` holds them too. `runtime_env().object_store(url)` thus works in DataFusion code.
- Do not parse `datasets://obs/a` as a URL. A URL parser reads `obs` as the host.
- The rule accepts each scheme and each absolute local path. The built-in table types refuse those
  when Beacon has a datasets store. Refuse in your plugin the locations that your users must not
  read.
- A glob in `listing_url` follows the DataFusion rule: `*` stays in one folder.
- `host.datasets_base_url()` gives the location of the datasets store, such as
  `file:///beacon/data/datasets/` or `s3://bucket/`. Use it for a reader that opens a path or URL
  by itself. The store of `s3://bucket/` is not the datasets store.
- The plugin cannot replace or remove a store of Beacon. `register_store` changes only the plugin.
- When Beacon has no datasets store, a relative `LOCATION` stays unchanged and has no store.
- Beacon cannot read a table on an object store as the source of an `INSERT ... SELECT` into a
  plugin table.

### Rules for native code

- Keep the DataFusion major version equal to the version in Beacon. Beacon refuses a different
  version. Beacon also refuses a different SDK ABI version.
- Set `panic = "unwind"`, the default. The SDK refuses `panic = "abort"` at compile time.
- A panic in a plugin call becomes a query error. A fault in native code stops the server.
- Do not panic in getters, such as `name` and `signature`. Do not panic in `Drop`.

## Pack the plugin

A plugin archive is a gzip tar file. The contents of the plugin folder are at the root:

```text
beacon-plugin.toml
sql/functions.sql            SQL files (optional)
lib/libexample_native.so     the native library (optional)
```

Put the library in `lib/`. Beacon reads `lib<name>.so` on Linux, `<name>.dll` on Windows and
`lib<name>.dylib` on macOS. The Beacon Docker image runs on Linux.

Use these archive names:

| Plugin type | Archive name |
| --- | --- |
| Native | `<name>-<version>-df<major>-<target>.tar.gz` |
| SQL only | `<name>-<version>-any.tar.gz` |

`<major>` is the DataFusion major version, for example `54`. `<target>` is the Rust target of the
server, for example `x86_64-unknown-linux-gnu`.

Build the library. Then pack the archive and write `SHA256SUMS`:

```sh
cargo build --release --manifest-path native/Cargo.toml
mkdir -p stage/lib dist
cp beacon-plugin.toml stage/
cp -r sql stage/
cp native/target/release/libexample_native.so stage/lib/
tar -czf dist/example-0.1.0-df54-x86_64-unknown-linux-gnu.tar.gz -C stage .
(cd dist && sha256sum *.tar.gz > SHA256SUMS)
```

`SHA256SUMS` uses the `sha256sum` format. A release can hold the archives of several targets.

## Test the plugin

Mount the `dist` folder into a Beacon container. Then install the archive and call a function.

```sh
docker run -d --name beacon -p 5001:5001 \
  -v ./dist:/plugins:ro \
  -e BEACON_ADMIN_USERNAME=admin -e BEACON_ADMIN_PASSWORD=<password> \
  ghcr.io/maris-development/beacon:v2.1.0-rc3
```

```sql
INSTALL example FROM 'file:///plugins/example-0.1.0-df54-x86_64-unknown-linux-gnu.tar.gz';
SELECT example_add_one(1.5);
SELECT name, status, error FROM beacon.system.plugins;
```

If the status is `failed`, the `error` column gives the reason. Run `UNINSTALL example` before you
install a new build. Restart the server after many reinstalls of a native plugin, because each
reinstall keeps the old library in memory.

## Release the plugin

Create a GitHub release in your plugin repository. Upload each archive and the `SHA256SUMS` file as
release assets. Users then install the plugin with the GitHub source:

```sql
INSTALL example FROM 'github:my-org/my-plugin@v0.1.0'
```

Beacon reads `SHA256SUMS` from the release. Then it picks the archive for its own platform, or the
`any` archive.
