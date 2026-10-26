# beacon-plugin-registry

This crate holds the plugin logic of Beacon that does not need `beacon-core`.
It reads, checks, stores and loads plugins.
It does not register functions in DataFusion and does not plan SQL.
`beacon-core` does those steps and calls this crate.

A plugin adds scalar functions to Beacon.
It has native functions (a dynamic library), SQL functions (`CREATE FUNCTION` files), or both.
Plugin authors use the `beacon-plugin-sdk` crate.

## Modules

| Module | Job |
|---|---|
| `manifest` | Parses `beacon-plugin.toml`. Checks the plugin name and the Beacon version range. |
| `archive` | Packs and unpacks plugin archives. Computes and reads SHA-256 sums. |
| `source` | Parses `INSTALL` sources. Downloads archives. Checks the allowlist. |
| `native` | Opens a native library. Checks its ABI. Reads its functions. |
| `store` | Reads and writes the plugin folders in the plugins directory. |
| `registry` | Keeps the state of each plugin and the owner of each function name. |
| `names` | Shares the names of the native functions with other parts of Beacon. |

## The plugins directory

The plugins directory is the only store of plugins. `beacon.db` holds no copy.

```text
<plugins_dir>/
  example/
    beacon-plugin.toml      the manifest
    install.json            the install record
    sql/functions.sql       SQL functions (optional)
    lib/libexample.so       the native library (optional)
  .tmp-<random>/            an install or an uninstall that is not complete
```

- Each folder name is a plugin name: lowercase letters, digits and `_`, with a letter first.
- `store::list` skips `.tmp-*` folders, files and folders with a name that is not a plugin name.
- A folder without a readable `install.json` is a broken entry. The caller shows it as failed.

## The manifest

```toml
[plugin]
name = "example"
version = "0.1.0"
beacon = ">=2.1, <3"

[native]
crate = "native"
library = "example"

[sql]
files = ["sql/functions.sql"]
```

- A plugin needs `[native]`, `[sql]` or both.
- `check_beacon` ignores the pre-release part of the Beacon version, so `2.1.0-rc.1` matches `>=2.1`.
- `library` is the base name. `native::library_file_name` adds the platform prefix and suffix.

## Archives

- A plugin archive is a gzip tar with the plugin folder content at its root.
- Release names: `<name>-<version>-df<major>-<target>.tar.gz` for a native plugin, and `<name>-<version>-any.tar.gz` for a SQL plugin.
- `SHA256SUMS` lists the archives of a release, in `sha256sum` format.
- `pack_dir` makes the same bytes for the same folder, so the SHA-256 is stable.

`unpack` reads data from sources that Beacon does not trust. It does these checks:

- It checks every entry before it writes a file. A bad archive writes nothing.
- It refuses absolute paths, `..`, links and special files.
- It skips pax header entries, such as the commit id that `git archive` adds.
- It refuses a compressed archive larger than 128 MiB, and more than 512 MiB of unpacked data.
- Unpack only into a new, empty folder. `unpack` follows a link that is already in the target folder.

## Sources

| Source | Example | Checksum |
|---|---|---|
| File | `file:///srv/plugins/example-0.1.0-any.tar.gz` | `SHA256SUMS` next to the file, when it exists |
| Folder | `file:///srv/plugins/example/` | none: the folder is packed |
| HTTPS | `https://plugins.example.org/example/0.1.0/` | `SHA256SUMS` in the folder, mandatory |
| GitHub | `github:my-org/my-plugin@v0.1.0` | the `SHA256SUMS` release asset, mandatory |

- `source::fetch_archive` picks the native archive for this build first, else the `any` archive.
- `source::check_allowed` lets `file://` through. A remote source must start with a prefix from the allowlist.
- A prefix matches only at a boundary: the prefix ends in `/`, the source equals the prefix, or the next character is `/` or `@`.
- A source with a user name or a password is refused, also in `check_allowed`.
- Plain `http://` works only for the local computer (`127.0.0.1`, `[::1]` or `localhost`).
- A redirect goes only to `https://`, or to a local `http://` host when the source is local.
- A download larger than 128 MiB is refused.

`fetch_archive` does not check the allowlist. Call `check_allowed` first.

## Native libraries

`native::load_native` opens the library and does these checks:

1. The library file for this platform exists: `lib<name>.so`, `<name>.dll` or `lib<name>.dylib`. When it is missing, the error names the files in `lib/`, such as a `.so` file on Windows.
2. The library has the `beacon_plugin_abi` and `beacon_plugin_functions` symbols.
3. The SDK ABI of the plugin equals `BEACON_PLUGIN_SDK_ABI`.
4. The DataFusion FFI major version of the plugin equals the version of this build.
5. The plugin init returns its functions without an error.

A library that opens stays loaded until the process stops, also when a check fails.
The functions point into its code, and the library ran code when it opened.
So each install of a native plugin adds one library to the memory of the process.

## Names and ownership

The `registry` records which plugin owns each function name and alias.

- `begin_install` reserves a plugin name. A loaded name, a failed name or an install that runs refuses a new install.
- `begin_uninstall` reserves a loaded or failed plugin for removal.
- `try_claim` takes all names of a plugin in one step, or none of them. Two installs of the same function name cannot both succeed.
- `claim_guard` releases the claimed names when the install fails or stops. Call `disarm` when the install is complete.
- `add_loaded` and `add_failed` record the result. `set_uninstalled` frees the plugin name.

## Install flow

The caller in `beacon-core` uses this crate in this order:

1. `PluginSource::parse`, then `check_allowed`, then `registry.begin_install`.
2. `fetch_archive` reads the archive and its SHA-256.
3. `unpack` into `<plugins_dir>/.tmp-<random>/`, then `PluginManifest::parse` and `check_beacon`.
4. `load_native` for a native plugin. The caller plans the SQL files.
5. `registry.try_claim` and `claim_guard` for every function name and alias.
6. `store::write_record`, then rename the temp folder to `<plugins_dir>/<name>/`.
7. The caller registers the functions. Then `add_loaded` and `claim_guard.disarm()`.

At startup, the caller deletes the `.tmp-*` folders, calls `store::list`, and loads each folder from step 3.
It does not check the SHA-256 again.
