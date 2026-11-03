---
description: Extend a Beacon server with plugins. Add SQL functions, native functions, file formats and table types. Start a new plugin from the beacon-plugin-template repository.
---

# Plugins

A plugin adds features to a running Beacon server. You do not build a new Beacon release, and you do
not change the server image.

| A plugin adds | You use it with |
| --- | --- |
| Scalar functions in SQL | `SELECT my_function(x)` |
| Scalar functions in Rust | `SELECT my_function(x)` |
| File formats | `read_<name>(...)`, `STORED AS <name>` and `COPY ... TO ... STORED AS <name>` |
| Table types | `CREATE EXTERNAL TABLE ... STORED AS <name>` |

## SQL plugins

A SQL plugin is the simplest plugin. It holds `CREATE FUNCTION` statements and a manifest. It has no
Rust code, so it works on every server and every platform.

```sql
CREATE FUNCTION example_add_two(x DOUBLE) RETURNS DOUBLE
RETURN x + 2;
```

Use a SQL plugin to share a set of functions between servers. A function in a plugin works like a
[user function](/docs/2.1.0-rc2/sql/create-function), but the plugin installs and uninstalls the
whole set in one step.

A native plugin holds a Rust library. Only a native plugin adds file formats and table types. A
plugin can hold native code and SQL files together.

## Install a plugin

Only a super-user can install a plugin.

```sql
INSTALL example FROM 'github:my-org/my-plugin@v0.1.0';

SELECT name, version, status, error FROM beacon.system.plugins;

UNINSTALL example;
```

To install plugins at each start, set `BEACON_PLUGINS`:

```sh
BEACON_PLUGINS=example=github:my-org/my-plugin@v0.1.0
```

Beacon keeps the plugins in `<BEACON_DATA_DIR>/plugins`. With Docker, mount
`/beacon/data/plugins`, or set `BEACON_PLUGINS`. Then the plugins stay when you replace the
container.

::: warning
A native plugin runs in the Beacon process with the permissions of the server. A fault in native
code stops the server. Install a native plugin only from a source that you trust.
:::

The [Plugins SQL reference](/docs/2.1.0-rc2/sql/plugins) gives all sources, the startup rules, the
storage, the system table and the limits.

## Write a plugin

Start from the
[beacon-plugin-template](https://github.com/maris-development/beacon-plugin-template) repository.
It is a GitHub template with a working example of each plugin part:

| Part | Example in the template |
| --- | --- |
| Native functions | `example_add_one`, `example_hypot`, `example_in_box`, `example_round` |
| SQL function | `example_add_two` |
| Planner hooks | `example_add_one`, `example_in_box` |
| File format | `example_tsv` |
| Table type | `example_memory` |

The template also holds a release workflow. A tag `v<version>` builds the archives for Linux, macOS
and Windows, writes `SHA256SUMS` and attaches them to the GitHub release.

1. Click **Use this template** on GitHub. Clone the new repository.
2. Rename the plugin in `beacon-plugin.toml`, `native/Cargo.toml` and the code.
3. Write your functions, file formats and table types. Delete the parts that you do not use.
4. Run `cargo test --manifest-path native/Cargo.toml`.
5. Set `plugin.version` in `beacon-plugin.toml`. Push a tag `v<version>`.
6. Install the release: `INSTALL <name> FROM 'github:<owner>/<repo>@v<version>'`.

The [template wiki](https://github.com/maris-development/beacon-plugin-template/wiki) explains each
part in detail. [Write a plugin](/docs/2.1.0-rc2/guides/write-a-plugin) explains the manifest, the
archive layout and a local test.

::: tip
Build a native plugin with the SDK of the Beacon release that runs it. Beacon refuses a plugin with a
different DataFusion major version or SDK ABI version.

The template takes `beacon-plugin-sdk` from the `main` branch of Beacon. `main` can change the SDK
ABI before a release. In `native/Cargo.toml`, replace `branch = "main"` with the tag of your
server release:

```toml
beacon-plugin-sdk = { git = "https://github.com/maris-development/beacon", tag = "v2.1.0-rc2" }
```
:::
