# beacon-plugin-sdk

This crate is the SDK for Beacon native function plugins.
A native plugin is a dynamic library that adds scalar functions, file formats or table types to Beacon.
The plugin depends only on this crate.
The crate re-exports `datafusion`, `datafusion_ffi`, `datafusion_proto` and `stabby`.
Use these copies. Then the versions match the version in Beacon.

## Add the dependency

Use a Git dependency. Set the tag to the Beacon release that the plugin targets.
The tag fixes the DataFusion version.

```toml
[lib]
crate-type = ["cdylib"]

[dependencies]
beacon-plugin-sdk = { git = "https://github.com/maris-development/beacon", tag = "v2.1.0" }
```

## Write the functions

Scalar functions live in the `scalar_functions` module. Use `function` to build one from a closure.
Call `export_plugin!` with `scalar =` and a function that returns the list of functions.

```rust,no_run
use std::sync::Arc;

use beacon_plugin_sdk::datafusion::arrow::array::{Array, Float64Array};
use beacon_plugin_sdk::datafusion::arrow::datatypes::DataType;
use beacon_plugin_sdk::datafusion::common::exec_err;
use beacon_plugin_sdk::datafusion::logical_expr::ScalarUDF;
use beacon_plugin_sdk::scalar_functions::function;

fn functions() -> Vec<ScalarUDF> {
    let add_one = function("add_one")
        .args([DataType::Float64])
        .returns(DataType::Float64)
        .alias("inc")
        .invoke_arrays(|arrays| {
            let Some(values) = arrays[0].as_any().downcast_ref::<Float64Array>() else {
                return exec_err!("add_one expects a Float64 argument");
            };
            let result: Float64Array = values.iter().map(|v| v.map(|v| v + 1.0)).collect();
            Ok(Arc::new(result))
        });
    vec![add_one]
}

beacon_plugin_sdk::export_plugin!(scalar = functions);
```

`invoke_arrays` gives the closure arrays only, also for a call such as `add_one(1.5)`.
Use `invoke` to get each argument as an array or a scalar.
A literal argument, such as `2.0` in `f(x, 2.0)`, arrives as a `ColumnarValue::Scalar`.
So the body of `invoke` or of a `ScalarUDFImpl` must accept a scalar for each argument.
A function is `Immutable` by default. Call `stable()` or `volatile()` to change this.
For full control, write a `ScalarUDFImpl` and wrap it with `ScalarUDF::from`.

## Add planner hooks

DataFusion cannot see into a plugin function, so a filter on it prunes no files.
Planner hooks tell Beacon what a call means. All hooks are optional.

| Builder method | Hook | Use |
|---|---|---|
| `implies(f)` | `FunctionHooks::implied` | A plain predicate that the call implies. |
| `bounds(f)` | `FunctionHooks::bounds` | The output interval for the argument intervals. |
| `propagate(f)` | `FunctionHooks::propagate` | The argument intervals for an output interval. |

```rust,no_run
use beacon_plugin_sdk::datafusion::arrow::datatypes::DataType;
use beacon_plugin_sdk::datafusion::logical_expr::ScalarUDF;
use beacon_plugin_sdk::scalar_functions::{Implied, function};

fn in_box() -> ScalarUDF {
    function("in_box")
        .args(vec![DataType::Float64; 6])
        .returns(DataType::Boolean)
        // in_box(lon, lat, 2, 50, 8, 56) implies lon BETWEEN 2 AND 8 AND lat BETWEEN 50 AND 56.
        .implies(|args| {
            let lon = args[0].clone().between(args[2].clone(), args[4].clone());
            let lat = args[1].clone().between(args[3].clone(), args[5].clone());
            Ok(Some(Implied::Necessary(lon.and(lat))))
        })
        .invoke_arrays(|_arrays| todo!())
}
```

Beacon applies the hooks once per query, after type coercion and before filter pushdown.
Then file statistics pruning, table providers and remote sources see plain predicates.

- `Implied::Necessary(expr)`: Beacon adds `expr` next to the call. The rule must be correct. A row where the call is true and `expr` is not true disappears from the result.
- `Implied::Exact(expr)`: Beacon replaces the call with `expr`. Beacon trusts this rule.
- `propagate`: for a filter `f(..)` or `f(..) <op> <literal>`, Beacon adds `col >= low` and `col <= high` for each column argument. Beacon uses it only when `implies` gives no rule.
- For an argument that is a call with hooks, such as `f(g(x))`, Beacon calls the `bounds` hook of `g` to get the input interval of `f`. Beacon then calls the `propagate` hook of `g` with the interval that `f` gives for that argument. So the range reaches column `x`, also when `f` and `g` come from different plugins.
- Beacon also follows `+` and `-` with a constant, `*` and `/` with a constant for float types, negation and casts. So `f(x + 1)` gives a range on `x`, and `f(CAST(x AS DOUBLE))` gives a range on the cast column.
- A conjunct such as `x >= 0` or `x BETWEEN 2 AND 8` in the same filter gives the input interval of column `x`. So a `propagate` hook that uses its input intervals can relate columns, such as `gap(a, b) > 45 AND b >= 0`, which gives `a >= 45`.
- `bounds` and `propagate` also answer DataFusion interval analysis, through `evaluate_bounds` and `propagate_constraints`.
- Beacon folds a constant argument, such as `CAST(2 AS DOUBLE)`, to a literal before `implies` sees it.
- Beacon applies the `Exact` rules of calls inside the arguments before `implies` sees them.
- An expression crosses the plugin boundary as `datafusion-proto` bytes. Both sides decode it with the functions of the Beacon session. So the arguments and the result of `implies` can call any function that Beacon knows, also a function of another plugin.
- A function from another library is an FFI wrapper. Find it by its name, not with a downcast.
- A hook that returns `Ok(None)`, an error or a panic gives no answer. The query does not fail.
- For a `ScalarUDFImpl`, implement `FunctionHooks` and wrap the UDF with `HookedUdf::new`.

## Write the manifest

Put a `beacon-plugin.toml` file in the plugin folder.

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

## Object stores

The `object_store` module moves an `object_store::ObjectStore` across the plugin boundary.
The module re-exports the `object_store` crate as `object_store::store`.
Use this copy. The SDK pins `object_store` 0.13, the version in Beacon.

- `FFI_ObjectStore` is the FFI table. `FFI_ObjectStore::new(store, handle)` exports an `Arc<dyn ObjectStore>`.
- `Arc::<dyn ObjectStore>::try_from(table)` imports a table. A table from another library becomes a `ForeignObjectStore`.
- A table from the same library gives back the original store.
- The import refuses a table with a different `BEACON_PLUGIN_SDK_ABI`. All FFI tables of the SDK carry this one version.

```rust,no_run
use std::sync::Arc;

use beacon_plugin_sdk::object_store::FFI_ObjectStore;
use beacon_plugin_sdk::object_store::store::path::Path;
use beacon_plugin_sdk::object_store::store::{ObjectStore, ObjectStoreExt, Result};

async fn read(table: FFI_ObjectStore) -> Result<usize> {
    let store = Arc::<dyn ObjectStore>::try_from(table)?;
    let bytes = store.get(&Path::from("data/file.nc")).await?.bytes().await?;
    Ok(bytes.len())
}
```

### Runtime

- No tokio runtime handle crosses the boundary.
- The exporter keeps its own runtime handle. Each poll of an exported future or stream enters that runtime.
- The plugin does not need a runtime for store calls.
- Plugin code that uses tokio I/O, such as `tokio::fs` or `tokio::net`, must build its own runtime.
- The API is async. To block, call `futures::executor::block_on` on a thread outside a tokio runtime.
- Do not block inside a tokio runtime.

### Errors and cleanup

- A panic in the exporter store becomes an `Error::Generic`. The message starts with `panic in object store call:`.
- A dropped get or list stream releases the exporter stream at once.
- A dropped multipart upload without `complete` or `abort` aborts the upload in the background.
- `object_store` itself does not abort a dropped upload. The import of a local table keeps that behavior.
- On the raw `FFI_MultipartUpload` table, `put_part` fails while a `complete` or `abort` is pending.
- `http::Extensions` does not cross. The exporter gets an empty map.

## File formats

The `file_format` module lets a plugin add a file format to Beacon.
Implement two traits:

- `FormatFactory` describes the format: name, extensions, output kind, `read_<name>` arguments and writer flags.
- `FormatFactory::create` gets the table options and returns a `FormatReader`.
- `FormatReader` reads and writes one file in each call.

Export the factories with the `formats` key:

```rust,ignore
beacon_plugin_sdk::export_plugin!(scalar = functions, formats = file_formats);
```

Beacon does the work for many files:

- It lists the files and merges their schemas. It widens the types.
- It casts each column to the table type. It fills a missing column with nulls.
- It adds the partition columns.
- It applies the filters again. The filters in `ScanRequest` are hints only.
- For `COPY ... TO`, it splits the output into files. It calls `write` one time for each output file.

### Output kinds

- `OutputKind::Flat`: the batches have normal Arrow columns.
- `OutputKind::Nd`: each column is a `beacon.nd` struct. Use `beacon_plugin_sdk::nd` to build an `NdRecordBatch` and call `encode_nd_record_batch`. Beacon broadcasts the arrays to rows.

### Rules for format code

- Do all I/O through the given `ObjectStore`.
- Do not call `tokio::spawn`, `tokio::time` or `Handle::current()`. The plugin copy of tokio has no runtime there.
- CPU work in a poll blocks one Beacon worker thread. Keep each poll short.
- When `WriteRequest::local_path` is set, write the whole output to that path. Beacon uploads it when necessary. Do not write through the store then.
- When `local_path` is not set, write through the store. Use a multipart upload for a large file.
- Remove your own spool files, also on an error.
- A panic in a call, a future or a stream becomes a query error.

## Table types

The `table_type` module lets a plugin add an external table type, such as a remote catalog or a web service.
Implement a DataFusion `TableProviderFactory`. Give it a name in a `TableType`.
Export the types with the `tables` key.

```rust,no_run
use std::sync::Arc;

use beacon_plugin_sdk::datafusion::arrow::datatypes::{DataType, Field, Schema};
use beacon_plugin_sdk::datafusion::catalog::{Session, TableProvider, TableProviderFactory};
use beacon_plugin_sdk::datafusion::datasource::MemTable;
use beacon_plugin_sdk::datafusion::error::Result;
use beacon_plugin_sdk::datafusion::logical_expr::CreateExternalTable;
use beacon_plugin_sdk::table_type::{TableType, table_option};

#[derive(Debug)]
struct Sensors;

#[async_trait::async_trait]
impl TableProviderFactory for Sensors {
    async fn create(
        &self,
        _state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        let _url = &cmd.location;
        let _token = table_option(cmd, "token");
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Float64, true)]));
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![]])?))
    }
}

fn table_types() -> Vec<TableType> {
    vec![TableType::new("sensors", Arc::new(Sensors))]
}

beacon_plugin_sdk::export_plugin!(tables = table_types);
```

The user then creates a table:

```sql
CREATE EXTERNAL TABLE readings STORED AS sensors
LOCATION 'https://example.com/api' OPTIONS ('token' 'abc');
SELECT * FROM readings;
INSERT INTO readings VALUES (1.5);
```

- `cmd` has the table name, `LOCATION`, `OPTIONS`, the column list and `PARTITIONED BY`.
- DataFusion prefixes an option key without a `.` with `format.`. `table_option` reads both forms.
- `SELECT` calls `scan`. `INSERT INTO` calls `insert_into`. The DataFusion default of `insert_into` refuses the write.
- `DELETE` and `UPDATE` are not available on plugin tables.
- A name must be lower case `[a-z0-9_]+`. It must not equal a Beacon table type or a file format.
- Beacon stores the table and calls `create` again at each start. When the plugin is missing, Beacon skips the table and keeps its definition.
- Beacon stores `LOCATION` and `OPTIONS` as plain text in the table definition. Do not put secrets there that the storage must not see.

### Rules for table code

- The table gets no Beacon object store. Open your own connections.
- The SDK runs `create`, `scan`, `insert_into` and the output streams inside a tokio runtime of the plugin. Tokio I/O, `tokio::spawn` and `tokio::time` work there.
- Filters arrive as DataFusion expressions. Beacon sends only filters that use DataFusion functions. Beacon applies the other filters itself.
- A panic in a call, a future or a stream becomes a query error.

## Rules

- Set `panic = "unwind"`. This is the default. The SDK refuses `panic = "abort"` at compile time.
- Keep the DataFusion major version equal to the version in Beacon. Beacon refuses a different version.
- A fault in native code stops the Beacon server. Test the plugin well.
- A panic in a function becomes a query error.
- Do not panic in getters, such as `name`, `aliases` and `signature`. Do not panic in `Drop`.
