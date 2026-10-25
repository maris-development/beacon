# beacon-plugin-sdk

This crate is the SDK for Beacon native function plugins.
A native plugin is a dynamic library that adds scalar functions to Beacon.
The plugin depends only on this crate.
The crate re-exports `datafusion`, `datafusion_ffi` and `stabby`.
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

Use `function` to build a function from a closure.
Call `export_plugin!` with a function that returns the list of functions.

```rust,no_run
use std::sync::Arc;

use beacon_plugin_sdk::datafusion::arrow::array::{Array, Float64Array};
use beacon_plugin_sdk::datafusion::arrow::datatypes::DataType;
use beacon_plugin_sdk::datafusion::common::exec_err;
use beacon_plugin_sdk::datafusion::logical_expr::ScalarUDF;
use beacon_plugin_sdk::function;

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

beacon_plugin_sdk::export_plugin!(functions);
```

`invoke_arrays` gives the closure arrays only, also for a call such as `add_one(1.5)`.
Use `invoke` to get each argument as an array or a scalar.
A function is `Immutable` by default. Call `stable()` or `volatile()` to change this.
For full control, write a `ScalarUDFImpl` and wrap it with `ScalarUDF::from`.

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

## Rules

- Set `panic = "unwind"`. This is the default. The SDK refuses `panic = "abort"` at compile time.
- Keep the DataFusion major version equal to the version in Beacon. Beacon refuses a different version.
- A fault in native code stops the Beacon server. Test the plugin well.
- A panic in a function becomes a query error.
- Do not panic in getters, such as `name`, `aliases` and `signature`. Do not panic in `Drop`.
