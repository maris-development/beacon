# beacon-plugin-sdk

This crate is the SDK for Beacon native function plugins.
A native plugin is a dynamic library that adds scalar functions to Beacon.
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
- `bounds` and `propagate` also answer DataFusion interval analysis, through `evaluate_bounds` and `propagate_constraints`.
- Beacon folds a constant argument, such as `CAST(2 AS DOUBLE)`, to a literal before `implies` sees it.
- An expression crosses the plugin boundary as `datafusion-proto` bytes. It can call only DataFusion built-in functions.
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

## Rules

- Set `panic = "unwind"`. This is the default. The SDK refuses `panic = "abort"` at compile time.
- Keep the DataFusion major version equal to the version in Beacon. Beacon refuses a different version.
- A fault in native code stops the Beacon server. Test the plugin well.
- A panic in a function becomes a query error.
- Do not panic in getters, such as `name`, `aliases` and `signature`. Do not panic in `Drop`.
