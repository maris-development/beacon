# Architecture of beacon-plugin-sdk

This document describes how the SDK is built. For how to write a plugin, read [README.md](README.md).

## Purpose

A native plugin is a dynamic library. Beacon loads it at run time.
The plugin and Beacon are two separate builds. Each build has its own copy of DataFusion, Arrow and tokio.
Rust gives no stable layout for traits and structs across two builds.
The SDK thus gives two things:

- **Traits and types** that a plugin author implements or uses.
- **FFI tables** that carry those traits across the boundary in a stable C layout.

Both sides compile the same SDK crate. The plugin uses the export half. Beacon uses the import half.

## The two sides

```mermaid
flowchart LR
    subgraph PLUGIN["Plugin library (.so / .dll / .dylib)"]
        direction TB
        AUTHOR["Author code<br/>functions, formats, table types"]
        SDKP["beacon-plugin-sdk<br/>export half"]
        AUTHOR --> SDKP
    end

    subgraph BEACON["Beacon server"]
        direction TB
        SDKH["beacon-plugin-sdk<br/>import half"]
        REG["beacon-plugin-registry<br/>opens the library"]
        FMT["beacon-plugin-formats<br/>file formats in Beacon"]
        TBL["beacon-plugin-tables<br/>table types in Beacon"]
        CORE["beacon-core<br/>loader, SQL, session"]
        CORE --> REG
        CORE --> FMT
        CORE --> TBL
        REG --> SDKH
        FMT --> SDKH
        TBL --> SDKH
    end

    SDKP <-->|"C ABI: FFI tables"| SDKH
```

| Crate | Side | Task |
| --- | --- | --- |
| `beacon-plugin-sdk` | both | the one dependency of a plugin: re-exports `beacon-ffi`, adds `export_plugin!` |
| `beacon-ffi` and `beacon-ffi-*` | both | the traits and the FFI tables, one crate for each part |
| `beacon-object-store` | Beacon | the store registry, the secrets and the `LOCATION` rule |
| `beacon-plugin-registry` | Beacon | opens the library, checks the ABI, reads the symbols |
| `beacon-plugin-formats` | Beacon | makes a DataFusion file format from a plugin format |
| `beacon-plugin-tables` | Beacon | makes a table from a plugin table type, and is the host of table code |
| `beacon-core` | Beacon | `INSTALL`, `UNINSTALL`, the loader, the session |

## What a plugin exports

`export_plugin!` generates up to four `extern "C"` symbols. A plugin exports one part or more.

```mermaid
flowchart TB
    MACRO["export_plugin!(scalar = f, formats = g, tables = h)"]
    MACRO --> ABI["beacon_plugin_abi()<br/>SDK ABI and DataFusion FFI major"]
    MACRO --> S["beacon_plugin_scalar_functions()"]
    MACRO --> F["beacon_plugin_file_formats()"]
    MACRO --> T["beacon_plugin_table_types(FFI_Host)"]

    S --> SM["scalar_functions<br/>FFI_ScalarFunction"]
    F --> FM["file_format<br/>FFI_FileFormatFactory"]
    T --> TM["table_type<br/>FFI_PluginTableType"]
```

Only the table types symbol takes an argument: the host. See [The host](#the-host).

## Load sequence

```mermaid
sequenceDiagram
    participant U as User
    participant C as beacon-core loader
    participant R as beacon-plugin-registry
    participant P as Plugin library

    U->>C: INSTALL plugin
    C->>R: load_native(path, host)
    R->>P: open the library
    R->>P: beacon_plugin_abi()
    P-->>R: SDK ABI, DataFusion FFI major
    Note over R: Refuse the plugin when a version differs.<br/>No other symbol is called before this check.
    R->>P: beacon_plugin_scalar_functions()
    R->>P: beacon_plugin_file_formats()
    R->>P: beacon_plugin_table_types(FFI_Host)
    P-->>R: FFI tables
    R-->>C: functions, formats, table types
    Note over C: Check the names. One collision refuses the full plugin.
    C->>C: register in the session
```

A library that opens stays loaded until the process stops. Plugin code can still run after `UNINSTALL`, because plans and streams hold pointers into the library.

## Crates

Each part of the SDK is its own crate in the folder `beacon-db/beacon-ffi/`. A crate holds
the trait of its part and the FFI table of that trait.

```text
beacon-db/
  beacon-ffi/                     beacon-ffi: re-exports each part
    beacon-ffi-abi/               ABI version, panic helpers
    beacon-ffi-object-store/      one object store across the boundary
    beacon-ffi-scalar-functions/  scalar function plugins
    beacon-ffi-file-format/       file format plugins
    beacon-ffi-host/              the Beacon server for table code
    beacon-ffi-table-type/        table type plugins
  beacon-plugin-sdk/              the one dependency of a plugin
  beacon-object-store/            the stores of the server (Beacon only)
```

```mermaid
flowchart TB
    SDK["beacon-plugin-sdk<br/>export_plugin!, re-exports"]
    UMB["beacon-ffi<br/>re-exports each part"]

    subgraph PARTS["Parts that a plugin exports"]
        SF["beacon-ffi-scalar-functions"]
        FF["beacon-ffi-file-format"]
        TT["beacon-ffi-table-type"]
    end

    subgraph SERVICES["Services from Beacon to the plugin"]
        OS["beacon-ffi-object-store<br/>one store across the boundary"]
        HOST["beacon-ffi-host<br/>the Beacon server for table code"]
    end

    ABI["beacon-ffi-abi<br/>ABI version, panic helpers"]

    SDK --> UMB
    UMB --> SF
    UMB --> FF
    UMB --> TT
    FF --> OS
    TT --> HOST
    TT --> FF
    HOST --> OS
    SF --> ABI
    OS --> ABI
```

| Crate | SDK path | Direction | Content |
| --- | --- | --- | --- |
| `beacon-ffi-scalar-functions` | `scalar_functions` | plugin to Beacon | the function builder, planner hooks, scalar arguments |
| `beacon-ffi-file-format` | `file_format` | plugin to Beacon | `FormatFactory` and `FormatReader`, with their FFI tables |
| `beacon-ffi-table-type` | `table_type` | plugin to Beacon | `TableType`, the plugin runtime, the session wrapper |
| `beacon-ffi-object-store` | `object_store` | Beacon to plugin | `FFI_ObjectStore`: one `ObjectStore` across the boundary |
| `beacon-ffi-host` | `host` | Beacon to plugin | `HostApi`, `Host`, `FFI_Host`: the server for table code |
| `beacon-ffi-abi` | crate root | both | `BEACON_PLUGIN_SDK_ABI`, the panic text, a drop that catches a panic |

- **A plugin depends on `beacon-plugin-sdk` only.** The SDK paths, such as `beacon_plugin_sdk::host::Host`, are re-exports.
- **The dependencies have one direction.** `beacon-ffi-abi` is the base. No `beacon-ffi-*` crate depends on `beacon-ffi` or on the SDK.
- **`beacon-object-store` is not part of the SDK.** It builds the S3, GCS and Azure clients. Only Beacon uses it; `BeaconHost` gets its `LOCATION` rule from it.
- **Test helpers of `beacon-ffi-host` are behind the `test-util` feature.** The table type crate and the SDK use them in tests.

The SDK also re-exports `datafusion`, `datafusion_ffi`, `datafusion_proto`, `stabby` and the `object_store` crate. A plugin depends only on the SDK, so its versions match the versions in Beacon.

## The pattern of an FFI table

Each trait that crosses the boundary uses the same three parts.

```mermaid
flowchart LR
    subgraph EXPORTER["Exporter side"]
        IMPL["Arc of dyn Trait<br/>the real value"]
        TABLE["FFI table, repr(C)<br/>function pointers + private_data"]
        IMPL -->|"FFI_X::new"| TABLE
    end

    subgraph IMPORTER["Importer side"]
        FOREIGN["Foreign wrapper<br/>implements Trait"]
        USER["Code that uses dyn Trait"]
        FOREIGN --> USER
    end

    TABLE -->|"TryFrom"| FOREIGN
    FOREIGN -.->|"each call"| TABLE
```

Each table follows these rules:

- **`version` is the first field.** It returns `BEACON_PLUGIN_SDK_ABI`. The importer refuses another version before it reads other fields.
- **`private_data` belongs to the exporter.** Only the exporter reads it. `clone` and `release` manage it.
- **`library_marker_id` identifies the build.** When the importer is the same build, it takes the original value and skips the table. Tests in one binary set another marker to force the foreign path.
- **No panic crosses `extern "C"`.** Each exported function uses `catch_unwind` and returns an error text.
- **Errors cross as text.** The importer makes a DataFusion or object store error from the text.

`BEACON_PLUGIN_SDK_ABI` is one version for all tables. It increases on each change of a table layout.

## Scalar functions

```mermaid
flowchart LR
    B["function(name) builder<br/>or a ScalarUDFImpl"] --> UDF["ScalarUDF"]
    UDF --> PSU["PanicSafeUdf<br/>panic becomes an error"]
    PSU --> FFI["FFI_ScalarFunction<br/>udf + invoke + hooks"]
    FFI -->|"import_scalar_function"| HOSTUDF["ScalarUDF in Beacon"]
    HOSTUDF --> PLAN["Planner: implied, bounds, propagate"]
    HOSTUDF --> EXEC["Execution: invoke"]
```

- The function crosses as a `datafusion-ffi` `FFI_ScalarUDF`.
- `invoke` calls the function and keeps each scalar argument as a scalar.
- `FunctionHooks` are optional. They let a filter on the function prune files.

## File formats

A file format has two traits. Beacon does the work for many files; the plugin reads or writes one file in each call.

```mermaid
flowchart TB
    subgraph PLUGINSIDE["Plugin"]
        FAC["FormatFactory<br/>name, extensions, output kind, writer flags"]
        RDR["FormatReader<br/>infer_schema, infer_stats, open, write"]
        FAC -->|"create(options)"| RDR
    end

    subgraph BEACONSIDE["Beacon: beacon-plugin-formats"]
        PFF["PluginFileFormatFactory"]
        LIST["Lists files, merges schemas,<br/>adds partition columns, splits COPY output"]
        PFF --> LIST
    end

    FAC -->|"FFI_FileFormatFactory"| PFF
    RDR -->|"FFI_FileFormat"| LIST
    LIST -->|"FFI_ObjectStore + one file in each call"| RDR
```

Beacon gives the store to each reader call. A format plugin does all I/O through that store.

## Table types

A table type is a DataFusion `TableProviderFactory` with a `STORED AS` name.
The factory crosses as a `datafusion-ffi` `FFI_TableProviderFactory`. The SDK adds three wrappers around the author code.

```mermaid
flowchart TB
    subgraph PLUGINSIDE["Plugin"]
        direction TB
        CTX["PluginContext<br/>plugin tokio runtime + host binding"]
        RF["RuntimeFactory<br/>wraps create"]
        RP["RuntimeProvider<br/>wraps scan and insert_into"]
        PSP["PanicSafePlan<br/>wraps execute and the streams"]
        AF["Author TableProviderFactory"]
        AT["Author TableProvider"]
        AP["Author ExecutionPlan"]

        RF --> AF
        RP --> AT
        PSP --> AP
        RF -.-> CTX
        RP -.-> CTX
        PSP -.-> CTX
    end

    subgraph BEACONSIDE["Beacon: beacon-plugin-tables"]
        PT["PluginTable<br/>keeps the definition, checks installed, filters pushdown"]
    end

    PT -->|"FFI_TableProviderFactory / FFI_TableProvider"| RF
    PT --> RP
```

Each wrapper has two tasks:

1. **Enter the plugin runtime.** `datafusion-ffi` 54.1 enters the runtime of a table only for record batch streams. The wrappers enter it for each poll of `create`, `scan` and `insert_into`.
2. **Catch panics.** `datafusion-ffi` catches none. A panic becomes a query error.

One `PluginContext` exists for each export call. Each Beacon instance in a process thus has its own plugin runtime and its own host. The SDK keeps no `static` for this.

## The host

The host is the Beacon server as table code sees it. It has three layers, each in its own file.

```mermaid
flowchart TB
    subgraph L1["Interface: beacon-ffi-host, lib.rs (no FFI, no unsafe)"]
        API["trait HostApi<br/>object_store, resolve,<br/>datasets_url, datasets_base_url"]
        H["struct Host<br/>of, open, listing_url, resolve"]
        H -->|"uses"| API
    end

    subgraph L2["Transport: beacon-ffi-host, ffi.rs (all unsafe code of the host)"]
        FH["FFI_Host<br/>one table"]
        FOR["ForeignHost<br/>implements HostApi"]
        FH -->|"TryFrom"| FOR
    end

    subgraph L3["DataFusion adapter: beacon-ffi-host, stores.rs"]
        HS["HostStores<br/>implements ObjectStoreRegistry"]
    end

    subgraph L4["Glue: beacon-ffi-table-type, session.rs"]
        HB["HostBinding"]
        SESS["HostSession<br/>the session that table code gets"]
        HB --> SESS
    end

    BH["BeaconHost in beacon-plugin-tables<br/>implements HostApi"]

    BH -->|"FFI_Host::new"| FH
    FOR -.->|"is a"| API
    BH -.->|"is a"| API
    HS -->|"reads"| API
    HB --> H
    HB --> HS
```

| Layer | Type | Who uses it |
| --- | --- | --- |
| Interface | `HostApi` | Beacon implements it. A plugin test can implement it. |
| Interface | `Host` | plugin table code |
| Transport | `FFI_Host`, `ForeignHost` | the SDK only |
| Adapter | `HostStores` | DataFusion code in the plugin, through the session |
| Glue | `HostBinding`, `HostSession` | the SDK only |

### What the host gives

- **Object stores.** `object_store(url)` returns the Beacon store for a scheme and authority. It crosses as an `FFI_ObjectStore`. The I/O runs in Beacon, with the credentials of Beacon.
- **The `LOCATION` rule.** `resolve(location)` returns the store URL and the path. Beacon owns the rule, so a change needs no plugin rebuild.
- **The datasets store.** `datasets_url()` and `datasets_base_url()`.

The table has no function that changes the host. `HostStores` keeps a store that the plugin registers in the plugin only.

### How table code reaches the host

Table code has two paths to the same stores.

```mermaid
flowchart LR
    CODE["Table code"]
    CODE -->|"Host::of(state)"| HOSTT["Host"]
    CODE -->|"state.runtime_env().object_store(url)"| ENV["RuntimeEnv of HostSession"]
    ENV --> HS["HostStores"]
    HOSTT --> API["HostApi"]
    HS --> API
    API -->|"FFI_Host"| BEACON["BeaconHost: registry of the session"]
```

- **`Host::of(state)`** is the direct path. `HostSession::as_any` returns the `Host`, because a borrowed session cannot be an `Any`.
- **`runtime_env()`** is the path for DataFusion code, such as a `ListingTable`. `HostSession` returns an environment whose registry is `HostStores`. The task context of `execute` gets the same registry.

### A read from the datasets store

```mermaid
sequenceDiagram
    participant Q as Beacon query
    participant PT as PluginTable (Beacon)
    participant RP as RuntimeProvider (plugin)
    participant A as Author scan code
    participant H as Host (plugin)
    participant BH as BeaconHost (Beacon)
    participant S as Datasets store (Beacon)

    Q->>PT: scan(session)
    PT->>RP: FFI scan
    Note over RP: Wrap the session in HostSession.<br/>Enter the plugin runtime.
    RP->>A: scan(HostSession)
    A->>H: Host::of(state)?.open("obs/a.txt")
    H->>BH: resolve("obs/a.txt")
    BH-->>H: store datasets://, path obs/a.txt
    H->>BH: object_store(datasets://)
    BH-->>H: FFI_ObjectStore
    A->>S: get(obs/a.txt) through FFI_ObjectStore
    Note over S: The I/O runs on the Beacon query runtime.
    S-->>A: bytes
    A-->>Q: plan and record batches
```

No lookup result is cached in the plugin. A store that Beacon adds or replaces is visible at the next call.

## Runtimes and threads

Three tokio runtimes can take part in one plugin table query.

```mermaid
flowchart LR
    subgraph BEACONRT["Beacon"]
        API["API runtime<br/>HTTP, startup"]
        QRY["Query runtime<br/>plans, store I/O"]
    end
    subgraph PLUGINRT["Plugin"]
        PRT["Plugin runtime<br/>create, scan, insert_into, streams"]
    end

    QRY -->|"calls table code"| PRT
    PRT -->|"store I/O through FFI_ObjectStore"| QRY
```

- The plugin has its own tokio copy. The Beacon runtime cannot drive plugin I/O, so the SDK starts a plugin runtime for each export call.
- An author can give a runtime with `TableType::with_runtime`. The SDK then starts none for that type.
- An exported store or host enters the Beacon query runtime for each call. The loader passes that handle, not the handle of the thread that loads the plugin.

## Rules that hold in all crates

| Rule | Reason |
| --- | --- |
| `panic = "unwind"` is necessary. The SDK refuses `abort` at compile time. | A panic must become an error, not stop the server. |
| No panic crosses `extern "C"`. | An unwind across the boundary is undefined behavior. |
| The plugin and Beacon must have the same SDK ABI and DataFusion FFI major. | The table layouts must match. |
| No state of one server in a `static`. | Several Beacon instances can share one process and one library. |
| The traits have no FFI types in their signatures. | An author implements a trait and never writes FFI code. |

## Where to put new code

- **A new service from Beacon to table code:** add a method to `HostApi` and a function to `FFI_Host` in `beacon-ffi-host`, and the implementation to `BeaconHost`. Increase `BEACON_PLUGIN_SDK_ABI`.
- **A new kind of plugin part:** add a `beacon-ffi-*` crate with its trait, its FFI table and its foreign wrapper. Re-export it in `beacon-ffi` and in the SDK. Add a key to `export_plugin!` and a symbol to `beacon-plugin-registry`.
- **A change of a table layout:** increase `BEACON_PLUGIN_SDK_ABI` in `beacon-ffi-abi`. Each native plugin then needs a rebuild.
- **A change of which store a URL means:** change `beacon-object-store`. No plugin needs a rebuild.

`beacon-ffi-host` separates the interface from the transport in two files. The older crates `beacon-ffi-file-format` and `beacon-ffi-object-store` keep both together.
