//! Inferring a table's schema, one schema per URL.
//!
//! The format infers the schema of each URL with `ListingOptions::infer_schema`.
//! A URL that matches no file gets an error that names the path.

use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::Session,
    common::exec_datafusion_err,
    datasource::listing::{ListingOptions, ListingTableUrl},
    error::DataFusionError,
};
use futures::{StreamExt, TryStreamExt, future};

use crate::type_widening::LabeledSchema;

/// One schema per URL.
///
/// The caller merges them. `FastObjectTable` applies the session's widening
/// rule, and an external table has one URL and takes it as it is — so the split
/// stays here rather than being folded into one schema, and neither caller's
/// merge changes.
///
/// Each schema names its URL. A merge across URLs that refuses a column then
/// names both URLs. A merge inside one URL names the two files. See issue #424.
pub async fn infer_url_schemas(
    state: &dyn Session,
    options: &ListingOptions,
    urls: &[ListingTableUrl],
) -> Result<Vec<LabeledSchema>, DataFusionError> {
    let mut schemas = Vec::with_capacity(urls.len());
    for url in urls {
        tracing::debug!("Infer schema for table/file url: {}", url);
        let outcome = options.infer_schema(state, url).await;
        let schema = name_the_path_if_nothing_matched(state, options, url, outcome).await?;
        schemas.push(LabeledSchema::new(schema, url.as_str()));
    }
    Ok(schemas)
}

/// Turn "the format could not infer a schema" into "the path matched no file",
/// where that is what happened.
///
/// A path that matches nothing is the most common mistake a caller makes, and each
/// format used to report it in its own words, or not at all:
///
/// - Parquet, CSV, Arrow IPC, ODV and BBF merged zero schemas, so the merge rule's
///   own complaint reached the caller: "Failed to infer schema: Schema error: No
///   schemas provided for merging". It names neither the path nor the cause.
/// - netCDF, HDF5, GeoParquet and TIFF answered with an empty schema, so a
///   mistyped path returned zero rows and zero columns, and no error at all. That
///   reads exactly like a table that is genuinely empty.
/// - Zarr, Delta, Iceberg and Icechunk already named the path. They stay as they
///   are: Delta, Iceberg and Icechunk never reach this function, and Zarr reaches
///   it only when its store did list objects.
///
/// The listing is only consulted when the outcome is already an error or an empty
/// schema, so a query that succeeds pays nothing for this. It also keeps a
/// format's own message when the path *did* match: "files are here, but I cannot
/// read them" is a different fault from "nothing is here", and only the format
/// knows the first one.
async fn name_the_path_if_nothing_matched(
    state: &dyn Session,
    options: &ListingOptions,
    url: &ListingTableUrl,
    outcome: Result<SchemaRef, DataFusionError>,
) -> Result<SchemaRef, DataFusionError> {
    match outcome {
        Ok(schema) if schema.fields().is_empty() => {
            match empty_listing_error(state, options, url).await {
                Some(error) => Err(error),
                // A format may report no column for a path that does hold files.
                // That is the format's answer, so keep it.
                None => Ok(schema),
            }
        }
        Err(error) => Err(empty_listing_error(state, options, url)
            .await
            .unwrap_or(error)),
        Ok(schema) => Ok(schema),
    }
}

/// `Some(error)` when `url` matches no object, naming the path the caller gave.
///
/// `None` when the path holds at least one object, and also when the listing
/// itself fails: the caller then keeps the error it already had, which describes
/// the fault better than a second failure would.
async fn empty_listing_error(
    state: &dyn Session,
    options: &ListingOptions,
    url: &ListingTableUrl,
) -> Option<DataFusionError> {
    lists_no_file(state, options, url).await.then(|| {
        exec_datafusion_err!(
            "no file matched '{}'{}",
            // `as_str` gives the prefix alone: a glob is parsed off the path and
            // held beside it, so a caller who mistyped `obs/*.parquet` would be
            // shown `obs/` and read it as the directory being wrong.
            match url.get_glob() {
                Some(glob) => format!("{}{}", url.as_str(), glob.as_str()),
                None => url.as_str().to_string(),
            },
            match options.file_extension.as_str() {
                "" => String::new(),
                extension => format!(" (this reader reads a '{extension}' file)"),
            }
        )
    })
}

/// `true` when `url` lists no object with data.
///
/// `false` when the listing itself fails: a listing that fails is not an empty
/// listing.
pub async fn lists_no_file(
    state: &dyn Session,
    options: &ListingOptions,
    url: &ListingTableUrl,
) -> bool {
    let Ok(store) = state.runtime_env().object_store(url) else {
        return false;
    };
    // One page of the listing answers this. A collection of 100000 files costs the
    // same as a collection of one.
    let Ok(objects) = url
        .list_all_files(state, store.as_ref(), &options.file_extension)
        .await
    else {
        return false;
    };
    let mut objects = Box::pin(objects.try_filter(|object| future::ready(object.size > 0)));
    objects.next().await.is_none()
}
