//! Tests of the NetCDF format factory: the table options and the schema cache key.

use std::collections::HashMap;
use std::sync::Arc;

use beacon_arrow_netcdf::attributes::CombineAttrs;
use beacon_arrow_netcdf::datafusion::options::NetcdfOptions;
use beacon_arrow_netcdf::datafusion::{
    NetCDFFormatFactory, NetcdfConfig, NetcdfFormat, NETCDF_EXTENSION,
};
use beacon_datafusion_ext::format_ext::{FileFormatFactoryExt, SchemaOptions};
use beacon_datafusion_ext::listing_factory::ListingFactory;
use datafusion::common::GetExt;
use datafusion::datasource::file_format::FileFormatFactory;
use datafusion::error::Result;
use object_store::path::Path;
use object_store::ObjectMeta;

mod common;
use common::one_partition_session;

fn factory() -> NetCDFFormatFactory {
    NetCDFFormatFactory::new(
        Arc::new(ListingFactory::dynamic()),
        NetcdfOptions::default(),
        NetcdfConfig::default(),
    )
}

/// The options of the format that the factory builds from `format_options`.
fn options_of(pairs: &[(&str, &str)]) -> Result<NetcdfOptions> {
    let format_options: HashMap<String, String> = pairs
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
    let format = factory().create(&one_partition_session().state(), &format_options)?;
    Ok(format
        .downcast_ref::<NetcdfFormat>()
        .expect("the factory builds a NetcdfFormat")
        .options
        .clone())
}

/// A stored table can still hold the option of the removed reader.
#[test]
fn the_removed_reader_option_does_not_change_the_format() {
    for value in ["false", "on"] {
        assert!(options_of(&[("use_rust_reader", value)]).is_ok());
    }

    let error = options_of(&[("use_rust_reader", "maybe")])
        .unwrap_err()
        .to_string();
    assert!(error.contains("use_rust_reader"), "{error}");
}

/// The key is the one that earlier releases wrote for this reader.
#[test]
fn the_schema_cache_key_does_not_change() {
    let factory = factory();
    let format = FileFormatFactory::default(&factory);
    let expected = SchemaOptions::new(NETCDF_EXTENSION).str("oxcdf").finish();
    assert_eq!(
        factory.schema_options_fingerprint(format.as_ref()),
        Some(expected)
    );
}

/// A skipped file caches an empty schema, so the option must change the key.
/// Without the option the key stays the same as for older cache entries.
#[test]
fn skip_invalid_files_changes_the_schema_cache_key() {
    let factory = factory();
    let format = |skip_invalid_files| {
        NetcdfFormat::new(
            Arc::new(ListingFactory::dynamic()),
            NetcdfOptions {
                skip_invalid_files,
                ..NetcdfOptions::default()
            },
        )
    };
    let expected_default = SchemaOptions::new(NETCDF_EXTENSION).str("oxcdf").finish();

    let strict = factory.schema_options_fingerprint(&format(false));
    let lenient = factory.schema_options_fingerprint(&format(true));

    assert_eq!(strict, Some(expected_default));
    assert!(lenient.is_some());
    assert_ne!(strict, lenient);
}

/// The `OPTIONS` of `CREATE EXTERNAL TABLE` reach the format.
#[test]
fn the_factory_reads_the_skip_options() {
    let options = options_of(&[
        ("skip_unbroadcastable", "true"),
        ("skip_invalid_files", "true"),
    ])
    .unwrap();
    assert!(options.skip_unbroadcastable);
    assert!(options.skip_invalid_files);

    let default = options_of(&[]).unwrap();
    assert!(!default.skip_unbroadcastable);
    assert!(!default.skip_invalid_files);
}

#[test]
fn combine_attrs_is_off_by_default() {
    assert_eq!(options_of(&[]).unwrap().writer.combine_attrs, None);
}

#[test]
fn combine_attrs_reads_the_bare_and_the_prefixed_key() {
    let bare = options_of(&[("combine_attrs", "identical")]).unwrap();
    assert_eq!(bare.writer.combine_attrs, Some(CombineAttrs::Identical));

    let prefixed = options_of(&[("format.combine_attrs", "drop_conflicts")]).unwrap();
    assert_eq!(
        prefixed.writer.combine_attrs,
        Some(CombineAttrs::DropConflicts)
    );
}

#[test]
fn a_bad_combine_attrs_is_an_error() {
    let error = options_of(&[("combine_attrs", "union")])
        .unwrap_err()
        .to_string();
    assert!(error.contains("combine_attrs"), "{error}");
}

/// The registry keys a format by these, so each spelling resolves to netCDF.
#[test]
fn the_factory_names_every_netcdf_extension() {
    let factory = factory();
    assert_eq!(factory.get_ext(), "nc");
    assert_eq!(
        factory.file_extensions(),
        vec!["nc", "nc3", "nc4", "netcdf"]
    );
}

/// Discovery keeps every spelling, in any case, and labels it `nc`.
#[test]
fn discovery_keeps_every_netcdf_extension() {
    let objects: Vec<ObjectMeta> = [
        "a.nc",
        "b.nc3",
        "c.nc4",
        "d.netcdf",
        "e.cdf",
        "f.NC4",
        "g.h5",
        "h.ncml",
        "i.parquet",
    ]
    .into_iter()
    .map(|path| ObjectMeta {
        location: Path::from(path),
        last_modified: Default::default(),
        size: 0,
        e_tag: None,
        version: None,
    })
    .collect();

    let datasets = factory().discover_datasets(&objects).unwrap();

    let paths: Vec<&str> = datasets.iter().map(|d| d.file_path.as_str()).collect();
    assert_eq!(paths, ["a.nc", "b.nc3", "c.nc4", "d.netcdf", "f.NC4"]);
    assert!(datasets.iter().all(|d| d.format == "nc"));
}
