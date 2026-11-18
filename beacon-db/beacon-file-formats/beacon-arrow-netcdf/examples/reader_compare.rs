//! Compares the netcdf-c reader with the `oxcdf` reader, file by file.
//!
//! ```text
//! cargo run -p beacon-arrow-netcdf --example reader_compare -- <folder or file>...
//! ```
//!
//! The exit code is 1 when a file differs.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use beacon_nd_array::arrow::array::ndarray_to_arrow_array;
use beacon_nd_array::dataset::AnyDataset;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;

const EXTENSIONS: [&str; 4] = ["nc", "nc3", "nc4", "netcdf"];

fn collect(path: &Path, files: &mut Vec<PathBuf>) {
    if path.is_dir() {
        let Ok(entries) = std::fs::read_dir(path) else {
            return;
        };
        for entry in entries.flatten() {
            collect(&entry.path(), files);
        }
    } else if path
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| EXTENSIONS.iter().any(|known| ext.eq_ignore_ascii_case(known)))
    {
        files.push(path.to_path_buf());
    }
}

async fn open_oxcdf(path: &Path) -> anyhow::Result<AnyDataset> {
    let path = std::fs::canonicalize(path)?;
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
    let location = object_store::path::Path::from_filesystem_path(&path)?;
    beacon_arrow_netcdf::oxcdf_reader::open_dataset(store, location).await
}

/// The first difference between the two datasets, if one exists.
async fn first_difference(c: &AnyDataset, rust: &AnyDataset) -> anyhow::Result<Option<String>> {
    let mut c_fields: Vec<_> = c.fields().into_iter().collect();
    let mut rust_fields: Vec<_> = rust.fields().into_iter().collect();
    c_fields.sort_by(|a, b| a.0.cmp(&b.0));
    rust_fields.sort_by(|a, b| a.0.cmp(&b.0));

    let c_names: Vec<&String> = c_fields.iter().map(|(name, _)| name).collect();
    let rust_names: Vec<&String> = rust_fields.iter().map(|(name, _)| name).collect();
    if c_names != rust_names {
        let only_c: Vec<_> = c_names.iter().filter(|n| !rust_names.contains(n)).collect();
        let only_rust: Vec<_> = rust_names.iter().filter(|n| !c_names.contains(n)).collect();
        return Ok(Some(format!(
            "fields: only netcdf-c {only_c:?}, only oxcdf {only_rust:?}"
        )));
    }

    for ((name, c_type), (_, rust_type)) in c_fields.iter().zip(&rust_fields) {
        if format!("{c_type:?}") != format!("{rust_type:?}") {
            return Ok(Some(format!(
                "{name}: type netcdf-c {c_type:?}, oxcdf {rust_type:?}"
            )));
        }
        let (Some(c_array), Some(rust_array)) = (c.get_array(name), rust.get_array(name)) else {
            return Ok(Some(format!("{name}: array missing in one reader")));
        };
        if c_array.dimensions() != rust_array.dimensions() || c_array.shape() != rust_array.shape()
        {
            return Ok(Some(format!(
                "{name}: dimensions netcdf-c {:?}{:?}, oxcdf {:?}{:?}",
                c_array.dimensions(),
                c_array.shape(),
                rust_array.dimensions(),
                rust_array.shape()
            )));
        }
        let c_values = ndarray_to_arrow_array(c_array.as_ref()).await;
        let rust_values = ndarray_to_arrow_array(rust_array.as_ref()).await;
        match (c_values, rust_values) {
            (Ok(c_values), Ok(rust_values)) => {
                if c_values.as_ref() != rust_values.as_ref() {
                    return Ok(Some(format!("{name}: values differ")));
                }
            }
            (Err(_), Err(_)) => {}
            (c_values, rust_values) => {
                return Ok(Some(format!(
                    "{name}: read netcdf-c {}, oxcdf {}",
                    outcome(&c_values),
                    outcome(&rust_values)
                )));
            }
        }
    }
    Ok(None)
}

fn outcome<T, E: std::fmt::Display>(result: &Result<T, E>) -> String {
    match result {
        Ok(_) => "ok".to_string(),
        Err(e) => format!("error ({e})"),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut files = Vec::new();
    for arg in std::env::args().skip(1) {
        collect(Path::new(&arg), &mut files);
    }
    files.sort();

    let mut differ = 0usize;
    for file in &files {
        let c = beacon_arrow_netcdf::reader::open_dataset(file).await;
        let rust = open_oxcdf(file).await;
        let difference = match (&c, &rust) {
            (Ok(c), Ok(rust)) => first_difference(c, rust)
                .await?
                .map(|what| format!("DIFF {}: {what}", file.display())),
            (Err(_), Err(_)) => None,
            _ => Some(format!(
                "OPEN-DIFF {}: netcdf-c={} oxcdf={}",
                file.display(),
                outcome(&c),
                outcome(&rust)
            )),
        };
        if let Some(line) = difference {
            differ += 1;
            println!("{line}");
        }
    }

    println!("compared {} files, {differ} differ", files.len());
    if differ > 0 {
        std::process::exit(1);
    }
    Ok(())
}
