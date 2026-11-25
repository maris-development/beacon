//! Small Icechunk repositories, built in code, for tests.
//!
//! This is test support, not part of the read path: it is the only place in the
//! crate that *writes* Icechunk. `beacon-core`'s integration tests build the
//! same repositories the crate's own tests use, so the fixture lives here
//! rather than being duplicated per test binary.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use zarrs::array::{ArrayBuilder, data_type};
use zarrs::group::GroupBuilder;
use zarrs_icechunk::AsyncIcechunkStore;
use zarrs_icechunk::icechunk::format::manifest::{VirtualChunkLocation, VirtualChunkRef};
use zarrs_icechunk::icechunk::{Repository, Store, storage};

/// The two versions [`write_gridded_repository`] commits.
pub struct FixtureSnapshots {
    /// The first commit: `sst` holds [`FIRST_SST`].
    pub first: String,
    /// The second commit: the same cells hold [`SECOND_SST`].
    pub second: String,
}

/// Grid extents of the fixture: 1 time step × 4 latitudes × 5 longitudes.
pub const TIME: usize = 1;
pub const LATS: usize = 4;
pub const LONS: usize = 5;
/// Rows a full scan of the fixture returns.
pub const ROWS: usize = TIME * LATS * LONS;

/// `sst` values written by the first commit, and by the second.
pub const FIRST_SST: f64 = 10.0;
pub const SECOND_SST: f64 = 20.0;

fn attributes(pairs: &[(&str, serde_json::Value)]) -> serde_json::Map<String, serde_json::Value> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_string(), value.clone()))
        .collect()
}

/// Write a two-commit Icechunk repository at `path` and return both snapshot ids.
///
/// The hierarchy is a CF-style grid — `lat`, `lon`, `time` coordinates and an
/// `sst` variable over all three — so the reader exercises dimension handling,
/// CF time decoding and attribute columns, exactly as a plain zarr store does.
/// The second commit rewrites `sst`, leaving the first snapshot's values intact.
pub async fn write_gridded_repository(path: &Path) -> anyhow::Result<FixtureSnapshots> {
    std::fs::create_dir_all(path)
        .with_context(|| format!("failed to create {}", path.display()))?;
    let storage = storage::new_local_filesystem_storage(path)
        .await
        .context("failed to open local Icechunk storage")?;
    let repository = Repository::create(None, storage, HashMap::new(), None, true)
        .await
        .context("failed to create the Icechunk repository")?;

    let first = write_commit(&repository, FIRST_SST, "initial grid").await?;
    let second = write_commit(&repository, SECOND_SST, "rewrite sst").await?;

    Ok(FixtureSnapshots { first, second })
}

/// Land one more commit on `main` of an existing fixture repository, rewriting
/// `sst` with `sst_value`. Returns the new snapshot id.
pub async fn append_commit(path: &Path, sst_value: f64) -> anyhow::Result<String> {
    let storage = storage::new_local_filesystem_storage(path)
        .await
        .context("failed to open local Icechunk storage")?;
    let repository = Repository::open(None, storage, HashMap::new())
        .await
        .context("failed to open the Icechunk repository")?;
    write_commit(&repository, sst_value, "rewrite sst again").await
}

/// Write the whole hierarchy with `sst` set to `sst_value`, and commit it.
async fn write_commit(
    repository: &Repository,
    sst_value: f64,
    message: &str,
) -> anyhow::Result<String> {
    let session = repository
        .writable_session("main")
        .await
        .context("failed to open a writable Icechunk session")?;
    let store = Arc::new(AsyncIcechunkStore::new(session));

    GroupBuilder::new()
        .attributes(attributes(&[
            ("Conventions", serde_json::json!("CF-1.8")),
            ("title", serde_json::json!("Icechunk gridded example")),
        ]))
        .build(store.clone(), "/")?
        .async_store_metadata()
        .await?;

    // Coordinates. `time` is CF-encoded so the reader surfaces it as a timestamp.
    let lat: Vec<f32> = (0..LATS).map(|i| 40.0 + i as f32).collect();
    let lon: Vec<f32> = (0..LONS).map(|i| 10.0 + i as f32).collect();
    let time: Vec<i32> = vec![0; TIME];

    let mut builder = ArrayBuilder::new(
        vec![LATS as u64],
        vec![LATS as u64],
        data_type::float32(),
        0.0f32,
    );
    let array = builder
        .dimension_names(Some(["lat"]))
        .attributes(attributes(&[("units", serde_json::json!("degrees_north"))]))
        .build(store.clone(), "/lat")?;
    array.async_store_metadata().await?;
    array.async_store_chunk(&[0], lat.as_slice()).await?;

    let mut builder = ArrayBuilder::new(
        vec![LONS as u64],
        vec![LONS as u64],
        data_type::float32(),
        0.0f32,
    );
    let array = builder
        .dimension_names(Some(["lon"]))
        .attributes(attributes(&[("units", serde_json::json!("degrees_east"))]))
        .build(store.clone(), "/lon")?;
    array.async_store_metadata().await?;
    array.async_store_chunk(&[0], lon.as_slice()).await?;

    let mut builder = ArrayBuilder::new(
        vec![TIME as u64],
        vec![TIME as u64],
        data_type::int32(),
        0i32,
    );
    let array = builder
        .dimension_names(Some(["time"]))
        .attributes(attributes(&[(
            "units",
            serde_json::json!("seconds since 1981-01-01"),
        )]))
        .build(store.clone(), "/time")?;
    array.async_store_metadata().await?;
    array.async_store_chunk(&[0], time.as_slice()).await?;

    let mut builder = ArrayBuilder::new(
        vec![TIME as u64, LATS as u64, LONS as u64],
        vec![TIME as u64, LATS as u64, LONS as u64],
        data_type::float64(),
        0.0f64,
    );
    let array = builder
        .dimension_names(Some(["time", "lat", "lon"]))
        .attributes(attributes(&[("units", serde_json::json!("kelvin"))]))
        .build(store.clone(), "/sst")?;
    array.async_store_metadata().await?;
    array
        .async_store_chunk(&[0, 0, 0], vec![sst_value; ROWS].as_slice())
        .await?;

    let snapshot = store
        .session()
        .write()
        .await
        .commit(message)
        .execute()
        .await
        .with_context(|| format!("failed to commit {message:?}"))?;
    Ok(snapshot.to_string())
}

/// Write a repository whose only chunk is a *virtual* reference into a file
/// outside it, and create that file.
///
/// The reference is written without container validation, so the repository can
/// name a file that no configured container covers — the shape a VirtualiZarr
/// conversion produces. Reading it back requires an authorized virtual chunk
/// container, which Beacon never supplies.
pub async fn write_virtual_chunk_repository(
    path: &Path,
    referenced_file: &Path,
) -> anyhow::Result<()> {
    // The referenced file holds the raw little-endian bytes of one chunk.
    let values: Vec<f64> = (0..LONS).map(|i| i as f64).collect();
    let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
    std::fs::write(referenced_file, &bytes)
        .with_context(|| format!("failed to write {}", referenced_file.display()))?;

    std::fs::create_dir_all(path)
        .with_context(|| format!("failed to create {}", path.display()))?;
    let storage = storage::new_local_filesystem_storage(path)
        .await
        .context("failed to open local Icechunk storage")?;
    let repository = Repository::create(None, storage, HashMap::new(), None, true)
        .await
        .context("failed to create the Icechunk repository")?;

    let session = repository.writable_session("main").await?;
    let store = Arc::new(AsyncIcechunkStore::new(session));

    GroupBuilder::new()
        .build(store.clone(), "/")?
        .async_store_metadata()
        .await?;
    let mut builder = ArrayBuilder::new(
        vec![LONS as u64],
        vec![LONS as u64],
        data_type::float64(),
        0.0f64,
    );
    let array = builder
        .dimension_names(Some(["lon"]))
        .build(store.clone(), "/values")?;
    array.async_store_metadata().await?;

    let location = VirtualChunkLocation::from_url(
        url::Url::from_file_path(referenced_file)
            .map_err(|()| anyhow::anyhow!("referenced file must be an absolute path"))?
            .as_str(),
    )?;
    Store::from_session(store.session())
        .await
        .set_virtual_ref(
            "values/c/0",
            VirtualChunkRef {
                location,
                offset: 0,
                length: bytes.len() as u64,
                checksum: None,
            },
            false,
        )
        .await?;

    store
        .session()
        .write()
        .await
        .commit("virtual chunk reference")
        .execute()
        .await
        .context("failed to commit the virtual chunk reference")?;
    Ok(())
}
