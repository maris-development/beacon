use std::ops::Range;
use std::sync::Arc;

use array_format::Storage;
use bytes::Bytes;
use futures::future::BoxFuture;
use object_store::ObjectStore;
use object_store::path::Path;

// ─── Storage Adapter ──────────────────────────────────────────────────────────

/// Adapter implementing `array_format::Storage` over the workspace `object_store` crate.
#[derive(Clone)]
pub(crate) struct AtlasStorage {
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) path: Path,
}

impl Storage for AtlasStorage {
    fn read_range(&self, range: Range<u64>) -> BoxFuture<'_, array_format::Result<Bytes>> {
        Box::pin(async move {
            let bytes = self
                .store
                .get_range(&self.path, range)
                .await
                .map_err(|e| array_format::Error::Storage(e.to_string()))?;
            Ok(bytes)
        })
    }

    fn write(&self, data: Bytes) -> BoxFuture<'_, array_format::Result<()>> {
        Box::pin(async move {
            self.store
                .put(&self.path, data.into())
                .await
                .map_err(|e| array_format::Error::Storage(e.to_string()))?;
            Ok(())
        })
    }

    fn size(&self) -> BoxFuture<'_, array_format::Result<u64>> {
        Box::pin(async move {
            let meta = self
                .store
                .head(&self.path)
                .await
                .map_err(|e| array_format::Error::Storage(e.to_string()))?;
            Ok(meta.size as u64)
        })
    }
}

// ─── Path Helpers ─────────────────────────────────────────────────────────────

/// Convert an array name to its storage path.
///
/// Dots in array names become directory separators so that related arrays
/// are stored hierarchically, avoiding huge fan-out in a single directory:
///
/// - `"temperature"` → `{base}/columns/temperature.arrf`
/// - `"temperature.units"` → `{base}/columns/temperature/units.arrf`
/// - `".convention"` → `{base}/columns/.convention.arrf`
pub(crate) fn array_name_to_path(base_path: &Path, array_name: &str) -> Path {
    // Global attributes start with '.' — don't split the leading dot
    let relative = if let Some(rest) = array_name.strip_prefix('.') {
        // Global attribute: ".convention" → ".convention.arrf"
        format!("columns/.{}.arrf", rest)
    } else if array_name.contains('.') {
        // Nested: "temperature.units" → "columns/temperature/units.arrf"
        let parts: Vec<&str> = array_name.splitn(2, '.').collect();
        format!("columns/{}/{}.arrf", parts[0], parts[1].replace('.', "/"))
    } else {
        // Plain column: "temperature" → "columns/temperature.arrf"
        format!("columns/{}.arrf", array_name)
    };
    Path::from(format!("{}/{}", base_path, relative))
}

/// Derive the stats file path for a given array.
pub(crate) fn stats_path(base_path: &Path, array_name: &str) -> Path {
    let col_path = array_name_to_path(base_path, array_name);
    let s = col_path.to_string();
    let stats_s = s.replace(".arrf", ".stats.atlas");
    Path::from(stats_s)
}

// ─── Chunking Helpers ─────────────────────────────────────────────────────────

use beacon_nd_array::array::subset::ArraySubset;

/// Generate all chunk subsets that tile a given shape with the given chunk shape.
pub(crate) fn generate_chunk_subsets(shape: &[usize], chunk_shape: &[usize]) -> Vec<ArraySubset> {
    if shape.is_empty() {
        return vec![ArraySubset::new(vec![], vec![])];
    }

    let chunk_counts: Vec<usize> = shape
        .iter()
        .zip(chunk_shape.iter())
        .map(|(&axis_len, &axis_chunk)| {
            if axis_len == 0 {
                0
            } else {
                axis_len.div_ceil(axis_chunk.max(1))
            }
        })
        .collect();

    if chunk_counts.contains(&0) {
        return vec![];
    }

    let total_chunks: usize = chunk_counts.iter().product();
    let mut subsets = Vec::with_capacity(total_chunks);

    for linear_idx in 0..total_chunks {
        let mut rem = linear_idx;
        let mut chunk_index = vec![0usize; shape.len()];
        for axis in (0..shape.len()).rev() {
            chunk_index[axis] = rem % chunk_counts[axis];
            rem /= chunk_counts[axis];
        }

        let start: Vec<usize> = chunk_index
            .iter()
            .zip(chunk_shape.iter())
            .map(|(&ci, &cs)| ci * cs.max(1))
            .collect();

        let sub_shape: Vec<usize> = shape
            .iter()
            .zip(start.iter())
            .zip(chunk_shape.iter())
            .map(|((&axis_len, &axis_start), &axis_chunk)| {
                axis_chunk.max(1).min(axis_len - axis_start)
            })
            .collect();

        subsets.push(ArraySubset::new(start, sub_shape));
    }

    subsets
}
