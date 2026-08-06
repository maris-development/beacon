//! The [`FileAnalyzer`] that connects the file-statistics store to Beacon's
//! formats.
//!
//! [`beacon_file_stats`] deliberately knows nothing about file formats: reading
//! a netCDF file's ranges needs the format layer, which needs DataFusion, and
//! the storage engine stays free of both. This module is the other side of that
//! seam.
//!
//! # What it costs
//!
//! Two opens per file. `infer_schema` names the columns, then `infer_stats`
//! fills them, and for netCDF both read the file. That is the dominant cost of a
//! backfill, and it is also the cost this whole subsystem removes from the
//! *query* path: `FileCollection` scans with `collect_stat(true)`, so today
//! every cold query over a netCDF collection generates these same statistics
//! inline, cached only in a 10 000-entry map that dies on restart.
//!
//! # Formats that yield nothing
//!
//! ODV, Zarr, TIFF and CSV return `Statistics::new_unknown`, so every column
//! comes back `Absent`. Those files analyze successfully and contribute **zero
//! columns**. That is deliberate: a row with a null range costs bytes and prunes
//! nothing. [`FileAnalysis::columns`] being empty is the signal, and the
//! collector records it so a format that yields nothing is visible rather than
//! silently inert.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, SchemaRef};
use beacon_datafusion_ext::format_ext::try_file_format_factory_ext;
use beacon_datafusion_ext::listing_factory::try_listing_factory_from_session;
use beacon_file_stats::segment::ColumnStat;
use beacon_file_stats::{FileAnalysis, FileAnalyzer, FileRecord, FileStatsError};
use chrono::TimeZone;
use datafusion::common::{ColumnStatistics, Statistics};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
use object_store::{ObjectMeta, path::Path};

use crate::statement_plan::{SessionCell, upgrade_session};

/// Reads one file's statistics through Beacon's format registry.
pub struct FormatFileAnalyzer {
    /// Weak, like every other holder: the session owns the runtime that owns
    /// the collector that owns this.
    session: SessionCell,
    /// The store bare dataset paths resolve against.
    datasets_url: ObjectStoreUrl,
}

impl FormatFileAnalyzer {
    pub fn new(session: SessionCell, datasets_url: ObjectStoreUrl) -> Self {
        Self {
            session,
            datasets_url,
        }
    }

    fn session(&self) -> Result<Arc<SessionContext>, FileStatsError> {
        upgrade_session(&self.session, "file statistics analyzer")
            .map_err(|e| FileStatsError::Format(e.to_string()))
    }
}

#[async_trait::async_trait]
impl FileAnalyzer for FormatFileAnalyzer {
    async fn analyze(&self, record: &FileRecord) -> beacon_file_stats::Result<FileAnalysis> {
        let session = self.session()?;
        let state = session.state();

        let object = object_meta(record)?;
        let store = state
            .runtime_env()
            .object_store(&self.datasets_url)
            .map_err(|e| FileStatsError::Format(format!("datasets store unavailable: {e}")))?;

        let (format_name, format) = resolve_format(&session, &self.datasets_url, &object)?;

        // The file's *own* schema, not the table's. `column_statistics` is
        // positional against whatever schema is passed, so handing over a merged
        // table schema would make every file report every column in the
        // collection. At 160K column names that is the dense matrix this crate
        // exists to avoid.
        let schema = format
            .infer_schema(&state, &store, std::slice::from_ref(&object))
            .await
            .map_err(|e| {
                FileStatsError::Format(format!("schema for {}: {e}", record.path))
            })?;

        let statistics = format
            .infer_stats(&state, &store, schema.clone(), &object)
            .await
            .map_err(|e| {
                FileStatsError::Format(format!("statistics for {}: {e}", record.path))
            })?;

        Ok(to_analysis(&format_name, &schema, &statistics))
    }
}

/// Build the object metadata from the registry record.
///
/// No `head` call: the record already carries size, last-modified and etag,
/// because a listing supplied them and the registry kept them to decide whether
/// the file changed. A backfill over a million files does not need a million
/// extra round trips to learn what it already knows.
fn object_meta(record: &FileRecord) -> Result<ObjectMeta, FileStatsError> {
    let location = Path::parse(&record.path)
        .map_err(|e| FileStatsError::Format(format!("bad path {}: {e}", record.path)))?;
    let last_modified = chrono::Utc
        .timestamp_millis_opt(record.last_modified_millis)
        .single()
        .unwrap_or_else(chrono::Utc::now);
    Ok(ObjectMeta {
        location,
        last_modified,
        size: record.size,
        e_tag: record.e_tag.clone(),
        version: None,
    })
}

/// Resolve the format for one object, honouring native readers.
///
/// Not `factory.default()`. A netCDF format read through netcdf-c carries a
/// resolver that turns an object into a local path, and only
/// `create_with_native_root` can build it, because `create` has no location to
/// build it from. Taking the default would hand back a format that cannot open
/// the file it was asked about.
fn resolve_format(
    session: &Arc<SessionContext>,
    datasets_url: &ObjectStoreUrl,
    object: &ObjectMeta,
) -> Result<(String, Arc<dyn FileFormat>), FileStatsError> {
    let state = session.state();
    let key = format_key(object).ok_or_else(|| {
        FileStatsError::Format(format!("no file extension on {}", object.location))
    })?;

    let factory = try_file_format_factory_ext(&state, &key).ok_or_else(|| {
        FileStatsError::Format(format!("no format registered for '{key}'"))
    })?;
    let name = factory.file_format_name();

    let listing = try_listing_factory_from_session(&state).ok_or_else(|| {
        FileStatsError::Format("the session has no listing factory".to_string())
    })?;
    let url = ListingTableUrl::parse(format!("{}{}", datasets_url.as_str(), object.location))
        .map_err(|e| FileStatsError::Format(format!("bad listing url: {e}")))?;

    let format = factory
        .create_with_native_root(&state, &HashMap::new(), &url, &listing)
        .map_err(|e| FileStatsError::Format(format!("cannot open {}: {e}", object.location)))?;
    Ok((name, format))
}

/// The registry key for an object: its extension, with Zarr's metadata file
/// special-cased the way the rest of Beacon special-cases it.
fn format_key(object: &ObjectMeta) -> Option<String> {
    let extension = object.location.extension()?;
    if extension == "json"
        && object
            .location
            .filename()
            .is_some_and(|name| name.starts_with("zarr"))
    {
        return Some("zarr".to_string());
    }
    Some(extension.to_string())
}

/// Turn DataFusion's positional statistics into named per-column ranges.
fn to_analysis(format: &str, schema: &SchemaRef, statistics: &Statistics) -> FileAnalysis {
    let num_rows = statistics.num_rows.get_value().map(|n| *n as u64);
    let total_byte_size = statistics.total_byte_size.get_value().map(|n| *n as u64);

    let columns = schema
        .fields()
        .iter()
        .zip(&statistics.column_statistics)
        .filter_map(|(field, column)| {
            to_column_stat(column, field.data_type(), num_rows)
                .map(|stat| (field.name().clone(), stat))
        })
        .collect();

    FileAnalysis {
        format: format.to_string(),
        num_rows,
        total_byte_size,
        columns,
    }
}

/// One column's range, or `None` when it carries nothing worth storing.
///
/// # On `Precision`
///
/// `get_value` accepts `Exact` and `Inexact` alike, which is what DataFusion's
/// own pruning consumers do. That is only sound while `Inexact` means a *widened*
/// estimate: a min above the true minimum, or a max below the true maximum,
/// would silently drop rows. Every format Beacon reads ranges from today
/// (netCDF, Parquet) derives them from real data or real file metadata, so they
/// are bounds. A format that starts narrowing its estimates would need this
/// tightened to `Exact` only.
fn to_column_stat(
    column: &ColumnStatistics,
    data_type: &DataType,
    num_rows: Option<u64>,
) -> Option<ColumnStat> {
    let min = column.min_value.get_value();
    let max = column.max_value.get_value();

    // A column with no range prunes nothing, and a row storing two nulls costs
    // 40 bytes to say so. Formats returning `new_unknown` land here for every
    // column, and contribute no rows at all.
    if min.is_none() && max.is_none() {
        return None;
    }

    let min = min.and_then(|value| value.to_array().ok());
    let max = max.and_then(|value| value.to_array().ok());

    Some(ColumnStat::from_arrays(
        min.as_ref(),
        max.as_ref(),
        column.null_count.get_value().map(|n| *n as u64).unwrap_or(0),
        num_rows.unwrap_or(0),
        data_type,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use datafusion::common::stats::Precision;
    use datafusion::scalar::ScalarValue;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("TEMP", DataType::Float64, true),
            Field::new("PSAL", DataType::Float64, true),
        ]))
    }

    fn known(min: f64, max: f64, nulls: usize) -> ColumnStatistics {
        ColumnStatistics {
            null_count: Precision::Exact(nulls),
            max_value: Precision::Exact(ScalarValue::Float64(Some(max))),
            min_value: Precision::Exact(ScalarValue::Float64(Some(min))),
            ..Default::default()
        }
    }

    #[test]
    fn named_ranges_come_out_positionally_matched_to_the_schema() {
        let statistics = Statistics {
            num_rows: Precision::Exact(1_000),
            total_byte_size: Precision::Exact(4_096),
            column_statistics: vec![known(0.0, 10.0, 3), known(34.0, 35.0, 0)],
        };

        let analysis = to_analysis("netcdf", &schema(), &statistics);
        assert_eq!(analysis.format, "netcdf");
        assert_eq!(analysis.num_rows, Some(1_000));
        assert_eq!(analysis.total_byte_size, Some(4_096));

        let names: Vec<&str> = analysis.columns.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, vec!["TEMP", "PSAL"]);
        assert_eq!(analysis.columns[0].1.null_count, 3);
        assert_eq!(analysis.columns[0].1.row_count, 1_000);
    }

    /// A format returning `new_unknown` must analyze cleanly and contribute
    /// nothing. This is ODV, Zarr, TIFF and CSV today.
    #[test]
    fn a_format_with_no_statistics_yields_no_columns() {
        let statistics = Statistics::new_unknown(&schema());
        let analysis = to_analysis("odv", &statistics_schema(), &statistics);
        assert!(
            analysis.columns.is_empty(),
            "absent ranges must not become rows"
        );
        assert_eq!(analysis.num_rows, None);
    }

    fn statistics_schema() -> SchemaRef {
        schema()
    }

    /// One known column beside one absent one keeps the known and drops the
    /// other, rather than padding the file out to the schema's width.
    #[test]
    fn absent_columns_are_dropped_individually() {
        let statistics = Statistics {
            num_rows: Precision::Exact(10),
            total_byte_size: Precision::Absent,
            column_statistics: vec![known(1.0, 2.0, 0), ColumnStatistics::default()],
        };

        let analysis = to_analysis("parquet", &schema(), &statistics);
        assert_eq!(analysis.columns.len(), 1);
        assert_eq!(analysis.columns[0].0, "TEMP");
        assert_eq!(analysis.total_byte_size, None);
    }

    /// Inexact bounds are accepted, matching DataFusion's own consumers.
    #[test]
    fn inexact_bounds_are_still_bounds() {
        let statistics = Statistics {
            num_rows: Precision::Inexact(50),
            total_byte_size: Precision::Absent,
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Absent,
                    max_value: Precision::Inexact(ScalarValue::Float64(Some(9.0))),
                    min_value: Precision::Inexact(ScalarValue::Float64(Some(1.0))),
                    ..Default::default()
                },
                ColumnStatistics::default(),
            ],
        };

        let analysis = to_analysis("parquet", &schema(), &statistics);
        assert_eq!(analysis.columns.len(), 1);
        assert_eq!(analysis.num_rows, Some(50));
        assert_eq!(analysis.columns[0].1.null_count, 0, "absent nulls read as zero");
    }

    #[test]
    fn zarr_metadata_files_resolve_to_the_zarr_format() {
        let meta = |path: &str| ObjectMeta {
            location: Path::from(path),
            last_modified: chrono::Utc::now(),
            size: 1,
            e_tag: None,
            version: None,
        };
        assert_eq!(format_key(&meta("a/zarr.json")).as_deref(), Some("zarr"));
        assert_eq!(format_key(&meta("a/other.json")).as_deref(), Some("json"));
        assert_eq!(format_key(&meta("a/b.nc")).as_deref(), Some("nc"));
        assert_eq!(format_key(&meta("a/noext")), None);
    }

    /// The record already holds everything an `ObjectMeta` needs, so a backfill
    /// never re-stats a million files to learn what the listing told it.
    #[test]
    fn object_metadata_comes_from_the_record() {
        let mut record = FileRecord::pending("argo/2024/0.nc", 4096, 1_700_000_000_000);
        record.e_tag = Some("abc".into());

        let meta = object_meta(&record).unwrap();
        assert_eq!(meta.location.as_ref(), "argo/2024/0.nc");
        assert_eq!(meta.size, 4096);
        assert_eq!(meta.e_tag.as_deref(), Some("abc"));
        assert_eq!(meta.last_modified.timestamp_millis(), 1_700_000_000_000);
    }
}
