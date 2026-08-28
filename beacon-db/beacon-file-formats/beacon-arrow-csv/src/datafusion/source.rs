//! [`FileSource`] for CSV, adapting each file to the merged schema.
//!
//! DataFusion's own `CsvSource` hands the merged schema of the collection to the
//! record parser and reads every file with it. The parser counts fields, so a
//! file that holds fewer columns than the collection fails on its first row:
//! "incorrect number of fields for line 1, expected 3 got 2". The parser also
//! maps fields by position, so a file that holds the same columns in another
//! order reads them into the wrong columns.
//!
//! This source reads each file with the columns that file's own header names,
//! and then maps those columns onto the merged schema by name. A column the file
//! lacks reads null. A column the collection does not hold is dropped.
//!
//! A file with no header names no columns, so nothing can be mapped by name.
//! Such a file keeps the positional reading, and so does a file whose header
//! shares no name with the table. The second case is a table whose schema
//! renames the columns of its files, which only a positional reading serves.
//!
//! # A column the merge could not join
//!
//! The parser reads each column straight into the type the table reports, so a
//! type the value does not hold fails the parse itself. A column that
//! `TypeConflict::KeepFirst` settled holds one family in one file and another
//! family in the next, so no such type exists. This source reads that column as
//! text and leaves the type to `AdaptingOpener`, which reads a value the type
//! cannot hold as null. See
//! [`scan_adapt`](beacon_datafusion_ext::scan_adapt).

use std::any::Any;
use std::io::{Cursor, Read};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use beacon_datafusion_ext::scan_adapt::AdaptingOpener;
use beacon_datafusion_ext::type_widening::is_type_conflict;
use datafusion::common::config::CsvOptions;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    CsvOpener, CsvSource, FileOpenFuture, FileOpener, FileScanConfig, FileSource,
};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_datasource::projection::{ProjectionOpener, SplitProjection};
use object_store::{GetOptions, GetRange, ObjectStore};

/// How many bytes the header probe reads first.
///
/// The probe costs one ranged read per file, on top of the read of the file
/// itself. That is the price of knowing which columns a file holds before it is
/// parsed, and a CSV file states that nowhere but its first record.
///
/// A header longer than this is read again, in full. The size therefore only
/// decides how often that second read happens, never whether the names are
/// complete.
const HEADER_PROBE_BYTES: u64 = 64 * 1024;

/// A [`FileSource`] that reads CSV and produces the merged schema.
#[derive(Debug, Clone)]
pub struct BeaconCsvSource {
    options: CsvOptions,
    batch_size: Option<usize>,
    /// The table schema: the file schema, plus the partition columns.
    table_schema: TableSchema,
    /// The projection the scan pushed down, split into the file columns to read
    /// and a remainder applied on top of them.
    ///
    /// A `FileSource` that accepts a projection must apply it in full, so this
    /// source selects plain columns and leaves everything else — aliases,
    /// computed expressions, partition columns — to [`ProjectionOpener`].
    projection: SplitProjection,
    metrics: ExecutionPlanMetricsSet,
}

impl BeaconCsvSource {
    /// A source over `table_schema`, reading with `options`.
    pub fn new(table_schema: TableSchema, options: CsvOptions) -> Self {
        Self {
            options,
            batch_size: None,
            projection: SplitProjection::unprojected(&table_schema),
            table_schema,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// The same source, reading with `options`.
    ///
    /// The format resolves two options against the session before it plans, so
    /// it restates them here on the source the listing table built.
    pub fn with_csv_options(mut self, options: CsvOptions) -> Self {
        self.options = options;
        self
    }

    /// The options this source reads with.
    pub fn options(&self) -> &CsvOptions {
        &self.options
    }

    /// Whether the first record of a file names its columns.
    fn has_header(&self) -> bool {
        self.options.has_header.unwrap_or(true)
    }
}

impl FileSource for BeaconCsvSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        let file_schema = self.table_schema.file_schema();
        // The columns the scan reads, in table order. `ProjectionOpener` derives
        // its input schema the same way, so the two always agree.
        let read_schema = Arc::new(file_schema.project(&self.projection.file_indices)?);

        let raw = Arc::new(BeaconCsvOpener {
            object_store,
            options: self.options.clone(),
            batch_size: self.batch_size.unwrap_or(8192),
            compression: base_config.file_compression_type,
            file_schema: Arc::clone(file_schema),
        }) as Arc<dyn FileOpener>;

        let adapting = AdaptingOpener::wrap(raw, read_schema);
        ProjectionOpener::try_new(self.projection.clone(), adapting, file_schema)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(Self {
            batch_size: Some(batch_size),
            ..self.clone()
        })
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        "csv"
    }

    /// A value that holds a line terminator hides where a record ends, so a
    /// reader cannot start at a byte offset. Such a file reads in one partition.
    fn supports_repartitioning(&self) -> bool {
        !self.options.newlines_in_values.unwrap_or(false)
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection.source)
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let merged = self.projection.source.try_merge(projection)?;
        let source = Self {
            projection: SplitProjection::new(self.table_schema.file_schema(), &merged),
            ..self.clone()
        };
        Ok(Some(Arc::new(source)))
    }

    fn fmt_extra(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            datafusion::physical_plan::DisplayFormatType::Default
            | datafusion::physical_plan::DisplayFormatType::Verbose => {
                write!(f, ", has_header={}", self.has_header())
            }
            datafusion::physical_plan::DisplayFormatType::TreeRender => Ok(()),
        }
    }
}

/// Reads one CSV file with the columns its own header names.
///
/// The batches this opener produces carry the file's own schema, not the merged
/// one. `AdaptingOpener` above it maps them onto the schema the scan reports.
struct BeaconCsvOpener {
    object_store: Arc<dyn ObjectStore>,
    options: CsvOptions,
    batch_size: usize,
    compression: FileCompressionType,
    /// The merged file schema. It types the columns the header names.
    file_schema: SchemaRef,
}

impl FileOpener for BeaconCsvOpener {
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        let object_store = Arc::clone(&self.object_store);
        let options = self.options.clone();
        let batch_size = self.batch_size;
        let compression = self.compression;
        let file_schema = Arc::clone(&self.file_schema);

        Ok(Box::pin(async move {
            let schema = file_read_schema(
                &object_store,
                &partitioned_file,
                &options,
                compression,
                &file_schema,
            )
            .await?;

            // DataFusion's source reads the file. It knows the byte ranges of a
            // split file, the compression, and where a record ends. Only the
            // schema it reads with changes here.
            //
            // That source holds a metrics set of its own, which nothing reads:
            // the plan node reports the metrics of `BeaconCsvSource`. The one
            // counter lost is the decode time the CSV reader records; the scan
            // node still reports the time it spent reading.
            let inner = CsvSource::new(schema).with_csv_options(options);
            let inner = inner.with_batch_size(batch_size);
            let inner = inner
                .as_any()
                .downcast_ref::<CsvSource>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "a sized CsvSource is no longer a CsvSource".to_string(),
                    )
                })?
                .clone();

            let opener = CsvOpener::new(Arc::new(inner), compression, object_store);
            opener.open(partitioned_file)?.await
        }))
    }
}

/// The schema to read `file` with.
///
/// The header names the columns of the file, in the order the records hold
/// them. Each name takes its type from the merged schema, so a column parses
/// straight into the type the table reports. A name the table does not hold
/// parses as text and is dropped afterwards.
///
/// The merged schema itself is the answer where the header cannot serve: a file
/// with no header, an empty file, or a header that shares no name with the
/// table. See the [module docs](self).
async fn file_read_schema(
    store: &Arc<dyn ObjectStore>,
    file: &PartitionedFile,
    options: &CsvOptions,
    compression: FileCompressionType,
    file_schema: &SchemaRef,
) -> Result<SchemaRef> {
    if !options.has_header.unwrap_or(true) {
        return Ok(positional_schema(file_schema));
    }

    let Some(names) = header_names(store, file, options, compression).await? else {
        return Ok(positional_schema(file_schema));
    };
    if !names
        .iter()
        .any(|name| file_schema.field_with_name(name).is_ok())
    {
        return Ok(positional_schema(file_schema));
    }

    let fields: Vec<Field> = names
        .iter()
        .map(|name| match file_schema.field_with_name(name) {
            // A column the merge could not join reads as text. No type parses
            // every file of it, and the adapter above casts the text.
            Ok(field) if is_type_conflict(field) => Field::new(name, DataType::Utf8, true),
            // Every column is read as nullable. A file states no null count
            // before it is read, and the merge already widened the type.
            Ok(field) => Field::new(name, field.data_type().clone(), true),
            Err(_) => Field::new(name, DataType::Utf8, true),
        })
        .collect();
    Ok(Arc::new(Schema::new(fields)))
}

/// The merged schema, with text for every column the merge could not join.
///
/// The positional reading takes this where no header can be matched. The parser
/// would otherwise read a column into a type that no file of it holds. See the
/// [module docs](self).
fn positional_schema(file_schema: &SchemaRef) -> SchemaRef {
    if !file_schema.fields().iter().any(|f| is_type_conflict(f)) {
        return Arc::clone(file_schema);
    }
    let fields: Vec<Field> = file_schema
        .fields()
        .iter()
        .map(|field| {
            if is_type_conflict(field) {
                Field::new(field.name(), DataType::Utf8, true)
            } else {
                field.as_ref().clone()
            }
        })
        .collect();
    Arc::new(Schema::new(fields))
}

/// The column names the first record of `file` holds, or `None` when it names
/// none that can be read.
async fn header_names(
    store: &Arc<dyn ObjectStore>,
    file: &PartitionedFile,
    options: &CsvOptions,
    compression: FileCompressionType,
) -> Result<Option<Vec<String>>> {
    let size = file.object_meta.size;
    if size == 0 {
        return Ok(None);
    }
    let terminator = options.terminator.unwrap_or(b'\n');

    // The header ends at the first record terminator. A probe that holds one
    // holds the whole header; a probe that holds none may have cut it, so the
    // file is read again in full.
    let probe = HEADER_PROBE_BYTES.min(size);
    let mut header = decode_to_terminator(
        read_prefix(store, file, probe).await?,
        compression,
        terminator,
    )?;
    if probe < size && !header.contains(&terminator) {
        header = decode_to_terminator(
            read_prefix(store, file, size).await?,
            compression,
            terminator,
        )?;
    }
    if header.is_empty() {
        return Ok(None);
    }

    let mut format = arrow::csv::reader::Format::default()
        .with_header(true)
        .with_delimiter(options.delimiter)
        .with_quote(options.quote);
    if let Some(escape) = options.escape {
        format = format.with_escape(escape);
    }
    if let Some(terminator) = options.terminator {
        format = format.with_terminator(terminator);
    }
    if let Some(comment) = options.comment {
        format = format.with_comment(comment);
    }

    // No record beyond the header is read. The types come from the merged
    // schema, and the header states none.
    let Ok((schema, _)) = format.infer_schema(Cursor::new(header), Some(0)) else {
        // A header that cannot be read leaves the merged schema in place, which
        // is what this format read with before. See the [module docs](self).
        return Ok(None);
    };

    let names: Vec<String> = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    Ok((!names.is_empty()).then_some(names))
}

/// `bytes` decoded up to and including the first `terminator`.
///
/// A compressed probe is a cut stream, so the decoder reports an error once it
/// runs past the bytes it was given. That error ends the decode and keeps what
/// came before it, which is all the header needs.
fn decode_to_terminator(
    bytes: Vec<u8>,
    compression: FileCompressionType,
    terminator: u8,
) -> Result<Vec<u8>> {
    if !compression.is_compressed() {
        return Ok(bytes);
    }

    let mut reader = compression.convert_read(Cursor::new(bytes))?;
    let mut decoded = Vec::new();
    let mut buffer = [0u8; 8 * 1024];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => {
                decoded.extend_from_slice(&buffer[..read]);
                if decoded.contains(&terminator) {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    Ok(decoded)
}

/// The first `length` bytes of `file`.
async fn read_prefix(
    store: &Arc<dyn ObjectStore>,
    file: &PartitionedFile,
    length: u64,
) -> Result<Vec<u8>> {
    let options = GetOptions {
        range: Some(GetRange::Bounded(0..length)),
        ..Default::default()
    };
    let bytes = store
        .get_opts(&file.object_meta.location, options)
        .await?
        .bytes()
        .await?;
    Ok(bytes.to_vec())
}
