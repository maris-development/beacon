use std::{
    fs::File,
    io::{Read, Seek, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow::{
    array::{
        Array, ArrayRef, ArrowPrimitiveType, AsArray, PrimitiveArray, RecordBatch, StringArray,
        StringBuilder,
    },
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use async_zip::{base::write::ZipFileWriter, ZipEntryBuilder};
use futures::{AsyncSeekExt, AsyncWrite, AsyncWriteExt};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use tokio_util::compat::TokioAsyncReadCompatExt;
use utoipa::ToSchema;
use walkdir::WalkDir;
use zip::write::FileOptions;
/// Information about a column in ODV format
///
/// This struct contains metadata about how a data column should be formatted and written in ODV output,
/// including its label, comments, precision, quality flags, and units.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ColumnInfo {
    /// The name/label of the column
    pub column_name: String,
    /// Optional descriptive comment about the column's contents
    pub comment: Option<String>,
    /// Number of significant digits to use when writing numeric values
    pub significant_digits: Option<u32>,
    /// Name of an optional quality flag column associated with this data column
    pub qf_column: Option<String>,
    /// Physical units of the column values (e.g. "degrees_C", "PSU")
    #[serde(alias = "unit")]
    pub units: Option<String>,
}

/// Configuration options for ODV output
///
/// This struct contains all the configuration needed to properly format data for ODV output,
/// including column specifications and mappings.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct OdvOptions {
    /// Specification for the longitude column
    pub longitude_column: ColumnInfo,
    /// Specification for the latitude column
    pub latitude_column: ColumnInfo,
    /// Specification for the depth/pressure column
    pub depth_column: ColumnInfo,
    /// Name of the column containing timestamp data
    pub time_column: ColumnInfo,
    /// Name of the column containing cruise/deployment identifiers
    pub key_column: String,
    /// Quality flag schema identifier
    pub qf_schema: String,
    /// Specifications for data value columns
    pub data_columns: Vec<ColumnInfo>,
    /// Specifications for metadata columns
    #[serde(alias = "metadata_columns")]
    pub meta_columns: Vec<ColumnInfo>,
    #[serde(default)]
    pub archiving: ArchivingMethod,
    #[serde(default)]
    pub feature_type_column: Option<String>,
    #[serde(default)]
    pub compression: CompressionType,
}

#[derive(Default, Clone, Debug, Serialize, Deserialize, ToSchema)]
pub enum CompressionType {
    Stored,
    #[default]
    Deflate,
    Zstd,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, ToSchema)]
pub enum ArchivingMethod {
    #[default]
    ZipStd,
    #[serde(rename = "zip_deflate")]
    ZipDeflate,
}

impl ArchivingMethod {
    fn impl_archive_directory<T>(
        src_dir: &Path,
        writer: T,
        method: zip::CompressionMethod,
    ) -> zip::result::ZipResult<()>
    where
        T: Write + Seek,
    {
        let mut zip = zip::ZipWriter::new(writer);
        let options: FileOptions<'_, ()> = FileOptions::default()
            .compression_method(method)
            .large_file(true);

        let src_dir = src_dir.canonicalize()?;
        let mut buffer = Vec::new();

        for entry in WalkDir::new(&src_dir) {
            let entry = entry.unwrap();
            let path = entry.path();
            let name = path.strip_prefix(&src_dir).unwrap();

            if path.is_file() {
                // Start a new file entry in the archive
                zip.start_file(name.to_str().unwrap(), options)?;
                let mut f = File::open(path)?;
                f.read_to_end(&mut buffer)?;
                zip.write_all(&buffer)?;
                buffer.clear();
            }
        }
        zip.finish()?;
        Ok(())
    }

    pub fn archive_directory<T>(&self, src_dir: &Path, writer: T) -> zip::result::ZipResult<()>
    where
        T: Write + Seek,
    {
        match self {
            Self::ZipStd => {
                Self::impl_archive_directory(src_dir, writer, zip::CompressionMethod::Stored)
            }
            Self::ZipDeflate => {
                Self::impl_archive_directory(src_dir, writer, zip::CompressionMethod::Deflated)
            }
        }
    }
}

impl OdvOptions {
    pub fn try_from_arrow_schema(schema: SchemaRef) -> anyhow::Result<Self> {
        let mut options = Self::default();

        let columns: indexmap::IndexMap<String, Arc<Field>> = schema
            .fields()
            .iter()
            .map(|field| (field.name().to_lowercase(), field.clone()))
            .collect();

        for (column, field) in &columns {
            if !column.ends_with("_qc") && !column.ends_with("_qf") {
                if column != &options.depth_column.column_name.to_lowercase()
                    && column != &options.time_column.column_name.to_lowercase()
                    && column != &options.latitude_column.column_name.to_lowercase()
                    && column != &options.longitude_column.column_name.to_lowercase()
                    && column != &options.key_column.to_lowercase()
                {
                    // Assume its a metadata column
                    let mut column_info = ColumnInfo {
                        column_name: field.name().clone(),
                        comment: None,
                        significant_digits: None,
                        qf_column: None,
                        units: None,
                    };

                    //Check if we can find a quality flag column for this data column
                    let expected_qf_col_names =
                        vec![format!("{}_qf", column), format!("{}_qc", column)];

                    for qf_col_name in expected_qf_col_names {
                        if let Some(qf_field) = columns.get(&qf_col_name) {
                            column_info.qf_column = Some(qf_field.name().to_string());
                            break;
                        }
                    }

                    if column_info.qf_column.is_some() {
                        options.data_columns.push(column_info);
                    } else {
                        options.meta_columns.push(column_info);
                    }
                }
            }
        }

        Ok(options)
    }

    /// Sets the longitude column specification
    ///
    /// # Arguments
    /// * `column` - Column info for longitude values
    pub fn with_longitude_column(mut self, column: ColumnInfo) -> Self {
        self.longitude_column = column;
        self
    }

    /// Sets the latitude column specification
    ///
    /// # Arguments
    /// * `column` - Column info for latitude values
    pub fn with_latitude_column(mut self, column: ColumnInfo) -> Self {
        self.latitude_column = column;
        self
    }

    /// Sets the depth column specification
    ///
    /// # Arguments
    /// * `column` - Column info for depth/pressure values
    pub fn with_depth_column(mut self, column: ColumnInfo) -> Self {
        self.depth_column = column;
        self
    }

    /// Sets the time column name
    ///
    /// # Arguments
    /// * `column` - Name of column containing timestamps
    pub fn with_time_column(mut self, column: ColumnInfo) -> Self {
        self.time_column = column;
        self
    }

    /// Sets the key column name
    ///
    /// # Arguments
    /// * `column` - Name of column containing cruise/deployment IDs
    pub fn with_key_column(mut self, column: String) -> Self {
        self.key_column = column;
        self
    }

    /// Sets the quality flag schema
    ///
    /// # Arguments
    /// * `schema` - Quality flag schema identifier
    pub fn with_qf_schema(mut self, schema: String) -> Self {
        self.qf_schema = schema;
        self
    }
}

impl Default for OdvOptions {
    fn default() -> Self {
        Self {
            time_column: ColumnInfo {
                column_name: "yyyy-MM-ddTHH:mm:ss.SSS".to_string(),
                comment: None,
                significant_digits: None,
                qf_column: None,
                units: None,
            },
            key_column: "Cruise".to_string(),
            qf_schema: "SEADATANET".to_string(),
            depth_column: ColumnInfo {
                column_name: "Depth [m]".to_string(),
                comment: None,
                significant_digits: None,
                qf_column: None,
                units: None,
            },
            latitude_column: ColumnInfo {
                column_name: "Latitude [degrees north]".to_string(),
                comment: None,
                significant_digits: None,
                qf_column: None,
                units: None,
            },
            longitude_column: ColumnInfo {
                column_name: "Longitude [degrees east]".to_string(),
                comment: None,
                significant_digits: None,
                qf_column: None,
                units: None,
            },
            data_columns: vec![],
            meta_columns: vec![],
            archiving: ArchivingMethod::default(),
            feature_type_column: None,
            compression: CompressionType::default(),
        }
    }
}

/// Asynchronous writer for ODV formatted data files
///
/// This struct handles writing data in ODV format, automatically classifying data into different ODV types
/// (profiles, time series, trajectories) and writing to appropriate output files.
pub struct AsyncOdvWriter<W: AsyncWrite + Unpin + Send> {
    /// Configuration options for ODV output
    options: OdvOptions,
    /// Compression settings for ODV output
    compression: async_zip::Compression,
    /// File for writing records that cannot be clearly classified
    error_file: OdvFile<Error, File>,
    /// File for writing time series data
    timeseries_file: OdvFile<TimeSeries, File>,
    /// File for writing profile data
    profile_file: OdvFile<Profiles, File>,
    /// File for writing trajectory data
    trajectory_file: OdvFile<Trajectories, File>,

    zip_file_writer: ZipFileWriter<W>,
}

impl<W: AsyncWrite + Unpin + Send> AsyncOdvWriter<W> {
    pub async fn new_from_dyn(
        writer: W,
        input_schema: SchemaRef,
        options: OdvOptions,
    ) -> anyhow::Result<Self> {
        let zip_file_writer = ZipFileWriter::new(writer);

        let error_file =
            OdvFile::<Error, File>::new(tempfile::tempfile()?, &options, input_schema.clone())?;

        let timeseries_file = OdvFile::<TimeSeries, File>::new(
            tempfile::tempfile()?,
            &options,
            input_schema.clone(),
        )?;

        let profile_file =
            OdvFile::<Profiles, File>::new(tempfile::tempfile()?, &options, input_schema.clone())?;

        let trajectory_file = OdvFile::<Trajectories, File>::new(
            tempfile::tempfile()?,
            &options,
            input_schema.clone(),
        )?;

        let compression = match options.compression {
            CompressionType::Stored => async_zip::Compression::Stored,
            CompressionType::Deflate => async_zip::Compression::Deflate,
            CompressionType::Zstd => async_zip::Compression::Zstd,
        };

        Ok(Self {
            options,
            error_file,
            timeseries_file,
            profile_file,
            trajectory_file,
            zip_file_writer,
            compression,
        })
    }

    /// Creates a compressed zip archive containing all ODV output files
    ///
    /// # Arguments
    /// * `file_name` - Base name for the archive file
    /// * `dir_path` - Directory where archive should be written
    ///
    /// # Returns
    /// Result containing path to created archive or error if archival fails
    pub async fn finish(mut self) -> anyhow::Result<W> {
        // Process each odv into the zip file by copying over the bytes
        let mut profiles = tokio::fs::File::from_std(self.profile_file.finish()?).compat();
        let mut timeseries = tokio::fs::File::from_std(self.timeseries_file.finish()?).compat();
        let mut trajectories = tokio::fs::File::from_std(self.trajectory_file.finish()?).compat();
        let mut errors = tokio::fs::File::from_std(self.error_file.finish()?).compat();

        // Move back file pointers to start
        profiles.seek(std::io::SeekFrom::Start(0)).await?;
        timeseries.seek(std::io::SeekFrom::Start(0)).await?;
        trajectories.seek(std::io::SeekFrom::Start(0)).await?;
        errors.seek(std::io::SeekFrom::Start(0)).await?;

        let mut profile_writer = self
            .zip_file_writer
            .write_entry_stream(ZipEntryBuilder::new("profile.txt".into(), self.compression))
            .await?;
        futures::io::copy(profiles, &mut profile_writer).await?;
        profile_writer.close().await?;

        let mut timeseries_writer = self
            .zip_file_writer
            .write_entry_stream(ZipEntryBuilder::new(
                "timeseries.txt".into(),
                self.compression,
            ))
            .await?;
        futures::io::copy(timeseries, &mut timeseries_writer).await?;
        timeseries_writer.close().await?;

        let mut trajectories_writer = self
            .zip_file_writer
            .write_entry_stream(ZipEntryBuilder::new(
                "trajectories.txt".into(),
                self.compression,
            ))
            .await?;
        futures::io::copy(trajectories, &mut trajectories_writer).await?;
        trajectories_writer.close().await?;

        let mut errors_writer = self
            .zip_file_writer
            .write_entry_stream(ZipEntryBuilder::new("errors.txt".into(), self.compression))
            .await?;
        futures::io::copy(errors, &mut errors_writer).await?;
        errors_writer.close().await?;

        let mut inner_writer = self.zip_file_writer.close().await?;
        inner_writer.flush().await?;

        Ok(inner_writer)
    }

    /// Writes a record batch of data in ODV format
    ///
    /// This method:
    /// 1. Splits the batch by key values
    /// 2. Classifies each sub-batch into an ODV type
    /// 3. Writes to the appropriate output file
    ///
    /// # Arguments
    /// * `record_batch` - Arrow record batch containing data to write
    ///
    /// # Returns
    /// Result indicating success or error during writing
    pub async fn write(&mut self, record_batch: RecordBatch) -> anyhow::Result<()> {
        //Key batches
        let key_batches = Self::key_batches(record_batch, &self.options)?;
        let mut classified_batches = vec![];
        for batch in key_batches {
            let batch_type = Self::classify_batch(batch, &self.options)?;
            classified_batches.push(batch_type);
        }

        for batch in classified_batches {
            match batch {
                OdvBatchType::Ambiguous(batch) => {
                    self.error_file.write_batch(batch)?;
                }
                OdvBatchType::TimeSeries(batch) => {
                    self.timeseries_file.write_batch(batch)?;
                }
                OdvBatchType::Profile(batch) => {
                    self.profile_file.write_batch(batch)?;
                }
                OdvBatchType::Trajectory(batch) => {
                    self.trajectory_file.write_batch(batch)?;
                }
                OdvBatchType::TrajectoryProfile(batches) => {
                    for batch in batches {
                        self.profile_file.write_batch(batch)?;
                    }
                }
                OdvBatchType::TimeSeriesProfile(batches) => {
                    for batch in batches {
                        self.profile_file.write_batch(batch)?;
                    }
                }
            }
        }

        Ok(())
    }

    /// Splits a record batch into sub-batches based on key column values
    ///
    /// # Arguments
    /// * `batch` - Record batch to split
    /// * `odv_options` - ODV configuration options
    ///
    /// # Returns
    /// Result containing vector of sub-batches or error if splitting fails
    fn key_batches(
        batch: RecordBatch,
        odv_options: &OdvOptions,
    ) -> anyhow::Result<Vec<RecordBatch>> {
        let mut batches = vec![];

        let key_array = batch
            .column_by_name(&odv_options.key_column)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Key column '{}' not found in batch.",
                    odv_options.key_column
                )
            })?;

        let casted_array = arrow::compute::cast(key_array, &DataType::Utf8).map_err(|e| {
            anyhow::anyhow!(
                "Failed to cast key column '{}' to UTF-8: {:?}",
                odv_options.key_column,
                e
            )
        })?;

        let ranges = arrow::compute::kernels::partition::partition(&[casted_array])
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to partition key column '{}': {:?}",
                    odv_options.key_column,
                    e
                )
            })?
            .ranges();

        for range in ranges {
            let batch = batch.slice(range.start, range.end - range.start);
            batches.push(batch);
        }

        Ok(batches)
    }

    /// Classifies a record batch into an ODV data type
    ///
    /// Classification is based on which columns show variation:
    /// - Changing position + time = Trajectory
    /// - Fixed position + changing time = Time Series
    /// - Fixed position + time + changing depth = Profile
    ///
    /// # Arguments
    /// * `batch` - Record batch to classify
    /// * `odv_options` - ODV configuration options
    ///
    /// # Returns
    /// Result containing classified ODV batch type or error if classification fails
    fn classify_batch(
        batch: RecordBatch,
        odv_options: &OdvOptions,
    ) -> anyhow::Result<OdvBatchType> {
        let lon_col = batch
            .column_by_name(&odv_options.longitude_column.column_name)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Longitude column '{}' not found in batch.",
                    odv_options.longitude_column.column_name
                )
            })?;
        let lat_col = batch
            .column_by_name(&odv_options.latitude_column.column_name)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Latitude column '{}' not found in batch.",
                    odv_options.latitude_column.column_name
                )
            })?;
        let time_col = batch
            .column_by_name(&odv_options.time_column.column_name)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Time column '{}' not found in batch.",
                    odv_options.time_column.column_name
                )
            })?;
        let depth_col = batch
            .column_by_name(&odv_options.depth_column.column_name)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Depth column '{}' not found in batch.",
                    odv_options.depth_column.column_name
                )
            })?;

        if let Some(feature_type_column) = &odv_options.feature_type_column {
            if let Some(feature_type) = Self::classify_batch_feature_type(
                batch.clone(),
                feature_type_column,
                &odv_options.longitude_column.column_name,
                &odv_options.latitude_column.column_name,
                &odv_options.time_column.column_name,
                &odv_options.key_column,
            )? {
                return Ok(feature_type);
            }
        }

        let has_moving_position = Self::has_changes(lon_col) || Self::has_changes(lat_col);
        let has_changing_time = Self::has_changes(time_col);
        let has_changing_depth = Self::has_changes(depth_col);

        let mut types = vec![];

        if has_moving_position && has_changing_time && !has_changing_depth {
            types.push(OdvBatchType::Trajectory(batch.clone()));
        }
        if !has_moving_position && has_changing_time && !has_changing_depth {
            types.push(OdvBatchType::TimeSeries(batch.clone()));
        }
        if !has_moving_position && !has_changing_time && has_changing_depth {
            types.push(OdvBatchType::Profile(batch.clone()));
        }

        if types.len() > 1 || (has_moving_position && has_changing_time && has_changing_depth) {
            return Ok(OdvBatchType::Ambiguous(batch));
        }

        Ok(types.pop().unwrap_or(OdvBatchType::Profile(batch))) // Default to Profile
    }

    fn classify_batch_feature_type(
        batch: RecordBatch,
        feature_type_columns: &str,
        longitude_column: &str,
        latitude_column: &str,
        time_column: &str,
        key_column: &str,
    ) -> anyhow::Result<Option<OdvBatchType>> {
        let feature_array = batch.column_by_name(feature_type_columns).ok_or_else(|| {
            anyhow::anyhow!(
                "Feature type column '{}' not found in batch.",
                feature_type_columns
            )
        })?;
        let feature_type_str_array = feature_array
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Feature type column '{}' is not a String.",
                    feature_type_columns
                )
            })?;

        if feature_type_str_array.is_null(0) {
            return Ok(None); // No feature type specified
        }

        let feature_type = feature_type_str_array.value(0);

        match feature_type {
            "profile" => Ok(Some(OdvBatchType::Profile(batch))),
            "timeSeries" => Ok(Some(OdvBatchType::TimeSeries(batch))),
            "trajectory" => Ok(Some(OdvBatchType::Trajectory(batch))),
            "trajectoryProfile" => {
                let longitude_column = batch
                    .column_by_name(longitude_column)
                    .expect("Longitude column not found")
                    .clone();
                let latitude_column = batch
                    .column_by_name(latitude_column)
                    .expect("Latitude column not found")
                    .clone();
                let time_column = batch
                    .column_by_name(time_column)
                    .expect("Time column not found")
                    .clone();

                // Partition the batch into trajectory profiles
                let partitions = arrow::compute::kernels::partition::partition(&[
                    longitude_column,
                    latitude_column,
                    time_column,
                ])
                .map_err(|e| {
                    anyhow::anyhow!("Failed to partition trajectory profile batch: {:?}", e)
                })?
                .ranges();

                let mut trajectory_profiles = vec![];
                for (i, range) in partitions.iter().enumerate() {
                    let profile = batch.slice(range.start, range.end - range.start);

                    // Map the key column of the profile record batch to the key_name[+i] format
                    let key_column_array = profile
                        .column_by_name(key_column)
                        .expect("Key column not found")
                        .as_string::<i32>();

                    let updated_key_column =
                        Arc::new(Self::append_suffix(key_column_array, &format!("[+{}]", i)));

                    // Update the record batch key column
                    let profile =
                        Self::replace_column(&profile, key_column, updated_key_column).unwrap();

                    trajectory_profiles.push(profile);
                }

                Ok(Some(OdvBatchType::TrajectoryProfile(trajectory_profiles)))
            }
            "timeSeriesProfile" => {
                let time_column = batch
                    .column_by_name(time_column)
                    .expect("Time column not found")
                    .clone();

                let partitions = arrow::compute::kernels::partition::partition(&[time_column])
                    .map_err(|e| {
                        anyhow::anyhow!("Failed to partition time series profile batch: {:?}", e)
                    })?
                    .ranges();

                let mut time_series_profiles = vec![];
                for (i, range) in partitions.iter().enumerate() {
                    let profile = batch.slice(range.start, range.end - range.start);

                    // Map the key column of the profile record batch to the key_name[+i] format
                    let key_column_array = profile
                        .column_by_name(key_column)
                        .expect("Key column not found")
                        .as_string::<i32>();

                    let updated_key_column =
                        Arc::new(Self::append_suffix(key_column_array, &format!("[+{}]", i)));

                    // Update the record batch key column
                    let profile =
                        Self::replace_column(&profile, key_column, updated_key_column).unwrap();

                    time_series_profiles.push(profile);
                }
                Ok(Some(OdvBatchType::TimeSeriesProfile(time_series_profiles)))
            }
            _ => Ok(None),
        }
    }

    fn append_suffix(arr: &StringArray, suffix: &str) -> StringArray {
        let len = arr.len();
        let mut builder = StringBuilder::new();

        for i in 0..len {
            if arr.is_null(i) {
                builder.append_null();
            } else {
                // arr.value(i) is &str
                let new_val = arr.value(i).to_string() + suffix;
                builder.append_value(&new_val);
            }
        }

        builder.finish()
    }

    fn replace_column(
        batch: &RecordBatch,
        col_name: &str,
        new_col: ArrayRef,
    ) -> anyhow::Result<RecordBatch> {
        let col_index = batch.schema().index_of(col_name)?;

        let mut new_fields = batch.schema().fields().to_vec();
        let old_field = batch.schema().field(col_index).clone();
        let new_field = Field::new(
            old_field.name(),
            new_col.data_type().clone(),
            old_field.is_nullable(),
        );
        new_fields[col_index] = new_field.into();

        let new_schema: SchemaRef = Arc::new(Schema::new(new_fields));

        // 2) Build new columns Vec<ArrayRef>, swapping in new_col at position col_index.
        let mut new_columns = batch.columns().to_vec();
        new_columns[col_index] = new_col;

        // 3) Construct the new RecordBatch
        Ok(RecordBatch::try_new(new_schema, new_columns)?)
    }

    /// Checks if an array contains varying values
    ///
    /// # Arguments
    /// * `array` - Array to check for changes
    ///
    /// # Returns
    /// true if the array contains different values, false if all values are the same
    fn has_changes(array: &dyn arrow::array::Array) -> bool {
        if array.len() < 2 {
            return false; // If there's only one row, no changes are possible
        }

        macro_rules! check_array_type {
            ($array_type:ty) => {
                if let Some(array) = array.as_any().downcast_ref::<$array_type>() {
                    for i in 1..array.len() {
                        if array.value(i) != array.value(i - 1) {
                            return true;
                        }
                    }
                    return false;
                }
            };
        }

        check_array_type!(arrow::array::StringArray);
        check_array_type!(arrow::array::TimestampSecondArray);
        check_array_type!(arrow::array::TimestampMillisecondArray);
        check_array_type!(arrow::array::TimestampMicrosecondArray);
        check_array_type!(arrow::array::TimestampNanosecondArray);
        check_array_type!(arrow::array::Float64Array);
        check_array_type!(arrow::array::Float32Array);
        check_array_type!(arrow::array::Int64Array);
        check_array_type!(arrow::array::Int32Array);
        check_array_type!(arrow::array::Int16Array);
        check_array_type!(arrow::array::Int8Array);
        check_array_type!(arrow::array::UInt64Array);
        check_array_type!(arrow::array::UInt32Array);
        check_array_type!(arrow::array::UInt16Array);
        check_array_type!(arrow::array::UInt8Array);
        check_array_type!(arrow::array::BooleanArray);

        false
    }
}

/// Represents different types of ODV (Ocean Data View) data batches
#[derive(Debug)]
pub enum OdvBatchType {
    /// Data that could not be clearly classified as one specific type
    Ambiguous(RecordBatch),
    /// Time series data with measurements at fixed locations over time
    TimeSeries(RecordBatch),
    /// Vertical profile data at fixed locations
    Profile(RecordBatch),
    /// Moving trajectory data with changing positions and times
    Trajectory(RecordBatch),
    /// Trajectory with profile data
    TrajectoryProfile(Vec<RecordBatch>),
    /// Timeseries Profile
    TimeSeriesProfile(Vec<RecordBatch>),
}

/// File handler for writing ODV formatted data
///
/// Generic over type T which must implement OdvType to specify the data format
pub struct OdvFile<T: OdvType, W: Write> {
    /// Phantom data to track the ODV type parameter
    _type: std::marker::PhantomData<T>,
    /// CSV writer for the underlying file
    writer: arrow_csv::Writer<W>,
    /// Schema mapper to transform input data to ODV format
    schema_mapper: OdvBatchSchemaMapper,
}

impl<T: OdvType, W: Write> OdvFile<T, W> {
    /// Creates a new ODV file with the specified options and schema
    ///
    /// # Arguments
    /// * `path` - Path where the ODV file should be created
    /// * `options` - Configuration options for the ODV output
    /// * `input_schema` - Schema of the input data to be written
    ///
    /// # Returns
    /// Result containing the OdvFile or an error if creation fails
    pub fn new(
        mut writer: W,
        options: &OdvOptions,
        input_schema: arrow::datatypes::SchemaRef,
    ) -> anyhow::Result<Self> {
        let schema_mapper =
            OdvBatchSchemaMapper::new(T::map_schema(input_schema.clone()), options.clone())?;
        Self::write_header(&mut writer, options, &schema_mapper)?;

        Ok(Self {
            _type: std::marker::PhantomData,
            writer: arrow_csv::WriterBuilder::new()
                .with_header(true)
                .with_timestamp_format("%Y-%m-%dT%H:%M:%S%.3f".to_string())
                .with_timestamp_tz_format("%Y-%m-%dT%H:%M:%S%.3f%".to_string())
                .with_datetime_format("%Y-%m-%dT%H:%M:%S%.3f".to_string())
                .with_delimiter(b'\t')
                .build(writer),
            schema_mapper,
        })
    }

    /// Writes the ODV header information to the file
    ///
    /// # Arguments
    /// * `writer` - File writer to output header
    /// * `options` - ODV configuration options
    /// * `output_schema` - Schema for the output data
    ///
    /// # Returns
    /// Result indicating success or error writing header
    fn write_header(
        writer: &mut W,
        options: &OdvOptions,
        schema_mapper: &OdvBatchSchemaMapper,
    ) -> anyhow::Result<()> {
        let output_schema = schema_mapper.output_schema();
        let input_schema = schema_mapper.input_schema();
        let projection = schema_mapper.projection();
        writeln!(writer, "//<Encoding>UTF-8</Encoding>")?;
        writeln!(writer, "//<DataField>Ocean</DataField>")?;
        writeln!(writer, "{}", T::type_header())?;
        writeln!(writer, "//")?;

        //Write required column header
        writeln!(
            writer,
            "{}",
            Self::meta_header(
                "Cruise",
                &options.qf_schema,
                &Self::arrow_to_value_type(
                    output_schema
                        .field_with_name("Cruise")
                        .expect("")
                        .data_type()
                )?,
                ""
            )
        )?;
        //Write required station header
        writeln!(
            writer,
            "{}",
            Self::meta_header("Station", &options.qf_schema, "INDEXED_TEXT", "")
        )?;
        //Write required station header
        writeln!(
            writer,
            "{}",
            Self::meta_header("Type", &options.qf_schema, "TEXT:2", "")
        )?;
        //Write longitude column
        writeln!(
            writer,
            "{}",
            Self::meta_header(
                "Longitude [degrees east]",
                &options.qf_schema,
                &Self::arrow_to_value_type(
                    output_schema
                        .field_with_name("Longitude [degrees east]")
                        .expect("")
                        .data_type()
                )?,
                &options.longitude_column.comment.as_deref().unwrap_or("")
            )
        )?;
        //Write latitude column
        writeln!(
            writer,
            "{}",
            Self::meta_header(
                "Latitude [degrees north]",
                &options.qf_schema,
                &Self::arrow_to_value_type(
                    output_schema
                        .field_with_name("Latitude [degrees north]")
                        .expect("")
                        .data_type()
                )?,
                &options.latitude_column.comment.as_deref().unwrap_or("")
            )
        )?;

        //Write all the metadata variables/columns
        for column in options.meta_columns.iter() {
            // Get the index using the column name from the input schema
            let (index, _) = input_schema
                .column_with_name(&column.column_name)
                .expect("Column not found in input schema.");
            let inverse_proj_index = projection
                .iter()
                .position(|&i| i == index)
                .expect("Column not found in projection.");

            let value_type = Self::arrow_to_value_type(
                input_schema
                    .field_with_name(&column.column_name)
                    .expect("Column not found in output schema.")
                    .data_type(),
            )?;

            writeln!(
                writer,
                "{}",
                Self::meta_header(
                    &output_schema.field(inverse_proj_index).name(),
                    &options.qf_schema,
                    &value_type,
                    &column.comment.as_deref().unwrap_or("")
                )
            )?;
        }

        writeln!(writer, "//")?;

        match T::file_type() {
            OdvFileType::Profiles => {
                //Write depth column
                let (index, _) = input_schema
                    .column_with_name(&options.depth_column.column_name)
                    .expect("Column not found in input schema.");
                let inverse_proj_index = projection
                    .iter()
                    .position(|&i| i == index)
                    .expect("Column not found in projection.");

                writeln!(
                    writer,
                    "{}",
                    Self::primary_data_header(
                        &output_schema.field(inverse_proj_index).name(),
                        &options.qf_schema,
                        &Self::arrow_to_value_type(
                            input_schema
                                .field_with_name(&options.depth_column.column_name)
                                .expect("")
                                .data_type()
                        )?,
                        &options.depth_column.comment.as_deref().unwrap_or(""),
                        "T"
                    )
                )?;

                //Write time column
                writeln!(
                    writer,
                    "{}",
                    Self::primary_data_header(
                        "time_ISO8601",
                        &options.qf_schema,
                        &Self::arrow_to_value_type(
                            input_schema
                                .field_with_name(&options.time_column.column_name)
                                .expect("Time column not found in input schema.")
                                .data_type()
                        )?,
                        options.time_column.comment.as_deref().unwrap_or(""),
                        "F"
                    )
                )?;
            }
            OdvFileType::TimeSeries | OdvFileType::Trajectories => {
                //Write depth column
                let (index, _) = input_schema
                    .column_with_name(&options.depth_column.column_name)
                    .expect("Column not found in input schema.");
                let inverse_proj_index = projection
                    .iter()
                    .position(|&i| i == index)
                    .expect("Column not found in projection.");

                writeln!(
                    writer,
                    "{}",
                    Self::primary_data_header(
                        &output_schema.field(inverse_proj_index).name(),
                        &options.qf_schema,
                        &Self::arrow_to_value_type(
                            input_schema
                                .field_with_name(&options.depth_column.column_name)
                                .expect("")
                                .data_type()
                        )?,
                        &options.depth_column.comment.as_deref().unwrap_or(""),
                        "F"
                    )
                )?;

                //Write time column
                writeln!(
                    writer,
                    "{}",
                    Self::primary_data_header(
                        "time_ISO8601",
                        &options.qf_schema,
                        &Self::arrow_to_value_type(
                            input_schema
                                .field_with_name(&options.time_column.column_name)
                                .expect("Time column not found in input schema.")
                                .data_type()
                        )?,
                        options.time_column.comment.as_deref().unwrap_or(""),
                        "T"
                    )
                )?;
            }
        }

        //Write all the data variables/columns
        for column in options.data_columns.iter() {
            let (index, _) = input_schema
                .column_with_name(&column.column_name)
                .expect("Column not found in input schema.");
            let inverse_proj_index = projection
                .iter()
                .position(|&i| i == index)
                .expect("Column not found in projection.");

            let value_type = Self::arrow_to_value_type(
                input_schema
                    .field_with_name(&column.column_name)
                    .expect("Column not found in output schema.")
                    .data_type(),
            )?;

            writeln!(
                writer,
                "{}",
                Self::data_header(
                    &output_schema.field(inverse_proj_index).name(),
                    &options.qf_schema,
                    &value_type,
                    &column.comment.as_deref().unwrap_or("")
                )
            )?;
        }

        writeln!(writer, "//")?;

        Ok(())
    }

    /// Converts Arrow data types to ODV value types
    ///
    /// # Arguments
    /// * `arrow_type` - Arrow data type to convert
    ///
    /// # Returns
    /// Result containing the ODV value type string or error if type is unsupported
    fn arrow_to_value_type(arrow_type: &DataType) -> anyhow::Result<String> {
        match arrow_type {
            DataType::Null => Ok("INDEXED_TEXT".to_string()),
            DataType::Boolean => Ok("INDEXED_TEXT".to_string()),
            DataType::Int8 => Ok("INTEGER".to_string()),
            DataType::Int16 => Ok("INTEGER".to_string()),
            DataType::Int32 => Ok("INTEGER".to_string()),
            DataType::Int64 => Ok("INTEGER".to_string()),
            DataType::UInt8 => Ok("INTEGER".to_string()),
            DataType::UInt16 => Ok("INTEGER".to_string()),
            DataType::UInt32 => Ok("INTEGER".to_string()),
            DataType::UInt64 => Ok("INTEGER".to_string()),
            DataType::Float32 => Ok("FLOAT".to_string()),
            DataType::Float64 => Ok("FLOAT".to_string()),
            DataType::Timestamp(_, _) => Ok("DOUBLE".to_string()),
            DataType::Utf8 => Ok("INDEXED_TEXT".to_string()),
            dtype => anyhow::bail!("Unsupported data type for ODV export: {:?}", dtype),
        }
    }

    /// Generates the ODV header for a data variable
    ///
    /// # Arguments
    /// * `label` - Name of the variable
    /// * `qf_schema` - Quality flag schema
    /// * `value_type` - ODV value type
    /// * `comment` - Optional comment about the variable
    fn data_header(label: &str, qf_schema: &str, value_type: &str, comment: &str) -> String {
        const GENERIC_DATA_HEADER : &'static str = "//<DataVariable> label=\"$LABEL\" value_type=\"$VALUE_TYPE\" qf_scheme=\"$QF_SCHEMA\" comment=\"$COMMENT\" </DataVariable>";

        let header = GENERIC_DATA_HEADER
            .replace("$LABEL", label)
            .replace("$VALUE_TYPE", value_type)
            .replace("$QF_SCHEMA", qf_schema)
            .replace("$COMMENT", comment);

        header
    }

    /// Generates the ODV header for a data variable
    ///
    /// # Arguments
    /// * `label` - Name of the variable
    /// * `qf_schema` - Quality flag schema
    /// * `value_type` - ODV value type
    /// * `comment` - Optional comment about the variable
    fn primary_data_header(
        label: &str,
        qf_schema: &str,
        value_type: &str,
        comment: &str,
        primary_variable: &str,
    ) -> String {
        const GENERIC_DATA_HEADER : &'static str = "//<DataVariable> label=\"$LABEL\" value_type=\"$VALUE_TYPE\" qf_scheme=\"$QF_SCHEMA\" comment=\"$COMMENT\" is_primary_variable=\"$PRIMARY_VARIABLE\" </DataVariable>";

        let header = GENERIC_DATA_HEADER
            .replace("$LABEL", label)
            .replace("$VALUE_TYPE", value_type)
            .replace("$QF_SCHEMA", qf_schema)
            .replace("$COMMENT", comment)
            .replace("$PRIMARY_VARIABLE", primary_variable);

        header
    }

    /// Generates the ODV header for a metadata variable
    ///
    /// # Arguments
    /// * `label` - Name of the metadata variable
    /// * `qf_schema` - Quality flag schema
    /// * `value_type` - ODV value type
    /// * `comment` - Optional comment about the variable
    fn meta_header(label: &str, qf_schema: &str, value_type: &str, comment: &str) -> String {
        const GENERIC_META_HEADER: &'static str = "//<MetaVariable> label=\"$LABEL\" value_type=\"$VALUE_TYPE\" qf_schema=\"$QF_SCHEMA\" comment=\"$COMMENT\"</MetaVariable>";

        let header = GENERIC_META_HEADER
            .replace("$LABEL", label)
            .replace("$QF_SCHEMA", qf_schema)
            .replace("$VALUE_TYPE", value_type)
            .replace("$COMMENT", comment);

        header
    }

    /// Writes a record batch to the ODV file
    ///
    /// # Arguments
    /// * `batch` - Record batch to write
    ///
    /// # Returns
    /// Result indicating success or error writing batch
    pub fn write_batch(&mut self, batch: RecordBatch) -> anyhow::Result<()> {
        let batch = self.schema_mapper.map(T::map_batch(batch));
        self.writer.write(&batch)?;
        Ok(())
    }

    /// Finishes writing and returns the underlying file
    pub fn finish(self) -> anyhow::Result<W> {
        Ok(self.writer.into_inner())
    }
}

/// Generates a column name with optional units
///
/// # Arguments
/// * `name` - Base name of the column
/// * `units` - Optional units to append in brackets
///
/// # Returns
/// Column name with units if provided, otherwise just the name
fn generate_column_name<S: AsRef<str>>(name: &str, units: Option<S>) -> String {
    match units {
        Some(units) => format!("{} [{}]", name, units.as_ref()),
        None => name.to_string(),
    }
}

pub enum OdvFileType {
    Profiles,
    TimeSeries,
    Trajectories,
}

/// Trait defining behavior for different ODV data types
pub trait OdvType {
    fn file_type() -> OdvFileType;
    /// Returns the ODV type header string
    fn type_header() -> String;
    fn map_schema(schema: SchemaRef) -> SchemaRef {
        let mut fields = schema.fields().to_vec();
        fields.push(Arc::new(Field::new("Station", DataType::Int64, false)));
        fields.push(Arc::new(Field::new("Type", DataType::Utf8, false)));
        Arc::new(Schema::new(fields))
    }

    fn map_batch(record_batch: RecordBatch) -> RecordBatch {
        Self::map_type(Self::map_station(record_batch))
    }

    /// Maps station information into a record batch
    ///
    /// # Arguments
    /// * `record_batch` - Input batch to add station info to
    ///
    /// # Returns
    /// New record batch with station column added
    fn map_station(record_batch: RecordBatch) -> RecordBatch {
        let mut fields = record_batch.schema().fields().to_vec();
        fields.push(Arc::new(Field::new("Station", DataType::Int64, false)));

        let mut arrays = record_batch.columns().to_vec();
        let station = arrow::array::Int64Array::from(vec![1; record_batch.num_rows()]);
        arrays.push(Arc::new(station) as Arc<dyn arrow::array::Array>);

        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .expect("Mapping of station number failed.")
    }

    /// Maps type information into a record batch
    ///
    /// # Arguments
    /// * `record_batch` - Input batch to add type info to
    ///
    /// # Returns
    /// New record batch with type column added
    fn map_type(record_batch: RecordBatch) -> RecordBatch {
        let mut fields = record_batch.schema().fields().to_vec();
        fields.push(Arc::new(Field::new("Type", DataType::Utf8, false)));

        let mut arrays = record_batch.columns().to_vec();
        let _type = arrow::array::StringArray::from(vec!["*"; record_batch.num_rows()]);
        arrays.push(Arc::new(_type) as Arc<dyn arrow::array::Array>);

        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .expect("Mapping of type number failed.")
    }
}

/// ODV Profile data type
pub struct Profiles;
impl OdvType for Profiles {
    fn type_header() -> String {
        format!("//<DataType>Profiles</DataType>")
    }
    fn file_type() -> OdvFileType {
        OdvFileType::Profiles
    }
}

/// ODV Time Series data type
pub struct TimeSeries;
impl OdvType for TimeSeries {
    fn type_header() -> String {
        format!("//<DataType>TimeSeries</DataType>")
    }
    fn file_type() -> OdvFileType {
        OdvFileType::TimeSeries
    }
}

/// ODV Trajectory data type
pub struct Trajectories;
impl OdvType for Trajectories {
    fn file_type() -> OdvFileType {
        OdvFileType::Trajectories
    }
    fn type_header() -> String {
        format!("//<DataType>Trajectories</DataType>")
    }

    fn map_station(record_batch: RecordBatch) -> RecordBatch {
        //Add the station column as an incremental number for each row
        let len = record_batch.num_rows();
        let station = arrow::array::Int64Array::from((1..len as i64 + 1).collect::<Vec<i64>>());

        let mut fields = record_batch.schema().fields().to_vec();
        fields.push(Arc::new(Field::new("Station", DataType::Int64, false)));

        let mut arrays = record_batch.columns().to_vec();
        arrays.push(Arc::new(station) as Arc<dyn arrow::array::Array>);
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .expect("Mapping of station number failed.")
    }
}

/// ODV Error data type for problematic data
pub struct Error;
impl OdvType for Error {
    fn type_header() -> String {
        format!("//<DataType>Error</DataType>")
    }
    fn file_type() -> OdvFileType {
        OdvFileType::Profiles
    }
}
/// A schema mapper that handles the transformation of input Arrow record batches to ODV-compatible formats.
/// This struct maintains the mapping between input and output schemas and handles column projections.
pub struct OdvBatchSchemaMapper {
    /// The original input schema from the source data
    input_schema: SchemaRef,
    /// The transformed schema that matches ODV format requirements
    output_schema: SchemaRef,
    /// Column indices used for projecting input data to output format
    projection: Arc<[usize]>,
}

impl OdvBatchSchemaMapper {
    /// Creates a new ODV batch schema mapper with the given input schema and ODV options.
    ///
    /// This function:
    /// 1. Maps required ODV columns (Cruise, Station, Type, Time, Longitude, Latitude)
    /// 2. Handles data columns including their quality flags if specified
    /// 3. Maps metadata columns
    /// 4. Creates projection indices for efficient data transformation
    ///
    /// # Arguments
    /// * `input_schema` - The schema of the input data
    /// * `odv_options` - Configuration options for ODV output format
    ///
    /// # Returns
    /// A Result containing the new OdvBatchSchemaMapper or an error if schema mapping fails
    pub fn new(input_schema: SchemaRef, odv_options: OdvOptions) -> anyhow::Result<Self> {
        //Get the projection to create the output schema
        let mut output_fields = vec![];
        let mut projection = vec![];

        let (projection_idx, field) = input_schema
            .column_with_name(&odv_options.key_column)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Key column '{}' not found in input schema.",
                    odv_options.key_column
                )
            })?;

        projection.push(projection_idx);
        output_fields.push(field.clone().with_name("Cruise"));

        let (projection_idx, field) = input_schema
            .column_with_name("Station")
            .ok_or_else(|| anyhow::anyhow!("Station column not found in input schema.",))?;

        projection.push(projection_idx);
        output_fields.push(field.clone().with_name("Station"));

        let (projection_idx, field) = input_schema
            .column_with_name("Type")
            .ok_or_else(|| anyhow::anyhow!("Type column not found in input schema.",))?;

        projection.push(projection_idx);
        output_fields.push(field.clone().with_name("Type"));

        let (projection_idx, field) = input_schema
            .column_with_name(&odv_options.time_column.column_name)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Time column '{}' not found in input schema.",
                    odv_options.time_column.column_name
                )
            })?;

        projection.push(projection_idx);
        output_fields.push(field.clone().with_name("yyyy-MM-ddTHH:mm:ss.SSS"));

        let (projection_idx, field) = input_schema
            .column_with_name(&odv_options.longitude_column.column_name)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Longitude column '{}' not found in input schema.",
                    odv_options.longitude_column.column_name
                )
            })?;

        projection.push(projection_idx);
        output_fields.push(field.clone().with_name("Longitude [degrees east]"));

        let (projection_idx, field) = input_schema
            .column_with_name(&odv_options.latitude_column.column_name)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Latitude column '{}' not found in input schema.",
                    odv_options.latitude_column.column_name
                )
            })?;

        projection.push(projection_idx);
        output_fields.push(field.clone().with_name("Latitude [degrees north]"));

        //Write time column
        let (projection_idx, field) = input_schema
            .column_with_name(&odv_options.time_column.column_name)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Time column '{}' not found in input schema.",
                    odv_options.time_column.column_name
                )
            })?;

        projection.push(projection_idx);
        output_fields.push(field.clone().with_name("time_ISO8601"));

        if let Some(time_qc_col) = odv_options.time_column.qf_column {
            let (projection_idx, field) =
                input_schema.column_with_name(&time_qc_col).ok_or_else(|| {
                    anyhow::anyhow!(
                        "Time QF column '{}' not found in input schema.",
                        time_qc_col
                    )
                })?;

            projection.push(projection_idx);
            output_fields.push(field.clone().with_name(format!(
                "QV:{}:{}",
                odv_options.qf_schema, odv_options.time_column.column_name
            )));
        }

        let (projection_idx, field) = input_schema
            .column_with_name(&odv_options.depth_column.column_name)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Depth column '{}' not found in input schema.",
                    odv_options.depth_column.column_name
                )
            })?;
        let depth_column_name = generate_column_name(
            &odv_options.depth_column.column_name,
            odv_options.depth_column.units.as_deref(),
        );
        projection.push(projection_idx);
        output_fields.push(field.clone().with_name(depth_column_name));

        if let Some(depth_qc_col) = odv_options.depth_column.qf_column {
            let (projection_idx, field) =
                input_schema
                    .column_with_name(&depth_qc_col)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "Depth QF column '{}' not found in input schema.",
                            depth_qc_col
                        )
                    })?;

            projection.push(projection_idx);
            output_fields.push(field.clone().with_name(format!(
                "QV:{}:{}",
                odv_options.qf_schema, odv_options.depth_column.column_name
            )));
        }

        for data_column in odv_options.data_columns.iter() {
            let (projection_idx, field) = input_schema
                .column_with_name(&data_column.column_name)
                .ok_or_else(|| {
                anyhow::anyhow!(
                    "Data column '{}' not found in input schema.",
                    data_column.column_name
                )
            })?;
            let column_name =
                generate_column_name(&data_column.column_name, data_column.units.as_deref());
            projection.push(projection_idx);
            output_fields.push(field.clone().with_name(column_name.clone()));

            if let Some(qf_col) = &data_column.qf_column {
                let (projection_idx, field) =
                    input_schema.column_with_name(qf_col).ok_or_else(|| {
                        anyhow::anyhow!("QF column '{}' not found in input schema.", qf_col)
                    })?;

                projection.push(projection_idx);
                output_fields.push(
                    field
                        .clone()
                        .with_name(format!("QV:{}:{}", odv_options.qf_schema, column_name)),
                );
            }
        }

        for meta_column in &odv_options.meta_columns {
            let (projection_idx, field) = input_schema
                .column_with_name(&meta_column.column_name)
                .ok_or_else(|| {
                anyhow::anyhow!(
                    "Meta column '{}' not found in input schema.",
                    meta_column.column_name
                )
            })?;
            let column_name =
                generate_column_name(&meta_column.column_name, meta_column.units.as_deref());
            projection.push(projection_idx);
            output_fields.push(field.clone().with_name(column_name));
        }

        let output_schema = Arc::new(Schema::new(output_fields));

        Ok(Self {
            input_schema,
            output_schema,
            projection: projection.into(),
        })
    }

    /// Returns a reference to the output schema that will be used for ODV data.
    ///
    /// # Returns
    /// A cloned reference to the output schema
    pub fn output_schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    pub fn input_schema(&self) -> SchemaRef {
        self.input_schema.clone()
    }

    pub fn projection(&self) -> Arc<[usize]> {
        self.projection.clone()
    }

    /// Maps an input record batch to the ODV output format using the configured schema mapping.
    ///
    /// # Arguments
    /// * `batch` - The input record batch to transform
    ///
    /// # Returns
    /// A new record batch with the ODV-compatible schema
    ///
    /// # Panics
    /// Will panic if the input batch schema doesn't match the expected input schema
    pub fn map(&self, batch: RecordBatch) -> RecordBatch {
        assert_eq!(
            &batch.schema(),
            &self.input_schema,
            "Batch schema does not match expected input schema. This is a bug."
        );
        let projected_batch = batch.project(&self.projection).unwrap();
        let arrays = projected_batch.columns().to_vec();
        RecordBatch::try_new(self.output_schema.clone(), arrays).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::*;
    use arrow::datatypes::*;
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    use crate::writer::AsyncOdvWriter;
    use crate::writer::OdvOptions;

    use super::OdvBatchType;

    // #[test]
    // fn test_key_batches() {
    //     let schema = Schema::new(vec![
    //         Field::new("key", DataType::Utf8, false),
    //         Field::new("value", DataType::Int64, false),
    //     ]);
    //     let batch = RecordBatch::try_new(
    //         Arc::new(schema),
    //         vec![
    //             Arc::new(arrow::array::StringArray::from(vec!["a", "a", "b", "b"])),
    //             Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
    //         ],
    //     )
    //     .unwrap();

    //     let options = OdvOptions::default().with_key_column("key".to_string());

    //     let result = AsyncOdvWriter::key_batches(batch, &options).unwrap();

    //     assert_eq!(result.len(), 2);
    //     assert_eq!(result[0].num_rows(), 2);
    //     assert_eq!(result[1].num_rows(), 2);
    // }

    // #[test]
    // fn test_classify_batch() {
    //     let schema = Schema::new(vec![
    //         Field::new("Cruise", DataType::Utf8, false),
    //         Field::new("Longitude [degrees east]", DataType::Float64, false),
    //         Field::new("Latitude [degrees north]", DataType::Float64, false),
    //         Field::new("yyyy-MM-ddTHH:mm:ss.SSS", DataType::Int64, false),
    //         Field::new("Depth [m]", DataType::Float64, false),
    //     ]);

    //     // Test Profile
    //     let profile_batch = RecordBatch::try_new(
    //         Arc::new(schema.clone()),
    //         vec![
    //             Arc::new(arrow::array::StringArray::from(vec!["a", "a"])),
    //             Arc::new(Float64Array::from(vec![1.0, 1.0])),
    //             Arc::new(Float64Array::from(vec![2.0, 2.0])),
    //             Arc::new(Int64Array::from(vec![100, 100])),
    //             Arc::new(Float64Array::from(vec![10.0, 20.0])),
    //         ],
    //     )
    //     .unwrap();

    //     match AsyncOdvWriter::classify_batch(profile_batch, &OdvOptions::default()).unwrap() {
    //         OdvBatchType::Profile(_) => (),
    //         _ => panic!("Expected Profile type"),
    //     }

    //     // Test TimeSeries
    //     let timeseries_batch = RecordBatch::try_new(
    //         Arc::new(schema.clone()),
    //         vec![
    //             Arc::new(arrow::array::StringArray::from(vec!["a", "a"])),
    //             Arc::new(Float64Array::from(vec![1.0, 1.0])),
    //             Arc::new(Float64Array::from(vec![2.0, 2.0])),
    //             Arc::new(Int64Array::from(vec![100, 200])),
    //             Arc::new(Float64Array::from(vec![10.0, 10.0])),
    //         ],
    //     )
    //     .unwrap();

    //     match AsyncOdvWriter::classify_batch(timeseries_batch, &OdvOptions::default()).unwrap() {
    //         OdvBatchType::TimeSeries(_) => (),
    //         _ => panic!("Expected TimeSeries type"),
    //     }

    //     // Test Trajectory
    //     let trajectory_batch = RecordBatch::try_new(
    //         Arc::new(schema),
    //         vec![
    //             Arc::new(arrow::array::StringArray::from(vec!["a", "a"])),
    //             Arc::new(Float64Array::from(vec![1.0, 2.0])),
    //             Arc::new(Float64Array::from(vec![2.0, 3.0])),
    //             Arc::new(Int64Array::from(vec![100, 200])),
    //             Arc::new(Float64Array::from(vec![10.0, 10.0])),
    //         ],
    //     )
    //     .unwrap();

    //     match AsyncOdvWriter::classify_batch(trajectory_batch, &OdvOptions::default()).unwrap() {
    //         OdvBatchType::Trajectory(_) => (),
    //         _ => panic!("Expected Trajectory type"),
    //     }
    // }

    // #[test]
    // fn test_has_changes() {
    //     let array = Float64Array::from(vec![1.0, 1.0, 1.0]);
    //     assert!(!AsyncOdvWriter::has_changes(&array));

    //     let array = Float64Array::from(vec![1.0, 2.0, 1.0]);
    //     assert!(AsyncOdvWriter::has_changes(&array));

    //     let array = Float64Array::from(vec![1.0]);
    //     assert!(!AsyncOdvWriter::has_changes(&array));
    // }
}
