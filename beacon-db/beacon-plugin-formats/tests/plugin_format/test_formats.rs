//! In-process plugin formats for the wrapper tests. They cross the FFI tables
//! through the foreign path, as a plugin library does.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::task::Poll;

use arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use beacon_plugin_sdk::datafusion_ffi::get_library_marker_id;
use beacon_plugin_sdk::file_format::{
    FFI_FileFormat, FFI_FileFormatFactory, ForeignFormatReader, FormatFactory, FormatReader,
    OutputKind, ScanRequest, import_format_factory,
};
use beacon_plugin_sdk::nd::encoding::{encode_nd_record_batch, encoded_schema};
use beacon_plugin_sdk::nd::{Dimension, Dimensions, NdArrowArray, NdRecordBatch};
use datafusion::common::Statistics;
use datafusion::common::stats::Precision;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use tokio::runtime::Handle;

extern "C" fn foreign_marker_id() -> usize {
    get_library_marker_id() + 1
}

/// Exports `factory` and imports it through the foreign path.
pub fn foreign(factory: Arc<dyn FormatFactory>) -> Arc<dyn FormatFactory> {
    let mut ffi = FFI_FileFormatFactory::new(factory);
    ffi.library_marker_id = foreign_marker_id;
    import_format_factory(ffi, Handle::current()).expect("import the factory")
}

/// Exports `reader` and imports it through the foreign path. A format from another
/// library gives such a reader; in one test binary the table would take the local
/// shortcut instead.
pub fn foreign_reader(reader: Arc<dyn FormatReader>) -> Arc<dyn FormatReader> {
    let mut ffi = FFI_FileFormat::new(reader);
    ffi.library_marker_id = foreign_marker_id;
    ForeignFormatReader::try_new(ffi, Handle::current()).expect("import the reader")
}

/// What the line format saw, shared with the test.
#[derive(Debug, Default)]
pub struct Seen {
    pub options: Mutex<Vec<HashMap<String, String>>>,
    pub filters: Mutex<Vec<usize>>,
    pub requested: Mutex<Vec<Vec<String>>>,
}

/// `linetext` (`.ltxt`): a header line `name:type,...`, then one comma-separated row
/// per line. Types: `Int32`, `Int64`, `Float64`, `Utf8`. A file with no bytes has no
/// batch; a file with only the header has one batch of no rows.
#[derive(Debug, Default)]
pub struct LineText {
    pub seen: Arc<Seen>,
}

impl FormatFactory for LineText {
    fn name(&self) -> &str {
        "linetext"
    }

    fn extensions(&self) -> Vec<String> {
        vec!["ltxt".to_string()]
    }

    fn output_kind(&self) -> OutputKind {
        OutputKind::Flat
    }

    fn create(&self, options: &HashMap<String, String>) -> Result<Arc<dyn FormatReader>> {
        self.seen.options.lock().unwrap().push(options.clone());
        if options.contains_key("bad") {
            return datafusion::common::plan_err!("unknown option bad");
        }
        Ok(foreign_reader(Arc::new(LineReader {
            seen: self.seen.clone(),
            panic_poll: options.get("panic_poll").is_some_and(|v| v == "true"),
            fail_open: options.get("fail_open").is_some_and(|v| v == "true"),
        })))
    }
}

#[derive(Debug)]
struct LineReader {
    seen: Arc<Seen>,
    panic_poll: bool,
    fail_open: bool,
}

fn data_type(name: &str) -> DataType {
    match name {
        "Int32" => DataType::Int32,
        "Int64" => DataType::Int64,
        "Float64" => DataType::Float64,
        _ => DataType::Utf8,
    }
}

fn column(data_type: &DataType, values: &[&str]) -> ArrayRef {
    match data_type {
        DataType::Int32 => Arc::new(Int32Array::from(
            values
                .iter()
                .map(|v| v.parse().ok())
                .collect::<Vec<Option<i32>>>(),
        )),
        DataType::Int64 => Arc::new(Int64Array::from(
            values
                .iter()
                .map(|v| v.parse().ok())
                .collect::<Vec<Option<i64>>>(),
        )),
        DataType::Float64 => Arc::new(Float64Array::from(
            values
                .iter()
                .map(|v| v.parse().ok())
                .collect::<Vec<Option<f64>>>(),
        )),
        _ => Arc::new(StringArray::from(values.to_vec())),
    }
}

/// The batches of one `linetext` file.
pub async fn read_file(
    store: &Arc<dyn ObjectStore>,
    object: &ObjectMeta,
) -> Result<Vec<RecordBatch>> {
    let bytes = store
        .get(&object.location)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .bytes()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let text = String::from_utf8_lossy(&bytes).to_string();
    let mut lines = text.lines();
    let Some(header) = lines.next() else {
        return Ok(Vec::new());
    };
    let fields: Vec<Field> = header
        .split(',')
        .map(|spec| {
            let (name, kind) = spec.split_once(':').unwrap();
            Field::new(name, data_type(kind), true)
        })
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let rows: Vec<Vec<&str>> = lines.map(|l| l.split(',').collect()).collect();
    let columns = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let values: Vec<&str> = rows.iter().map(|row| row[i]).collect();
            column(field.data_type(), &values)
        })
        .collect();
    Ok(vec![RecordBatch::try_new(schema, columns)?])
}

/// The schema in the header of one `linetext` file.
async fn read_schema(store: &Arc<dyn ObjectStore>, object: &ObjectMeta) -> Result<SchemaRef> {
    let batches = read_file(store, object).await?;
    Ok(batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| Arc::new(Schema::empty())))
}

#[async_trait]
impl FormatReader for LineReader {
    async fn infer_schema(
        &self,
        store: Arc<dyn ObjectStore>,
        object: &ObjectMeta,
    ) -> Result<SchemaRef> {
        read_schema(&store, object).await
    }

    async fn infer_stats(
        &self,
        store: Arc<dyn ObjectStore>,
        object: &ObjectMeta,
        table_schema: SchemaRef,
    ) -> Result<Option<Statistics>> {
        let rows: usize = read_file(&store, object)
            .await?
            .iter()
            .map(|b| b.num_rows())
            .sum();
        let mut statistics = Statistics::new_unknown(&table_schema);
        statistics.num_rows = Precision::Exact(rows);
        Ok(Some(statistics))
    }

    async fn open(
        &self,
        store: Arc<dyn ObjectStore>,
        object: &ObjectMeta,
        request: ScanRequest,
    ) -> Result<SendableRecordBatchStream> {
        if self.fail_open {
            return datafusion::common::exec_err!("cannot open this file");
        }
        self.seen
            .filters
            .lock()
            .unwrap()
            .push(request.filters.len());
        self.seen.requested.lock().unwrap().push(
            request
                .schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect(),
        );
        let batches = read_file(&store, object).await?;
        let schema = read_schema(&store, object).await?;
        let items = stream::iter(batches.into_iter().map(Ok));
        if self.panic_poll {
            // The poll after the last batch panics.
            let boom = stream::poll_fn(|_| -> Poll<Option<Result<RecordBatch>>> {
                panic!("boom in plugin poll")
            });
            let stream = futures::StreamExt::chain(items, boom);
            return Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)));
        }
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, items)))
    }
}

/// `gridtext` (`.grid`), an nd format. Lines:
/// `var <name> <type> <dim>=<size>[,<dim>=<size>] : v1,v2,...` (C order). A variable with
/// no dimension is a scalar: `var <name> <type> : v`. A file with no bytes has no batch.
#[derive(Debug, Default)]
pub struct GridText {
    pub seen: Arc<Seen>,
}

impl FormatFactory for GridText {
    fn name(&self) -> &str {
        "gridtext"
    }

    fn extensions(&self) -> Vec<String> {
        vec!["grid".to_string()]
    }

    fn output_kind(&self) -> OutputKind {
        OutputKind::Nd
    }

    fn create(&self, options: &HashMap<String, String>) -> Result<Arc<dyn FormatReader>> {
        self.seen.options.lock().unwrap().push(options.clone());
        Ok(foreign_reader(Arc::new(GridReader {
            seen: self.seen.clone(),
            zero_rows: options.get("zero_rows").is_some_and(|v| v == "true"),
        })))
    }
}

#[derive(Debug)]
struct GridReader {
    seen: Arc<Seen>,
    zero_rows: bool,
}

struct GridVar {
    field: Field,
    dims: Dimensions,
    values: ArrayRef,
}

async fn read_grid(store: &Arc<dyn ObjectStore>, object: &ObjectMeta) -> Result<Vec<GridVar>> {
    let bytes = store
        .get(&object.location)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .bytes()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let text = String::from_utf8_lossy(&bytes).to_string();
    let mut vars = Vec::new();
    for line in text.lines().filter(|l| !l.is_empty()) {
        let (head, values) = line.split_once(" : ").unwrap();
        let parts: Vec<&str> = head.split_whitespace().collect();
        let field = Field::new(parts[1], data_type(parts[2]), true);
        let dims = match parts.get(3) {
            None => Dimensions::scalar(),
            Some(spec) => Dimensions::try_new(
                spec.split(',')
                    .map(|d| {
                        let (name, size) = d.split_once('=').unwrap();
                        Dimension::new(name, size.parse().unwrap())
                    })
                    .collect(),
            )?,
        };
        let values: Vec<&str> = values.split(',').collect();
        vars.push(GridVar {
            values: column(field.data_type(), &values),
            field,
            dims,
        });
    }
    Ok(vars)
}

#[async_trait]
impl FormatReader for GridReader {
    async fn infer_schema(
        &self,
        store: Arc<dyn ObjectStore>,
        object: &ObjectMeta,
    ) -> Result<SchemaRef> {
        let vars = read_grid(&store, object).await?;
        Ok(Arc::new(Schema::new(
            vars.into_iter().map(|v| v.field).collect::<Vec<_>>(),
        )))
    }

    async fn open(
        &self,
        store: Arc<dyn ObjectStore>,
        object: &ObjectMeta,
        request: ScanRequest,
    ) -> Result<SendableRecordBatchStream> {
        let wanted: Vec<String> = request
            .schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        self.seen.requested.lock().unwrap().push(wanted.clone());
        let vars: Vec<GridVar> = read_grid(&store, object)
            .await?
            .into_iter()
            .filter(|v| wanted.contains(v.field.name()))
            .collect();
        let schema = Arc::new(Schema::new(
            vars.iter().map(|v| v.field.clone()).collect::<Vec<_>>(),
        ));
        let encoded = Arc::new(encoded_schema(&schema));
        if vars.is_empty() {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                encoded,
                stream::empty(),
            )));
        }
        if self.zero_rows {
            let empty = RecordBatch::new_empty(encoded.clone());
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                encoded,
                stream::iter(vec![Ok(empty)]),
            )));
        }
        // The target grid: every dimension, in the order the variables name them.
        let mut target: Vec<Dimension> = Vec::new();
        for var in &vars {
            for dim in var.dims.iter() {
                if !target.iter().any(|d| d.name() == dim.name()) {
                    target.push(dim.clone());
                }
            }
        }
        let columns = vars
            .iter()
            .map(|v| NdArrowArray::try_new(v.values.clone(), v.dims.clone()))
            .collect::<Result<Vec<_>>>()?;
        let nd = NdRecordBatch::try_new(schema, columns, Dimensions::try_new(target)?)?;
        let batch = encode_nd_record_batch(&nd)?;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            batch.schema(),
            stream::iter(vec![Ok(batch)]),
        )))
    }
}
