//! The `.dods` response: the DDS, `Data:\n`, then the XDR rows and the end marker.

use std::future::ready;

use arrow::record_batch::RecordBatch;
use axum::body::Body;
use axum::response::Response;
use beacon_core::query::Query;
use beacon_core::runtime::Runtime;
use beacon_core::AuthIdentity;
use bytes::{Bytes, BytesMut};
use futures::{stream, Stream, StreamExt};

use crate::compile;
use crate::constraint;
use crate::dds::dds;
use crate::error::DapError;
use crate::headers::dap2_response;
use crate::http::load_schema;
use crate::types::{convert, DapType, ServedColumn};
use crate::xdr::{encode_rows, END_OF_SEQUENCE};

/// The `.dods` response of `table` for the raw constraint `query`.
pub(crate) async fn dods_response(
    runtime: &Runtime,
    identity: AuthIdentity,
    table: &str,
    query: &str,
) -> Result<Response, DapError> {
    let (reference, schema) = load_schema(runtime, &identity, table).await?;
    let constraint = constraint::parse(query)?;
    let compiled = compile::compile(&reference, &schema, &constraint)?;
    let columns: Vec<&ServedColumn> = compiled.columns.iter().collect();
    let dap_types: Vec<DapType> = compiled.columns.iter().map(|c| c.dap_type).collect();

    let result = runtime
        .run_query(
            Query::sql_with_params(compiled.sql.clone(), compiled.params.clone()),
            identity,
        )
        .await
        .map_err(|error| DapError::Query(error.to_string()))?;
    let mut batches = result
        .into_record_stream()
        .map_err(|error| DapError::Internal(error.to_string()))?;

    // Pull the first batch before the status goes out, as `/api/query` does. An error here
    // still gets a DAP2 error document.
    let mut head = BytesMut::from(format!("{}Data:\n", dds(table, &columns)).as_bytes());
    match batches.next().await {
        Some(Err(error)) => return Err(DapError::Query(error.to_string())),
        Some(Ok(batch)) => encode_batch(&batch, &dap_types, &mut head)?,
        None => {}
    }

    Ok(dap2_response(
        "dods-data",
        "application/octet-stream",
        Body::from_stream(body(head.freeze(), dap_types, batches)),
    ))
}

/// The body after the first batch: `head`, the rows of each batch in `rest`, then the end
/// marker.
///
/// DAP2 has no error inside the data. An error ends the body without the end marker, so a
/// client sees a short stream and never a partial result that looks complete.
fn body<S>(
    head: Bytes,
    dap_types: Vec<DapType>,
    rest: S,
) -> impl Stream<Item = Result<Bytes, std::io::Error>>
where
    S: Stream<Item = datafusion::error::Result<RecordBatch>>,
{
    let rows = rest.map(move |batch| {
        let batch = batch.map_err(|error| {
            tracing::warn!(%error, "DAP2 data stream failed after the first batch");
            std::io::Error::other(error.to_string())
        })?;
        let mut out = BytesMut::new();
        encode_batch(&batch, &dap_types, &mut out)
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        Ok(out.freeze())
    });
    let end = stream::once(ready(Ok(Bytes::from_static(&END_OF_SEQUENCE))));
    stream::once(ready(Ok(head)))
        .chain(rows)
        .chain(end)
        .scan(false, |failed, chunk| {
            if *failed {
                return ready(None);
            }
            *failed = chunk.is_err();
            ready(Some(chunk))
        })
}

/// Appends the rows of `batch`. Its columns are in the order of `dap_types`.
fn encode_batch(
    batch: &RecordBatch,
    dap_types: &[DapType],
    out: &mut BytesMut,
) -> Result<(), DapError> {
    let converted = batch
        .columns()
        .iter()
        .zip(dap_types)
        .map(|(array, dap_type)| convert(array, *dap_type))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| DapError::Internal(format!("DAP2 value conversion failed: {error}")))?;
    encode_rows(&converted, out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::error::DataFusionError;
    use futures::{executor::block_on, stream, StreamExt};
    use std::sync::Arc;

    fn batch(values: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    #[test]
    fn batches_arrive_in_order_and_the_end_marker_comes_last() {
        let rest = stream::iter(vec![Ok(batch(vec![1])), Ok(batch(vec![2]))]);

        let chunks: Vec<_> = block_on(
            body(Bytes::from_static(b"HEAD"), vec![DapType::Int32], rest).collect::<Vec<_>>(),
        );

        let bytes: Vec<u8> = chunks
            .into_iter()
            .flat_map(|chunk| chunk.unwrap().to_vec())
            .collect();
        let mut expected = b"HEAD".to_vec();
        expected.extend([0x5A, 0, 0, 0, 0, 0, 0, 1, 0x5A, 0, 0, 0, 0, 0, 0, 2]);
        expected.extend(END_OF_SEQUENCE);
        assert_eq!(bytes, expected);
    }

    #[test]
    fn a_failure_ends_the_body_without_the_end_marker() {
        let rest = stream::iter(vec![
            Ok(batch(vec![1])),
            Err(DataFusionError::Execution("disk gone".into())),
            Ok(batch(vec![2])),
        ]);

        let chunks: Vec<_> = block_on(
            body(Bytes::from_static(b"HEAD"), vec![DapType::Int32], rest).collect::<Vec<_>>(),
        );

        assert_eq!(chunks.len(), 3, "head, one batch, then the error");
        assert!(chunks[2].is_err());
        for chunk in chunks.iter().take(2) {
            assert!(!chunk.as_ref().unwrap().ends_with(&END_OF_SEQUENCE));
        }
    }
}
