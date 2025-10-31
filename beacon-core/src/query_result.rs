use beacon_query::output::QueryOutputFile;
use datafusion::execution::SendableRecordBatchStream;
use either::Either;
use futures::stream::BoxStream;

pub struct QueryResult {
    pub output_buffer: Either<QueryOutputFile, SendableRecordBatchStream>,
    pub query_id: uuid::Uuid,
}
