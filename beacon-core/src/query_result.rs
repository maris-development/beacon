use beacon_query::output::QueryOutput;
use datafusion::execution::SendableRecordBatchStream;
use either::Either;
use futures::stream::BoxStream;

pub struct QueryResult {
    pub output_buffer: Either<QueryOutput, SendableRecordBatchStream>,
    pub query_id: uuid::Uuid,
}
