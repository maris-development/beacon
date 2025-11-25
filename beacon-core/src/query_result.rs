use beacon_query::output::QueryOutput;

pub struct QueryResult {
    pub output_buffer: QueryOutput,
    pub query_id: uuid::Uuid,
}
