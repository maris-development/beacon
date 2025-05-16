use beacon_query::output::QueryOutputFile;

pub struct QueryResult {
    pub output_buffer: QueryOutputFile,
    pub query_id: uuid::Uuid,
}
