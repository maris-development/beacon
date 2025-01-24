

pub struct Runtime {

}

impl Runtime {
    pub fn new() {
        datafusion::dataframe::DataFrame::execute_stream(self)
    }
}