pub mod error;
pub mod reader;
pub mod writer;

pub type OdvResult<T> = std::result::Result<T, error::OdvError>;
