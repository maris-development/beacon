#[derive(thiserror::Error, Debug)]
pub enum StorageError {
    #[error("Event error: {0}")]
    EventError(#[from] crate::event::EventError),
    #[error("Object store error: {0}")]
    ObjectStoreError(#[from] object_store::Error),
    #[error("Event handling not enabled for this store: {0}")]
    EventHandlingError(String),
    #[error("Initialization error: {0}")]
    InitializationError(String),

    #[error("Missing configuration: {key}")]
    MissingConfig { key: &'static str },

    #[error("Invalid configuration for {key}: {message}")]
    InvalidConfig { key: &'static str, message: String },

    #[error("Missing environment variable {var}: {source}")]
    MissingEnvVar {
        var: &'static str,
        #[source]
        source: std::env::VarError,
    },
}
