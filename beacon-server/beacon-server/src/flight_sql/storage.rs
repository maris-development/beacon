//! Expiring storage for Flight SQL statement and prepared-statement handles.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use tonic::Status;
use uuid::Uuid;

struct StoredSql {
    sql: String,
    expires_at: Instant,
}

/// Stores SQL text behind opaque handles with time-based eviction.
#[derive(Clone)]
pub(super) struct SqlHandleStore {
    ttl: Duration,
    statements: Arc<tokio::sync::RwLock<HashMap<String, StoredSql>>>,
}

impl SqlHandleStore {
    /// Creates a new handle store with the supplied time-to-live.
    pub(super) fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            statements: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        }
    }

    /// Stores SQL text and returns the generated opaque handle.
    pub(super) async fn insert(&self, sql: String) -> Bytes {
        let handle = Uuid::new_v4().to_string();
        let expires_at = Instant::now() + self.ttl;

        self.statements
            .write()
            .await
            .insert(handle.clone(), StoredSql { sql, expires_at });

        Bytes::from(handle)
    }

    /// Consumes a one-shot statement handle and returns the stored SQL text.
    pub(super) async fn take(&self, handle: &Bytes) -> Result<String, Status> {
        let key = Self::parse_handle(handle)?;
        let mut statements = self.statements.write().await;
        Self::retain_unexpired(&mut statements);

        statements
            .remove(key)
            .map(|statement| statement.sql)
            .ok_or_else(|| Status::not_found("statement handle not found"))
    }

    /// Resolves a prepared statement handle and refreshes its expiry window.
    pub(super) async fn get(&self, handle: &Bytes) -> Result<String, Status> {
        let key = Self::parse_handle(handle)?;
        let mut statements = self.statements.write().await;
        Self::retain_unexpired(&mut statements);

        statements
            .get_mut(key)
            .map(|statement| {
                // Prepared statements can be executed multiple times, so each successful access
                // extends the handle lifetime.
                statement.expires_at = Instant::now() + self.ttl;
                statement.sql.clone()
            })
            .ok_or_else(|| Status::not_found("prepared statement handle not found"))
    }

    /// Removes a prepared statement handle from the store.
    pub(super) async fn close(&self, handle: &Bytes) -> Result<(), Status> {
        let key = Self::parse_handle(handle)?;
        let removed = self.statements.write().await.remove(key);

        if removed.is_some() {
            Ok(())
        } else {
            Err(Status::not_found("prepared statement handle not found"))
        }
    }

    /// Parses the opaque wire handle into its UTF-8 string representation.
    fn parse_handle(handle: &Bytes) -> Result<&str, Status> {
        std::str::from_utf8(handle)
            .map_err(|_| Status::invalid_argument("statement handle must be valid UTF-8"))
    }

    /// Drops expired handles before the store is queried.
    fn retain_unexpired(statements: &mut HashMap<String, StoredSql>) {
        let now = Instant::now();
        statements.retain(|_, statement| statement.expires_at > now);
    }
}