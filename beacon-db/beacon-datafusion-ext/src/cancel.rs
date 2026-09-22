//! A cancellation token for one query, carried in the session config.
//!
//! Dropping a result stream stops a query at the next `await`. Some work has
//! no `await` to stop at: a blocking pivot over a million rows, or a wait on
//! another partition's open. A scan that reads through a token stops there
//! too, and it stops when the query is cancelled by name while the stream is
//! still held.
//!
//! The runtime registers one [`QueryCancellation`] per query, as an extension
//! on the `SessionConfig` of the state it plans with. A format reads it at
//! plan time with [`query_cancellation`]. Without one, the format gets a fresh
//! token that never fires, and dropping the stream stays the one way to stop
//! the query.

use datafusion::catalog::Session;
use tokio_util::sync::CancellationToken;

/// The token of one query. Fires once, when the query is cancelled.
#[derive(Debug, Clone, Default)]
pub struct QueryCancellation {
    token: CancellationToken,
}

impl QueryCancellation {
    pub fn new(token: CancellationToken) -> Self {
        Self { token }
    }

    pub fn token(&self) -> &CancellationToken {
        &self.token
    }
}

/// The token of the query `state` plans, or one that never fires.
pub fn query_cancellation(state: &dyn Session) -> CancellationToken {
    state
        .config()
        .get_extension::<QueryCancellation>()
        .map(|cancellation| cancellation.token().clone())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::prelude::{SessionConfig, SessionContext};

    use super::*;

    #[test]
    fn a_session_without_a_token_gets_one_that_never_fires() {
        let ctx = SessionContext::new();
        assert!(!query_cancellation(&ctx.state()).is_cancelled());
    }

    #[test]
    fn a_registered_token_is_the_one_the_scan_gets() {
        let token = CancellationToken::new();
        let config =
            SessionConfig::new().with_extension(Arc::new(QueryCancellation::new(token.clone())));
        let ctx = SessionContext::new_with_config(config);

        let seen = query_cancellation(&ctx.state());
        assert!(!seen.is_cancelled());
        token.cancel();
        assert!(seen.is_cancelled(), "the scan holds the query's own token");
    }
}
