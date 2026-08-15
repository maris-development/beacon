//! Runtime-owned settings.
//!
//! Settings are supplied to [`RuntimeBuilder`](crate::runtime_builder::RuntimeBuilder)
//! by the embedder — never read from a process-global — and are published on the
//! session config, so plan- and execution-time code (where no `Runtime` handle
//! exists) can recover them from the `SessionContext`.
//!
//! The types here are the embedder-facing shape of the builder's settings. What
//! is published on the session is
//! [`BeaconOptions`](beacon_datafusion_ext::settings::BeaconOptions), the
//! `beacon.*` config namespace — so the same values an embedder supplies are also
//! what `SET beacon.default_table = 'obs'` writes, and a reader here sees the
//! change without a restart.

use std::time::Duration;

use beacon_datafusion_ext::settings::BeaconOptions;
use datafusion::execution::context::SessionConfig;
use datafusion::prelude::SessionContext;

/// SQL-facing settings: how client queries are compiled and how their results are
/// streamed back.
///
/// Published on the session config, so [`SqlSettings::from_session`] recovers the
/// runtime's settings wherever a `SessionContext` is in hand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlSettings {
    /// The table a JSON query without a `from` resolves against.
    pub default_table: String,
    /// Whether the JSON query compiler pushes the selected columns into the scan.
    pub enable_pushdown_projection: bool,
    /// How result batches are coalesced before reaching a client.
    pub stream_coalesce: SqlStreamCoalesceSettings,
}

impl Default for SqlSettings {
    fn default() -> Self {
        Self {
            default_table: "default".to_string(),
            enable_pushdown_projection: true,
            stream_coalesce: SqlStreamCoalesceSettings::default(),
        }
    }
}

impl SqlSettings {
    /// Recovers the settings published on `session_ctx`, falling back to the
    /// defaults if the namespace is absent (e.g. a bare session in a unit test).
    ///
    /// Read per statement, never cached: a `SET` rewrites the namespace on the
    /// shared session, and the next query has to see it.
    pub fn from_session(session_ctx: &SessionContext) -> Self {
        Self::from_config(session_ctx.state().config())
    }

    /// [`Self::from_session`] against a `SessionConfig` directly, for callers
    /// holding a `&dyn Session` rather than a `SessionContext`.
    pub fn from_config(config: &SessionConfig) -> Self {
        Self::from_options(&BeaconOptions::from_config(config))
    }

    /// The subset of the `beacon.*` namespace these settings mirror.
    pub fn from_options(options: &BeaconOptions) -> Self {
        Self {
            default_table: options.default_table.clone(),
            enable_pushdown_projection: options.enable_pushdown_projection,
            stream_coalesce: SqlStreamCoalesceSettings {
                enabled: options.sql.stream_coalesce.enabled,
                target_rows: options.sql.stream_coalesce.target_rows,
                flush_timeout_ms: options.sql.stream_coalesce.flush_timeout_ms,
                max_rows: options.sql.stream_coalesce.max_rows,
            },
        }
    }

    /// Writes these settings into `options`, which is how the builder's values
    /// become the namespace's startup state.
    pub fn apply_to(&self, options: &mut BeaconOptions) {
        options.default_table = self.default_table.clone();
        options.enable_pushdown_projection = self.enable_pushdown_projection;
        options.sql.stream_coalesce.enabled = self.stream_coalesce.enabled;
        options.sql.stream_coalesce.target_rows = self.stream_coalesce.target_rows;
        options.sql.stream_coalesce.flush_timeout_ms = self.stream_coalesce.flush_timeout_ms;
        options.sql.stream_coalesce.max_rows = self.stream_coalesce.max_rows;
    }
}

/// Settings for the SQL result-stream coalescer: small record batches coming out
/// of a physical plan are merged into larger ones before they reach a client, so
/// transports do not pay per-batch overhead on a stream of tiny batches.
///
/// See [`CoalesceSqlStream`](crate::statement_plan::CoalesceSqlStream) for the
/// coalescer itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqlStreamCoalesceSettings {
    /// Whether to coalesce at all. Disabled passes batches through untouched.
    pub enabled: bool,
    /// Buffer batches until at least this many rows have accumulated.
    pub target_rows: usize,
    /// Flush a non-empty buffer after this long, even below `target_rows`, so a
    /// slow-producing plan stays responsive. `0` disables the timeout.
    pub flush_timeout_ms: u64,
    /// Hard upper bound on a buffered batch, so one oversized input batch cannot
    /// grow the buffer without limit.
    pub max_rows: usize,
}

impl Default for SqlStreamCoalesceSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            target_rows: 64 * 1024,
            flush_timeout_ms: 25,
            max_rows: 256 * 1024,
        }
    }
}

impl SqlStreamCoalesceSettings {
    /// The flush deadline as a duration, or `None` when timed flushing is off.
    pub(crate) fn flush_timeout(&self) -> Option<Duration> {
        if self.flush_timeout_ms == 0 {
            None
        } else {
            Some(Duration::from_millis(self.flush_timeout_ms))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::execution::context::SessionConfig;

    /// `0` is the documented "no timed flushing" sentinel, not a zero-length
    /// deadline — a `Some(Duration::ZERO)` would make the coalescer flush on every
    /// poll. Every other value maps to that many milliseconds.
    #[test]
    fn zero_flush_timeout_disables_timed_flushing() {
        let disabled = SqlStreamCoalesceSettings {
            flush_timeout_ms: 0,
            ..Default::default()
        };
        assert_eq!(disabled.flush_timeout(), None);

        let enabled = SqlStreamCoalesceSettings {
            flush_timeout_ms: 25,
            ..Default::default()
        };
        assert_eq!(enabled.flush_timeout(), Some(Duration::from_millis(25)));
    }

    /// The settings the runtime publishes on the session config must be the ones
    /// plan-/execution-time code recovers — the `beacon.*` namespace is the only
    /// channel between the two.
    #[test]
    fn from_session_recovers_the_published_settings() {
        let published = SqlSettings {
            default_table: "observations".to_string(),
            enable_pushdown_projection: false,
            stream_coalesce: SqlStreamCoalesceSettings {
                enabled: false,
                target_rows: 7,
                flush_timeout_ms: 0,
                max_rows: 9,
            },
        };
        let mut options = BeaconOptions::default();
        published.apply_to(&mut options);
        let mut config = SessionConfig::new();
        config.options_mut().extensions.insert(options);
        let session_ctx = SessionContext::new_with_config(config);

        assert_eq!(SqlSettings::from_session(&session_ctx), published);
    }

    /// A bare session (a unit test, or any context built without the runtime)
    /// carries no namespace; recovery must fall back to the defaults rather than
    /// fail, since every caller treats the settings as always available.
    #[test]
    fn from_session_falls_back_to_defaults_without_the_namespace() {
        let session_ctx = SessionContext::new();
        assert_eq!(SqlSettings::from_session(&session_ctx), SqlSettings::default());
    }

    /// A `SET` writes the namespace on the shared session, so a reader that ran
    /// once must not have cached the old value. This pins the whole point of
    /// moving off the type-keyed extension map.
    #[test]
    fn a_later_set_is_visible_to_the_next_read() {
        let mut config = SessionConfig::new();
        config.options_mut().extensions.insert(BeaconOptions::default());
        let session_ctx = SessionContext::new_with_config(config);

        assert_eq!(SqlSettings::from_session(&session_ctx).default_table, "default");

        session_ctx
            .state_ref()
            .write()
            .config_mut()
            .options_mut()
            .set("beacon.default_table", "observations")
            .unwrap();

        assert_eq!(
            SqlSettings::from_session(&session_ctx).default_table,
            "observations"
        );
    }
}
