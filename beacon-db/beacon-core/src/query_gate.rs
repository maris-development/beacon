//! The gate that gives `VACUUM` the runtime for itself.
//!
//! Each query holds a [`QueryGuard`] until its result stream ends. `VACUUM`
//! holds an [`ExclusiveGuard`] for its run. The gate refuses the exclusive
//! guard while a different query is active, and it refuses each new query
//! while the exclusive guard exists.

use std::sync::{Arc, Mutex, MutexGuard};

/// The error text of a query that arrives during a `VACUUM`.
const VACUUM_IN_PROGRESS: &str = "a VACUUM is in progress";

#[derive(Debug, Default)]
struct GateState {
    /// The queries that hold a [`QueryGuard`].
    active: usize,
    /// Whether an [`ExclusiveGuard`] exists.
    exclusive: bool,
}

/// Counts the active queries and holds the exclusive mode of a `VACUUM`.
#[derive(Debug, Default)]
pub(crate) struct QueryGate {
    state: Mutex<GateState>,
}

impl QueryGate {
    /// The state. A poisoned lock is still consistent: each change is one
    /// assignment.
    fn state(&self) -> MutexGuard<'_, GateState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Registers a query.
    ///
    /// # Errors
    ///
    /// Returns an error if a `VACUUM` is in progress.
    pub(crate) fn enter(self: &Arc<Self>) -> anyhow::Result<QueryGuard> {
        let mut state = self.state();
        if state.exclusive {
            anyhow::bail!(VACUUM_IN_PROGRESS);
        }
        state.active += 1;
        Ok(QueryGuard(Arc::clone(self)))
    }

    /// Starts the exclusive mode. The caller is one of the active queries.
    ///
    /// # Errors
    ///
    /// Returns an error if a different query is active or a `VACUUM` is in
    /// progress.
    pub(crate) fn begin_exclusive(self: &Arc<Self>) -> anyhow::Result<ExclusiveGuard> {
        let mut state = self.state();
        if state.exclusive {
            anyhow::bail!(VACUUM_IN_PROGRESS);
        }
        if state.active > 1 {
            anyhow::bail!(
                "VACUUM needs exclusive access: {} other queries are active",
                state.active - 1
            );
        }
        state.exclusive = true;
        Ok(ExclusiveGuard(Arc::clone(self)))
    }
}

/// One active query. The drop removes the query from the gate.
#[derive(Debug)]
pub(crate) struct QueryGuard(Arc<QueryGate>);

impl Drop for QueryGuard {
    fn drop(&mut self) {
        self.0.state().active -= 1;
    }
}

/// The exclusive mode of a `VACUUM`. The drop opens the gate again.
#[derive(Debug)]
pub(crate) struct ExclusiveGuard(Arc<QueryGate>);

impl Drop for ExclusiveGuard {
    fn drop(&mut self) {
        self.0.state().exclusive = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enter_is_refused_in_exclusive_mode() {
        let gate = Arc::new(QueryGate::default());
        let _vacuum = gate.enter().unwrap();
        let _exclusive = gate.begin_exclusive().unwrap();

        let error = gate.enter().unwrap_err();

        assert_eq!(error.to_string(), "a VACUUM is in progress");
    }

    #[test]
    fn exclusive_is_refused_with_another_query() {
        let gate = Arc::new(QueryGate::default());
        let _vacuum = gate.enter().unwrap();
        let _other = gate.enter().unwrap();

        let error = gate.begin_exclusive().unwrap_err();

        assert!(error.to_string().contains("1 other queries"), "{error}");
    }

    #[test]
    fn exclusive_is_refused_in_exclusive_mode() {
        let gate = Arc::new(QueryGate::default());
        let _exclusive = gate.begin_exclusive().unwrap();

        let error = gate.begin_exclusive().unwrap_err();

        assert_eq!(error.to_string(), "a VACUUM is in progress");
    }

    #[test]
    fn exclusive_is_allowed_for_the_only_query() {
        let gate = Arc::new(QueryGate::default());
        let _vacuum = gate.enter().unwrap();

        assert!(gate.begin_exclusive().is_ok());
    }

    #[test]
    fn dropped_exclusive_guard_opens_the_gate() {
        let gate = Arc::new(QueryGate::default());
        drop(gate.begin_exclusive().unwrap());

        assert!(gate.enter().is_ok());
    }

    #[test]
    fn dropped_query_guard_frees_its_slot() {
        let gate = Arc::new(QueryGate::default());
        drop(gate.enter().unwrap());
        let _vacuum = gate.enter().unwrap();

        assert!(gate.begin_exclusive().is_ok());
    }
}
