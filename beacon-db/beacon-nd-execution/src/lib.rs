//! Dimension-aware physical operators and the optimizer rules that place them.
//!
//! The nd data types ([`NdRecordBatch`](beacon_datafusion_ext::nd::NdRecordBatch),
//! [`Dimensions`](beacon_datafusion_ext::nd::Dimensions)) live in
//! `beacon_datafusion_ext::nd`. This crate keeps columns *un-broadcast* through
//! the physical plan until a terminal node materializes them:
//!
//! - [`exec::NdSourceExec`]: streams nd batches from an encoded child plan.
//! - [`exec::NdFilterExec`] and [`exec::NdProjectionExec`]: evaluate on the
//!   un-broadcast columns.
//! - [`exec::NdBroadcastExec`]: the terminal node that materializes nd batches
//!   into flat `RecordBatch`es.
//! - [`NdProjectionPushdown`] and [`NdFilterPushdown`]: sink projections and
//!   filters below the broadcast.

pub mod exec;
pub mod optimizer;

pub use optimizer::{NdFilterPushdown, NdProjectionPushdown, is_pushable_expr};

#[cfg(test)]
mod tests;
