//! Dimension-aware physical operators and the optimizer rules that place them.
//!
//! The nd data types ([`NdRecordBatch`](beacon_nd_encoding::NdRecordBatch),
//! [`Dimensions`](beacon_nd_encoding::Dimensions)) and their encoding live in
//! `beacon-nd-encoding`. This crate keeps columns *un-broadcast* through the
//! physical plan until a terminal node materializes them:
//!
//! - [`BroadcastMap`]: a broadcast expressed as stride arithmetic; no data
//!   moves.
//! - [`NdArrowArrayExt`] and [`NdRecordBatchExt`]: materialize nd data onto its
//!   target grid.
//! - [`exec::NdSourceExec`]: streams nd batches from an encoded child plan.
//! - [`exec::NdFilterExec`] and [`exec::NdProjectionExec`]: evaluate on the
//!   un-broadcast columns.
//! - [`exec::NdBroadcastExec`]: the terminal node that materializes nd batches
//!   into flat `RecordBatch`es.
//! - [`NdProjectionPushdown`] and [`NdFilterPushdown`]: sink projections and
//!   filters below the broadcast.

pub mod broadcast;
pub mod exec;
pub mod materialize;
pub mod optimizer;

pub use broadcast::BroadcastMap;
pub use materialize::{NdArrowArrayExt, NdRecordBatchExt};
pub use optimizer::{NdFilterPushdown, NdProjectionPushdown, is_pushable_expr};

#[cfg(test)]
mod tests;
