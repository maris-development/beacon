//! Durable, column-addressable statistics for every file a Beacon instance
//! knows about.
//!
//! # The problem
//!
//! A dense file-by-column matrix over 1M files and 160K distinct column names
//! holds 1.6e11 cells. It cannot be built. Only the pairs that exist may cost
//! anything: at ~50 columns per file that is ~50M cells, which fits in about a
//! gigabyte and answers a three-column predicate in three ranged reads.
//!
//! Everything here follows from that. Files and columns become dense ordinals
//! ([`registry`]) so a cell costs 8 bytes rather than a 200-byte path. Segments
//! store one block per column, holding only the files that declare it
//! ([`segment`]). The manifest skips whole segments before any read
//! ([`manifest`]).
//!
//! # Layers
//!
//! | Module | Holds | Answers |
//! |---|---|---|
//! | [`registry`] | path ↔ [`FileId`], name ↔ [`ColumnId`], per-file summary | "what is this file's id, and its row count?" |
//! | [`segment`] | immutable per-batch blocks, one per column | "what are this column's ranges, per file?" |
//! | [`manifest`] | file id range and column ids per segment | "which segments need reading at all?" |
//! | [`store`] | all three behind one handle | "give me this column's statistics" |
//!
//! # What this crate deliberately does not do
//!
//! It does not depend on DataFusion, and it does not depend on the Beacon binary
//! format. Neither appears in this crate's dependency graph, so
//! `cargo test -p beacon-file-stats` compiles a small tree and runs in seconds.
//! The pruning adapter that turns [`ColumnStats`] into a `PruningPredicate`
//! input belongs behind an optional feature, so the core never gains that
//! dependency.
//!
//! The workspace is a separate matter: `beacon-binary-format` is still a
//! workspace member, so cargo needs that submodule checked out to load the
//! workspace, whichever package `-p` names.
//!
//! # Two rules a caller must not break
//!
//! 1. Push files to a [`SegmentBuilder`] in ascending id order. That is what
//!    keeps blocks sorted without a sort at finish.
//! 2. Batch a segment by path prefix, not by arrival order. Files under one
//!    prefix share columns, so a prefix-local segment holds few distinct columns
//!    and most segments then skip on the manifest alone. Batching at random
//!    makes every segment match every query, and the skip stops working.

pub mod collector;
pub mod error;
pub mod manifest;
#[cfg(feature = "datafusion")]
pub mod pruning;
pub mod registry;
pub mod scalar;
pub mod segment;
pub mod store;
pub mod types;

pub use collector::{CollectReport, CollectorConfig, FileAnalysis, FileAnalyzer, StatsCollector};
pub use error::{FileStatsError, Result};
pub use manifest::{Manifest, SegmentEntry};
#[cfg(feature = "datafusion")]
pub use pruning::{FileStatsPruningStatistics, prune_files};
pub use registry::Registry;
pub use scalar::{StatScalar, super_type};
pub use segment::{ColumnStat, ColumnStats, SegmentBuilder, SegmentReader};
pub use store::FileStatsStore;
pub use types::{ColumnId, FileId, FileRecord, FileState, ObservedFile};
