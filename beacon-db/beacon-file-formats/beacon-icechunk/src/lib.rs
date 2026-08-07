//! Icechunk repositories as a second storage backend for Beacon's zarr reader.
//!
//! [Icechunk](https://icechunk.io) holds a Zarr v3 hierarchy with transactions,
//! snapshots and branches. Beacon reads one through the same code a plain zarr
//! store goes through: the repository supplies the storage a group is opened
//! over, and schema inference, the leaf-group walk, the `beacon-nd-array` scan
//! and the predicate pushdown are unchanged (see `beacon-arrow-zarr`).
//!
//! Two surfaces are exposed:
//! - [`ReadIcechunkFunc`], the `read_icechunk(...)` table function.
//! - [`IcechunkTableDefinition`], the persisted `CREATE EXTERNAL TABLE ... STORED
//!   AS ICECHUNK` definition.
//!
//! ```sql
//! SELECT * FROM read_icechunk('argo/repo');                  -- tip of main
//! SELECT * FROM read_icechunk('argo/repo', 'dev');           -- tip of a branch
//! SELECT * FROM read_icechunk('argo/repo', NULL, 'AB3…');    -- a snapshot
//!
//! CREATE EXTERNAL TABLE argo STORED AS ICECHUNK LOCATION 'argo/repo'
//!   OPTIONS ('branch' 'dev');
//! ```
//!
//! A repository reads from wherever it lives — the local datasets store, S3, GCS
//! or Azure — with no local copy. Remote credentials come from the environment,
//! the same source the rest of Beacon's object stores use.
//!
//! # Read only
//!
//! Beacon reads Icechunk; it does not write it. There is no commit, no branch
//! creation and no `INSERT`.
//!
//! # Virtual chunk references
//!
//! An Icechunk repository may reference chunks that live inside files outside
//! it — a netCDF or HDF5 file left in place, as VirtualiZarr produces. **Beacon
//! does not read those.** A repository is opened with no authorized virtual
//! chunk containers, so a query that touches a virtual reference fails with an
//! icechunk authorization error naming the container it would have needed.
//!
//! The reason is that such a read is not a read of the repository. It needs the
//! credentials of the referenced file's own store, which is a different store
//! from the repository's and outside the permissions the caller was granted on
//! the dataset. Silently following the reference would let a repository name any
//! object the *server* can reach. Chunks stored inside the repository — the
//! normal case — read exactly as any zarr chunk does.

pub mod definition;
pub mod fixture;
pub mod provider;
pub mod repository;
pub mod table_function;

pub use definition::IcechunkTableDefinition;
pub use provider::IcechunkTable;
pub use repository::{
    DEFAULT_BRANCH, IcechunkVersion, RepositoryBackend, is_icechunk_repository, open_repository,
    resolve_location,
};
pub use table_function::ReadIcechunkFunc;
