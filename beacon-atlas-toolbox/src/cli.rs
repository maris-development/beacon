use std::path::PathBuf;

use beacon_atlas::prelude::AtlasSuperTypingMode;
use clap::{ArgAction, Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(name = "atlas-toolbox", version, about, long_about = None)]
pub(crate) struct Cli {
    /// Increase verbosity (-v, -vv, -vvv). Use -q to quiet.
    #[arg(short, long, action = ArgAction::Count, global = true)]
    pub(crate) verbose: u8,

    /// Decrease output. Overrides -v.
    #[arg(short = 'q', long, action = ArgAction::SetTrue, global = true)]
    pub(crate) quiet: bool,

    #[command(subcommand)]
    pub(crate) command: Commands,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Commands {
    /// Create a new Atlas collection in a local directory.
    CreateCollection(CreateCollectionCommand),
    /// Add files matched by a glob as one partition in an existing collection.
    AddPartition(AddPartitionCommand),
    /// Inspect the merged collection schema in a nested tree view.
    InspectSchema(InspectSchemaCommand),
    /// Inspect a single partition: dataset status and partition schema.
    InspectPartition(InspectPartitionCommand),
}

#[derive(Debug, clap::Args)]
pub(crate) struct CreateCollectionCommand {
    /// Filesystem path to the collection directory.
    #[arg(value_name = "PATH", short, long, required = true)]
    pub(crate) path: PathBuf,

    /// Collection name stored in collection metadata.
    #[arg(value_name = "NAME", short, long, required = true)]
    pub(crate) name: String,

    /// Optional collection description.
    #[arg(value_name = "DESCRIPTION", short, long)]
    pub(crate) description: Option<String>,

    /// Super-typing mode for schema merging across partitions.
    #[arg(value_name = "MODE", short = 'm', long, default_value = "general")]
    pub(crate) super_typing_mode: CliSuperTypingMode,
}

#[derive(Debug, clap::Args)]
pub(crate) struct AddPartitionCommand {
    /// Filesystem path to an existing collection directory.
    #[arg(value_name = "COLLECTION_PATH", short = 'c', long, required = true)]
    pub(crate) collection_path: PathBuf,

    /// New partition name.
    #[arg(value_name = "PARTITION", short = 'p', long, required = true)]
    pub(crate) partition_name: String,

    /// Optional partition description.
    #[arg(value_name = "DESCRIPTION", short, long)]
    pub(crate) partition_description: Option<String>,

    /// Glob pattern for input files. Currently NetCDF (*.nc) is supported.
    #[arg(value_name = "GLOB", short, long, required = true)]
    pub(crate) glob: String,

    /// Fail immediately on first file read/parse error.
    #[arg(value_name = "FAIL_FAST", short = 'f', long, default_value = "false")]
    pub(crate) fail_fast: bool,
}

#[derive(Debug, clap::Args)]
pub(crate) struct InspectSchemaCommand {
    /// Filesystem path to an existing collection directory.
    #[arg(value_name = "COLLECTION_PATH", short = 'c', long, required = true)]
    pub(crate) collection_path: PathBuf,
}

#[derive(Debug, clap::Args)]
pub(crate) struct InspectPartitionCommand {
    /// Filesystem path to an existing collection directory.
    #[arg(value_name = "COLLECTION_PATH", short = 'c', long, required = true)]
    pub(crate) collection_path: PathBuf,

    /// Partition name to inspect (for example: part-00000).
    #[arg(value_name = "PARTITION_NAME", short = 'p', long, required = true)]
    pub(crate) partition_name: String,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum CliSuperTypingMode {
    General,
    GroupBased,
}

impl From<CliSuperTypingMode> for AtlasSuperTypingMode {
    fn from(value: CliSuperTypingMode) -> Self {
        match value {
            CliSuperTypingMode::General => AtlasSuperTypingMode::General,
            CliSuperTypingMode::GroupBased => AtlasSuperTypingMode::GroupBased,
        }
    }
}
