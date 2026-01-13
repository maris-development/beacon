use std::str::FromStr;

use beacon_binary_format::array::compression::Compression;
use clap::{ArgAction, Parser, Subcommand};

use crate::list::{
    datasets_regex::list_datasets_regex, footer::list_footer, pruning_index::list_pruning_index,
};

pub mod create;
pub mod list;
pub mod update;

#[derive(Debug, Parser)]
#[command(name = "bbf-toolbox", version, about, long_about = None)]
struct Cli {
    /// Increase verbosity (-v, -vv, -vvv). Use -q to quiet.
    #[arg(short, long, action = ArgAction::Count, global = true)]
    verbose: u8,

    /// Decrease output. Overrides -v.
    #[arg(short = 'q', long, action = ArgAction::SetTrue, global = true)]
    quiet: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Create {
        #[arg(value_name = "GLOB", short, long, required = true)]
        glob: String,

        #[arg(value_name = "GROUP_SIZE", long ,value_parser = parse_bytes, default_value = "4MB")]
        group_size: u64,

        #[arg(
            value_name = "COMPRESSION",
            short = 'c',
            long,
            default_value = "zstd:3",
            help = "Compression algorithm to use",
            value_parser = parse_compression
        )]
        compression: Option<Compression>,

        #[arg(
            value_name = "PRUNING",
            short = 'p',
            long,
            help = "Enable pruning index [default: true]",
            default_value = "true"
        )]
        pruning: bool,

        #[arg(
            value_name = "OUTPUT",
            short = 'o',
            long,
            help = "Output file path",
            default_value = "output.bbf"
        )]
        output: String,

        #[arg(
            value_name = "FAIL_FAST",
            short = 'f',
            long,
            help = "Fail fast on errors [default: false]",
            default_value = "false"
        )]
        fail_fast: bool,

        #[arg(
            value_name = "SKIP_COLUMN_ON_ERROR",
            short = 's',
            long,
            help = "Skip columns that error while reading the source or are unsupported [default: false]",
            default_value = "false"
        )]
        skip_column_on_error: bool,

        #[arg(
            value_name = "CSV_DELIMITER",
            short = 'd',
            long,
            help = "CSV delimiter",
            default_value = ",",
            value_parser = parse_csv_delimiter
        )]
        csv_delimiter: u8,
    },
    ListDatasetsRegex {
        #[arg(
            value_name = "FILE_PATH",
            short,
            long,
            required = true,
            help = "Path to the input file"
        )]
        file_path: String,
        #[arg(
            value_name = "PATTERN",
            short,
            long,
            required = true,
            help = "Regex pattern to match"
        )]
        pattern: String,
        #[arg(value_name = "OFFSET", short, long, help = "Offset for the results")]
        offset: Option<usize>,
        #[arg(
            value_name = "LIMIT",
            short,
            long,
            help = "Limit the number of results"
        )]
        limit: Option<usize>,
        #[arg(value_name = "COLUMN", short, long, help = "Column to match")]
        column: Option<String>,
    },
    ListDatasetsOffsetLimit {
        #[arg(
            value_name = "FILE_PATH",
            short,
            long,
            required = true,
            help = "Path to the input file"
        )]
        file_path: String,
        #[arg(value_name = "OFFSET", short, long, help = "Offset for the results")]
        offset: Option<usize>,
        #[arg(
            value_name = "LIMIT",
            short,
            long,
            help = "Limit the number of results"
        )]
        limit: Option<usize>,
        #[arg(value_name = "COLUMN", short, long, help = "Column to match")]
        column: Option<String>,
    },
    ListFooter {
        #[arg(
            value_name = "FILE_PATH",
            short,
            long,
            required = true,
            help = "Path to the input file"
        )]
        file_path: String,
    },
    ListPruningIndex {
        #[arg(
            value_name = "FILE_PATH",
            short,
            long,
            required = true,
            help = "Path to the input file"
        )]
        file_path: String,
        #[arg(value_name = "COLUMN", short, long, help = "Column to match")]
        column: Option<String>,
    },
    UpdateSchema {
        #[arg(
            value_name = "FILE_PATH",
            short,
            long,
            required = true,
            help = "Path to the input file"
        )]
        file_path: String,
        #[arg(
            value_name = "COLUMN",
            short,
            long,
            required = true,
            help = "Column to update"
        )]
        column: String,
        #[arg(
            value_name = "DATA_TYPE",
            short,
            long,
            required = true,
            help = "New data type (e.g., Null, Boolean, Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float16, Float32, Float64, Utf8, Binary, Timestamp(Second|Millisecond|Microsecond|Nanosecond)"
        )]
        data_type: String,
    },
}

fn parse_csv_delimiter(s: &str) -> Result<u8, String> {
    if s.len() != 1 {
        return Err(format!(
            "Invalid CSV delimiter: '{}'. Must be a single character.",
            s
        ));
    }
    Ok(s.as_bytes()[0])
}

fn parse_bytes(s: &str) -> Result<u64, String> {
    byte_unit::Byte::from_str(s)
        .map(|b| b.as_u64())
        .map_err(|e| e.to_string())
}

fn parse_compression(s: &str) -> Result<Compression, String> {
    let s = s.to_ascii_lowercase();
    if s.starts_with("zstd") {
        // Possible formats: "zstd" or "zstd:LEVEL"
        let parts: Vec<&str> = s.split(':').collect();
        let level = if parts.len() == 2 {
            parts[1]
                .parse::<i32>()
                .map_err(|_| format!("Invalid zstd level: {}", parts[1]))?
        } else {
            3 // Default zstd level
        };
        return Ok(Compression::zstd(level));
    }
    match s.as_str() {
        "lz4" => Ok(Compression::Lz4),
        "lz4hc" => Ok(Compression::Lz4hc),
        _ => Err(format!(
            "Unknown compression algorithm: {} (valid: zstd[:LEVEL], lz4, lz4hc)",
            s
        )),
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    match cli.command {
        Commands::Create {
            glob,
            group_size,
            compression,
            pruning,
            output,
            csv_delimiter,
            skip_column_on_error,
            fail_fast,
        } => {
            create::create(
                glob,
                group_size,
                compression,
                pruning,
                output,
                csv_delimiter,
                skip_column_on_error,
                fail_fast,
            );
        }
        Commands::ListDatasetsRegex {
            file_path,
            pattern,
            offset,
            limit,
            column,
        } => list_datasets_regex(file_path, pattern, offset, limit, column).await,
        Commands::ListDatasetsOffsetLimit {
            file_path,
            offset,
            limit,
            column,
        } => todo!(),
        Commands::ListFooter { file_path } => list_footer(file_path).await,
        Commands::ListPruningIndex { file_path, column } => {
            list_pruning_index(file_path, column).await
        }
        Commands::UpdateSchema {
            file_path,
            column,
            data_type,
        } => update::update_schema::update_schema_field(file_path, column, data_type),
    }
}
