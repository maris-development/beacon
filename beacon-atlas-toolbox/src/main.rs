mod cli;
pub mod create;
pub mod header;
mod input;
mod partition_inspect;
mod schema_tree;

use std::sync::Arc;

use clap::Parser;
use futures::{StreamExt, stream};
use object_store::{ObjectStore, local::LocalFileSystem};

use crate::cli::{Cli, Commands};
use crate::input::{collect_glob_paths, only_netcdf_paths};
use crate::partition_inspect::inspect_partition;
use crate::schema_tree::render_schema_tree;

fn local_store() -> Arc<dyn ObjectStore> {
    Arc::new(LocalFileSystem::new())
}

async fn handle_create_collection(command: cli::CreateCollectionCommand) -> anyhow::Result<()> {
    let store = local_store();
    create::create_collection_from_directory(
        store,
        command.path,
        command.name,
        command.description,
        command.super_typing_mode.into(),
    )
    .await?;
    Ok(())
}

async fn handle_add_partition(command: cli::AddPartitionCommand) -> anyhow::Result<()> {
    let store = local_store();
    let collection =
        create::open_collection_from_directory(store.clone(), command.collection_path).await?;

    let paths = collect_glob_paths(&command.glob)?;
    let paths = only_netcdf_paths(paths, command.fail_fast)?;
    println!("Adding partition with {} files:", paths.len());
    let path_stream = stream::iter(paths.into_iter().map(Ok::<_, anyhow::Error>)).boxed();
    let datasets = create::netcdf::read_as_dataset_stream(path_stream).await;

    let dataset_stream = if command.fail_fast {
        datasets
    } else {
        datasets
            .filter_map(|dataset_res| async move {
                match dataset_res {
                    Ok(dataset) => Some(Ok(dataset)),
                    Err(err) => {
                        eprintln!("Skipping dataset due to read error: {err}");
                        None
                    }
                }
            })
            .boxed()
    };

    let _atlas = create::create_partition(
        collection,
        command.partition_name,
        command.partition_description,
        dataset_stream,
    )
    .await?;

    Ok(())
}

async fn handle_inspect_schema(command: cli::InspectSchemaCommand) -> anyhow::Result<()> {
    let store = local_store();
    let collection = create::open_collection_from_directory(store, command.collection_path).await?;
    let schema = collection.arrow_schema()?;
    let snapshot = collection.snapshot()?;

    println!("Collection: {}", snapshot.metadata().name);
    println!("Partitions: {}", snapshot.metadata().partitions.len());
    println!("Super typing mode: {:?}", collection.super_typing_mode());
    println!("\nSchema:");
    println!("{}", render_schema_tree(schema.as_ref()));

    Ok(())
}

async fn handle_inspect_partition(command: cli::InspectPartitionCommand) -> anyhow::Result<()> {
    let store = local_store();
    let collection =
        create::open_collection_from_directory(store, &command.collection_path).await?;
    let inspection = inspect_partition(&collection, &command.partition_name)?;

    println!("Partition: {}", inspection.metadata.name);
    println!(
        "Description: {}",
        inspection
            .metadata
            .description
            .as_deref()
            .unwrap_or("<none>")
    );
    println!("Total datasets: {}", inspection.total_datasets);
    println!("Loaded datasets: {}", inspection.loaded_count);
    println!("Deleted datasets: {}", inspection.deleted_count);

    println!("\nLoaded dataset names:");
    if inspection.loaded_dataset_names.is_empty() {
        println!("  <none>");
    } else {
        for dataset in &inspection.loaded_dataset_names {
            println!("  - {}", dataset);
        }
    }

    println!("\nDeleted dataset names:");
    if inspection.deleted_dataset_names.is_empty() {
        println!("  <none>");
    } else {
        for dataset in &inspection.deleted_dataset_names {
            println!("  - {}", dataset);
        }
    }

    println!("\nPartition schema:");
    let schema = inspection.metadata.schema.to_arrow_schema();
    println!("{}", render_schema_tree(&schema));

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::CreateCollection(command) => handle_create_collection(command).await?,
        Commands::AddPartition(command) => handle_add_partition(command).await?,
        Commands::InspectSchema(command) => handle_inspect_schema(command).await?,
        Commands::InspectPartition(command) => handle_inspect_partition(command).await?,
    }

    Ok(())
}
