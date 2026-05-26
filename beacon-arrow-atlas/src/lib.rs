//! `beacon-arrow-atlas` bridges Atlas array stores and Beacon ND Arrow arrays.
//!
//! Atlas (<https://github.com/robinskil/atlas>) is a directory-based store
//! where a single `atlas.json` registry describes one or more named
//! datasets, each containing its own set of arrays. This crate exposes
//! each atlas dataset as a Beacon `AnyDataset` via lazy `NdArrayD`
//! backends and registers an `AtlasFormat` / `AtlasFormatFactory` so the
//! datasets are queryable through DataFusion.

pub use atlas;

pub mod backend;
pub mod compat;
pub mod datafusion;
pub mod reader;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_name() {
        let store = atlas::Atlas::open_path("data/datasets/example")
            .await
            .unwrap();

        println!("Store name: {:?}", store.list_datasets());

        let dataset = store.open_dataset("GL_PR_CT_2FGX5").await.unwrap();
    }
}
