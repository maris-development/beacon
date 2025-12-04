pub mod argo;
pub mod cmems;
pub mod common;
pub mod cora;
pub mod emodnet_chemistry;
pub mod seadatanet;
pub mod util;
pub mod world_ocean_database;

pub fn blue_cloud_udfs() -> Vec<datafusion::logical_expr::ScalarUDF> {
    let mut udfs = vec![];
    udfs.extend(argo::argo_udfs());
    udfs.extend(cmems::cmems_udfs());
    udfs.extend(common::common_udfs());
    udfs.extend(cora::cora_udfs());
    udfs.extend(emodnet_chemistry::emodnet_chemistry_udfs());
    udfs.extend(seadatanet::seadatanet_udfs());
    udfs.extend(world_ocean_database::world_ocean_database_udfs());
    udfs
}
