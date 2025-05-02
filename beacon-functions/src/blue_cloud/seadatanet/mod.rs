pub mod map_c17_l06;
pub mod map_units;

pub fn seadatanet_udfs() -> Vec<datafusion::logical_expr::ScalarUDF> {
    vec![map_c17_l06::map_platform_c17_l06(), map_units::map_units()]
}
