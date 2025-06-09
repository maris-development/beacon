pub mod map_instrument_l05;
pub mod map_platform_l06;

pub fn emodnet_chemistry_udfs() -> Vec<datafusion::logical_expr::ScalarUDF> {
    vec![
        map_instrument_l05::map_emodnet_chemistry_instrument_l05(),
        map_platform_l06::map_emodnet_chemistry_platform_l06(),
    ]
}
