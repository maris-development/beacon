pub mod map_instrument_l05;
pub mod map_instrument_l22;
pub mod map_platform_l06;

pub fn cora_udfs() -> Vec<datafusion::logical_expr::ScalarUDF> {
    vec![
        map_instrument_l05::map_cora_instrument_l05(),
        map_instrument_l22::map_cora_instrument_l22(),
        map_platform_l06::map_cora_platform_l06(),
    ]
}
