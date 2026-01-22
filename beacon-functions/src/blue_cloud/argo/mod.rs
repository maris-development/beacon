pub mod map_instrument_l05;
pub mod map_platform_edmo;
pub mod map_platform_l06;

pub fn argo_udfs() -> Vec<datafusion::logical_expr::ScalarUDF> {
    vec![
        map_platform_edmo::map_argo_platform_edmo(),
        map_instrument_l05::map_argo_instrument_l05(),
        map_platform_l06::map_argo_platform_l06(),
    ]
}
