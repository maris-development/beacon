pub mod map_country_institute_edmo;
pub mod map_instrument_l05;
pub mod map_instrument_l22;
pub mod map_instrument_l33;
pub mod map_platform_c17;
pub mod map_quality_flag;

pub fn world_ocean_database_udfs() -> Vec<datafusion::logical_expr::ScalarUDF> {
    vec![
        map_instrument_l05::map_wod_instrument_l05(),
        map_instrument_l22::map_wod_instrument_l22(),
        map_instrument_l33::map_wod_instrument_l33(),
        map_platform_c17::map_wod_platform_c17(),
        map_quality_flag::map_wod_quality_flag(),
    ]
}
