pub mod map_c17_l06;
pub mod map_instrument_l05;
pub mod map_instrument_l05_salinity;
pub mod map_instrument_l05_temperature;
pub mod map_originator_edmo;
pub mod map_platform_l06;
pub mod map_units;

pub fn seadatanet_udfs() -> Vec<datafusion::logical_expr::ScalarUDF> {
    vec![
        map_c17_l06::map_platform_c17_l06(),
        map_platform_l06::map_seadatanet_platform_l06(),
        map_instrument_l05::map_seadatanet_instrument_l05(),
        map_instrument_l05_salinity::map_seadatanet_instrument_l05_salinity(),
        map_instrument_l05_temperature::map_seadatanet_instrument_l05_temperature(),
        map_originator_edmo::map_originator_edmo(),
        map_units::map_units(),
    ]
}
