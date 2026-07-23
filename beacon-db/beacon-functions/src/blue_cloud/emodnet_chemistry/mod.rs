pub mod map_instrument_info_l22;
pub mod map_instrument_l05;
pub mod map_instrument_l05_multi;
pub mod map_originator_edmo;
pub mod map_p35_contributor_codes_p01;
pub mod map_platform_l06;

pub fn emodnet_chemistry_udfs() -> Vec<datafusion::logical_expr::ScalarUDF> {
    vec![
        map_instrument_info_l22::map_emodnet_chemistry_instrument_info_l22(),
        map_instrument_l05_multi::map_emodnet_chemistry_instrument_l05_multi(),
        map_instrument_l05::map_emodnet_chemistry_instrument_l05(),
        map_originator_edmo::map_emodnet_chemistry_originator_edmo(),
        map_p35_contributor_codes_p01::map_emodnet_chemistry_p35_contributor_codes_p01(),
        map_platform_l06::map_emodnet_chemistry_platform_l06(),
    ]
}
