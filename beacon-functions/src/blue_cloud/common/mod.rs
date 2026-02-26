pub mod map_c17;
pub mod map_c17_l06;
pub mod map_call_sign_c17;
pub mod map_l22_l05;
pub mod map_measuring_area_type_feature_type;
pub mod map_p01_p25;
pub mod map_p25_l05;
pub mod map_wmo_instrument_type_l05;
pub mod map_wmo_instrument_type_l22;
pub mod pressure_to_depth_teos_10;

pub fn common_udfs() -> Vec<datafusion::logical_expr::ScalarUDF> {
    vec![
        map_c17_l06::map_c17_l06(),
        map_c17::map_c17(),
        map_call_sign_c17::map_call_sign_c17(),
        map_l22_l05::map_l22_l05(),
        map_wmo_instrument_type_l05::map_wmo_instrument_type_l05(),
        map_wmo_instrument_type_l22::map_wmo_instrument_type_l22(),
        map_measuring_area_type_feature_type::map_measuring_area_type_feature_type(),
        pressure_to_depth_teos_10::pressure_to_depth_teos_10(),
    ]
}
