pub mod map_c17_l06;
pub mod map_l22_l05;
pub mod pressure_to_depth_teos_10;

pub fn common_udfs() -> Vec<datafusion::logical_expr::ScalarUDF> {
    vec![
        pressure_to_depth_teos_10::pressure_to_depth_teos_10(),
        map_c17_l06::map_c17_l06(),
        map_l22_l05::map_l22_l05(),
    ]
}
