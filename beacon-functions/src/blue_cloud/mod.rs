use datafusion::logical_expr::ScalarUDF;

pub mod map_argo_instrument_l05;
pub mod map_argo_instrument_l06;
pub mod map_argo_instrument_l35;
pub mod map_cora_instrument_l05;
pub mod map_cora_instrument_l06;
pub mod map_cora_instrument_l35;
pub mod map_pres_depth;
pub mod map_units_sdn;
pub mod map_wod_flag_sdn;
pub mod map_wod_instrument_l05;
pub mod map_wod_instrument_l06;
pub mod map_wod_instrument_l35;

pub fn blue_cloud_udfs() -> Vec<ScalarUDF> {
    vec![
        map_argo_instrument_l05::map_argo_instrument_l05(),
        map_argo_instrument_l06::map_argo_instrument_l06(),
        map_argo_instrument_l35::map_argo_instrument_l35(),
        map_cora_instrument_l05::map_cora_instrument_l05(),
        map_cora_instrument_l06::map_cora_instrument_l06(),
        map_cora_instrument_l35::map_cora_instrument_l35(),
        map_pres_depth::map_pressure_to_depth_function(),
        map_units_sdn::map_units_seadatanet(),
        map_wod_flag_sdn::map_wod_flag_sdn_function(),
        map_wod_instrument_l05::map_wod_instrument_l05(),
        map_wod_instrument_l06::map_wod_instrument_l06(),
        map_wod_instrument_l35::map_wod_instrument_l35(),
    ]
}
