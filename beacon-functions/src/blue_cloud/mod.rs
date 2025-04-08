use datafusion::logical_expr::ScalarUDF;

pub mod map_argo_instrument_l05;
pub mod map_cmems_bigram_l05;
pub mod map_cora_instrument_l05;
pub mod map_cora_instrument_l22;
pub mod map_pres_depth;
pub mod map_sdn_instrument;
pub mod map_units_sdn;
pub mod map_wod_flag_sdn;
pub mod map_wod_instrument_l05;
pub mod map_wod_instrument_l22;
pub mod map_wod_instrument_l33;
pub mod util;

pub fn blue_cloud_udfs() -> Vec<ScalarUDF> {
    vec![
        map_argo_instrument_l05::map_argo_instrument_l05(),
        map_cmems_bigram_l05::map_cmems_bigram_l05(),
        map_cora_instrument_l05::map_cora_instrument_l05(),
        map_cora_instrument_l22::map_cora_instrument_l22(),
        map_pres_depth::map_pressure_to_depth_function(),
        map_sdn_instrument::map_sdn_instrument_l05(),
        map_units_sdn::map_units_seadatanet(),
        map_wod_flag_sdn::map_wod_flag_sdn_function(),
        map_wod_instrument_l05::map_wod_instrument_l05(),
        map_wod_instrument_l22::map_wod_instrument_l22(),
        map_wod_instrument_l33::map_wod_instrument_l33(),
    ]
}
