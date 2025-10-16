pub mod map_bigram_l05;
pub mod map_bigram_l06;

pub fn cmems_udfs() -> Vec<datafusion::logical_expr::ScalarUDF> {
    vec![
        map_bigram_l05::map_cmems_bigram_l05(),
        map_bigram_l06::map_cmems_bigram_l06(),
    ]
}
