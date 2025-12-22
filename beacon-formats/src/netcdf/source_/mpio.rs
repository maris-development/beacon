pub struct MPIOOptions {
    num_processes: usize,
}

pub fn get_mpio_opts() -> MPIOOptions {
    let num_threads = beacon_config::CONFIG
        .netcdf_multiplexer_threads
        .unwrap_or_else(|| num_cpus::get() / 2);

    assert!(
        num_threads > 0,
        "Number of MPIO threads must be greater than 0"
    );

    MPIOOptions {
        num_processes: num_threads,
    }
}
