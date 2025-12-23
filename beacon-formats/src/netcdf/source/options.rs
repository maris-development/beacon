pub struct Options {
    mpio: Option<MPIOOptions>,
}

pub fn get_options() -> Options {
    let mpio = if beacon_config::CONFIG.enable_multiplexer_netcdf {
        Some(get_mpio_opts())
    } else {
        None
    };

    Options { mpio }
}

pub struct MPIOOptions {
    pub num_processes: usize,
}

pub fn get_mpio_opts() -> MPIOOptions {
    let num_processes = beacon_config::CONFIG
        .netcdf_multiplexer_processes
        .unwrap_or_else(|| num_cpus::get() / 2);

    assert!(
        num_processes > 0,
        "Number of MPIO threads must be greater than 0"
    );

    MPIOOptions { num_processes }
}
