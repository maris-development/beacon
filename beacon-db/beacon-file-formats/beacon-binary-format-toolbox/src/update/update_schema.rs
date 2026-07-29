use std::{fs::OpenOptions, path::Path};

use beacon_binary_format::footer::FooterUpdater;

pub fn update_schema_field<P: AsRef<Path>>(path: P, column: String, data_type: String) {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();

    let mut footer_updater = FooterUpdater::new(file).unwrap();

    footer_updater
        .update_datatype(column, data_type.as_str())
        .unwrap();

    footer_updater.save().unwrap();
}
