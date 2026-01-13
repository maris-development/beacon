use beacon_binary_format::{array::compression::Compression, writer::BBFWriter};

pub mod arrow;
pub mod csv;
pub mod netcdf;
pub mod parquet;

pub fn create(
    glob: String,
    group_size: u64,
    compression: Option<Compression>,
    pruning: bool,
    output: String,
    csv_delimiter: u8,
    skip_column_on_error: bool,
    fail_fast: bool,
) {
    // Create the file
    let path = std::path::Path::new(&output);

    let mut writer = BBFWriter::new(path, group_size as usize, compression, pruning)
        .expect("Failed to create BBFWriter.");

    // Process the glob pattern

    for entry in glob::glob(&glob).expect("Failed to read glob pattern") {
        match entry {
            Ok(path) => {
                // Here you would read the file and write to the BBFWriter
                // For example:
                println!("Processing file: {:?}", path);

                // Check file extension and handle accordingly
                match path.extension().and_then(std::ffi::OsStr::to_str) {
                    Some(parquet::FILE_EXTENSION) => {
                        println!("Found Parquet file: {:?}", path);

                        match parquet::read_entry(path.as_os_str().to_str().unwrap()) {
                            Ok(entry) => writer.append(
                                entry,
                                path.file_name().unwrap().to_string_lossy().to_string(),
                            ),
                            Err(err) => {
                                if fail_fast {
                                    eprintln!(
                                        "Error reading Parquet file '{}': {}",
                                        path.display(),
                                        err
                                    );
                                    panic!("Failing fast due to error.");
                                } else {
                                    eprintln!(
                                        "Error reading Parquet file '{}': {}",
                                        path.display(),
                                        err
                                    );
                                }
                            }
                        }
                    }
                    Some(csv::FILE_EXTENSION) => {
                        println!("Found CSV file: {:?}", path);
                        match csv::read_entry(path.as_os_str().to_str().unwrap(), csv_delimiter) {
                            Ok(entry) => writer.append(
                                entry,
                                path.file_name().unwrap().to_string_lossy().to_string(),
                            ),
                            Err(err) => {
                                if fail_fast {
                                    eprintln!(
                                        "Error reading CSV file '{}': {}",
                                        path.display(),
                                        err
                                    );
                                    panic!("Failing fast due to error.");
                                } else {
                                    eprintln!(
                                        "Error reading CSV file '{}': {}",
                                        path.display(),
                                        err
                                    );
                                }
                            }
                        }
                    }
                    Some(netcdf::FILE_EXTENSION) => {
                        println!("Found NetCDF file: {:?}", path);
                        match netcdf::read_entry(
                            path.as_os_str().to_str().unwrap(),
                            skip_column_on_error,
                        ) {
                            Ok(entry) => writer.append(
                                entry,
                                path.file_name().unwrap().to_string_lossy().to_string(),
                            ),
                            Err(err) => {
                                if fail_fast {
                                    eprintln!(
                                        "Error reading NetCDF file '{}': {}",
                                        path.display(),
                                        err
                                    );
                                    panic!("Failing fast due to error.");
                                } else {
                                    eprintln!(
                                        "Error reading NetCDF file '{}': {}",
                                        path.display(),
                                        err
                                    );
                                }
                            }
                        }
                    }
                    Some(arrow::FILE_EXTENSION) => {
                        println!("Found Arrow file: {:?}", path);
                        match arrow::read_entry(path.as_os_str().to_str().unwrap()) {
                            Ok(entry) => writer.append(
                                entry,
                                path.file_name().unwrap().to_string_lossy().to_string(),
                            ),
                            Err(err) => {
                                if fail_fast {
                                    eprintln!(
                                        "Error reading Arrow file '{}': {}",
                                        path.display(),
                                        err
                                    );
                                    panic!("Failing fast due to error.");
                                } else {
                                    eprintln!(
                                        "Error reading Arrow file '{}': {}",
                                        path.display(),
                                        err
                                    );
                                }
                            }
                        }
                    }
                    _ => {
                        eprintln!("Unsupported file type: {:?}", path);
                    }
                }
            }
            Err(e) => eprintln!("Error reading entry: {}", e),
        }
    }

    // Finalize the writer
    writer.finish().expect("Failed to finish BBFWriter.");
}
