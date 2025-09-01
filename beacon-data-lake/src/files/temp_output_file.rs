use crate::DataLake;

pub struct TempOutputFile {
    temp_file: tempfile::NamedTempFile,
    object_path: object_store::path::Path,
    file_name: String,
}

impl TempOutputFile {
    pub fn new(datalake: &DataLake, extension: &str) -> Self {
        let base_path = datalake
            .tmp_directory_object_store
            .path_to_filesystem(&object_store::path::Path::from("tmp/"))
            .unwrap();
        let temp_file = tempfile::Builder::new()
            .prefix("beacon_temp_")
            .suffix(extension)
            .tempfile_in(base_path)
            .unwrap();

        let file_name = temp_file.path().file_name().unwrap().to_string_lossy();
        let object_path = object_store::path::Path::from(format!("tmp/{}", file_name));
        Self {
            file_name: file_name.to_string(),
            temp_file,
            object_path,
        }
    }

    pub fn get_object_path(&self) -> &object_store::path::Path {
        &self.object_path
    }

    pub fn into_temp_file(self) -> tempfile::NamedTempFile {
        self.temp_file
    }

    pub fn output_url(&self) -> String {
        format!("file:///tmp/{}", self.file_name)
    }
}
