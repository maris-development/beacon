use std::path::Path;

pub struct TempOutputFile {
    temp_file: tempfile::NamedTempFile,
    object_path: object_store::path::Path,
    file_name: String,
}

impl TempOutputFile {
    /// Create a temporary output file inside `dir`.
    ///
    /// `dir` MUST be the same directory the tmp object store
    /// ([`crate::TMP_OBJECT_STORE_URL`]) is rooted at: the COPY plan writes to
    /// `tmp://<file_name>` (see [`Self::output_url`]) which resolves under the
    /// tmp store's root, while the returned [`tempfile::NamedTempFile`] is read
    /// back from this filesystem path. If the two diverge the written bytes are
    /// invisible to the caller.
    pub fn new(dir: &Path, extension: &str) -> Self {
        let temp_file = tempfile::Builder::new()
            .prefix("beacon_temp_")
            .suffix(extension)
            .tempfile_in(dir)
            .unwrap();

        let file_name = temp_file.path().file_name().unwrap().to_string_lossy();
        let object_path = object_store::path::Path::from(file_name.to_string());
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
        format!("{}{}", crate::TMP_OBJECT_STORE_URL.as_str(), self.file_name)
    }
}
