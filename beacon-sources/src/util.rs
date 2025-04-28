use std::path::PathBuf;

use object_store::ObjectMeta;

pub fn parse_object_meta_path(obj: &ObjectMeta) -> PathBuf {
    PathBuf::from(format!(
        "{}/{}",
        beacon_config::DATA_DIR.to_string_lossy(),
        obj.location.to_string()
    ))
}
