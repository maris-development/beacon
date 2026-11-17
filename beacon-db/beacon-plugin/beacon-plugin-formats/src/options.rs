//! The table options that a plugin format gets.

use std::collections::HashMap;

use datafusion::error::Result;

use beacon_datafusion_ext::format_options::parse_bool_option;

const SINGLE_FILE_OUTPUT: &str = "single_file_output";

/// Splits raw table options into the options of the plugin and `single_file_output`.
///
/// The plugin gets the `format.` keys without the prefix, and bare keys. Other dotted keys
/// (for example `execution.*`) are DataFusion's own and are dropped. `single_file_output`
/// is Beacon's and does not reach the plugin.
pub fn plugin_options(
    raw: &HashMap<String, String>,
) -> Result<(HashMap<String, String>, Option<bool>)> {
    let mut options = HashMap::new();
    let mut single_file_output = None;
    for (key, value) in raw {
        let key = key.to_ascii_lowercase();
        let bare = match key.strip_prefix("format.") {
            Some(bare) => bare.to_string(),
            None if !key.contains('.') => key,
            None => continue,
        };
        if bare == SINGLE_FILE_OUTPUT {
            single_file_output = Some(parse_bool_option(SINGLE_FILE_OUTPUT, value)?);
            continue;
        }
        options.insert(bare, value.clone());
    }
    Ok((options, single_file_output))
}
