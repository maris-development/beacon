//! The DAP2 Dataset Descriptor Structure.

use crate::names::encode;
use crate::types::ServedColumn;

/// The name of the one Sequence in each dataset. ERDDAP tabledap uses the same name.
pub const SEQUENCE: &str = "s";

/// The DDS of `dataset`, with `columns` in the Sequence.
pub fn dds(dataset: &str, columns: &[&ServedColumn]) -> String {
    let mut out = String::from("Dataset {\n    Sequence {\n");
    for column in columns {
        out.push_str(&format!(
            "        {} {};\n",
            column.dap_type.name(),
            encode(&column.name)
        ));
    }
    out.push_str(&format!("    }} {SEQUENCE};\n}} {};\n", encode(dataset)));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DapType;
    use arrow::datatypes::DataType;

    fn column(name: &str, dap_type: DapType) -> ServedColumn {
        ServedColumn {
            name: name.to_string(),
            data_type: DataType::Float64,
            dap_type,
            comment: None,
        }
    }

    #[test]
    fn dds_lists_the_columns_in_one_sequence() {
        let time = column("time", DapType::Float64);
        let temp = column("temp", DapType::Float32);
        let platform = column("platform", DapType::String);

        assert_eq!(
            dds("argo", &[&time, &temp, &platform]),
            "Dataset {\n    Sequence {\n        Float64 time;\n        Float32 temp;\n        String platform;\n    } s;\n} argo;\n"
        );
    }

    #[test]
    fn dds_escapes_names_with_dots() {
        let units = column("temp.units", DapType::String);

        let text = dds("my.table", &[&units]);

        assert!(text.contains("        String temp%2Eunits;\n"), "{text}");
        assert!(text.ends_with("} my%2Etable;\n"), "{text}");
    }
}
