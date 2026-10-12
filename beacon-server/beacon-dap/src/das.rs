//! The DAP2 Dataset Attribute Structure.

use crate::dds::SEQUENCE;
use crate::names::{encode, quote};
use crate::types::ServedSchema;

/// CF time units for the seconds that `types::convert` makes.
pub const TIME_UNITS: &str = "seconds since 1970-01-01T00:00:00Z";

/// The DAS of `dataset`.
pub fn das(dataset: &str, schema: &ServedSchema) -> String {
    let mut out = format!("Attributes {{\n    {SEQUENCE} {{\n");
    for column in &schema.columns {
        out.push_str(&format!("        {} {{\n", encode(&column.name)));
        if let Some(fill) = column.dap_type.fill_value() {
            out.push_str(&format!(
                "            {} _FillValue {fill};\n",
                column.dap_type.name()
            ));
        }
        if let Some(comment) = &column.comment {
            out.push_str(&format!("            String comment {};\n", quote(comment)));
        }
        if column.is_time() {
            out.push_str(&format!(
                "            String units {};\n",
                quote(TIME_UNITS)
            ));
        }
        out.push_str("        }\n");
    }
    out.push_str("    }\n    NC_GLOBAL {\n");
    out.push_str(&format!("        String title {};\n", quote(dataset)));
    if let Some(summary) = &schema.table_comment {
        out.push_str(&format!("        String summary {};\n", quote(summary)));
    }
    out.push_str("        String Conventions \"CF-1.10\";\n");
    out.push_str("        String cdm_data_type \"Other\";\n");
    if !schema.omitted.is_empty() {
        out.push_str(&format!(
            "        String beacon_omitted_columns {};\n",
            quote(&schema.omitted.join(", "))
        ));
    }
    out.push_str("    }\n}\n");
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DapType, ServedColumn};
    use arrow::datatypes::{DataType, TimeUnit};

    #[test]
    fn das_gives_fill_values_comments_units_and_global_attributes() {
        let schema = ServedSchema {
            columns: vec![
                ServedColumn {
                    name: "time".into(),
                    data_type: DataType::Timestamp(TimeUnit::Microsecond, None),
                    dap_type: DapType::Float64,
                    comment: None,
                },
                ServedColumn {
                    name: "temp".into(),
                    data_type: DataType::Float32,
                    dap_type: DapType::Float32,
                    comment: Some("sea \"surface\" temperature".into()),
                },
                ServedColumn {
                    name: "platform".into(),
                    data_type: DataType::Utf8,
                    dap_type: DapType::String,
                    comment: None,
                },
            ],
            omitted: vec!["raw".into(), "tags".into()],
            table_comment: Some("Argo floats".into()),
        };

        assert_eq!(
            das("argo", &schema),
            "Attributes {\n\
             \x20   s {\n\
             \x20       time {\n\
             \x20           Float64 _FillValue NaN;\n\
             \x20           String units \"seconds since 1970-01-01T00:00:00Z\";\n\
             \x20       }\n\
             \x20       temp {\n\
             \x20           Float32 _FillValue NaN;\n\
             \x20           String comment \"sea \\\"surface\\\" temperature\";\n\
             \x20       }\n\
             \x20       platform {\n\
             \x20       }\n\
             \x20   }\n\
             \x20   NC_GLOBAL {\n\
             \x20       String title \"argo\";\n\
             \x20       String summary \"Argo floats\";\n\
             \x20       String Conventions \"CF-1.10\";\n\
             \x20       String cdm_data_type \"Other\";\n\
             \x20       String beacon_omitted_columns \"raw, tags\";\n\
             \x20   }\n\
             }\n"
        );
    }

    #[test]
    fn das_leaves_out_empty_global_attributes() {
        let schema = ServedSchema {
            columns: vec![],
            omitted: vec![],
            table_comment: None,
        };

        let text = das("argo", &schema);

        assert!(!text.contains("summary"), "{text}");
        assert!(!text.contains("beacon_omitted_columns"), "{text}");
    }
}
