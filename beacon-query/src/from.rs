use utoipa::ToSchema;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
#[serde(deny_unknown_fields)]
pub enum From {
    #[serde(untagged)]
    Table(String),
    #[serde(untagged)]
    Format {
        #[serde(flatten)]
        format: FromFormat,
    },
}

impl Default for From {
    fn default() -> Self {
        From::Table(beacon_config::CONFIG.default_table.clone())
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
#[serde(deny_unknown_fields)]
pub enum FromFormat {
    #[serde(rename = "csv")]
    Csv {
        delimiter: Option<char>,
        paths: Vec<String>,
    },
    #[serde(rename = "parquet")]
    Parquet { paths: Vec<String> },
    #[serde(rename = "arrow")]
    Arrow { paths: Vec<String> },
    #[serde(rename = "netcdf")]
    NetCDF { paths: Vec<String> },
}
