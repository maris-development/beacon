use std::{collections::HashMap, io::Write, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::{
    config::CsvOptions,
    dataframe::DataFrameWriteOptions,
    logical_expr::SortExpr,
    parquet::data_type,
    prelude::{col, lit, DataFrame, Expr, SessionContext},
};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use utoipa::ToSchema;

use super::Output;

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct DataColumn {
    column: String,
    comment: Option<String>,
    qf_column: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct Options {
    data_columns: Vec<DataColumn>,
    depth_column: DataColumn,
    meta_columns: Vec<String>,
    longitude_column: String,
    latitude_column: String,
    time_column: String,
    key_column: String,
    qf_schema: String,
}

struct OdvField {
    column_name: String,
    odv_value_type: String,
    comment: Option<String>,
}

impl OdvField {
    pub fn new(
        column: &str,
        arrow_type: &DataType,
        comment: Option<String>,
    ) -> anyhow::Result<Self> {
        let odv_value_type = arrow_to_value_type(arrow_type)?;

        Ok(Self {
            column_name: column.to_string(),
            odv_value_type,
            comment,
        })
    }
}

struct OdvSchema {
    data_columns: Vec<OdvField>,
    meta_columns: Vec<OdvField>,
    sorts: Vec<SortExpr>,
    selects: Vec<Expr>,
}

impl OdvSchema {
    fn new(input_schema: &Schema, options: &Options) -> anyhow::Result<Self> {
        let mut meta_columns = vec![];
        let mut data_columns = vec![];

        let mut sort_exprs: Vec<datafusion::logical_expr::SortExpr> = vec![];
        sort_exprs.push(col(&options.key_column).sort(true, false));
        sort_exprs.push(col(&options.depth_column.column).sort(true, false));

        let mut select_exprs = vec![];
        select_exprs.push(col(&options.key_column).alias("Cruise"));
        select_exprs.push(lit(1).alias("Station"));
        select_exprs.push(lit("*").alias("Type"));
        select_exprs.push(col(&options.time_column).alias("YYYY-MM-DDThh:mm:ss.sss"));
        select_exprs.push(col(&options.longitude_column).alias("Longitude [degrees_east]"));
        select_exprs.push(col(&options.latitude_column).alias("Latitude [degrees_north]"));
        select_exprs.push(col(&options.time_column).alias("time_ISO8601"));

        meta_columns.push(OdvField::new(
            "Longitude [degrees_east]",
            input_schema
                .field_with_name(&options.longitude_column)?
                .data_type(),
            None,
        )?);
        meta_columns.push(OdvField::new(
            "Latitude [degrees_north]",
            input_schema
                .field_with_name(&options.latitude_column)?
                .data_type(),
            None,
        )?);

        data_columns.push(OdvField::new(
            &options.depth_column.column,
            input_schema
                .field_with_name(&options.depth_column.column)?
                .data_type(),
            options.depth_column.comment.clone(),
        )?);

        data_columns.push(OdvField::new(
            &options.time_column,
            input_schema
                .field_with_name(&options.time_column)?
                .data_type(),
            None,
        )?);

        for data_column in options.data_columns.iter() {
            select_exprs.push(col(&data_column.column));
            if let Some(qf_column) = &data_column.qf_column {
                select_exprs.push(col(qf_column).alias(generate_qf_col_name(
                    &data_column.column,
                    &options.qf_schema,
                )));
            }

            data_columns.push(OdvField::new(
                &data_column.column,
                input_schema
                    .field_with_name(&data_column.column)?
                    .data_type(),
                data_column.comment.clone(),
            )?);
        }

        for meta_column in options.meta_columns.iter() {
            select_exprs.push(col(meta_column));
            meta_columns.push(OdvField::new(
                meta_column,
                input_schema.field_with_name(meta_column)?.data_type(),
                None,
            )?);
        }

        Ok(Self {
            data_columns,
            meta_columns,
            selects: select_exprs,
            sorts: sort_exprs,
        })
    }

    fn apply_to_df(&self, df: DataFrame) -> anyhow::Result<DataFrame> {
        Ok(df.sort(self.sorts.clone())?.select(self.selects.clone())?)
    }
}

fn generate_qf_col_name(column: &str, qf_schema: &str) -> String {
    format!("QV:{}:{}", qf_schema, column)
}

pub async fn output(
    ctx: Arc<SessionContext>,
    df: DataFrame,
    options: Options,
) -> anyhow::Result<Output> {
    let schema = df.schema();

    let odv_schema = OdvSchema::new(schema.as_arrow(), &options)?;
    let df = odv_schema.apply_to_df(df)?;

    //Create temp file
    let temp_f = tempfile::Builder::new()
        .prefix("beacon-odv-")
        .suffix(".txt")
        .tempfile()?;
    //Create a new writer
    let tokio_f = tokio::fs::File::from_std(temp_f.reopen()?);
    let mut writer = tokio::io::BufWriter::new(tokio_f);

    write_base_headers(&odv_schema, &options.qf_schema, &mut writer).await?;
    writer.flush().await?;

    let mut csv_opts: CsvOptions = CsvOptions::default().with_delimiter(b'\t');
    csv_opts.timestamp_format = Some("%Y-%m-%dT%H:%M:%S%.3f".to_string());

    df.write_csv(
        temp_f.path().to_str().unwrap(),
        DataFrameWriteOptions::default(),
        Some(csv_opts),
    )
    .await?;

    Ok(Output {
        output_method: super::OutputMethod::File(temp_f),
        content_type: "text/plain".to_string(),
        content_disposition: "attachment".to_string(),
    })
}

async fn write_base_headers<W: AsyncWriteExt + Unpin>(
    schema: &OdvSchema,
    qf_schema: &str,
    writer: &mut W,
) -> anyhow::Result<()> {
    for field in schema.meta_columns.iter() {
        writer
            .write_all(
                write_meta_header(&field.column_name, qf_schema, &field.odv_value_type).as_bytes(),
            )
            .await?;
        writer.write_all(b"\n").await?;
    }

    for field in schema.data_columns.iter() {
        writer
            .write_all(
                write_data_header(
                    &field.column_name,
                    qf_schema,
                    &field.odv_value_type,
                    &field.comment.clone().unwrap_or_default(),
                )
                .as_bytes(),
            )
            .await?;
        writer.write_all(b"\n").await?;
    }

    Ok(())
}

fn arrow_to_value_type(arrow_type: &DataType) -> anyhow::Result<String> {
    match arrow_type {
        DataType::Null => Ok("INDEXED_TEXT".to_string()),
        DataType::Boolean => Ok("INDEXED_TEXT".to_string()),
        DataType::Int8 => Ok("INTEGER".to_string()),
        DataType::Int16 => Ok("INTEGER".to_string()),
        DataType::Int32 => Ok("INTEGER".to_string()),
        DataType::Int64 => Ok("INTEGER".to_string()),
        DataType::UInt8 => Ok("INTEGER".to_string()),
        DataType::UInt16 => Ok("INTEGER".to_string()),
        DataType::UInt32 => Ok("INTEGER".to_string()),
        DataType::UInt64 => Ok("INTEGER".to_string()),
        DataType::Float32 => Ok("FLOAT".to_string()),
        DataType::Float64 => Ok("FLOAT".to_string()),
        DataType::Timestamp(_, _) => Ok("INDEXED_TEXT".to_string()),
        DataType::Utf8 => Ok("INDEXED_TEXT".to_string()),
        dtype => anyhow::bail!("Unsupported data type for ODV export: {:?}", dtype),
    }
}

fn write_data_header(label: &str, qf_schema: &str, value_type: &str, comment: &str) -> String {
    const GENERIC_DATA_HEADER : &'static str = "//<DataVariable> label=\"$LABEL\" value_type=\"$VALUE_TYPE\" qf_scheme=\"$QF_SCHEMA\" comment=\"$COMMENT\" </DataVariable>";

    let header = GENERIC_DATA_HEADER
        .replace("$LABEL", label)
        .replace("$VALUE_TYPE", value_type)
        .replace("$QF_SCHEMA", qf_schema)
        .replace("$COMMENT", comment);

    header
}

fn write_meta_header(label: &str, qf_schema: &str, value_type: &str) -> String {
    const GENERIC_META_HEADER: &'static str = "//<MetaVariable> label=\"$LABEL\" qf_schema=\"$QF_SCHEMA\" value_type=\"$VALUE_TYPE\"</MetaVariable>";

    let header = GENERIC_META_HEADER
        .replace("$LABEL", label)
        .replace("$QF_SCHEMA", qf_schema)
        .replace("$VALUE_TYPE", value_type);

    header
}
