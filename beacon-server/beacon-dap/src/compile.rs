//! Compiles a DAP2 constraint into SQL with positional parameters.

use datafusion::common::ScalarValue;
use datafusion::sql::TableReference;

use crate::constraint::{Constraint, Op, Selection, Value};
use crate::error::DapError;
use crate::types::{DapType, ServedColumn, ServedSchema};

/// A query for `Runtime::run_query`, with the columns it returns, in order.
#[derive(Debug, Clone, PartialEq)]
pub struct Compiled {
    pub sql: String,
    pub params: Vec<ScalarValue>,
    pub columns: Vec<ServedColumn>,
}

/// The projected columns in schema order. An empty projection selects every served column.
pub fn projected<'a>(
    schema: &'a ServedSchema,
    projection: &[String],
) -> Result<Vec<&'a ServedColumn>, DapError> {
    for name in projection {
        check_column(schema, name)?;
    }
    Ok(schema
        .columns
        .iter()
        .filter(|column| projection.is_empty() || projection.contains(&column.name))
        .collect())
}

/// Compiles `constraint` on `table` into SQL. Every value becomes a `$n` parameter.
pub fn compile(
    table: &TableReference,
    schema: &ServedSchema,
    constraint: &Constraint,
) -> Result<Compiled, DapError> {
    let columns: Vec<ServedColumn> = projected(schema, &constraint.projection)?
        .into_iter()
        .cloned()
        .collect();
    if columns.is_empty() {
        return Err(DapError::BadRequest(
            "the table has no column that DAP2 can serve".to_string(),
        ));
    }
    let select = columns
        .iter()
        .map(|column| quote_ident(&column.name))
        .collect::<Vec<_>>()
        .join(", ");
    let mut sql = format!("SELECT {select} FROM {}", table.to_quoted_string());
    let mut params = Vec::new();
    let mut conditions = Vec::new();
    for selection in &constraint.selections {
        let column = check_column(schema, &selection.column)?;
        conditions.push(condition(column, selection, &mut params)?);
    }
    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }
    Ok(Compiled {
        sql,
        params,
        columns,
    })
}

/// A SQL identifier in double quotes. A `"` inside the name is doubled.
pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn check_column<'a>(schema: &'a ServedSchema, name: &str) -> Result<&'a ServedColumn, DapError> {
    if let Some(column) = schema.column(name) {
        return Ok(column);
    }
    if schema.omitted.iter().any(|omitted| omitted == name) {
        return Err(DapError::BadRequest(format!(
            "column {name:?} has a type that DAP2 cannot serve"
        )));
    }
    Err(DapError::BadRequest(format!("unknown column {name:?}")))
}

/// One SQL condition. Each value becomes the next `$n` parameter in `params`.
fn condition(
    column: &ServedColumn,
    selection: &Selection,
    params: &mut Vec<ScalarValue>,
) -> Result<String, DapError> {
    let ident = quote_ident(&column.name);
    match (&selection.value, column.dap_type) {
        (Value::NaN, _) => {
            if !column.is_float() {
                return Err(DapError::BadRequest(format!(
                    "NaN is not a value of column {:?}",
                    column.name
                )));
            }
            match selection.op {
                Op::Eq => Ok(format!("isnan({ident})")),
                Op::Ne => Ok(format!("NOT isnan({ident})")),
                _ => Err(DapError::BadRequest(
                    "NaN works only with = and !=".to_string(),
                )),
            }
        }
        (Value::Text(text), DapType::String) => {
            if selection.op == Op::Regex {
                regex::Regex::new(text).map_err(|error| {
                    DapError::BadRequest(format!("invalid regular expression {text:?}: {error}"))
                })?;
                // `=~` is a full match, the same as ERDDAP.
                params.push(ScalarValue::Utf8(Some(format!("^(?:{text})$"))));
                Ok(format!(
                    "regexp_like({ident}, CAST(${} AS VARCHAR))",
                    params.len()
                ))
            } else {
                params.push(ScalarValue::Utf8(Some(text.clone())));
                Ok(format!(
                    "{ident} {} CAST(${} AS VARCHAR)",
                    sql_operator(selection.op)?,
                    params.len()
                ))
            }
        }
        (Value::Number(number), dap_type) if dap_type != DapType::String => {
            if selection.op == Op::Regex {
                return Err(DapError::BadRequest(format!(
                    "=~ needs a text column, and {:?} is numeric",
                    column.name
                )));
            }
            params.push(ScalarValue::Float64(Some(*number)));
            let value = format!("CAST(${} AS DOUBLE)", params.len());
            // The filter stays on the native column, so pushdown into the source still works.
            let value = if column.is_time() {
                format!("to_timestamp({value})")
            } else {
                value
            };
            Ok(format!("{ident} {} {value}", sql_operator(selection.op)?))
        }
        (Value::Text(_), _) => Err(DapError::BadRequest(format!(
            "column {:?} is numeric, so the value must be a number",
            column.name
        ))),
        (Value::Number(_), _) => Err(DapError::BadRequest(format!(
            "column {:?} holds text, so the value must be a quoted string",
            column.name
        ))),
    }
}

fn sql_operator(op: Op) -> Result<&'static str, DapError> {
    match op {
        Op::Eq => Ok("="),
        Op::Ne => Ok("<>"),
        Op::Lt => Ok("<"),
        Op::Le => Ok("<="),
        Op::Gt => Ok(">"),
        Op::Ge => Ok(">="),
        Op::Regex => Err(DapError::Internal(
            "=~ has no SQL operator; it compiles to regexp_like".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, TimeUnit};

    fn column(name: &str, data_type: DataType) -> ServedColumn {
        let dap_type = DapType::of(&data_type).expect("served type");
        ServedColumn {
            name: name.to_string(),
            data_type,
            dap_type,
            comment: None,
        }
    }

    fn schema() -> ServedSchema {
        ServedSchema {
            columns: vec![
                column("time", DataType::Timestamp(TimeUnit::Microsecond, None)),
                column("temp", DataType::Float32),
                column("depth", DataType::Float64),
                column("id", DataType::Int64),
                column("platform", DataType::Utf8),
            ],
            omitted: vec!["raw".into()],
            table_comment: None,
        }
    }

    fn table() -> TableReference {
        TableReference::bare("argo")
    }

    fn selection(column: &str, op: Op, value: Value) -> Selection {
        Selection {
            column: column.to_string(),
            op,
            value,
        }
    }

    fn compiled(projection: &[&str], selections: Vec<Selection>) -> Result<Compiled, DapError> {
        let constraint = Constraint {
            projection: projection.iter().map(|name| name.to_string()).collect(),
            selections,
        };
        compile(&table(), &schema(), &constraint)
    }

    fn error_text(result: Result<Compiled, DapError>) -> String {
        match result {
            Err(DapError::BadRequest(text)) => text,
            other => panic!("expected a bad request, got {other:?}"),
        }
    }

    #[test]
    fn projection_keeps_the_schema_order() {
        let out = compiled(&["platform", "time"], vec![]).unwrap();

        assert_eq!(out.sql, r#"SELECT "time", "platform" FROM argo"#);
        assert!(out.params.is_empty());
        let names: Vec<_> = out.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["time", "platform"]);
    }

    #[test]
    fn an_empty_projection_selects_every_served_column() {
        let out = compiled(&[], vec![]).unwrap();

        assert_eq!(
            out.sql,
            r#"SELECT "time", "temp", "depth", "id", "platform" FROM argo"#
        );
    }

    #[test]
    fn selections_become_parameters() {
        let out = compiled(
            &["temp"],
            vec![
                selection("temp", Op::Gt, Value::Number(3.0)),
                selection("platform", Op::Eq, Value::Text("6901".into())),
            ],
        )
        .unwrap();

        assert_eq!(
            out.sql,
            r#"SELECT "temp" FROM argo WHERE "temp" > CAST($1 AS DOUBLE) AND "platform" = CAST($2 AS VARCHAR)"#
        );
        assert_eq!(
            out.params,
            vec![
                ScalarValue::Float64(Some(3.0)),
                ScalarValue::Utf8(Some("6901".into()))
            ]
        );
    }

    #[test]
    fn not_equal_is_the_sql_operator() {
        let out = compiled(&["id"], vec![selection("id", Op::Ne, Value::Number(1.0))]).unwrap();

        assert!(
            out.sql.ends_with(r#"WHERE "id" <> CAST($1 AS DOUBLE)"#),
            "{}",
            out.sql
        );
    }

    #[test]
    fn time_columns_compare_with_to_timestamp() {
        let out = compiled(
            &["time"],
            vec![selection("time", Op::Ge, Value::Number(1_704_067_200.0))],
        )
        .unwrap();

        assert!(
            out.sql
                .ends_with(r#"WHERE "time" >= to_timestamp(CAST($1 AS DOUBLE))"#),
            "{}",
            out.sql
        );
    }

    #[test]
    fn regex_is_a_full_match() {
        let out = compiled(
            &["platform"],
            vec![selection("platform", Op::Regex, Value::Text("69.*".into()))],
        )
        .unwrap();

        assert!(
            out.sql
                .ends_with(r#"WHERE regexp_like("platform", CAST($1 AS VARCHAR))"#),
            "{}",
            out.sql
        );
        assert_eq!(
            out.params,
            vec![ScalarValue::Utf8(Some("^(?:69.*)$".into()))]
        );
    }

    #[test]
    fn regex_alternation_stays_anchored() {
        let out = compiled(
            &["platform"],
            vec![selection(
                "platform",
                Op::Regex,
                Value::Text("SHIP|BUOY".into()),
            )],
        )
        .unwrap();

        assert_eq!(
            out.params,
            vec![ScalarValue::Utf8(Some("^(?:SHIP|BUOY)$".into()))]
        );
    }

    #[test]
    fn nan_compiles_to_isnan() {
        let eq = compiled(&["depth"], vec![selection("depth", Op::Eq, Value::NaN)]).unwrap();
        let ne = compiled(&["depth"], vec![selection("depth", Op::Ne, Value::NaN)]).unwrap();

        assert!(eq.sql.ends_with(r#"WHERE isnan("depth")"#), "{}", eq.sql);
        assert!(
            ne.sql.ends_with(r#"WHERE NOT isnan("depth")"#),
            "{}",
            ne.sql
        );
        assert!(eq.params.is_empty());
        assert!(error_text(compiled(
            &["depth"],
            vec![selection("depth", Op::Lt, Value::NaN)]
        ))
        .contains("NaN"));
    }

    #[test]
    fn a_selection_can_use_a_column_outside_the_projection() {
        let out = compiled(
            &["temp"],
            vec![selection("depth", Op::Gt, Value::Number(10.0))],
        )
        .unwrap();

        assert_eq!(
            out.sql,
            r#"SELECT "temp" FROM argo WHERE "depth" > CAST($1 AS DOUBLE)"#
        );
    }

    #[test]
    fn identifiers_with_quotes_are_escaped() {
        assert_eq!(quote_ident(r#"a"b"#), r#""a""b""#);
    }

    #[test]
    fn invalid_constraints_are_rejected() {
        assert!(error_text(compiled(&["nope"], vec![])).contains("unknown column"));
        assert!(error_text(compiled(&["raw"], vec![])).contains("cannot serve"));
        assert!(error_text(compiled(
            &["temp"],
            vec![selection("temp", Op::Gt, Value::Text("x".into()))]
        ))
        .contains("must be a number"));
        assert!(error_text(compiled(
            &["platform"],
            vec![selection("platform", Op::Gt, Value::Number(1.0))]
        ))
        .contains("quoted string"));
        assert!(error_text(compiled(
            &["temp"],
            vec![selection("temp", Op::Regex, Value::Number(1.0))]
        ))
        .contains("text column"));
        assert!(error_text(compiled(
            &["platform"],
            vec![selection("platform", Op::Regex, Value::Text("(".into()))]
        ))
        .contains("invalid regular expression"));
        assert!(
            error_text(compiled(&["id"], vec![selection("id", Op::Eq, Value::NaN)]))
                .contains("NaN")
        );
    }

    #[test]
    fn a_table_without_served_columns_is_rejected() {
        let schema = ServedSchema {
            columns: vec![],
            omitted: vec!["raw".into()],
            table_comment: None,
        };

        let result = compile(&table(), &schema, &Constraint::default());

        assert!(error_text(result).contains("no column"));
    }
}
