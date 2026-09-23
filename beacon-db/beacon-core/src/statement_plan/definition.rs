//! The `CREATE` statement text that Beacon stores with a table.
//!
//! DataFusion renders a `CREATE EXTERNAL TABLE` without its columns, partitions
//! or `OPTIONS`, so Beacon renders the full statement itself. Credentials never
//! reach the stored text: secret option values and URL passwords are masked
//! before the statement is written.

use datafusion::sql::parser::CreateExternalTable;
use datafusion::sql::sqlparser::ast::Value;

/// The text that replaces a secret value.
const MASK: &str = "***";

/// Render `statement` as a complete `CREATE EXTERNAL TABLE`, with secrets masked.
pub(crate) fn render_create_external_table(statement: &CreateExternalTable) -> String {
    let mut sql = String::from("CREATE ");
    if statement.or_replace {
        sql.push_str("OR REPLACE ");
    }
    if statement.unbounded {
        sql.push_str("UNBOUNDED ");
    }
    sql.push_str("EXTERNAL ");
    if statement.temporary {
        sql.push_str("TEMPORARY ");
    }
    sql.push_str("TABLE ");
    if statement.if_not_exists {
        sql.push_str("IF NOT EXISTS ");
    }
    sql.push_str(&statement.name.to_string());

    let elements: Vec<String> = statement
        .columns
        .iter()
        .map(ToString::to_string)
        .chain(statement.constraints.iter().map(ToString::to_string))
        .collect();
    if !elements.is_empty() {
        sql.push_str(&format!(" ({})", elements.join(", ")));
    }

    sql.push_str(&format!(" STORED AS {}", statement.file_type));

    if !statement.table_partition_cols.is_empty() {
        sql.push_str(&format!(
            " PARTITIONED BY ({})",
            statement.table_partition_cols.join(", ")
        ));
    }

    for ordering in &statement.order_exprs {
        let exprs: Vec<String> = ordering.iter().map(ToString::to_string).collect();
        sql.push_str(&format!(" WITH ORDER ({})", exprs.join(", ")));
    }

    if !statement.options.is_empty() {
        let options: Vec<String> = statement
            .options
            .iter()
            .map(|(key, value)| {
                let value = if is_secret_option(key) {
                    quote_literal(MASK)
                } else {
                    option_value(value)
                };
                format!("{} {value}", quote_literal(key))
            })
            .collect();
        sql.push_str(&format!(" OPTIONS ({})", options.join(", ")));
    }

    sql.push_str(&format!(
        " LOCATION {}",
        quote_literal(&mask_url_password(&statement.location))
    ));
    sql
}

/// Whether the option `key` holds a credential. Only the last dotted segment
/// counts, so `format.password` and `aws.secret_access_key` both match.
fn is_secret_option(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    let leaf = key.rsplit('.').next().unwrap_or(&key);
    matches!(
        leaf,
        "pass" | "pwd" | "key" | "sas" | "access_key_id" | "account_key" | "api_key" | "private_key"
    ) || ["password", "passwd", "secret", "token", "credential"]
        .iter()
        .any(|word| leaf.contains(word))
}

/// An option value as SQL. String values are always single-quoted, so a value
/// the user wrote as a bare word still parses back as the same string.
fn option_value(value: &Value) -> String {
    match value {
        Value::SingleQuotedString(text)
        | Value::DoubleQuotedString(text)
        | Value::EscapedStringLiteral(text)
        | Value::UnicodeStringLiteral(text) => quote_literal(text),
        other => other.to_string(),
    }
}

/// A single-quoted SQL string literal.
fn quote_literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// Mask the password in the user-info part of a URL (`scheme://user:pass@host`).
fn mask_url_password(location: &str) -> String {
    let Some((scheme, rest)) = location.split_once("://") else {
        return location.to_string();
    };
    let authority_end = rest.find('/').unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    let Some((user_info, host)) = authority.rsplit_once('@') else {
        return location.to_string();
    };
    let Some((user, _password)) = user_info.split_once(':') else {
        return location.to_string();
    };
    format!("{scheme}://{user}:{MASK}@{host}{}", &rest[authority_end..])
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::parser::{DFParser, Statement};

    fn render(sql: &str) -> String {
        let mut statements = DFParser::parse_sql(sql).expect("SQL should parse");
        match statements.pop_front() {
            Some(Statement::CreateExternalTable(statement)) => {
                render_create_external_table(&statement)
            }
            other => panic!("expected CREATE EXTERNAL TABLE, got {other:?}"),
        }
    }

    #[test]
    fn keeps_every_clause() {
        let rendered = render(
            "CREATE EXTERNAL TABLE IF NOT EXISTS argo (temp DOUBLE, platform VARCHAR) \
             STORED AS PARQUET PARTITIONED BY (platform) \
             OPTIONS ('format.pushdown_filters' 'true') LOCATION 'argo/**/*.parquet'",
        );
        assert_eq!(
            rendered,
            "CREATE EXTERNAL TABLE IF NOT EXISTS argo (temp DOUBLE, platform VARCHAR) \
             STORED AS PARQUET PARTITIONED BY (platform) \
             OPTIONS ('format.pushdown_filters' 'true') LOCATION 'argo/**/*.parquet'"
        );
    }

    #[test]
    fn output_parses_back_to_the_same_statement() {
        let rendered = render(
            "CREATE EXTERNAL TABLE t STORED AS CSV WITH ORDER (a DESC) \
             OPTIONS (format.has_header true, 'format.delimiter' ';') LOCATION 'it''s/data.csv'",
        );
        assert_eq!(render(&rendered), rendered);
    }

    #[test]
    fn masks_secret_options() {
        let rendered = render(
            "CREATE EXTERNAL TABLE orders STORED AS POSTGRES \
             OPTIONS ('host' 'db', 'user' 'reader', 'password' 'hunter2', \
             'aws.secret_access_key' 'abc', 'aws.access_key_id' 'AKIA', 'token' 't0k') \
             LOCATION 'public.orders'",
        );
        for secret in ["hunter2", "abc", "AKIA", "t0k"] {
            assert!(!rendered.contains(secret), "{secret} leaked: {rendered}");
        }
        assert!(rendered.contains("'host' 'db'"), "{rendered}");
        assert!(rendered.contains("'user' 'reader'"), "{rendered}");
        assert!(rendered.contains("'password' '***'"), "{rendered}");
    }

    #[test]
    fn masks_a_password_in_the_location() {
        let rendered = render(
            "CREATE EXTERNAL TABLE t STORED AS PARQUET LOCATION 'https://bob:hunter2@example.com/t.parquet'",
        );
        assert!(
            rendered.ends_with("LOCATION 'https://bob:***@example.com/t.parquet'"),
            "{rendered}"
        );
    }
}
