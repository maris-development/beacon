//! The DAP2 constraint expression: a projection and value selections.

use std::sync::LazyLock;

use percent_encoding::percent_decode_str;
use regex::Regex;

use crate::dds::SEQUENCE;
use crate::error::DapError;
use crate::names;

/// A selection operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    /// `=~`, a regular expression match.
    Regex,
}

/// A selection value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Number(f64),
    /// The literal `NaN`.
    NaN,
    /// A quoted string, without its quotes and escapes.
    Text(String),
}

/// One `column op value` clause.
#[derive(Debug, Clone, PartialEq)]
pub struct Selection {
    pub column: String,
    pub op: Op,
    pub value: Value,
}

/// A parsed constraint. An empty projection selects every served column.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Constraint {
    pub projection: Vec<String>,
    pub selections: Vec<Selection>,
}

/// Two-character operators come first, so that `<=` is not read as `<`.
const OPERATORS: [(&str, Op); 7] = [
    ("!=", Op::Ne),
    ("<=", Op::Le),
    (">=", Op::Ge),
    ("=~", Op::Regex),
    ("=", Op::Eq),
    ("<", Op::Lt),
    (">", Op::Gt),
];

/// A decimal number. `f64::from_str` also takes `inf` and `nan`, and DAP2 has neither.
static NUMBER: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$").expect("valid number regex")
});

/// Parses the raw query string of a DAP2 request.
///
/// The URL decode does not turn `+` into a space, so `1e+5` stays a number.
pub fn parse(raw_query: &str) -> Result<Constraint, DapError> {
    let query = percent_decode_str(raw_query)
        .decode_utf8()
        .map_err(|_| DapError::BadRequest("the constraint is not UTF-8".to_string()))?;
    let mut constraint = Constraint::default();
    for (index, clause) in split_outside_quotes(&query, '&')?.into_iter().enumerate() {
        let clause = clause.trim();
        if clause.is_empty() {
            continue;
        }
        if index == 0 && find_operator(clause).is_none() {
            constraint.projection = parse_projection(clause)?;
        } else {
            constraint.selections.push(parse_selection(clause)?);
        }
    }
    Ok(constraint)
}

fn parse_projection(clause: &str) -> Result<Vec<String>, DapError> {
    let mut names = Vec::new();
    for item in clause.split(',') {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        if item == SEQUENCE {
            return Ok(Vec::new());
        }
        names.push(parse_name(item)?);
    }
    Ok(names)
}

fn parse_selection(clause: &str) -> Result<Selection, DapError> {
    let (position, operator, op) = find_operator(clause).ok_or_else(|| {
        DapError::BadRequest(format!(
            "{clause:?} has no operator; functions are not supported"
        ))
    })?;
    let column = parse_name(&clause[..position])?;
    let value = parse_value(clause[position + operator.len()..].trim())?;
    Ok(Selection { column, op, value })
}

/// A column name, with or without the `s.` prefix, after the `%XX` decode.
///
/// The schema is flat, so a `.` after the prefix belongs to the column name.
fn parse_name(text: &str) -> Result<String, DapError> {
    let text = text.trim();
    let bare = text
        .strip_prefix(SEQUENCE)
        .and_then(|rest| rest.strip_prefix('.'))
        .unwrap_or(text);
    if bare.is_empty() || bare.contains(['(', ')', '[', ']', '{', '}', '"', ',']) {
        return Err(DapError::BadRequest(format!(
            "{text:?} is not a column name; functions, index ranges and value lists are not supported"
        )));
    }
    names::decode(bare)
}

fn parse_value(text: &str) -> Result<Value, DapError> {
    if let Some(rest) = text.strip_prefix('"') {
        return parse_quoted(rest).map(Value::Text);
    }
    if text == "NaN" {
        return Ok(Value::NaN);
    }
    if NUMBER.is_match(text) {
        return text
            .parse::<f64>()
            .map(Value::Number)
            .map_err(|_| DapError::BadRequest(format!("{text:?} is not a number")));
    }
    Err(DapError::BadRequest(format!(
        "{text:?} is not a number or a quoted string; value lists are not supported"
    )))
}

/// The text of a quoted string after its opening quote. Nothing may follow the closing quote.
fn parse_quoted(rest: &str) -> Result<String, DapError> {
    let mut out = String::new();
    let mut chars = rest.chars();
    while let Some(c) = chars.next() {
        match c {
            // Only `\"` and `\\` are escapes. Other backslashes stay in the text.
            '\\' => match chars.next() {
                Some(next @ ('"' | '\\')) => out.push(next),
                Some(other) => {
                    out.push('\\');
                    out.push(other);
                }
                None => break,
            },
            '"' => {
                return if chars.as_str().trim().is_empty() {
                    Ok(out)
                } else {
                    Err(DapError::BadRequest(format!(
                        "text after the closing quote in {rest:?}"
                    )))
                };
            }
            other => out.push(other),
        }
    }
    Err(DapError::BadRequest(
        "a quoted string has no closing quote".to_string(),
    ))
}

/// Walks a clause and yields each character outside quoted strings, with its byte position.
/// The quote marks are not yielded.
struct Unquoted<'a> {
    chars: std::str::CharIndices<'a>,
    in_quotes: bool,
    escaped: bool,
}

impl<'a> Unquoted<'a> {
    fn new(text: &'a str) -> Self {
        Self {
            chars: text.char_indices(),
            in_quotes: false,
            escaped: false,
        }
    }
}

impl Iterator for Unquoted<'_> {
    type Item = (usize, char);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (position, c) = self.chars.next()?;
            if self.in_quotes {
                // A backslash skips the next character, so `\"` does not close the string.
                if self.escaped {
                    self.escaped = false;
                } else if c == '\\' {
                    self.escaped = true;
                } else if c == '"' {
                    self.in_quotes = false;
                }
                continue;
            }
            if c == '"' {
                self.in_quotes = true;
                continue;
            }
            return Some((position, c));
        }
    }
}

/// The first operator outside a quoted string: its byte position, its text and its kind.
fn find_operator(clause: &str) -> Option<(usize, &'static str, Op)> {
    Unquoted::new(clause).find_map(|(position, _)| {
        OPERATORS
            .iter()
            .find(|(text, _)| clause[position..].starts_with(text))
            .map(|(text, op)| (position, *text, *op))
    })
}

/// Splits `text` on `separator` outside quoted strings.
fn split_outside_quotes(text: &str, separator: char) -> Result<Vec<&str>, DapError> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut scan = Unquoted::new(text);
    for (position, c) in scan.by_ref() {
        if c == separator {
            parts.push(&text[start..position]);
            start = position + c.len_utf8();
        }
    }
    if scan.in_quotes {
        return Err(DapError::BadRequest(
            "a quoted string has no closing quote".to_string(),
        ));
    }
    parts.push(&text[start..]);
    Ok(parts)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn selection(column: &str, op: Op, value: Value) -> Selection {
        Selection {
            column: column.to_string(),
            op,
            value,
        }
    }

    #[test]
    fn an_empty_query_selects_everything() {
        assert_eq!(parse("").unwrap(), Constraint::default());
    }

    #[test]
    fn the_first_clause_without_an_operator_is_the_projection() {
        assert_eq!(
            parse("time,temp").unwrap().projection,
            vec!["time".to_string(), "temp".to_string()]
        );
    }

    #[test]
    fn names_may_have_the_sequence_prefix() {
        let constraint = parse("s.time,s.temp&s.temp>3").unwrap();

        assert_eq!(
            constraint.projection,
            vec!["time".to_string(), "temp".to_string()]
        );
        assert_eq!(
            constraint.selections,
            vec![selection("temp", Op::Gt, Value::Number(3.0))]
        );
    }

    #[test]
    fn the_sequence_name_alone_selects_every_column() {
        assert!(parse("s&temp>3").unwrap().projection.is_empty());
    }

    #[test]
    fn a_query_can_start_with_a_selection() {
        assert_eq!(
            parse("&temp>=-1.5e2").unwrap().selections,
            vec![selection("temp", Op::Ge, Value::Number(-150.0))]
        );
        assert_eq!(
            parse("temp<2").unwrap().selections,
            vec![selection("temp", Op::Lt, Value::Number(2.0))]
        );
    }

    #[test]
    fn every_operator_parses() {
        for (text, op) in [
            ("a=1", Op::Eq),
            ("a!=1", Op::Ne),
            ("a<1", Op::Lt),
            ("a<=1", Op::Le),
            ("a>1", Op::Gt),
            ("a>=1", Op::Ge),
        ] {
            assert_eq!(
                parse(&format!("&{text}")).unwrap().selections[0].op,
                op,
                "{text}"
            );
        }
        assert_eq!(
            parse("&a=~\"x.*\"").unwrap().selections,
            vec![selection("a", Op::Regex, Value::Text("x.*".into()))]
        );
    }

    #[test]
    fn quoted_strings_keep_separators_and_escapes() {
        assert_eq!(
            parse("&name=%22a%26b%22").unwrap().selections,
            vec![selection("name", Op::Eq, Value::Text("a&b".into()))]
        );
        assert_eq!(
            parse(r#"&name="a\"b>c""#).unwrap().selections,
            vec![selection("name", Op::Eq, Value::Text("a\"b>c".into()))]
        );
    }

    #[test]
    fn percent_encoded_operators_decode() {
        assert_eq!(
            parse("id,platform&depth%3E10").unwrap().selections,
            vec![selection("depth", Op::Gt, Value::Number(10.0))]
        );
    }

    #[test]
    fn a_plus_in_the_exponent_stays_a_number() {
        let expected = vec![selection("t", Op::Gt, Value::Number(100_000.0))];
        assert_eq!(parse("&t>1e+5").unwrap().selections, expected);
        assert_eq!(parse("&t>1e%2B5").unwrap().selections, expected);
    }

    #[test]
    fn a_name_escape_decodes_after_the_url_decode() {
        assert_eq!(
            parse("temp%252Eunits").unwrap().projection,
            vec!["temp.units".to_string()]
        );
    }

    #[test]
    fn a_dotted_column_name_parses_in_every_form() {
        assert_eq!(
            parse("s%2Etemp%2Eunits").unwrap().projection,
            vec!["temp.units".to_string()]
        );
        assert_eq!(
            parse("temp.units").unwrap().projection,
            vec!["temp.units".to_string()]
        );
        assert_eq!(parse("s.a.b").unwrap().projection, vec!["a.b".to_string()]);
        assert_eq!(
            parse("id&s%2Etemp%2Eunits%3E5").unwrap().selections,
            vec![selection("temp.units", Op::Gt, Value::Number(5.0))]
        );
    }

    #[test]
    fn nan_is_a_value() {
        assert_eq!(
            parse("&depth=NaN").unwrap().selections,
            vec![selection("depth", Op::Eq, Value::NaN)]
        );
    }

    #[test]
    fn unsupported_forms_are_bad_requests() {
        for query in [
            "&temp>abc",
            "&temp>inf",
            "orderBy(%22time%22)",
            "&orderBy(%22time%22)",
            "temp[0:9]",
            "&a={1,2}",
            "&name=\"open",
            "&name=\"a\"b",
            "%FF",
            "&=3",
        ] {
            assert!(
                matches!(parse(query), Err(DapError::BadRequest(_))),
                "{query} should be rejected"
            );
        }
    }

    #[test]
    fn an_unknown_escape_keeps_its_backslash() {
        assert_eq!(
            parse(r#"&a=~"69\.0""#).unwrap().selections,
            vec![selection("a", Op::Regex, Value::Text("69\\.0".into()))]
        );
        assert_eq!(
            parse(r#"&a=~"\d+""#).unwrap().selections,
            vec![selection("a", Op::Regex, Value::Text("\\d+".into()))]
        );
    }

    #[test]
    fn a_double_backslash_is_one_backslash() {
        assert_eq!(
            parse(r#"&a="x\\y""#).unwrap().selections,
            vec![selection("a", Op::Eq, Value::Text("x\\y".into()))]
        );
    }

    #[test]
    fn a_trailing_backslash_leaves_the_quote_open() {
        assert!(matches!(parse(r#"&a="x\"#), Err(DapError::BadRequest(_))));
    }

    #[test]
    fn non_ascii_names_and_values_parse() {
        assert_eq!(
            parse(r#"&n="é""#).unwrap().selections,
            vec![selection("n", Op::Eq, Value::Text("é".into()))]
        );
        assert_eq!(
            parse("&é>1").unwrap().selections,
            vec![selection("é", Op::Gt, Value::Number(1.0))]
        );
    }
}
