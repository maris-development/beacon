//! Binding query parameters — the injection-safe path for `execute(sql, params)`.
//!
//! Two jobs, both here so the SQL text and the values are prepared together:
//!
//! 1. Rewrite `qmark` placeholders (`?`) to the `$1..$n` positional form DataFusion's planner
//!    understands, skipping any `?` inside a string literal, quoted identifier, or comment — a
//!    naive `replace("?", …)` corrupts `'a?b'`.
//! 2. Convert the Python values to Arrow [`ScalarValue`]s, which are then bound to the plan's
//!    placeholders (never spliced into the SQL text).
//!
//! `$1`-style SQL is accepted too: if the text contains no `?`, it passes through unchanged and
//! the values bind to whatever `$n` placeholders the planner finds.

use datafusion::scalar::ScalarValue;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyFloat, PyInt, PyList, PyString, PyTuple};

use crate::errors::{programming_error, DataError};

/// Prepares a statement and its parameters for execution.
///
/// Returns the rewritten SQL and the bound values. With no parameters, the SQL is returned
/// untouched and the value list is empty.
pub fn prepare(
    sql: &str,
    parameters: Option<&Bound<'_, PyAny>>,
) -> PyResult<(String, Vec<ScalarValue>)> {
    let Some(parameters) = parameters else {
        return Ok((sql.to_string(), Vec::new()));
    };
    let values = extract_param_list(parameters)?;
    if values.is_empty() {
        return Ok((sql.to_string(), Vec::new()));
    }

    let (rewritten, qmark_count) = rewrite_qmark(sql);
    // When the caller used `?` placeholders, their count must match the parameters exactly —
    // catching the off-by-one here gives a clearer message than DataFusion's binding error.
    // When they used `$1` instead, `qmark_count` is 0 and DataFusion validates the count.
    if qmark_count > 0 && qmark_count != values.len() {
        return Err(programming_error(format!(
            "the statement has {qmark_count} '?' placeholder(s) but {} parameter(s) were given",
            values.len()
        )));
    }

    let scalars = values
        .iter()
        .enumerate()
        .map(|(index, value)| py_to_scalar(value, index))
        .collect::<PyResult<Vec<_>>>()?;

    Ok((rewritten, scalars))
}

/// Extracts a positional parameter sequence.
///
/// Restricted to `list`/`tuple` on purpose: a `str` is iterable, and accepting it would silently
/// treat `"abc"` as three char parameters.
fn extract_param_list<'py>(obj: &Bound<'py, PyAny>) -> PyResult<Vec<Bound<'py, PyAny>>> {
    if obj.is_instance_of::<PyList>() || obj.is_instance_of::<PyTuple>() {
        return obj.try_iter()?.collect::<PyResult<Vec<_>>>();
    }
    Err(programming_error(
        "parameters must be a list or tuple of values",
    ))
}

/// The scan state for [`rewrite_qmark`]. A `?` is a placeholder only in [`State::Normal`].
#[derive(PartialEq)]
enum State {
    Normal,
    SingleQuote,
    DoubleQuote,
    LineComment,
    BlockComment,
}

/// Rewrites `?` placeholders to `$1..$n`, leaving `?` inside strings/identifiers/comments alone.
///
/// Returns the rewritten SQL and the number of placeholders substituted.
fn rewrite_qmark(sql: &str) -> (String, usize) {
    let chars: Vec<char> = sql.chars().collect();
    let mut out = String::with_capacity(sql.len() + 8);
    let mut count = 0usize;
    let mut state = State::Normal;
    let mut i = 0;

    while i < chars.len() {
        let c = chars[i];
        let next = chars.get(i + 1).copied();
        match state {
            State::Normal => match c {
                '\'' => {
                    state = State::SingleQuote;
                    out.push(c);
                }
                '"' => {
                    state = State::DoubleQuote;
                    out.push(c);
                }
                '-' if next == Some('-') => {
                    state = State::LineComment;
                    out.push(c);
                }
                '/' if next == Some('*') => {
                    state = State::BlockComment;
                    out.push(c);
                }
                '?' => {
                    count += 1;
                    out.push('$');
                    out.push_str(&count.to_string());
                }
                _ => out.push(c),
            },
            State::SingleQuote => {
                out.push(c);
                if c == '\'' {
                    // A doubled quote (`''`) is an escaped quote *inside* the string.
                    if next == Some('\'') {
                        out.push('\'');
                        i += 1;
                    } else {
                        state = State::Normal;
                    }
                }
            }
            State::DoubleQuote => {
                out.push(c);
                if c == '"' {
                    if next == Some('"') {
                        out.push('"');
                        i += 1;
                    } else {
                        state = State::Normal;
                    }
                }
            }
            State::LineComment => {
                out.push(c);
                if c == '\n' {
                    state = State::Normal;
                }
            }
            State::BlockComment => {
                out.push(c);
                if c == '*' && next == Some('/') {
                    out.push('/');
                    i += 1;
                    state = State::Normal;
                }
            }
        }
        i += 1;
    }

    (out, count)
}

/// Converts one Python value to an Arrow [`ScalarValue`].
///
/// Deliberately covers the scalar kinds that appear in `WHERE`/`VALUES` bindings; an
/// unsupported type is refused (naming the 0-based parameter index) rather than coerced.
/// `bool` is checked before `int` because Python's `bool` is a subclass of `int`.
fn py_to_scalar(value: &Bound<'_, PyAny>, index: usize) -> PyResult<ScalarValue> {
    if value.is_none() {
        return Ok(ScalarValue::Null);
    }
    if value.is_instance_of::<PyBool>() {
        return Ok(ScalarValue::Boolean(Some(value.extract::<bool>()?)));
    }
    if value.is_instance_of::<PyInt>() {
        return Ok(ScalarValue::Int64(Some(value.extract::<i64>()?)));
    }
    if value.is_instance_of::<PyFloat>() {
        return Ok(ScalarValue::Float64(Some(value.extract::<f64>()?)));
    }
    if value.is_instance_of::<PyString>() {
        return Ok(ScalarValue::Utf8(Some(value.extract::<String>()?)));
    }
    if value.is_instance_of::<PyBytes>() {
        return Ok(ScalarValue::Binary(Some(value.extract::<Vec<u8>>()?)));
    }
    Err(DataError::new_err(format!(
        "parameter {index} has type `{}`, which cannot be bound; supported parameter types are \
         str, int, float, bool, bytes, and None",
        value.get_type().name()?
    )))
}

#[cfg(test)]
mod tests {
    use super::rewrite_qmark;

    #[test]
    fn rewrites_only_placeholders_outside_literals() {
        let (sql, n) = rewrite_qmark("SELECT * FROM t WHERE a = ? AND b = ?");
        assert_eq!(sql, "SELECT * FROM t WHERE a = $1 AND b = $2");
        assert_eq!(n, 2);
    }

    #[test]
    fn leaves_question_marks_inside_string_literals() {
        let (sql, n) = rewrite_qmark("SELECT 'a?b' AS s WHERE x = ?");
        assert_eq!(sql, "SELECT 'a?b' AS s WHERE x = $1");
        assert_eq!(n, 1);
    }

    #[test]
    fn respects_escaped_quotes_and_comments() {
        let (sql, n) = rewrite_qmark("SELECT 'o''brien?' /* ? */ , ? -- ?\n, ?");
        assert_eq!(sql, "SELECT 'o''brien?' /* ? */ , $1 -- ?\n, $2");
        assert_eq!(n, 2);
    }
}
