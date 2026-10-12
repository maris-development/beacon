//! DAP2 names and quoted text.

use percent_encoding::{percent_decode_str, utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};

use crate::error::DapError;

/// DAP2 uses `.` as a hierarchy separator, so every byte outside `[A-Za-z0-9_]` gets an escape.
const ESCAPED: &AsciiSet = &NON_ALPHANUMERIC.remove(b'_');

/// The DAP2 form of a table or column name, for example `temp.units` becomes `temp%2Eunits`.
pub fn encode(name: &str) -> String {
    utf8_percent_encode(name, ESCAPED).to_string()
}

/// The Beacon name of a DAP2 name.
pub fn decode(name: &str) -> Result<String, DapError> {
    percent_decode_str(name)
        .decode_utf8()
        .map(|decoded| decoded.into_owned())
        .map_err(|_| {
            DapError::BadRequest(format!(
                "the name {name:?} is not UTF-8 after the %XX decode"
            ))
        })
}

/// A DAP2 quoted string: `"` and `\` get a backslash.
pub fn quote(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for c in text.chars() {
        if c == '"' || c == '\\' {
            out.push('\\');
        }
        out.push(c);
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_names_stay_the_same() {
        assert_eq!(encode("temp"), "temp");
        assert_eq!(encode("t_1"), "t_1");
    }

    #[test]
    fn dots_spaces_and_non_ascii_get_a_percent_escape() {
        assert_eq!(encode("temp.units"), "temp%2Eunits");
        assert_eq!(encode("sea water"), "sea%20water");
        assert_eq!(encode("é"), "%C3%A9");
    }

    #[test]
    fn decode_reverses_encode() {
        for name in ["temp", "temp.units", "sea water", "é", "a\"b"] {
            assert_eq!(decode(&encode(name)).unwrap(), name);
        }
    }

    #[test]
    fn decode_rejects_bytes_that_are_not_utf8() {
        assert!(matches!(decode("%FF"), Err(DapError::BadRequest(_))));
    }

    #[test]
    fn quote_escapes_quotes_and_backslashes() {
        assert_eq!(quote(r#"a "b" \c"#), r#""a \"b\" \\c""#);
    }
}
