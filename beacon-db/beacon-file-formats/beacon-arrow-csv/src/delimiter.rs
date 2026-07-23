//! Parsing a user-supplied CSV delimiter string into the single byte the CSV
//! reader wants.
//!
//! Shared by the `read_csv` table function and the `CsvFormat` factory (external
//! tables) so both accept the same spellings: a bare character (`,`, `;`, `|`, a
//! literal tab) or a C-style escape (`\t`, `\n`, `\r`, `\0`, `\\`). Without escape
//! handling `'\t'` reaches SQL as the two characters `\` and `t`, and the reader
//! would silently split on backslashes.

/// Parse a delimiter string into a single byte.
///
/// Accepts:
/// - a single character: `,` `;` `|` or an actual tab, etc.
/// - a two-character C escape: `\t` `\n` `\r` `\0` `\\`.
///
/// Returns an error string (suitable for wrapping in a plan error) when the
/// value is empty, is more than one character and not a recognised escape, or
/// does not fit in a single byte (non-ASCII).
pub fn parse_delimiter(value: &str) -> Result<u8, String> {
    let resolved: char = match value {
        "\\t" => '\t',
        "\\n" => '\n',
        "\\r" => '\r',
        "\\0" => '\0',
        "\\\\" => '\\',
        other => {
            let mut chars = other.chars();
            match (chars.next(), chars.next()) {
                (Some(c), None) => c,
                (None, _) => {
                    return Err("CSV delimiter must not be empty".to_string());
                }
                (Some(_), Some(_)) => {
                    return Err(format!(
                        "CSV delimiter must be a single character or an escape \
                         (\\t, \\n, \\r, \\0, \\\\), got {value:?}"
                    ));
                }
            }
        }
    };

    if resolved.is_ascii() {
        Ok(resolved as u8)
    } else {
        Err(format!(
            "CSV delimiter must be a single-byte (ASCII) character, got {resolved:?}"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::parse_delimiter;

    #[test]
    fn single_characters() {
        assert_eq!(parse_delimiter(",").unwrap(), b',');
        assert_eq!(parse_delimiter(";").unwrap(), b';');
        assert_eq!(parse_delimiter("|").unwrap(), b'|');
        // A literal tab character resolves to the tab byte.
        assert_eq!(parse_delimiter("\t").unwrap(), b'\t');
    }

    #[test]
    fn escape_sequences() {
        assert_eq!(parse_delimiter("\\t").unwrap(), b'\t');
        assert_eq!(parse_delimiter("\\n").unwrap(), b'\n');
        assert_eq!(parse_delimiter("\\r").unwrap(), b'\r');
        assert_eq!(parse_delimiter("\\0").unwrap(), 0);
        assert_eq!(parse_delimiter("\\\\").unwrap(), b'\\');
    }

    #[test]
    fn rejects_empty_multichar_and_non_ascii() {
        assert!(parse_delimiter("").is_err());
        assert!(parse_delimiter("ab").is_err());
        assert!(parse_delimiter("\\x").is_err()); // unknown escape -> two chars
        assert!(parse_delimiter("€").is_err()); // multi-byte
    }
}
