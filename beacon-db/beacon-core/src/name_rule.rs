//! The characters a new table, view or function name can hold.
//!
//! Beacon puts names into object-store paths and `db://` URIs. A space or `é` is
//! percent-encoded there, `#` and `?` start a URI fragment or query, `/` adds a
//! folder, and `.` splits a qualified name. So a new name holds only ASCII
//! letters, digits, `_` and `-`. Names that exist already stay usable.

/// Refuse `name` for a new object of `kind` (`"Table"`, `"View"`, ...) when it
/// holds a character outside the allowed set.
pub(crate) fn check_new_name(kind: &str, name: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !name.is_empty()
            && name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-'),
        "{kind} name '{name}' is not valid: use only letters, digits, '_' and '-'"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_letters_digits_underscore_and_hyphen() {
        for name in [
            "obs",
            "MyTable",
            "ocean_profiles",
            "argo-2024",
            "2024",
            "_x",
        ] {
            assert!(check_new_name("Table", name).is_ok(), "{name}");
        }
    }

    #[test]
    fn refuses_characters_that_change_a_path_or_uri() {
        for name in [
            "", "odd name", "x#a", "x?a", "x/a", "x\\a", "x.a", "x%20a", "café",
        ] {
            let error = check_new_name("Table", name).expect_err(name).to_string();
            assert!(error.contains("Table name"), "{error}");
        }
    }
}
