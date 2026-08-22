//! Table references that keep the case the statement writes.
//!
//! Beacon turns DataFusion's identifier normalization off (see
//! [`crate::runtime_builder`]), so the catalog registers a table under the exact
//! name the user spells. `TableReference::parse_str` lowercases every unquoted
//! part, so a reference built that way asks the catalog for a name it does not
//! hold: `CREATE TABLE MyTable` then `No table named 'mytable'`.
//!
//! Every path that rebuilds a reference from a name outside the SQL planner goes
//! through this module.

use datafusion::sql::TableReference;
use datafusion::sql::sqlparser::ast::ObjectName;

/// The reference for a name that is already a bare value, not SQL text.
///
/// Splits a qualified `schema.table` the way `parse_str` does, and keeps the
/// case of every part.
pub(crate) fn table_reference(name: &str) -> TableReference {
    TableReference::parse_str_normalized(name, true)
}

/// The reference a parsed object name denotes, with the case kept.
///
/// Reads the identifier values straight from the AST, so a quoted part needs no
/// second parse and keeps any character that quoting allows.
pub(crate) fn object_name_table_reference(name: &ObjectName) -> TableReference {
    let mut parts: Vec<String> = name
        .0
        .iter()
        .map(|part| match part.as_ident() {
            Some(ident) => ident.value.clone(),
            None => part.to_string(),
        })
        .collect();

    match parts.len() {
        1 => TableReference::bare(parts.pop().expect("one part")),
        2 => {
            let table = parts.pop().expect("two parts");
            let schema = parts.pop().expect("two parts");
            TableReference::partial(schema, table)
        }
        3 => {
            let table = parts.pop().expect("three parts");
            let schema = parts.pop().expect("three parts");
            let catalog = parts.pop().expect("three parts");
            TableReference::full(catalog, schema, table)
        }
        // The parser never yields zero parts, and the catalog gives no meaning to
        // more than three. Both keep the whole text as one name, so the failure
        // names what the user wrote.
        _ => TableReference::bare(parts.join(".")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::ast::Ident;

    fn object_name(parts: &[&str]) -> ObjectName {
        ObjectName::from(parts.iter().map(|p| Ident::new(*p)).collect::<Vec<_>>())
    }

    #[test]
    fn a_value_keeps_its_case() {
        assert_eq!(table_reference("MyTable"), TableReference::bare("MyTable"));
        assert_eq!(table_reference("MYTABLE"), TableReference::bare("MYTABLE"));
        assert_eq!(
            table_reference("MySchema.MyTable"),
            TableReference::partial("MySchema", "MyTable")
        );
    }

    /// The bug this module exists for: DataFusion's own parse lowercases.
    #[test]
    fn parse_str_lowercases_and_this_module_does_not() {
        assert_eq!(TableReference::parse_str("MyTable").table(), "mytable");
        assert_eq!(table_reference("MyTable").table(), "MyTable");
    }

    /// A name that needs quoting cannot be re-parsed, so it stays one name.
    #[test]
    fn a_value_that_is_not_an_identifier_stays_whole() {
        assert_eq!(
            table_reference("my table"),
            TableReference::bare("my table")
        );
    }

    #[test]
    fn an_object_name_keeps_its_case() {
        assert_eq!(
            object_name_table_reference(&object_name(&["MyTable"])),
            TableReference::bare("MyTable")
        );
        assert_eq!(
            object_name_table_reference(&object_name(&["MySchema", "MyTable"])),
            TableReference::partial("MySchema", "MyTable")
        );
        assert_eq!(
            object_name_table_reference(&object_name(&["Cat", "MySchema", "MyTable"])),
            TableReference::full("Cat", "MySchema", "MyTable")
        );
    }

    /// A quoted part reaches the catalog whole, dot included.
    #[test]
    fn an_object_name_part_is_never_split() {
        let name = ObjectName::from(vec![Ident::with_quote('"', "My.Table")]);
        assert_eq!(
            object_name_table_reference(&name),
            TableReference::bare("My.Table")
        );
    }
}
