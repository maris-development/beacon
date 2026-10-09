//! The built-in guide of every Beacon server: how Beacon works, how to write a
//! query and how to use the `beacon-api` Python client. The `get_guide` tool
//! returns it.

/// The guide as Markdown. It is the same for every server and every caller.
pub const GUIDE: &str = include_str!("guide.md");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_guide_covers_queries_and_the_python_client() {
        assert!(GUIDE.contains("describe_table"));
        assert!(GUIDE.contains("pip install beacon-api"));
        assert!(GUIDE.contains("client.sql_query("));
    }
}
