use datafusion::error::{DataFusionError, Result};
use datafusion::sql::{
    parser::{DFParser, DFParserBuilder},
    sqlparser::{keywords::Keyword, tokenizer::Token},
};

use super::statement::{BeaconStatement, CreateMaterializedViewStatement, RefreshStatement};

/// A parser that extends `DFParser` with custom Beacon SQL syntax.
pub struct BeaconParser<'a> {
    df_parser: DFParser<'a>,
}

impl<'a> BeaconParser<'a> {
    pub fn new(sql: &'a str) -> Result<Self> {
        Ok(Self {
            df_parser: DFParserBuilder::new(sql).build()?,
        })
    }

    /// Parse a single statement, returning a `BeaconStatement`.
    pub fn parse_statement(&mut self) -> Result<BeaconStatement> {
        if self.is_refresh() {
            return self.parse_refresh();
        }

        if self.is_create_materialized_view() {
            return self.parse_create_materialized_view();
        }

        let df_statement = Box::new(self.df_parser.parse_statement()?);

        Ok(BeaconStatement::DFStatement(df_statement))
    }

    /// Check if the next tokens form a CREATE MATERIALIZED VIEW statement.
    fn is_create_materialized_view(&self) -> bool {
        let t1 = &self.df_parser.parser.peek_nth_token(0).token;
        let t2 = &self.df_parser.parser.peek_nth_token(1).token;
        let t3 = &self.df_parser.parser.peek_nth_token(2).token;

        matches!(t1, Token::Word(w) if w.keyword == Keyword::CREATE)
            && matches!(t2, Token::Word(w) if w.value.to_uppercase() == "MATERIALIZED")
            && matches!(t3, Token::Word(w) if w.keyword == Keyword::VIEW)
    }

    /// Parse: CREATE MATERIALIZED VIEW <view_name> AS <query>
    fn parse_create_materialized_view(&mut self) -> Result<BeaconStatement> {
        // Consume CREATE MATERIALIZED VIEW
        for _ in 0..3 {
            self.df_parser.parser.next_token();
        }

        let view_name = self
            .df_parser
            .parser
            .parse_object_name(false)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Expect AS
        self.df_parser
            .parser
            .expect_keyword(Keyword::AS)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Parse the defining query and capture its SQL text.
        let query = self
            .df_parser
            .parser
            .parse_query()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(BeaconStatement::CreateMaterializedView(
            CreateMaterializedViewStatement {
                view_name,
                query_sql: query.to_string(),
            },
        ))
    }

    /// Check if the next tokens form a REFRESH statement.
    fn is_refresh(&self) -> bool {
        let t = &self.df_parser.parser.peek_nth_token(0).token;
        matches!(t, Token::Word(w) if w.value.to_uppercase() == "REFRESH")
    }

    /// Parse: REFRESH [TABLE] <name>
    fn parse_refresh(&mut self) -> Result<BeaconStatement> {
        // Consume REFRESH
        self.df_parser.parser.next_token();

        // Optional TABLE keyword
        let t = &self.df_parser.parser.peek_nth_token(0).token;
        if matches!(t, Token::Word(w) if w.keyword == Keyword::TABLE) {
            self.df_parser.parser.next_token();
        }

        let name = self
            .df_parser
            .parser
            .parse_object_name(false)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(BeaconStatement::Refresh(RefreshStatement { name }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_regular_sql() {
        let sql = "SELECT 1";
        let mut parser = BeaconParser::new(sql).unwrap();
        let stmt = parser.parse_statement().unwrap();
        matches!(stmt, BeaconStatement::DFStatement(_));
    }

    #[test]
    fn test_parse_refresh_statement() {
        for sql in ["REFRESH my_table", "REFRESH TABLE my_table"] {
            let mut parser = BeaconParser::new(sql).unwrap();
            let stmt = parser.parse_statement().unwrap();
            match stmt {
                BeaconStatement::Refresh(refresh) => {
                    assert_eq!(refresh.name.to_string(), "my_table");
                }
                _ => panic!("Expected Refresh statement for `{sql}`"),
            }
        }
    }

    #[test]
    fn test_parse_refresh_display() {
        let sql = "REFRESH schema.table";
        let mut parser = BeaconParser::new(sql).unwrap();
        let stmt = parser.parse_statement().unwrap();
        assert_eq!(stmt.to_string(), "REFRESH schema.table");
    }

    #[test]
    fn test_parse_create_materialized_view() {
        let sql = "CREATE MATERIALIZED VIEW monthly AS SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id";
        let mut parser = BeaconParser::new(sql).unwrap();
        let stmt = parser.parse_statement().unwrap();

        match stmt {
            BeaconStatement::CreateMaterializedView(s) => {
                assert_eq!(s.view_name.to_string(), "monthly");
                assert!(s.query_sql.to_uppercase().contains("SELECT"));
                assert!(s.query_sql.contains("orders"));
            }
            _ => panic!("Expected CreateMaterializedView statement"),
        }
    }

    #[test]
    fn test_parse_create_materialized_view_display() {
        let sql = "CREATE MATERIALIZED VIEW mv AS SELECT 1 AS a";
        let mut parser = BeaconParser::new(sql).unwrap();
        let stmt = parser.parse_statement().unwrap();
        assert_eq!(
            stmt.to_string(),
            "CREATE MATERIALIZED VIEW mv AS SELECT 1 AS a"
        );
    }

    #[test]
    fn test_parse_create_materialized_view_missing_as() {
        let sql = "CREATE MATERIALIZED VIEW mv SELECT 1 AS a";
        let mut parser = BeaconParser::new(sql).unwrap();
        assert!(parser.parse_statement().is_err());
    }

    #[test]
    fn test_parse_regular_create_view_is_df_statement() {
        let sql = "CREATE VIEW v AS SELECT 1 AS a";
        let mut parser = BeaconParser::new(sql).unwrap();
        let stmt = parser.parse_statement().unwrap();
        assert!(matches!(stmt, BeaconStatement::DFStatement(_)));
    }

    #[test]
    fn test_parse_refresh_missing_name() {
        let sql = "REFRESH";
        let mut parser = BeaconParser::new(sql).unwrap();
        assert!(parser.parse_statement().is_err());
    }
}
