use std::collections::HashMap;

use datafusion::error::{DataFusionError, Result};
use datafusion::sql::{
    parser::{DFParser, DFParserBuilder},
    sqlparser::{keywords::Keyword, tokenizer::Token},
};

use super::statement::{
    BeaconStatement, CreateCrawlerStatement, CreateIndexStatement, CreateMaterializedViewStatement,
    DropCrawlerStatement, DropIndexStatement, RefreshStatement, RunCrawlerStatement,
    ShowIndexesStatement,
};

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

        if self.is_create_crawler() {
            return self.parse_create_crawler();
        }

        if self.is_run_crawler() {
            return self.parse_run_crawler();
        }

        if self.is_drop_crawler() {
            return self.parse_drop_crawler();
        }

        if self.is_show_crawlers() {
            return self.parse_show_crawlers();
        }

        if self.is_create_index() {
            return self.parse_create_index();
        }

        if self.is_drop_index() {
            return self.parse_drop_index();
        }

        if self.is_show_indexes() {
            return self.parse_show_indexes();
        }

        let df_statement = Box::new(self.df_parser.parse_statement()?);

        Ok(BeaconStatement::DFStatement(df_statement))
    }

    /// Whether the next two tokens are `<KW1> CRAWLER`, where `KW1` matches `first`.
    fn is_keyword_then_crawler(&self, first: impl Fn(&Token) -> bool) -> bool {
        let t1 = &self.df_parser.parser.peek_nth_token(0).token;
        let t2 = &self.df_parser.parser.peek_nth_token(1).token;
        first(t1) && matches!(t2, Token::Word(w) if w.value.to_uppercase() == "CRAWLER")
    }

    fn is_create_crawler(&self) -> bool {
        self.is_keyword_then_crawler(|t| matches!(t, Token::Word(w) if w.keyword == Keyword::CREATE))
    }

    fn is_run_crawler(&self) -> bool {
        self.is_keyword_then_crawler(|t| matches!(t, Token::Word(w) if w.value.to_uppercase() == "RUN"))
    }

    fn is_drop_crawler(&self) -> bool {
        self.is_keyword_then_crawler(|t| matches!(t, Token::Word(w) if w.keyword == Keyword::DROP))
    }

    fn is_show_crawlers(&self) -> bool {
        let t1 = &self.df_parser.parser.peek_nth_token(0).token;
        let t2 = &self.df_parser.parser.peek_nth_token(1).token;
        matches!(t1, Token::Word(w) if w.value.to_uppercase() == "SHOW")
            && matches!(t2, Token::Word(w) if w.value.to_uppercase() == "CRAWLERS")
    }

    /// Parse: CREATE CRAWLER <name> [ON '<prefix>'] [WITH (k 'v', ...)]
    fn parse_create_crawler(&mut self) -> Result<BeaconStatement> {
        self.df_parser.parser.next_token(); // CREATE
        self.df_parser.parser.next_token(); // CRAWLER

        let name = self
            .df_parser
            .parser
            .parse_object_name(false)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let target_prefix = if matches!(
            &self.df_parser.parser.peek_nth_token(0).token,
            Token::Word(w) if w.keyword == Keyword::ON
        ) {
            self.df_parser.parser.next_token(); // ON
            Some(self.parse_string_value()?)
        } else {
            None
        };

        let options = if matches!(
            &self.df_parser.parser.peek_nth_token(0).token,
            Token::Word(w) if w.keyword == Keyword::WITH
        ) {
            self.df_parser.parser.next_token(); // WITH
            self.parse_with_options()?
        } else {
            HashMap::new()
        };

        Ok(BeaconStatement::CreateCrawler(CreateCrawlerStatement {
            name,
            target_prefix,
            options,
        }))
    }

    /// Parse: RUN CRAWLER <name>
    fn parse_run_crawler(&mut self) -> Result<BeaconStatement> {
        self.df_parser.parser.next_token(); // RUN
        self.df_parser.parser.next_token(); // CRAWLER
        let name = self
            .df_parser
            .parser
            .parse_object_name(false)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(BeaconStatement::RunCrawler(RunCrawlerStatement { name }))
    }

    /// Parse: DROP CRAWLER <name>
    fn parse_drop_crawler(&mut self) -> Result<BeaconStatement> {
        self.df_parser.parser.next_token(); // DROP
        self.df_parser.parser.next_token(); // CRAWLER
        let name = self
            .df_parser
            .parser
            .parse_object_name(false)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(BeaconStatement::DropCrawler(DropCrawlerStatement { name }))
    }

    /// Parse: SHOW CRAWLERS
    fn parse_show_crawlers(&mut self) -> Result<BeaconStatement> {
        self.df_parser.parser.next_token(); // SHOW
        self.df_parser.parser.next_token(); // CRAWLERS
        Ok(BeaconStatement::ShowCrawlers)
    }

    fn is_create_index(&self) -> bool {
        let t1 = &self.df_parser.parser.peek_nth_token(0).token;
        let t2 = &self.df_parser.parser.peek_nth_token(1).token;
        matches!(t1, Token::Word(w) if w.keyword == Keyword::CREATE)
            && matches!(t2, Token::Word(w) if w.value.to_uppercase() == "INDEX")
    }

    fn is_drop_index(&self) -> bool {
        let t1 = &self.df_parser.parser.peek_nth_token(0).token;
        let t2 = &self.df_parser.parser.peek_nth_token(1).token;
        matches!(t1, Token::Word(w) if w.keyword == Keyword::DROP)
            && matches!(t2, Token::Word(w) if w.value.to_uppercase() == "INDEX")
    }

    fn is_show_indexes(&self) -> bool {
        let t1 = &self.df_parser.parser.peek_nth_token(0).token;
        let t2 = &self.df_parser.parser.peek_nth_token(1).token;
        matches!(t1, Token::Word(w) if w.value.to_uppercase() == "SHOW")
            && matches!(t2, Token::Word(w)
                if matches!(w.value.to_uppercase().as_str(), "INDEXES" | "INDEX" | "INDICES"))
    }

    /// Parse: CREATE INDEX [<name>] ON <table> (<column>) [USING <type>]
    fn parse_create_index(&mut self) -> Result<BeaconStatement> {
        self.df_parser.parser.next_token(); // CREATE
        self.df_parser.parser.next_token(); // INDEX

        // An index name is present unless the next token is `ON`.
        let name = if matches!(
            &self.df_parser.parser.peek_nth_token(0).token,
            Token::Word(w) if w.keyword == Keyword::ON
        ) {
            None
        } else {
            Some(
                self.df_parser
                    .parser
                    .parse_object_name(false)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?,
            )
        };

        self.df_parser
            .parser
            .expect_keyword(Keyword::ON)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let table = self
            .df_parser
            .parser
            .parse_object_name(false)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        self.df_parser
            .parser
            .expect_token(&Token::LParen)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let column = self.parse_string_value()?;
        self.df_parser
            .parser
            .expect_token(&Token::RParen)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let using = if matches!(
            &self.df_parser.parser.peek_nth_token(0).token,
            Token::Word(w) if w.keyword == Keyword::USING
        ) {
            self.df_parser.parser.next_token(); // USING
            Some(self.parse_string_value()?)
        } else {
            None
        };

        Ok(BeaconStatement::CreateIndex(CreateIndexStatement {
            name,
            table,
            column,
            using,
        }))
    }

    /// Parse: DROP INDEX <name> ON <table>
    fn parse_drop_index(&mut self) -> Result<BeaconStatement> {
        self.df_parser.parser.next_token(); // DROP
        self.df_parser.parser.next_token(); // INDEX
        let name = self
            .df_parser
            .parser
            .parse_object_name(false)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        self.df_parser
            .parser
            .expect_keyword(Keyword::ON)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let table = self
            .df_parser
            .parser
            .parse_object_name(false)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(BeaconStatement::DropIndex(DropIndexStatement { name, table }))
    }

    /// Parse: SHOW INDEXES [ON|FROM] <table>
    fn parse_show_indexes(&mut self) -> Result<BeaconStatement> {
        self.df_parser.parser.next_token(); // SHOW
        self.df_parser.parser.next_token(); // INDEXES

        // Optional `ON`/`FROM` before the table name.
        if matches!(
            &self.df_parser.parser.peek_nth_token(0).token,
            Token::Word(w) if w.keyword == Keyword::ON || w.keyword == Keyword::FROM
        ) {
            self.df_parser.parser.next_token();
        }

        let table = self
            .df_parser
            .parser
            .parse_object_name(false)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(BeaconStatement::ShowIndexes(ShowIndexesStatement { table }))
    }

    /// Read a single string value (single-quoted string, identifier, or number).
    fn parse_string_value(&mut self) -> Result<String> {
        let token = self.df_parser.parser.next_token();
        match token.token {
            Token::SingleQuotedString(s) => Ok(s),
            Token::DoubleQuotedString(s) => Ok(s),
            Token::Word(w) => Ok(w.value),
            Token::Number(n, _) => Ok(n),
            other => Err(DataFusionError::Plan(format!(
                "expected a string value, found {other}"
            ))),
        }
    }

    /// Parse `( key value, key value, ... )` into a map. Keys and values are
    /// string literals or bare words — the same shape as `CREATE EXTERNAL TABLE`'s
    /// `OPTIONS`.
    fn parse_with_options(&mut self) -> Result<HashMap<String, String>> {
        self.df_parser
            .parser
            .expect_token(&Token::LParen)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let mut options = HashMap::new();
        if self.df_parser.parser.consume_token(&Token::RParen) {
            return Ok(options);
        }

        loop {
            let key = self.parse_string_value()?;
            let value = self.parse_string_value()?;
            options.insert(key, value);

            if self.df_parser.parser.consume_token(&Token::Comma) {
                continue;
            }
            self.df_parser
                .parser
                .expect_token(&Token::RParen)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            break;
        }
        Ok(options)
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

    #[test]
    fn test_parse_create_crawler_full() {
        let sql = "CREATE CRAWLER argo ON 'argo/' WITH ('format' 'parquet', 'schedule' '15m')";
        let mut parser = BeaconParser::new(sql).unwrap();
        match parser.parse_statement().unwrap() {
            BeaconStatement::CreateCrawler(s) => {
                assert_eq!(s.name.to_string(), "argo");
                assert_eq!(s.target_prefix.as_deref(), Some("argo/"));
                assert_eq!(s.options.get("format").map(String::as_str), Some("parquet"));
                assert_eq!(s.options.get("schedule").map(String::as_str), Some("15m"));
            }
            other => panic!("expected CreateCrawler, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_create_crawler_minimal() {
        let sql = "CREATE CRAWLER c";
        let mut parser = BeaconParser::new(sql).unwrap();
        match parser.parse_statement().unwrap() {
            BeaconStatement::CreateCrawler(s) => {
                assert_eq!(s.name.to_string(), "c");
                assert!(s.target_prefix.is_none());
                assert!(s.options.is_empty());
            }
            other => panic!("expected CreateCrawler, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_run_and_drop_crawler() {
        let mut p = BeaconParser::new("RUN CRAWLER argo").unwrap();
        match p.parse_statement().unwrap() {
            BeaconStatement::RunCrawler(s) => assert_eq!(s.name.to_string(), "argo"),
            other => panic!("expected RunCrawler, got {other:?}"),
        }

        let mut p = BeaconParser::new("DROP CRAWLER argo").unwrap();
        match p.parse_statement().unwrap() {
            BeaconStatement::DropCrawler(s) => assert_eq!(s.name.to_string(), "argo"),
            other => panic!("expected DropCrawler, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_show_crawlers() {
        let mut p = BeaconParser::new("SHOW CRAWLERS").unwrap();
        assert!(matches!(
            p.parse_statement().unwrap(),
            BeaconStatement::ShowCrawlers
        ));
    }

    #[test]
    fn test_crawler_ddl_does_not_shadow_standard_sql() {
        // DROP TABLE / SHOW TABLES must still flow to the DataFusion parser.
        for sql in ["DROP TABLE t", "SHOW TABLES"] {
            let mut p = BeaconParser::new(sql).unwrap();
            assert!(matches!(
                p.parse_statement().unwrap(),
                BeaconStatement::DFStatement(_)
            ));
        }
    }

    #[test]
    fn test_create_crawler_display_roundtrip() {
        let sql = "CREATE CRAWLER argo ON 'argo/' WITH ('format' 'parquet')";
        let mut p = BeaconParser::new(sql).unwrap();
        let stmt = p.parse_statement().unwrap();
        assert_eq!(
            stmt.to_string(),
            "CREATE CRAWLER argo ON 'argo/' WITH ('format' 'parquet')"
        );
    }
}
