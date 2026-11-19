use std::collections::HashMap;

use datafusion::error::{DataFusionError, Result};
use datafusion::sql::{
    parser::{DFParser, DFParserBuilder},
    sqlparser::{keywords::Keyword, tokenizer::Token},
};

use super::statement::{
    AttachStatement, BeaconStatement, CompactTableStatement, CreateCrawlerStatement, CreateIndexStatement,
    CreateMaterializedViewStatement, CreateSecretStatement, DetachStatement, DropCrawlerStatement,
    DropIndexStatement, DropSecretStatement, RefreshStatement,
    AnalyzeFilesStatement, RunCrawlerStatement, ShowIndexesStatement,
    SummarizeStatement, InstallStatement, UninstallStatement,
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

    /// Parse the one statement of `sql` and refuse anything after it but semicolons.
    ///
    /// A request runs one statement, so a second one would otherwise not run and give no
    /// error.
    pub fn parse_single_statement(&mut self) -> Result<BeaconStatement> {
        let statement = self.parse_statement()?;
        let parser = &mut self.df_parser.parser;
        while parser.consume_token(&Token::SemiColon) {}
        let next = parser.peek_token();
        if next.token != Token::EOF {
            return Err(DataFusionError::Plan(format!(
                // The location renders with its own leading " at".
                "a request holds one SQL statement; found `{}`{} after it",
                next.token, next.span.start
            )));
        }
        Ok(statement)
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

        if self.is_compact_table() {
            return self.parse_compact_table();
        }

        if self.is_attach() {
            return self.parse_attach();
        }

        if self.is_detach() {
            return self.parse_detach();
        }

        if self.is_install() {
            return self.parse_install();
        }

        if self.is_uninstall() {
            return self.parse_uninstall();
        }

        if self.is_create_secret() {
            return self.parse_create_secret();
        }

        if self.is_drop_secret() {
            return self.parse_drop_secret();
        }

        if self.is_show_secrets() {
            return self.parse_show_secrets();
        }

        if self.is_summarize() {
            return self.parse_summarize();
        }

        if self.is_analyze_files() {
            return self.parse_analyze_files();
        }

        let df_statement = Box::new(self.df_parser.parse_statement()?);

        Ok(BeaconStatement::DFStatement(df_statement))
    }

    /// Whether the next two tokens are `ANALYZE FILES`.
    ///
    /// Both words are required. `ANALYZE` alone belongs to DataFusion, and
    /// taking it here would shadow it.
    fn is_analyze_files(&self) -> bool {
        let t1 = &self.df_parser.parser.peek_nth_token(0).token;
        let t2 = &self.df_parser.parser.peek_nth_token(1).token;
        matches!(t1, Token::Word(w) if w.value.to_uppercase() == "ANALYZE")
            && matches!(t2, Token::Word(w) if w.value.to_uppercase() == "FILES")
    }

    /// Parse: ANALYZE FILES [<table>]
    fn parse_analyze_files(&mut self) -> Result<BeaconStatement> {
        self.df_parser.parser.next_token(); // ANALYZE
        self.df_parser.parser.next_token(); // FILES

        let table = match &self.df_parser.parser.peek_token().token {
            Token::EOF | Token::SemiColon => None,
            _ => Some(self.parse_object_name()?),
        };

        Ok(BeaconStatement::AnalyzeFiles(AnalyzeFilesStatement {
            table,
        }))
    }

    /// Whether the next two tokens are `<KW1> CRAWLER`, where `KW1` matches `first`.
    fn is_keyword_then_crawler(&self, first: impl Fn(&Token) -> bool) -> bool {
        let t1 = &self.df_parser.parser.peek_nth_token(0).token;
        let t2 = &self.df_parser.parser.peek_nth_token(1).token;
        first(t1) && matches!(t2, Token::Word(w) if w.value.to_uppercase() == "CRAWLER")
    }

    fn is_create_crawler(&self) -> bool {
        let parser = &self.df_parser.parser;
        let word = |n: usize, value: &str| {
            matches!(&parser.peek_nth_token(n).token, Token::Word(w) if w.value.eq_ignore_ascii_case(value))
        };
        word(0, "CREATE") && (word(1, "CRAWLER") || (word(1, "OR") && word(2, "REPLACE") && word(3, "CRAWLER")))
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

    /// Parse: CREATE [OR REPLACE] CRAWLER <name> [ON '<prefix>'] [WITH (k 'v', ...)]
    fn parse_create_crawler(&mut self) -> Result<BeaconStatement> {
        self.df_parser.parser.next_token(); // CREATE
        let or_replace = self
            .df_parser
            .parser
            .parse_keywords(&[Keyword::OR, Keyword::REPLACE]);
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
            or_replace,
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

    /// Consume the expected keyword or error.
    fn expect_keyword(&mut self, keyword: Keyword) -> Result<()> {
        self.df_parser
            .parser
            .expect_keyword(keyword)
            .map(|_| ())
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    /// Parse a (possibly schema-qualified) object name.
    fn parse_object_name(&mut self) -> Result<datafusion::sql::sqlparser::ast::ObjectName> {
        self.df_parser
            .parser
            .parse_object_name(false)
            .map_err(|e| DataFusionError::External(Box::new(e)))
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

    /// Whether the next two tokens are `COMPACT TABLE`.
    ///
    /// Both words are required: `COMPACT` is not a reserved word, so a bare
    /// `COMPACT` would shadow any identifier a query starts with.
    fn is_compact_table(&self) -> bool {
        let t1 = &self.df_parser.parser.peek_nth_token(0).token;
        let t2 = &self.df_parser.parser.peek_nth_token(1).token;
        matches!(t1, Token::Word(w) if w.value.to_uppercase() == "COMPACT")
            && matches!(t2, Token::Word(w) if w.keyword == Keyword::TABLE)
    }

    /// Parse: COMPACT TABLE <table> [WITH (k 'v', ...)]
    fn parse_compact_table(&mut self) -> Result<BeaconStatement> {
        self.df_parser.parser.next_token(); // COMPACT
        self.df_parser.parser.next_token(); // TABLE

        let table = self.parse_object_name()?;

        let options = if matches!(
            &self.df_parser.parser.peek_nth_token(0).token,
            Token::Word(w) if w.keyword == Keyword::WITH
        ) {
            self.df_parser.parser.next_token(); // WITH
            self.parse_with_options()?
        } else {
            HashMap::new()
        };

        Ok(BeaconStatement::CompactTable(CompactTableStatement {
            table,
            options,
        }))
    }

    fn is_attach(&self) -> bool {
        matches!(&self.df_parser.parser.peek_nth_token(0).token,
            Token::Word(w) if w.value.to_uppercase() == "ATTACH")
    }

    fn is_detach(&self) -> bool {
        matches!(&self.df_parser.parser.peek_nth_token(0).token,
            Token::Word(w) if w.value.to_uppercase() == "DETACH")
    }

    /// Parse: ATTACH '<url>' AS <name> [WITH ('token' '<t>', 'tls' 'true')]
    fn parse_attach(&mut self) -> Result<BeaconStatement> {
        self.df_parser.parser.next_token(); // ATTACH
        let url = self.parse_string_value()?;
        self.expect_keyword(Keyword::AS)?;
        let name = self.parse_string_value()?;

        let options = if matches!(
            &self.df_parser.parser.peek_nth_token(0).token,
            Token::Word(w) if w.keyword == Keyword::WITH
        ) {
            self.df_parser.parser.next_token(); // WITH
            self.parse_with_options()?
        } else {
            HashMap::new()
        };

        Ok(BeaconStatement::Attach(AttachStatement { name, url, options }))
    }

    /// Parse: DETACH <name>
    fn parse_detach(&mut self) -> Result<BeaconStatement> {
        self.df_parser.parser.next_token(); // DETACH
        let name = self.parse_string_value()?;
        Ok(BeaconStatement::Detach(DetachStatement { name }))
    }

    fn is_install(&self) -> bool {
        matches!(&self.df_parser.parser.peek_nth_token(0).token,
            Token::Word(w) if w.value.eq_ignore_ascii_case("INSTALL"))
    }

    fn is_uninstall(&self) -> bool {
        matches!(&self.df_parser.parser.peek_nth_token(0).token,
            Token::Word(w) if w.value.eq_ignore_ascii_case("UNINSTALL"))
    }

    /// Parse: INSTALL <name> FROM '<source>'
    fn parse_install(&mut self) -> Result<BeaconStatement> {
        self.df_parser.parser.next_token(); // INSTALL
        let name = self.parse_name()?.to_lowercase();
        self.expect_keyword(Keyword::FROM)?;
        let source = self
            .df_parser
            .parser
            .parse_literal_string()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(BeaconStatement::Install(InstallStatement { name, source }))
    }

    /// Parse: UNINSTALL <name>
    fn parse_uninstall(&mut self) -> Result<BeaconStatement> {
        self.df_parser.parser.next_token(); // UNINSTALL
        let name = self.parse_name()?.to_lowercase();
        Ok(BeaconStatement::Uninstall(UninstallStatement { name }))
    }

    fn is_create_secret(&self) -> bool {
        let t1 = &self.df_parser.parser.peek_nth_token(0).token;
        let t2 = &self.df_parser.parser.peek_nth_token(1).token;
        let t3 = &self.df_parser.parser.peek_nth_token(2).token;
        let is_secret = |t: &Token| matches!(t, Token::Word(w) if w.value.to_uppercase() == "SECRET");
        let is_modifier = |t: &Token| {
            matches!(t, Token::Word(w) if matches!(w.value.to_uppercase().as_str(), "PERSISTENT" | "TEMPORARY"))
        };
        // CREATE SECRET … or CREATE {PERSISTENT|TEMPORARY} SECRET …
        matches!(t1, Token::Word(w) if w.keyword == Keyword::CREATE)
            && (is_secret(t2) || (is_modifier(t2) && is_secret(t3)))
    }

    fn is_drop_secret(&self) -> bool {
        let t1 = &self.df_parser.parser.peek_nth_token(0).token;
        let t2 = &self.df_parser.parser.peek_nth_token(1).token;
        matches!(t1, Token::Word(w) if w.keyword == Keyword::DROP)
            && matches!(t2, Token::Word(w) if w.value.to_uppercase() == "SECRET")
    }

    fn is_show_secrets(&self) -> bool {
        let t1 = &self.df_parser.parser.peek_nth_token(0).token;
        let t2 = &self.df_parser.parser.peek_nth_token(1).token;
        matches!(t1, Token::Word(w) if w.value.to_uppercase() == "SHOW")
            && matches!(t2, Token::Word(w) if w.value.to_uppercase() == "SECRETS")
    }

    /// Parse: CREATE [PERSISTENT|TEMPORARY] SECRET <name> (TYPE <type>, <key> '<value>', …)
    fn parse_create_secret(&mut self) -> Result<BeaconStatement> {
        self.df_parser.parser.next_token(); // CREATE
        // Optional PERSISTENT / TEMPORARY modifier (default: session-only).
        let persistent = match &self.df_parser.parser.peek_nth_token(0).token {
            Token::Word(w) if w.value.to_uppercase() == "PERSISTENT" => {
                self.df_parser.parser.next_token();
                true
            }
            Token::Word(w) if w.value.to_uppercase() == "TEMPORARY" => {
                self.df_parser.parser.next_token();
                false
            }
            _ => false,
        };
        self.df_parser.parser.next_token(); // SECRET
        let name = self.parse_string_value()?;
        let params = self.parse_with_options()?;
        Ok(BeaconStatement::CreateSecret(CreateSecretStatement {
            name,
            params,
            persistent,
        }))
    }

    /// Parse: DROP SECRET [IF EXISTS] <name>
    fn parse_drop_secret(&mut self) -> Result<BeaconStatement> {
        self.df_parser.parser.next_token(); // DROP
        self.df_parser.parser.next_token(); // SECRET
        let if_exists = self
            .df_parser
            .parser
            .parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let name = self.parse_string_value()?;
        Ok(BeaconStatement::DropSecret(DropSecretStatement {
            name,
            if_exists,
        }))
    }

    /// Parse: SHOW SECRETS
    fn parse_show_secrets(&mut self) -> Result<BeaconStatement> {
        self.df_parser.parser.next_token(); // SHOW
        self.df_parser.parser.next_token(); // SECRETS
        Ok(BeaconStatement::ShowSecrets)
    }

    fn is_summarize(&self) -> bool {
        matches!(&self.df_parser.parser.peek_nth_token(0).token,
            Token::Word(w) if w.value.to_uppercase() == "SUMMARIZE")
    }

    /// Parse: SUMMARIZE <table> | SUMMARIZE <query>
    ///
    /// A bare table name becomes `SELECT * FROM <name>`; anything starting a query (`SELECT`,
    /// `WITH`, `VALUES`, `TABLE`, or a parenthesized query) is taken as the source query verbatim.
    fn parse_summarize(&mut self) -> Result<BeaconStatement> {
        self.df_parser.parser.next_token(); // SUMMARIZE

        let starts_query = matches!(
            &self.df_parser.parser.peek_nth_token(0).token,
            Token::LParen
        ) || matches!(
            &self.df_parser.parser.peek_nth_token(0).token,
            Token::Word(w) if matches!(
                w.keyword,
                Keyword::SELECT | Keyword::WITH | Keyword::VALUES | Keyword::TABLE
            )
        );

        let source = if starts_query {
            self.df_parser
                .parser
                .parse_query()
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .to_string()
        } else {
            format!("SELECT * FROM {}", self.parse_object_name()?)
        };

        Ok(BeaconStatement::Summarize(SummarizeStatement { source }))
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

    /// Parse: REFRESH [TABLE | MATERIALIZED VIEW] <name>
    fn parse_refresh(&mut self) -> Result<BeaconStatement> {
        // Consume REFRESH
        self.df_parser.parser.next_token();

        // Optional TABLE, or the PostgreSQL spelling MATERIALIZED VIEW
        let parser = &mut self.df_parser.parser;
        if !parser.parse_keywords(&[Keyword::MATERIALIZED, Keyword::VIEW]) {
            parser.parse_keyword(Keyword::TABLE);
        }

        let name = self
            .df_parser
            .parser
            .parse_object_name(false)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(BeaconStatement::Refresh(RefreshStatement { name }))
    }

    /// Reads a single identifier as a string.
    fn parse_name(&mut self) -> Result<String> {
        self.df_parser
            .parser
            .parse_identifier()
            .map(|ident| ident.value)
            .map_err(|e| DataFusionError::External(Box::new(e)))
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

    /// A request runs one statement. A second one after the `;` is an error, not a
    /// statement that silently does not run.
    #[test]
    fn a_single_statement_refuses_a_second_one() {
        for sql in ["SELECT 1", "SELECT 1;", "SELECT 1 ; ;", "REFRESH my_table;"] {
            let mut parser = BeaconParser::new(sql).unwrap();
            assert!(parser.parse_single_statement().is_ok(), "{sql}");
        }
        for sql in ["SELECT 1; DROP TABLE x", "REFRESH a; REFRESH b", "SHOW CRAWLERS extra"] {
            let mut parser = BeaconParser::new(sql).unwrap();
            let err = parser.parse_single_statement().expect_err(sql);
            assert!(err.to_string().contains("one SQL statement"), "{sql}: {err}");
        }
    }

    #[test]
    fn test_parse_refresh_statement() {
        for sql in [
            "REFRESH my_table",
            "REFRESH TABLE my_table",
            "REFRESH MATERIALIZED VIEW my_table",
        ] {
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
    fn test_parse_compact_table() {
        let mut p = BeaconParser::new("COMPACT TABLE obs").unwrap();
        match p.parse_statement().unwrap() {
            BeaconStatement::CompactTable(s) => {
                assert_eq!(s.table.to_string(), "obs");
                assert!(s.options.is_empty());
            }
            other => panic!("expected CompactTable, got {other:?}"),
        }

        let sql = "COMPACT TABLE schema.obs WITH ('target_rows_per_fragment' '500000', 'cleanup_older_than' '0s')";
        let mut p = BeaconParser::new(sql).unwrap();
        match p.parse_statement().unwrap() {
            BeaconStatement::CompactTable(s) => {
                assert_eq!(s.table.to_string(), "schema.obs");
                assert_eq!(
                    s.options.get("target_rows_per_fragment").map(String::as_str),
                    Some("500000")
                );
                assert_eq!(
                    s.options.get("cleanup_older_than").map(String::as_str),
                    Some("0s")
                );
            }
            other => panic!("expected CompactTable, got {other:?}"),
        }
    }

    #[test]
    fn test_compact_table_display_roundtrip() {
        let sql = "COMPACT TABLE obs WITH ('cleanup_older_than' '7d')";
        let stmt = BeaconParser::new(sql).unwrap().parse_statement().unwrap();
        assert_eq!(stmt.to_string(), sql);
    }

    /// `COMPACT` is not a reserved word: only `COMPACT TABLE` is beacon's, and a
    /// query over a column or table called `compact` must still reach DataFusion.
    #[test]
    fn test_compact_does_not_shadow_standard_sql() {
        for sql in ["SELECT compact FROM t", "SELECT * FROM compact"] {
            let mut p = BeaconParser::new(sql).unwrap();
            assert!(
                matches!(p.parse_statement().unwrap(), BeaconStatement::DFStatement(_)),
                "`{sql}` should be a DataFusion statement"
            );
        }
    }

    #[test]
    fn test_standard_set_drop_show_and_comment_reach_datafusion() {
        for sql in [
            "SET timezone = 'UTC'",
            "DROP TABLE t",
            "SHOW TABLES",
            "COMMENT ON TABLE t IS 'x'",
            "COMMENT ON COLUMN t.c IS NULL",
        ] {
            let mut p = BeaconParser::new(sql).unwrap();
            assert!(
                matches!(p.parse_statement().unwrap(), BeaconStatement::DFStatement(_)),
                "`{sql}` should be a DataFusion statement"
            );
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

    #[test]
    fn test_parse_attach_with_options() {
        let sql = "ATTACH 'beacon://host:50051' AS lake WITH ('token' 'secret', 'tls' 'true')";
        let stmt = BeaconParser::new(sql).unwrap().parse_statement().unwrap();
        match stmt {
            BeaconStatement::Attach(attach) => {
                assert_eq!(attach.name, "lake");
                assert_eq!(attach.url, "beacon://host:50051");
                assert_eq!(attach.options.get("token").map(String::as_str), Some("secret"));
                assert_eq!(attach.options.get("tls").map(String::as_str), Some("true"));
            }
            other => panic!("expected ATTACH, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_attach_and_detach_bare() {
        match BeaconParser::new("ATTACH 'beacon://h:1' AS r")
            .unwrap()
            .parse_statement()
            .unwrap()
        {
            BeaconStatement::Attach(a) => {
                assert_eq!((a.name.as_str(), a.url.as_str()), ("r", "beacon://h:1"));
                assert!(a.options.is_empty());
            }
            other => panic!("expected ATTACH, got {other:?}"),
        }
        match BeaconParser::new("DETACH lake").unwrap().parse_statement().unwrap() {
            BeaconStatement::Detach(d) => assert_eq!(d.name, "lake"),
            other => panic!("expected DETACH, got {other:?}"),
        }
    }

    /// The rendered form must not leak the token (it is a credential).
    #[test]
    fn test_attach_display_omits_token() {
        let sql = "ATTACH 'beacon://h:1' AS r WITH ('token' 'secret')";
        let stmt = BeaconParser::new(sql).unwrap().parse_statement().unwrap();
        let rendered = stmt.to_string();
        assert_eq!(rendered, "ATTACH 'beacon://h:1' AS r");
        assert!(!rendered.contains("secret"));
    }

    #[test]
    fn test_parse_create_secret() {
        let sql = "CREATE SECRET my_s3 (TYPE S3, KEY_ID 'AKIA', SECRET 'shh', REGION 'eu-west-1', SCOPE 's3://bucket')";
        match BeaconParser::new(sql).unwrap().parse_statement().unwrap() {
            BeaconStatement::CreateSecret(s) => {
                assert_eq!(s.name, "my_s3");
                assert_eq!(s.params.get("TYPE").map(String::as_str), Some("S3"));
                assert_eq!(s.params.get("KEY_ID").map(String::as_str), Some("AKIA"));
                assert_eq!(s.params.get("SCOPE").map(String::as_str), Some("s3://bucket"));
            }
            other => panic!("expected CREATE SECRET, got {other:?}"),
        }
    }

    /// The rendered form must not leak credential values.
    #[test]
    fn test_create_secret_display_omits_values() {
        let sql = "CREATE SECRET s (TYPE S3, KEY_ID 'AKIA', SECRET 'topsecret')";
        let rendered = BeaconParser::new(sql)
            .unwrap()
            .parse_statement()
            .unwrap()
            .to_string();
        assert_eq!(rendered, "CREATE SECRET s");
        assert!(!rendered.contains("topsecret"));
    }

    #[test]
    fn test_parse_summarize() {
        // A bare table name is wrapped as SELECT * FROM <name>.
        match BeaconParser::new("SUMMARIZE obs").unwrap().parse_statement().unwrap() {
            BeaconStatement::Summarize(s) => assert_eq!(s.source, "SELECT * FROM obs"),
            other => panic!("expected SUMMARIZE, got {other:?}"),
        }
        // A schema-qualified name too.
        match BeaconParser::new("SUMMARIZE public.obs").unwrap().parse_statement().unwrap() {
            BeaconStatement::Summarize(s) => assert_eq!(s.source, "SELECT * FROM public.obs"),
            other => panic!("expected SUMMARIZE, got {other:?}"),
        }
        // A query source is captured as-is.
        match BeaconParser::new("SUMMARIZE SELECT a FROM t WHERE a > 0")
            .unwrap()
            .parse_statement()
            .unwrap()
        {
            BeaconStatement::Summarize(s) => assert!(s.source.contains("SELECT a FROM t")),
            other => panic!("expected SUMMARIZE, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_persistent_and_temporary_secret() {
        let persistent = BeaconParser::new("CREATE PERSISTENT SECRET p (TYPE S3)")
            .unwrap()
            .parse_statement()
            .unwrap();
        match persistent {
            BeaconStatement::CreateSecret(s) => {
                assert_eq!(s.name, "p");
                assert!(s.persistent);
            }
            other => panic!("expected CREATE SECRET, got {other:?}"),
        }
        // TEMPORARY (and the bare form) are session-only.
        for sql in ["CREATE TEMPORARY SECRET t (TYPE S3)", "CREATE SECRET t (TYPE S3)"] {
            match BeaconParser::new(sql).unwrap().parse_statement().unwrap() {
                BeaconStatement::CreateSecret(s) => assert!(!s.persistent, "{sql}"),
                other => panic!("expected CREATE SECRET for `{sql}`, got {other:?}"),
            }
        }
    }

    #[test]
    fn test_parse_drop_and_show_secrets() {
        match BeaconParser::new("DROP SECRET IF EXISTS s")
            .unwrap()
            .parse_statement()
            .unwrap()
        {
            BeaconStatement::DropSecret(s) => {
                assert_eq!(s.name, "s");
                assert!(s.if_exists);
            }
            other => panic!("expected DROP SECRET, got {other:?}"),
        }
        match BeaconParser::new("DROP SECRET s").unwrap().parse_statement().unwrap() {
            BeaconStatement::DropSecret(s) => assert!(!s.if_exists),
            other => panic!("expected DROP SECRET, got {other:?}"),
        }
        assert!(matches!(
            BeaconParser::new("SHOW SECRETS").unwrap().parse_statement().unwrap(),
            BeaconStatement::ShowSecrets
        ));
    }

    #[test]
    fn parses_install() {
        let sql = "INSTALL Example FROM 'file:///x'";
        match BeaconParser::new(sql).unwrap().parse_single_statement().unwrap() {
            BeaconStatement::Install(s) => {
                assert_eq!((s.name.as_str(), s.source.as_str()), ("example", "file:///x"));
            }
            other => panic!("expected INSTALL, got {other:?}"),
        }
    }

    #[test]
    fn parses_uninstall() {
        match BeaconParser::new("UNINSTALL Example")
            .unwrap()
            .parse_single_statement()
            .unwrap()
        {
            BeaconStatement::Uninstall(s) => assert_eq!(s.name, "example"),
            other => panic!("expected UNINSTALL, got {other:?}"),
        }
    }

    #[test]
    fn install_without_from_fails() {
        assert!(BeaconParser::new("INSTALL example 'file:///x'")
            .unwrap()
            .parse_single_statement()
            .is_err());
    }
}
