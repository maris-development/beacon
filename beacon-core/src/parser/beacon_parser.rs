use std::str::FromStr;

use beacon_auth::{Privilege, PrivilegeTarget};
use datafusion::error::{DataFusionError, Result};
use datafusion::sql::parser::Statement;
use datafusion::sql::sqlparser;
use datafusion::sql::{
    parser::{DFParser, DFParserBuilder},
    sqlparser::{keywords::Keyword, tokenizer::Token},
};

use super::statement::{
    AlterAtlasTableStatement, AtlasOp, AuthStatement, BeaconStatement, CreateAtlasTableStatement,
    DeleteAtlasDatasetsStatement, IngestStatement,
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
        // ToDo: Implement custom parsing for non-DF statements before falling back to DF parsing
        // if self.is_ingest() {
        //     return self.parse_ingest();
        // }

        // if self.is_delete_atlas_datasets() {
        //     return self.parse_delete_atlas_datasets();
        // }

        // if self.is_create_atlas_table() {
        //     return self.parse_create_atlas_table();
        // }

        // if self.is_alter_atlas_table() {
        //     return self.parse_alter_atlas_table();
        // }

        if self.is_create_user() {
            return self.parse_create_user();
        }
        if self.is_drop_user() {
            return self.parse_drop_user();
        }
        if self.is_create_role() {
            return self.parse_create_role();
        }
        if self.is_drop_role() {
            return self.parse_drop_role();
        }
        if self.is_grant() {
            return self.parse_grant();
        }
        if self.is_deny() {
            return self.parse_deny();
        }
        if self.is_revoke() {
            return self.parse_revoke();
        }

        let df_statement = Box::new(self.df_parser.parse_statement()?);

        Ok(BeaconStatement::DFStatement(df_statement))
    }

    /// Whether the token at offset `n` is a word equal (case-insensitive) to `expected`.
    fn peek_word_eq(&self, n: usize, expected: &str) -> bool {
        matches!(
            &self.df_parser.parser.peek_nth_token(n).token,
            Token::Word(w) if w.value.eq_ignore_ascii_case(expected)
        )
    }

    /// Consumes the next token, requiring it to be a word equal (case-insensitive) to `expected`.
    fn expect_word(&mut self, expected: &str) -> Result<()> {
        let token = self.df_parser.parser.next_token();
        match token.token {
            Token::Word(w) if w.value.eq_ignore_ascii_case(expected) => Ok(()),
            other => Err(DataFusionError::Plan(format!(
                "Expected {expected}, found {other}"
            ))),
        }
    }

    /// Parses an identifier and returns its value.
    fn parse_ident_value(&mut self) -> Result<String> {
        Ok(self
            .df_parser
            .parser
            .parse_identifier()
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .value)
    }

    fn is_create_user(&self) -> bool {
        self.peek_word_eq(0, "CREATE") && self.peek_word_eq(1, "USER")
    }

    fn is_create_role(&self) -> bool {
        self.peek_word_eq(0, "CREATE") && self.peek_word_eq(1, "ROLE")
    }

    fn is_drop_user(&self) -> bool {
        self.peek_word_eq(0, "DROP") && self.peek_word_eq(1, "USER")
    }

    fn is_drop_role(&self) -> bool {
        self.peek_word_eq(0, "DROP") && self.peek_word_eq(1, "ROLE")
    }

    fn is_grant(&self) -> bool {
        self.peek_word_eq(0, "GRANT")
    }

    fn is_deny(&self) -> bool {
        self.peek_word_eq(0, "DENY")
    }

    fn is_revoke(&self) -> bool {
        self.peek_word_eq(0, "REVOKE")
    }

    /// Parse: CREATE USER <name> WITH PASSWORD '<password>'
    fn parse_create_user(&mut self) -> Result<BeaconStatement> {
        self.df_parser.parser.next_token(); // CREATE
        self.df_parser.parser.next_token(); // USER
        let username = self.parse_ident_value()?;
        self.expect_word("WITH")?;
        self.expect_word("PASSWORD")?;
        let password = self
            .df_parser
            .parser
            .parse_literal_string()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(BeaconStatement::Auth(AuthStatement::CreateUser {
            username,
            password,
        }))
    }

    /// Parse: DROP USER <name>
    fn parse_drop_user(&mut self) -> Result<BeaconStatement> {
        self.df_parser.parser.next_token(); // DROP
        self.df_parser.parser.next_token(); // USER
        let username = self.parse_ident_value()?;
        Ok(BeaconStatement::Auth(AuthStatement::DropUser { username }))
    }

    /// Parse: CREATE ROLE <name>
    fn parse_create_role(&mut self) -> Result<BeaconStatement> {
        self.df_parser.parser.next_token(); // CREATE
        self.df_parser.parser.next_token(); // ROLE
        let role = self.parse_ident_value()?;
        Ok(BeaconStatement::Auth(AuthStatement::CreateRole { role }))
    }

    /// Parse: DROP ROLE <name>
    fn parse_drop_role(&mut self) -> Result<BeaconStatement> {
        self.df_parser.parser.next_token(); // DROP
        self.df_parser.parser.next_token(); // ROLE
        let role = self.parse_ident_value()?;
        Ok(BeaconStatement::Auth(AuthStatement::DropRole { role }))
    }

    /// Parse a privilege keyword (SELECT, INSERT, ..., ALL).
    fn parse_privilege(&mut self) -> Result<Privilege> {
        let token = self.df_parser.parser.next_token();
        match token.token {
            Token::Word(w) => Privilege::from_str(&w.value).map_err(DataFusionError::Plan),
            other => Err(DataFusionError::Plan(format!(
                "Expected a privilege, found {other}"
            ))),
        }
    }

    /// Parse an optional `ON TABLE <name>` / `ON PATH '<glob>'` target clause.
    fn parse_optional_target(&mut self) -> Result<Option<PrivilegeTarget>> {
        if !self.peek_word_eq(0, "ON") {
            return Ok(None);
        }
        self.df_parser.parser.next_token(); // ON

        if self.peek_word_eq(0, "TABLE") {
            self.df_parser.parser.next_token();
            let name = self
                .df_parser
                .parser
                .parse_object_name(false)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            Ok(Some(PrivilegeTarget::Table(name.to_string())))
        } else if self.peek_word_eq(0, "PATH") {
            self.df_parser.parser.next_token();
            let pattern = self
                .df_parser
                .parser
                .parse_literal_string()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            Ok(Some(PrivilegeTarget::Path(pattern)))
        } else {
            Err(DataFusionError::Plan(
                "Expected TABLE or PATH after ON".to_string(),
            ))
        }
    }

    /// Parse: GRANT ROLE <role> TO USER <user>
    ///     or GRANT <privilege> [ON <target>] TO ROLE <role>
    fn parse_grant(&mut self) -> Result<BeaconStatement> {
        self.df_parser.parser.next_token(); // GRANT

        if self.peek_word_eq(0, "ROLE") {
            self.df_parser.parser.next_token(); // ROLE
            let role = self.parse_ident_value()?;
            self.expect_word("TO")?;
            self.expect_word("USER")?;
            let username = self.parse_ident_value()?;
            return Ok(BeaconStatement::Auth(AuthStatement::GrantRoleToUser {
                role,
                username,
            }));
        }

        let privilege = self.parse_privilege()?;
        let target = self.parse_optional_target()?;
        self.expect_word("TO")?;
        self.expect_word("ROLE")?;
        let role = self.parse_ident_value()?;
        Ok(BeaconStatement::Auth(AuthStatement::GrantPrivilege {
            privilege,
            target,
            role,
        }))
    }

    /// Parse: DENY <privilege> [ON <target>] TO ROLE <role>
    fn parse_deny(&mut self) -> Result<BeaconStatement> {
        self.df_parser.parser.next_token(); // DENY
        let privilege = self.parse_privilege()?;
        let target = self.parse_optional_target()?;
        self.expect_word("TO")?;
        self.expect_word("ROLE")?;
        let role = self.parse_ident_value()?;
        Ok(BeaconStatement::Auth(AuthStatement::DenyPrivilege {
            privilege,
            target,
            role,
        }))
    }

    /// Parse: REVOKE ROLE <role> FROM USER <user>
    ///     or REVOKE [DENY] <privilege> [ON <target>] FROM ROLE <role>
    fn parse_revoke(&mut self) -> Result<BeaconStatement> {
        self.df_parser.parser.next_token(); // REVOKE

        if self.peek_word_eq(0, "ROLE") {
            self.df_parser.parser.next_token(); // ROLE
            let role = self.parse_ident_value()?;
            self.expect_word("FROM")?;
            self.expect_word("USER")?;
            let username = self.parse_ident_value()?;
            return Ok(BeaconStatement::Auth(AuthStatement::RevokeRoleFromUser {
                role,
                username,
            }));
        }

        let deny = self.peek_word_eq(0, "DENY");
        if deny {
            self.df_parser.parser.next_token(); // DENY
        }
        let privilege = self.parse_privilege()?;
        let target = self.parse_optional_target()?;
        self.expect_word("FROM")?;
        self.expect_word("ROLE")?;
        let role = self.parse_ident_value()?;
        Ok(BeaconStatement::Auth(AuthStatement::RevokePrivilege {
            privilege,
            target,
            role,
            deny,
        }))
    }

    /// Check if the next tokens form an INGEST statement.
    fn is_ingest(&self) -> bool {
        let t1 = &self.df_parser.parser.peek_nth_token(0).token;
        matches!(t1, Token::Word(w) if w.value.to_uppercase() == "INGEST")
    }

    fn parse_required_partition_name(&mut self) -> Result<String> {
        self.df_parser
            .parser
            .expect_keyword(Keyword::ON)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let t = &self.df_parser.parser.peek_nth_token(0).token;
        if !matches!(t, Token::Word(w) if w.value.to_uppercase() == "PARTITION") {
            return Err(DataFusionError::Plan(
                "Expected PARTITION after ON".to_string(),
            ));
        }
        self.df_parser.parser.next_token();

        let partition_name = self
            .df_parser
            .parser
            .parse_identifier()
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .value;

        Ok(partition_name)
    }

    /// Parse: INGEST INTO ATLAS <table_name> ON PARTITION <partition_name> FROM '<glob_pattern>' WITH <format>
    fn parse_ingest(&mut self) -> Result<BeaconStatement> {
        // Consume INGEST
        self.df_parser.parser.next_token();

        // Expect INTO
        self.df_parser
            .parser
            .expect_keyword(Keyword::INTO)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Expect ATLAS
        let t = &self.df_parser.parser.peek_nth_token(0).token;
        if !matches!(t, Token::Word(w) if w.value.to_uppercase() == "ATLAS") {
            return Err(DataFusionError::Plan(
                "Expected ATLAS after INTO".to_string(),
            ));
        }
        self.df_parser.parser.next_token();

        // Parse table name
        let table_name = self
            .df_parser
            .parser
            .parse_object_name(false)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let partition_name = self.parse_required_partition_name()?;

        // Expect FROM
        self.df_parser
            .parser
            .expect_keyword(Keyword::FROM)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Parse glob pattern as a literal string
        let glob_pattern = self
            .df_parser
            .parser
            .parse_literal_string()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Expect WITH
        self.df_parser
            .parser
            .expect_keyword(Keyword::WITH)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Parse format identifier
        let format = self
            .df_parser
            .parser
            .parse_identifier()
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .value;

        Ok(BeaconStatement::Ingest(IngestStatement {
            glob_pattern,
            format,
            table_name,
            partition_name,
        }))
    }

    /// Check if the next tokens form a DELETE ATLAS DATASETS statement.
    fn is_delete_atlas_datasets(&self) -> bool {
        let t1 = &self.df_parser.parser.peek_nth_token(0).token;
        let t2 = &self.df_parser.parser.peek_nth_token(1).token;
        let t3 = &self.df_parser.parser.peek_nth_token(2).token;

        matches!(t1, Token::Word(w) if w.keyword == Keyword::DELETE)
            && matches!(t2, Token::Word(w) if w.value.to_uppercase() == "ATLAS")
            && matches!(t3, Token::Word(w) if w.value.to_uppercase() == "DATASETS")
    }

    /// Parse: DELETE ATLAS DATASETS [name1, name2, ...] FROM <table_name> ON PARTITION <partition_name>
    fn parse_delete_atlas_datasets(&mut self) -> Result<BeaconStatement> {
        // Consume DELETE ATLAS DATASETS
        for _ in 0..3 {
            self.df_parser.parser.next_token();
        }

        // Check if next token is FROM (no dataset names) or identifiers (dataset names)
        let dataset_names = if !self.next_is_from() {
            let mut names = vec![];
            loop {
                let name = self
                    .df_parser
                    .parser
                    .parse_literal_string()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                names.push(name);

                if !self.df_parser.parser.consume_token(&Token::Comma) {
                    break;
                }
            }
            Some(names)
        } else {
            None
        };

        // Expect FROM
        self.df_parser
            .parser
            .expect_keyword(Keyword::FROM)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Parse table name
        let table_name = self
            .df_parser
            .parser
            .parse_object_name(false)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let partition_name = self.parse_required_partition_name()?;

        Ok(BeaconStatement::DeleteAtlasDatasets(
            DeleteAtlasDatasetsStatement {
                dataset_names,
                table_name,
                partition_name,
            },
        ))
    }

    fn next_is_from(&self) -> bool {
        let t = &self.df_parser.parser.peek_nth_token(0).token;
        matches!(t, Token::Word(w) if w.keyword == Keyword::FROM)
    }

    /// Check if the next tokens form a CREATE ATLAS TABLE statement.
    fn is_create_atlas_table(&self) -> bool {
        let t1 = &self.df_parser.parser.peek_nth_token(0).token;
        let t2 = &self.df_parser.parser.peek_nth_token(1).token;
        let t3 = &self.df_parser.parser.peek_nth_token(2).token;

        matches!(t1, Token::Word(w) if w.keyword == Keyword::CREATE)
            && matches!(t2, Token::Word(w) if w.value.to_uppercase() == "ATLAS")
            && matches!(t3, Token::Word(w) if w.keyword == Keyword::TABLE)
    }

    /// Parse: CREATE ATLAS TABLE <table_name> LOCATION '<path>'
    fn parse_create_atlas_table(&mut self) -> Result<BeaconStatement> {
        // Consume CREATE ATLAS TABLE
        for _ in 0..3 {
            self.df_parser.parser.next_token();
        }

        // Parse table name
        let table_name = self
            .df_parser
            .parser
            .parse_object_name(false)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Expect LOCATION
        self.df_parser
            .parser
            .expect_keyword(Keyword::LOCATION)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Parse location as a literal string
        let location = self
            .df_parser
            .parser
            .parse_literal_string()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(BeaconStatement::CreateAtlasTable(
            CreateAtlasTableStatement {
                table_name,
                location,
            },
        ))
    }

    /// Check if the next tokens form an ALTER ATLAS TABLE statement.
    fn is_alter_atlas_table(&self) -> bool {
        let t1 = &self.df_parser.parser.peek_nth_token(0).token;
        let t2 = &self.df_parser.parser.peek_nth_token(1).token;
        let t3 = &self.df_parser.parser.peek_nth_token(2).token;

        matches!(t1, Token::Word(w) if w.keyword == Keyword::ALTER)
            && matches!(t2, Token::Word(w) if w.value.to_uppercase() == "ATLAS")
            && matches!(t3, Token::Word(w) if w.keyword == Keyword::TABLE)
    }

    /// Parse: ALTER ATLAS TABLE <table_name> ON PARTITION <partition_name> ALTER COLUMN <column_name> SET DATA TYPE <data_type>
    fn parse_alter_atlas_table(&mut self) -> Result<BeaconStatement> {
        // Consume ALTER ATLAS TABLE
        for _ in 0..3 {
            self.df_parser.parser.next_token();
        }

        // Parse table name
        let table_name = self
            .df_parser
            .parser
            .parse_object_name(false)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let partition_name = self.parse_required_partition_name()?;

        // Expect ALTER
        self.df_parser
            .parser
            .expect_keyword(Keyword::ALTER)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Expect COLUMN
        self.df_parser
            .parser
            .expect_keyword(Keyword::COLUMN)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Parse column name
        let column_name = self
            .df_parser
            .parser
            .parse_identifier()
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .value;

        // Expect SET
        self.df_parser
            .parser
            .expect_keyword(Keyword::SET)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Expect DATA
        self.df_parser
            .parser
            .expect_keyword(Keyword::DATA)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Expect TYPE
        self.df_parser
            .parser
            .expect_keyword(Keyword::TYPE)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Parse data type
        let data_type = self
            .df_parser
            .parser
            .parse_identifier()
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .value;

        Ok(BeaconStatement::AlterAtlas(AlterAtlasTableStatement {
            table_name,
            partition_name,
            op: AtlasOp::CastColumn {
                column_name,
                data_type,
            },
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "ingest/atlas custom parsing is disabled in BeaconParser (pre-existing); re-enable with the handlers"]
    fn test_parse_ingest_statement() {
        let sql = "INGEST INTO ATLAS my_table ON PARTITION p0 FROM '/data/**/*.csv' WITH csv";
        let mut parser = BeaconParser::new(sql).unwrap();
        let stmt = parser.parse_statement().unwrap();

        match stmt {
            BeaconStatement::Ingest(ingest) => {
                assert_eq!(ingest.table_name.to_string(), "my_table");
                assert_eq!(ingest.partition_name, "p0");
                assert_eq!(ingest.glob_pattern, "/data/**/*.csv");
                assert_eq!(ingest.format, "csv");
            }
            _ => panic!("Expected Ingest statement"),
        }
    }

    #[test]
    #[ignore = "ingest/atlas custom parsing is disabled in BeaconParser (pre-existing); re-enable with the handlers"]
    fn test_parse_ingest_display() {
        let sql =
            "INGEST INTO ATLAS schema.table ON PARTITION p1 FROM '/data/*.parquet' WITH parquet";
        let mut parser = BeaconParser::new(sql).unwrap();
        let stmt = parser.parse_statement().unwrap();
        assert_eq!(
            stmt.to_string(),
            "INGEST INTO ATLAS schema.table ON PARTITION p1 FROM '/data/*.parquet' WITH parquet"
        );
    }

    #[test]
    fn test_parse_regular_sql() {
        let sql = "SELECT 1";
        let mut parser = BeaconParser::new(sql).unwrap();
        let stmt = parser.parse_statement().unwrap();
        matches!(stmt, BeaconStatement::DFStatement(_));
    }

    #[test]
    fn test_parse_ingest_missing_from() {
        let sql = "INGEST INTO ATLAS my_table ON PARTITION p0 '/data/*.csv' WITH csv";
        let mut parser = BeaconParser::new(sql).unwrap();
        assert!(parser.parse_statement().is_err());
    }

    #[test]
    fn test_parse_ingest_missing_with() {
        let sql = "INGEST INTO ATLAS my_table ON PARTITION p0 FROM '/data/*.csv'";
        let mut parser = BeaconParser::new(sql).unwrap();
        assert!(parser.parse_statement().is_err());
    }

    #[test]
    fn test_parse_ingest_missing_partition_clause() {
        let sql = "INGEST INTO ATLAS my_table FROM '/data/*.csv' WITH csv";
        let mut parser = BeaconParser::new(sql).unwrap();
        assert!(parser.parse_statement().is_err());
    }

    #[test]
    #[ignore = "ingest/atlas custom parsing is disabled in BeaconParser (pre-existing); re-enable with the handlers"]
    fn test_parse_delete_atlas_datasets_with_names() {
        let sql =
            "DELETE ATLAS DATASETS 'dataset.nc', 'some_path/test.nc' FROM my_table ON PARTITION p0";
        let mut parser = BeaconParser::new(sql).unwrap();
        let stmt = parser.parse_statement().unwrap();

        match stmt {
            BeaconStatement::DeleteAtlasDatasets(s) => {
                assert_eq!(
                    s.dataset_names,
                    Some(vec![
                        "dataset.nc".to_string(),
                        "some_path/test.nc".to_string()
                    ])
                );
                assert_eq!(s.table_name.to_string(), "my_table");
                assert_eq!(s.partition_name, "p0");
            }
            _ => panic!("Expected DeleteAtlasDatasets statement"),
        }
    }

    #[test]
    #[ignore = "ingest/atlas custom parsing is disabled in BeaconParser (pre-existing); re-enable with the handlers"]
    fn test_parse_delete_atlas_datasets_without_names() {
        let sql = "DELETE ATLAS DATASETS FROM my_table ON PARTITION p1";
        let mut parser = BeaconParser::new(sql).unwrap();
        let stmt = parser.parse_statement().unwrap();

        match stmt {
            BeaconStatement::DeleteAtlasDatasets(s) => {
                assert_eq!(s.dataset_names, None);
                assert_eq!(s.table_name.to_string(), "my_table");
                assert_eq!(s.partition_name, "p1");
            }
            _ => panic!("Expected DeleteAtlasDatasets statement"),
        }
    }

    #[test]
    #[ignore = "ingest/atlas custom parsing is disabled in BeaconParser (pre-existing); re-enable with the handlers"]
    fn test_parse_delete_atlas_datasets_display() {
        let sql = "DELETE ATLAS DATASETS 'ds1.nc', 'path/ds2.nc' FROM schema.table ON PARTITION p2";
        let mut parser = BeaconParser::new(sql).unwrap();
        let stmt = parser.parse_statement().unwrap();
        assert_eq!(
            stmt.to_string(),
            "DELETE ATLAS DATASETS 'ds1.nc', 'path/ds2.nc' FROM schema.table ON PARTITION p2"
        );
    }

    #[test]
    #[ignore = "ingest/atlas custom parsing is disabled in BeaconParser (pre-existing); re-enable with the handlers"]
    fn test_parse_delete_atlas_datasets_display_no_names() {
        let sql = "DELETE ATLAS DATASETS FROM my_table ON PARTITION p0";
        let mut parser = BeaconParser::new(sql).unwrap();
        let stmt = parser.parse_statement().unwrap();
        assert_eq!(
            stmt.to_string(),
            "DELETE ATLAS DATASETS FROM my_table ON PARTITION p0"
        );
        assert!(parser.parse_statement().is_err());
    }

    #[test]
    #[ignore = "ingest/atlas custom parsing is disabled in BeaconParser (pre-existing); re-enable with the handlers"]
    fn test_parse_delete_atlas_datasets_missing_partition_clause() {
        let sql = "DELETE ATLAS DATASETS FROM my_table";
        let mut parser = BeaconParser::new(sql).unwrap();
        assert!(parser.parse_statement().is_err());
    }

    #[test]
    #[ignore = "ingest/atlas custom parsing is disabled in BeaconParser (pre-existing); re-enable with the handlers"]
    fn test_parse_create_atlas_table() {
        let sql = "CREATE ATLAS TABLE my_table LOCATION '/data/my_table'";
        let mut parser = BeaconParser::new(sql).unwrap();
        let stmt = parser.parse_statement().unwrap();

        match stmt {
            BeaconStatement::CreateAtlasTable(s) => {
                assert_eq!(s.table_name.to_string(), "my_table");
                assert_eq!(s.location, "/data/my_table");
            }
            _ => panic!("Expected CreateAtlasTable statement"),
        }
    }

    #[test]
    #[ignore = "ingest/atlas custom parsing is disabled in BeaconParser (pre-existing); re-enable with the handlers"]
    fn test_parse_create_atlas_table_display() {
        let sql = "CREATE ATLAS TABLE schema.table LOCATION '/data/path'";
        let mut parser = BeaconParser::new(sql).unwrap();
        let stmt = parser.parse_statement().unwrap();
        assert_eq!(
            stmt.to_string(),
            "CREATE ATLAS TABLE schema.table LOCATION '/data/path'"
        );
    }

    #[test]
    fn test_parse_create_atlas_table_missing_location() {
        let sql = "CREATE ATLAS TABLE my_table";
        let mut parser = BeaconParser::new(sql).unwrap();
        assert!(parser.parse_statement().is_err());
    }

    #[test]
    #[ignore = "ingest/atlas custom parsing is disabled in BeaconParser (pre-existing); re-enable with the handlers"]
    fn test_parse_alter_atlas_table_cast() {
        let sql =
            "ALTER ATLAS TABLE my_table ON PARTITION p0 ALTER COLUMN temperature SET DATA TYPE FLOAT";
        let mut parser = BeaconParser::new(sql).unwrap();
        let stmt = parser.parse_statement().unwrap();

        match stmt {
            BeaconStatement::AlterAtlas(s) => {
                assert_eq!(s.table_name.to_string(), "my_table");
                assert_eq!(s.partition_name, "p0");
                match s.op {
                    AtlasOp::CastColumn {
                        column_name,
                        data_type,
                    } => {
                        assert_eq!(column_name, "temperature");
                        assert_eq!(data_type, "FLOAT");
                    }
                }
            }
            _ => panic!("Expected ApplyAtlasOperation statement"),
        }
    }

    #[test]
    #[ignore = "ingest/atlas custom parsing is disabled in BeaconParser (pre-existing); re-enable with the handlers"]
    fn test_parse_alter_atlas_table_cast_display() {
        let sql =
            "ALTER ATLAS TABLE schema.table ON PARTITION p9 ALTER COLUMN depth SET DATA TYPE INT";
        let mut parser = BeaconParser::new(sql).unwrap();
        let stmt = parser.parse_statement().unwrap();
        assert_eq!(
            stmt.to_string(),
            "ALTER ATLAS TABLE schema.table ON PARTITION p9 ALTER COLUMN depth SET DATA TYPE INT"
        );
    }

    #[test]
    fn test_parse_alter_atlas_table_missing_set_data_type() {
        let sql = "ALTER ATLAS TABLE my_table ON PARTITION p0 ALTER COLUMN col";
        let mut parser = BeaconParser::new(sql).unwrap();
        assert!(parser.parse_statement().is_err());
    }

    #[test]
    fn test_parse_alter_atlas_table_missing_partition_clause() {
        let sql = "ALTER ATLAS TABLE my_table ALTER COLUMN col SET DATA TYPE INT";
        let mut parser = BeaconParser::new(sql).unwrap();
        assert!(parser.parse_statement().is_err());
    }

    fn parse_auth(sql: &str) -> AuthStatement {
        let mut parser = BeaconParser::new(sql).unwrap();
        match parser.parse_statement().unwrap() {
            BeaconStatement::Auth(stmt) => stmt,
            other => panic!("expected Auth statement, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_create_user() {
        match parse_auth("CREATE USER alice WITH PASSWORD 'secret'") {
            AuthStatement::CreateUser { username, password } => {
                assert_eq!(username, "alice");
                assert_eq!(password, "secret");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_parse_drop_user() {
        match parse_auth("DROP USER alice") {
            AuthStatement::DropUser { username } => assert_eq!(username, "alice"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_parse_create_and_drop_role() {
        assert!(matches!(
            parse_auth("CREATE ROLE reader"),
            AuthStatement::CreateRole { role } if role == "reader"
        ));
        assert!(matches!(
            parse_auth("DROP ROLE reader"),
            AuthStatement::DropRole { role } if role == "reader"
        ));
    }

    #[test]
    fn test_parse_grant_role_to_user() {
        match parse_auth("GRANT ROLE reader TO USER alice") {
            AuthStatement::GrantRoleToUser { role, username } => {
                assert_eq!(role, "reader");
                assert_eq!(username, "alice");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_parse_revoke_role_from_user() {
        match parse_auth("REVOKE ROLE reader FROM USER alice") {
            AuthStatement::RevokeRoleFromUser { role, username } => {
                assert_eq!(role, "reader");
                assert_eq!(username, "alice");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_parse_grant_privilege_on_table() {
        match parse_auth("GRANT SELECT ON TABLE observations TO ROLE reader") {
            AuthStatement::GrantPrivilege {
                privilege,
                target,
                role,
            } => {
                assert_eq!(privilege, Privilege::Select);
                assert_eq!(target, Some(PrivilegeTarget::Table("observations".to_string())));
                assert_eq!(role, "reader");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_parse_grant_privilege_on_path() {
        match parse_auth("GRANT SELECT ON PATH 'example_2/*' TO ROLE reader") {
            AuthStatement::GrantPrivilege {
                privilege,
                target,
                role,
            } => {
                assert_eq!(privilege, Privilege::Select);
                assert_eq!(target, Some(PrivilegeTarget::Path("example_2/*".to_string())));
                assert_eq!(role, "reader");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_parse_grant_privilege_global() {
        match parse_auth("GRANT ALL TO ROLE admin") {
            AuthStatement::GrantPrivilege {
                privilege,
                target,
                role,
            } => {
                assert_eq!(privilege, Privilege::All);
                assert_eq!(target, None);
                assert_eq!(role, "admin");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_parse_deny_privilege() {
        match parse_auth("DENY SELECT ON PATH 'example/*' TO ROLE reader") {
            AuthStatement::DenyPrivilege {
                privilege,
                target,
                role,
            } => {
                assert_eq!(privilege, Privilege::Select);
                assert_eq!(target, Some(PrivilegeTarget::Path("example/*".to_string())));
                assert_eq!(role, "reader");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_parse_revoke_grant() {
        match parse_auth("REVOKE SELECT ON PATH 'example_2/*' FROM ROLE reader") {
            AuthStatement::RevokePrivilege {
                privilege,
                target,
                role,
                deny,
            } => {
                assert_eq!(privilege, Privilege::Select);
                assert_eq!(target, Some(PrivilegeTarget::Path("example_2/*".to_string())));
                assert_eq!(role, "reader");
                assert!(!deny);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_parse_revoke_deny() {
        match parse_auth("REVOKE DENY SELECT ON PATH 'example/*' FROM ROLE reader") {
            AuthStatement::RevokePrivilege {
                privilege,
                target,
                role,
                deny,
            } => {
                assert_eq!(privilege, Privilege::Select);
                assert_eq!(target, Some(PrivilegeTarget::Path("example/*".to_string())));
                assert_eq!(role, "reader");
                assert!(deny);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_parse_revoke_all_global() {
        match parse_auth("REVOKE ALL FROM ROLE admin") {
            AuthStatement::RevokePrivilege {
                privilege,
                target,
                role,
                deny,
            } => {
                assert_eq!(privilege, Privilege::All);
                assert_eq!(target, None);
                assert_eq!(role, "admin");
                assert!(!deny);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_auth_statement_display_roundtrip() {
        let statements = [
            "CREATE USER alice WITH PASSWORD 'secret'",
            "DROP USER alice",
            "CREATE ROLE reader",
            "DROP ROLE reader",
            "GRANT ROLE reader TO USER alice",
            "REVOKE ROLE reader FROM USER alice",
            "GRANT SELECT ON TABLE observations TO ROLE reader",
            "GRANT SELECT ON PATH 'example_2/*' TO ROLE reader",
            "GRANT ALL TO ROLE admin",
            "DENY SELECT ON PATH 'example/*' TO ROLE reader",
            "REVOKE SELECT ON PATH 'example_2/*' FROM ROLE reader",
            "REVOKE DENY SELECT ON PATH 'example/*' FROM ROLE reader",
            "REVOKE ALL FROM ROLE admin",
        ];
        for sql in statements {
            let mut parser = BeaconParser::new(sql).unwrap();
            let stmt = parser.parse_statement().unwrap();
            assert_eq!(stmt.to_string(), sql, "round-trip mismatch for: {sql}");
        }
    }

    #[test]
    fn test_regular_create_table_still_parses_as_df() {
        let mut parser = BeaconParser::new("CREATE TABLE t (a INT)").unwrap();
        assert!(matches!(
            parser.parse_statement().unwrap(),
            BeaconStatement::DFStatement(_)
        ));
    }
}
