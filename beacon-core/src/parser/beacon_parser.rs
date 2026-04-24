use datafusion::error::{DataFusionError, Result};
use datafusion::sql::parser::Statement;
use datafusion::sql::sqlparser;
use datafusion::sql::{
    parser::{DFParser, DFParserBuilder},
    sqlparser::{keywords::Keyword, tokenizer::Token},
};

use super::statement::{
    AlterAtlasTableStatement, AtlasOp, BeaconStatement, CreateAtlasTableStatement,
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
        if self.is_ingest() {
            return self.parse_ingest();
        }

        if self.is_delete_atlas_datasets() {
            return self.parse_delete_atlas_datasets();
        }

        if self.is_create_atlas_table() {
            return self.parse_create_atlas_table();
        }

        if self.is_alter_atlas_table() {
            return self.parse_alter_atlas_table();
        }

        let df_statement = Box::new(self.df_parser.parse_statement()?);

        Ok(BeaconStatement::DFStatement(df_statement))
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
    fn test_parse_delete_atlas_datasets_missing_partition_clause() {
        let sql = "DELETE ATLAS DATASETS FROM my_table";
        let mut parser = BeaconParser::new(sql).unwrap();
        assert!(parser.parse_statement().is_err());
    }

    #[test]
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
}
