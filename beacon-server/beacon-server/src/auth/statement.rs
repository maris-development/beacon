//! The SQL statements that manage users, roles and rules.
//!
//! The server parses these statements and applies them to its [`AuthContext`]. The engine
//! never sees them, because the engine knows no users and no roles.
//!
//! ```sql
//! CREATE USER <name> WITH PASSWORD '<password>'
//! DROP USER <name>
//! CREATE ROLE <role>
//! DROP ROLE <role>
//! GRANT ROLE <role> TO USER <name>
//! REVOKE ROLE <role> FROM USER <name>
//! GRANT <privilege> [ON <target>] TO ROLE <role>
//! DENY <privilege> [ON <target>] TO ROLE <role>
//! REVOKE [DENY] <privilege> [ON <target>] FROM ROLE <role>
//! ALTER ROLE <role> SET <key> = <value>
//! ALTER ROLE <role> RESET <key>
//! ```
//!
//! A `<target>` is `TABLE <name>`, `PATH '<pattern>'` or `ALL`.

use std::fmt::Display;
use std::str::FromStr;

use beacon_auth::{AuthContext, Privilege, PrivilegeRule, PrivilegeTarget};
use datafusion::error::{DataFusionError, Result};
use datafusion::sql::{
    parser::{DFParser, DFParserBuilder},
    planner::object_name_to_table_reference,
    sqlparser::{ast::ObjectName, tokenizer::Token},
};

/// A statement that manages users, roles, grants, denies or role settings.
#[derive(Debug, Clone)]
pub enum AuthStatement {
    CreateUser {
        username: String,
        password: String,
    },
    DropUser {
        username: String,
    },
    CreateRole {
        role: String,
    },
    DropRole {
        role: String,
    },
    GrantRoleToUser {
        role: String,
        username: String,
    },
    RevokeRoleFromUser {
        role: String,
        username: String,
    },
    GrantPrivilege {
        privilege: Privilege,
        target: Option<PrivilegeTarget>,
        role: String,
    },
    DenyPrivilege {
        privilege: Privilege,
        target: Option<PrivilegeTarget>,
        role: String,
    },
    RevokePrivilege {
        privilege: Privilege,
        target: Option<PrivilegeTarget>,
        role: String,
        /// When true, removes a matching deny rule rather than a grant rule.
        deny: bool,
    },
    /// `ALTER ROLE <role> SET <key> = <value>`. The key is lowercase.
    SetRoleSetting {
        role: String,
        key: String,
        value: String,
    },
    /// `ALTER ROLE <role> RESET <key>`. The key is lowercase.
    ResetRoleSetting {
        role: String,
        key: String,
    },
}

impl Display for AuthStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // The password stays out of logs and error messages.
            AuthStatement::CreateUser { username, .. } => {
                write!(f, "CREATE USER {username} WITH PASSWORD '***'")
            }
            AuthStatement::DropUser { username } => write!(f, "DROP USER {username}"),
            AuthStatement::CreateRole { role } => write!(f, "CREATE ROLE {role}"),
            AuthStatement::DropRole { role } => write!(f, "DROP ROLE {role}"),
            AuthStatement::GrantRoleToUser { role, username } => {
                write!(f, "GRANT ROLE {role} TO USER {username}")
            }
            AuthStatement::RevokeRoleFromUser { role, username } => {
                write!(f, "REVOKE ROLE {role} FROM USER {username}")
            }
            AuthStatement::GrantPrivilege {
                privilege,
                target,
                role,
            } => {
                write!(f, "GRANT {privilege}")?;
                if let Some(target) = target {
                    write!(f, " ON {target}")?;
                }
                write!(f, " TO ROLE {role}")
            }
            AuthStatement::DenyPrivilege {
                privilege,
                target,
                role,
            } => {
                write!(f, "DENY {privilege}")?;
                if let Some(target) = target {
                    write!(f, " ON {target}")?;
                }
                write!(f, " TO ROLE {role}")
            }
            AuthStatement::RevokePrivilege {
                privilege,
                target,
                role,
                deny,
            } => {
                write!(f, "REVOKE ")?;
                if *deny {
                    write!(f, "DENY ")?;
                }
                write!(f, "{privilege}")?;
                if let Some(target) = target {
                    write!(f, " ON {target}")?;
                }
                write!(f, " FROM ROLE {role}")
            }
            AuthStatement::SetRoleSetting { role, key, value } => {
                let value = escape_sql_literal(value);
                write!(f, "ALTER ROLE {role} SET {key} = '{value}'")
            }
            AuthStatement::ResetRoleSetting { role, key } => {
                write!(f, "ALTER ROLE {role} RESET {key}")
            }
        }
    }
}

/// Escapes `value` for a single-quoted SQL string literal, so the `Display` form parses back
/// to the same value.
fn escape_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

impl AuthStatement {
    /// Parses `sql` as an auth statement.
    ///
    /// # Arguments
    ///
    /// * `sql` - The text of one SQL statement
    ///
    /// # Returns
    ///
    /// `None` when `sql` is not an auth statement. The engine then gets the statement.
    ///
    /// # Errors
    ///
    /// Returns an error when `sql` starts as an auth statement and has a syntax error, or
    /// when a second statement follows.
    ///
    /// # Examples
    ///
    /// ```
    /// use beacon_server::auth::AuthStatement;
    ///
    /// let statement = AuthStatement::parse("CREATE ROLE reader").unwrap();
    /// assert!(matches!(statement, Some(AuthStatement::CreateRole { .. })));
    /// assert!(AuthStatement::parse("SELECT 1").unwrap().is_none());
    /// ```
    pub fn parse(sql: &str) -> Result<Option<Self>> {
        let mut parser = AuthParser {
            parser: DFParserBuilder::new(sql).build()?,
        };
        let Some(statement) = parser.try_parse_auth()? else {
            return Ok(None);
        };
        // A request runs one statement, so a second one would not run and give no error.
        let tokens = &mut parser.parser.parser;
        while tokens.consume_token(&Token::SemiColon) {}
        let next = tokens.peek_token();
        if next.token != Token::EOF {
            return Err(DataFusionError::Plan(format!(
                // The location renders with its own leading " at".
                "a request holds one SQL statement; found `{}`{} after it",
                next.token, next.span.start
            )));
        }
        Ok(Some(statement))
    }

    /// Applies the statement to `auth`.
    ///
    /// The caller must check first that the request comes from the super-user.
    ///
    /// # Errors
    ///
    /// Returns an error when the auth context refuses the change, for example for a role
    /// that does not exist, or when the store cannot save it.
    pub async fn apply(&self, auth: &AuthContext) -> anyhow::Result<()> {
        match self {
            Self::CreateUser { username, password } => auth.create_user(username, password).await,
            Self::DropUser { username } => auth.drop_user(username).await,
            Self::CreateRole { role } => auth.create_role(role).await,
            Self::DropRole { role } => auth.drop_role(role).await,
            Self::GrantRoleToUser { role, username } => {
                auth.grant_role_to_user(username, role).await
            }
            Self::RevokeRoleFromUser { role, username } => {
                auth.revoke_role_from_user(username, role).await
            }
            Self::GrantPrivilege {
                privilege,
                target,
                role,
            } => {
                auth.grant(role, PrivilegeRule::new(*privilege, target.clone()))
                    .await
            }
            Self::DenyPrivilege {
                privilege,
                target,
                role,
            } => {
                auth.deny(role, PrivilegeRule::new(*privilege, target.clone()))
                    .await
            }
            Self::RevokePrivilege {
                privilege,
                target,
                role,
                deny,
            } => {
                auth.revoke(role, &PrivilegeRule::new(*privilege, target.clone()), *deny)
                    .await
            }
            Self::SetRoleSetting { role, key, value } => {
                auth.set_role_setting(role, key, value).await
            }
            Self::ResetRoleSetting { role, key } => auth.reset_role_setting(role, key).await,
        }
    }
}

/// The parser of the auth statements, on top of the DataFusion tokenizer.
///
/// The lifetime `'a` is the lifetime of the SQL text that the tokenizer reads.
struct AuthParser<'a> {
    parser: DFParser<'a>,
}

impl AuthParser<'_> {
    /// Reads a single string value (single-quoted string, identifier, or number).
    fn parse_string_value(&mut self) -> Result<String> {
        let token = self.parser.parser.next_token();
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

    /// Whether the word token at `nth` (case-insensitively) equals `word`.
    fn word_at(&self, nth: usize, word: &str) -> bool {
        matches!(
            &self.parser.parser.peek_nth_token(nth).token,
            Token::Word(w) if w.value.eq_ignore_ascii_case(word)
        )
    }

    /// Dispatches the auth-management statements (CREATE/DROP USER/ROLE, GRANT/DENY/REVOKE),
    /// returning `None` when the next tokens are not an auth statement.
    fn try_parse_auth(&mut self) -> Result<Option<AuthStatement>> {
        let statement = if self.word_at(0, "CREATE") && self.word_at(1, "USER") {
            self.parse_create_user()?
        } else if self.word_at(0, "CREATE") && self.word_at(1, "ROLE") {
            self.parse_create_role()?
        } else if self.word_at(0, "DROP") && self.word_at(1, "USER") {
            self.parse_drop_user()?
        } else if self.word_at(0, "DROP") && self.word_at(1, "ROLE") {
            self.parse_drop_role()?
        } else if self.word_at(0, "GRANT") {
            self.parse_grant()?
        } else if self.word_at(0, "DENY") {
            self.parse_deny()?
        } else if self.word_at(0, "REVOKE") {
            self.parse_revoke()?
        } else if self.word_at(0, "ALTER") && self.word_at(1, "ROLE") {
            self.parse_alter_role()?
        } else {
            return Ok(None);
        };
        Ok(Some(statement))
    }

    /// Reads a single identifier (role/user name) as a string.
    fn parse_name(&mut self) -> Result<String> {
        self.parser
            .parser
            .parse_identifier()
            .map(|ident| ident.value)
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    /// Consumes a word token, erroring if it does not match `word` (case-insensitive).
    fn expect_word(&mut self, word: &str) -> Result<()> {
        let token = self.parser.parser.next_token();
        match token.token {
            Token::Word(w) if w.value.eq_ignore_ascii_case(word) => Ok(()),
            other => Err(DataFusionError::Plan(format!(
                "expected `{word}`, found {other}"
            ))),
        }
    }

    /// Parse: CREATE USER <name> WITH PASSWORD '<password>'
    fn parse_create_user(&mut self) -> Result<AuthStatement> {
        self.parser.parser.next_token(); // CREATE
        self.parser.parser.next_token(); // USER
        let username = self.parse_name()?;
        self.expect_word("WITH")?;
        self.expect_word("PASSWORD")?;
        let password = self.parse_string_value()?;
        Ok(AuthStatement::CreateUser { username, password })
    }

    /// Parse: DROP USER <name>
    fn parse_drop_user(&mut self) -> Result<AuthStatement> {
        self.parser.parser.next_token(); // DROP
        self.parser.parser.next_token(); // USER
        let username = self.parse_name()?;
        Ok(AuthStatement::DropUser { username })
    }

    /// Parse: CREATE ROLE <name>
    fn parse_create_role(&mut self) -> Result<AuthStatement> {
        self.parser.parser.next_token(); // CREATE
        self.parser.parser.next_token(); // ROLE
        let role = self.parse_name()?;
        Ok(AuthStatement::CreateRole { role })
    }

    /// Parse: DROP ROLE <name>
    fn parse_drop_role(&mut self) -> Result<AuthStatement> {
        self.parser.parser.next_token(); // DROP
        self.parser.parser.next_token(); // ROLE
        let role = self.parse_name()?;
        Ok(AuthStatement::DropRole { role })
    }

    /// Parse: GRANT ROLE <role> TO USER <user> | GRANT <priv> [ON <target>] TO ROLE <role>
    fn parse_grant(&mut self) -> Result<AuthStatement> {
        self.parser.parser.next_token(); // GRANT
        if self.word_at(0, "ROLE") {
            self.parser.parser.next_token(); // ROLE
            let role = self.parse_name()?;
            self.expect_word("TO")?;
            self.expect_word("USER")?;
            let username = self.parse_name()?;
            return Ok(AuthStatement::GrantRoleToUser { role, username });
        }

        let (privilege, target) = self.parse_privilege_and_target()?;
        self.expect_word("TO")?;
        self.expect_word("ROLE")?;
        let role = self.parse_name()?;
        Ok(AuthStatement::GrantPrivilege {
            privilege,
            target,
            role,
        })
    }

    /// Parse: DENY <priv> [ON <target>] TO ROLE <role>
    fn parse_deny(&mut self) -> Result<AuthStatement> {
        self.parser.parser.next_token(); // DENY
        let (privilege, target) = self.parse_privilege_and_target()?;
        self.expect_word("TO")?;
        self.expect_word("ROLE")?;
        let role = self.parse_name()?;
        Ok(AuthStatement::DenyPrivilege {
            privilege,
            target,
            role,
        })
    }

    /// Parse: REVOKE ROLE <role> FROM USER <user>
    ///      | REVOKE [DENY] <priv> [ON <target>] FROM ROLE <role>
    fn parse_revoke(&mut self) -> Result<AuthStatement> {
        self.parser.parser.next_token(); // REVOKE
        if self.word_at(0, "ROLE") {
            self.parser.parser.next_token(); // ROLE
            let role = self.parse_name()?;
            self.expect_word("FROM")?;
            self.expect_word("USER")?;
            let username = self.parse_name()?;
            return Ok(AuthStatement::RevokeRoleFromUser { role, username });
        }

        // `REVOKE DENY <priv> ...` removes a deny rule; `REVOKE <priv> ...` removes a grant rule.
        let deny = self.word_at(0, "DENY");
        if deny {
            self.parser.parser.next_token(); // DENY
        }
        let (privilege, target) = self.parse_privilege_and_target()?;
        self.expect_word("FROM")?;
        self.expect_word("ROLE")?;
        let role = self.parse_name()?;
        Ok(AuthStatement::RevokePrivilege {
            privilege,
            target,
            role,
            deny,
        })
    }

    /// Parse: ALTER ROLE <role> SET <key> { = | TO } <value>
    ///      | ALTER ROLE <role> RESET <key>
    ///
    /// The key is free and may hold dots, such as `wms.max_tiles`.
    fn parse_alter_role(&mut self) -> Result<AuthStatement> {
        self.parser.parser.next_token(); // ALTER
        self.parser.parser.next_token(); // ROLE
        let role = self.parse_name()?;
        let set = self.word_at(0, "SET");
        if !set && !self.word_at(0, "RESET") {
            let token = self.parser.parser.peek_token().token;
            return Err(DataFusionError::Plan(format!(
                "expected SET or RESET after ALTER ROLE {role}, found {token}"
            )));
        }
        self.parser.parser.next_token(); // SET or RESET
        let key = self.parse_setting_key()?;
        if !set {
            return Ok(AuthStatement::ResetRoleSetting { role, key });
        }
        if !self.parser.parser.consume_token(&Token::Eq) {
            self.expect_word("TO")?;
        }
        let value = self.parse_string_value()?;
        Ok(AuthStatement::SetRoleSetting { role, key, value })
    }

    /// Reads a role setting key: dotted identifier parts, joined with `.` and in lowercase.
    fn parse_setting_key(&mut self) -> Result<String> {
        let name = self
            .parser
            .parser
            .parse_object_name(false)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let key = name
            .0
            .iter()
            .map(|part| part.as_ident().map(|ident| ident.value.clone()))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| DataFusionError::Plan(format!("invalid role setting key {name}")))?
            .join(".");
        beacon_auth::normalize_setting_key(&key).map_err(|e| DataFusionError::Plan(e.to_string()))
    }

    /// Parse `<privilege> [ON <target>]`, where `<target>` is `TABLE <name>`, `PATH '<pattern>'`,
    /// or `ALL`.
    fn parse_privilege_and_target(&mut self) -> Result<(Privilege, Option<PrivilegeTarget>)> {
        let privilege_str = self.parse_string_value()?;
        let privilege = Privilege::from_str(&privilege_str).map_err(DataFusionError::Plan)?;

        let target = if self.word_at(0, "ON") {
            self.parser.parser.next_token(); // ON
            Some(self.parse_privilege_target()?)
        } else {
            None
        };

        Ok((privilege, target))
    }

    /// Parse a privilege target: `TABLE <name>`, `PATH '<pattern>'`, or `ALL`.
    fn parse_privilege_target(&mut self) -> Result<PrivilegeTarget> {
        if self.word_at(0, "TABLE") {
            self.parser.parser.next_token(); // TABLE
            let name = self
                .parser
                .parser
                .parse_object_name(false)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            Ok(PrivilegeTarget::Table(table_rule_name(name)?))
        } else if self.word_at(0, "PATH") {
            self.parser.parser.next_token(); // PATH
            Ok(PrivilegeTarget::Path(self.parse_string_value()?))
        } else if self.word_at(0, "ALL") {
            self.parser.parser.next_token(); // ALL
            Ok(PrivilegeTarget::All)
        } else {
            let token = self.parser.parser.peek_nth_token(0).token.clone();
            Err(DataFusionError::Plan(format!(
                "expected a privilege target (TABLE, PATH, or ALL), found {token}"
            )))
        }
    }
}

/// The name a table rule stores: the parts of `name` as the catalog holds them, joined with `.`.
///
/// The quotes go and the case stays, because the session turns identifier normalization off.
/// The authorization check builds its table targets in the same form.
fn table_rule_name(name: ObjectName) -> Result<String> {
    Ok(object_name_to_table_reference(name, false)?
        .to_vec()
        .join("."))
}

/// The table rule name for `stored`, when `stored` is a quoted SQL name.
///
/// Earlier versions stored the name of a table rule as written, quotes included, so the rule
/// never matched. `None` for a value that already holds the catalog form.
pub(super) fn unquoted_table_rule_name(stored: &str) -> Option<String> {
    if !stored.contains(['"', '`']) {
        return None;
    }
    let mut parser = DFParserBuilder::new(stored).build().ok()?;
    let name = parser.parser.parse_object_name(false).ok()?;
    let quoted = name.0.iter().any(|part| {
        part.as_ident()
            .is_some_and(|ident| ident.quote_style.is_some())
    });
    // A catalog name such as `quote"d` does not parse to the end, so it stays as it is.
    if !quoted || parser.parser.peek_token().token != Token::EOF {
        return None;
    }
    table_rule_name(name).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The auth statement that `sql` holds.
    fn parse_auth(sql: &str) -> AuthStatement {
        AuthStatement::parse(sql)
            .unwrap()
            .unwrap_or_else(|| panic!("expected an auth statement for `{sql}`"))
    }

    #[test]
    fn other_statements_are_not_auth_statements() {
        for sql in [
            "SELECT 1",
            "CREATE TABLE t (a INT)",
            "DROP TABLE t",
            "ALTER TABLE t ADD COLUMN b INT",
            "REFRESH t",
        ] {
            assert!(AuthStatement::parse(sql).unwrap().is_none(), "{sql}");
        }
    }

    #[test]
    fn a_second_statement_is_refused() {
        assert!(AuthStatement::parse("CREATE ROLE reader;")
            .unwrap()
            .is_some());
        let error = AuthStatement::parse("CREATE ROLE reader; DROP ROLE reader").unwrap_err();
        assert!(error.to_string().contains("one SQL statement"), "{error}");
    }

    #[test]
    fn the_display_form_hides_the_password() {
        let statement = parse_auth("CREATE USER alice WITH PASSWORD 'secret'");
        assert_eq!(
            statement.to_string(),
            "CREATE USER alice WITH PASSWORD '***'"
        );
    }

    #[test]
    fn parse_create_and_drop_user() {
        match parse_auth("CREATE USER alice WITH PASSWORD 'secret'") {
            AuthStatement::CreateUser { username, password } => {
                assert_eq!(username, "alice");
                assert_eq!(password, "secret");
            }
            other => panic!("unexpected: {other:?}"),
        }
        match parse_auth("DROP USER alice") {
            AuthStatement::DropUser { username } => assert_eq!(username, "alice"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parse_role_lifecycle_and_assignment() {
        assert!(
            matches!(parse_auth("CREATE ROLE reader"), AuthStatement::CreateRole { role } if role == "reader")
        );
        assert!(
            matches!(parse_auth("DROP ROLE reader"), AuthStatement::DropRole { role } if role == "reader")
        );
        match parse_auth("GRANT ROLE reader TO USER alice") {
            AuthStatement::GrantRoleToUser { role, username } => {
                assert_eq!(role, "reader");
                assert_eq!(username, "alice");
            }
            other => panic!("unexpected: {other:?}"),
        }
        match parse_auth("REVOKE ROLE reader FROM USER alice") {
            AuthStatement::RevokeRoleFromUser { role, username } => {
                assert_eq!(role, "reader");
                assert_eq!(username, "alice");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parse_privilege_grants_with_targets() {
        match parse_auth("GRANT SELECT ON PATH 'argo/**/*.nc' TO ROLE reader") {
            AuthStatement::GrantPrivilege {
                privilege,
                target,
                role,
            } => {
                assert_eq!(privilege, Privilege::Select);
                assert_eq!(
                    target,
                    Some(PrivilegeTarget::Path("argo/**/*.nc".to_string()))
                );
                assert_eq!(role, "reader");
            }
            other => panic!("unexpected: {other:?}"),
        }
        match parse_auth("GRANT SELECT ON TABLE observations TO ROLE reader") {
            AuthStatement::GrantPrivilege { target, .. } => {
                assert_eq!(
                    target,
                    Some(PrivilegeTarget::Table("observations".to_string()))
                );
            }
            other => panic!("unexpected: {other:?}"),
        }
        // A table target holds the name as the catalog does: no quotes, case kept.
        for (sql_name, stored) in [
            ("\"Mixed Case\"", "Mixed Case"),
            ("\"with space\"", "with space"),
            ("\"quote\"\"d\"", "quote\"d"),
            ("public.\"with space\"", "public.with space"),
            ("Obs", "Obs"),
        ] {
            match parse_auth(&format!("GRANT SELECT ON TABLE {sql_name} TO ROLE reader")) {
                AuthStatement::GrantPrivilege { target, .. } => {
                    assert_eq!(
                        target,
                        Some(PrivilegeTarget::Table(stored.to_string())),
                        "{sql_name}"
                    );
                }
                other => panic!("unexpected: {other:?}"),
            }
        }
        // No `ON` clause means the grant applies to every target.
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
    fn only_a_quoted_stored_table_name_is_rewritten() {
        assert_eq!(
            unquoted_table_rule_name("\"with space\"").as_deref(),
            Some("with space")
        );
        assert_eq!(
            unquoted_table_rule_name("public.\"Mixed Case\"").as_deref(),
            Some("public.Mixed Case")
        );
        assert_eq!(
            unquoted_table_rule_name("\"quote\"\"d\"").as_deref(),
            Some("quote\"d")
        );
        assert_eq!(unquoted_table_rule_name("`obs`").as_deref(), Some("obs"));
        // Values already in the catalog form stay as they are.
        for stored in ["obs", "Mixed Case", "with space", "quote\"d", "public.obs"] {
            assert_eq!(unquoted_table_rule_name(stored), None, "{stored}");
        }
    }

    #[test]
    fn parse_deny_and_revoke_variants() {
        match parse_auth("DENY SELECT ON PATH 'argo/restricted/*' TO ROLE reader") {
            AuthStatement::DenyPrivilege {
                privilege,
                target,
                role,
            } => {
                assert_eq!(privilege, Privilege::Select);
                assert_eq!(
                    target,
                    Some(PrivilegeTarget::Path("argo/restricted/*".to_string()))
                );
                assert_eq!(role, "reader");
            }
            other => panic!("unexpected: {other:?}"),
        }
        // `REVOKE <priv>` removes a grant; `REVOKE DENY <priv>` removes a deny.
        match parse_auth("REVOKE SELECT ON TABLE observations FROM ROLE reader") {
            AuthStatement::RevokePrivilege { deny, .. } => assert!(!deny),
            other => panic!("unexpected: {other:?}"),
        }
        match parse_auth("REVOKE DENY SELECT ON PATH 'argo/restricted/*' FROM ROLE reader") {
            AuthStatement::RevokePrivilege { deny, .. } => assert!(deny),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn auth_statement_round_trips_through_display() {
        for sql in [
            "CREATE ROLE reader",
            "GRANT ROLE reader TO USER alice",
            "GRANT SELECT ON PATH 'argo/*' TO ROLE reader",
            "DENY SELECT ON TABLE observations TO ROLE reader",
            "ALTER ROLE reader SET max_rows = '30000'",
            "ALTER ROLE reader RESET max_rows",
            "ALTER ROLE reader SET wms.max_tiles = 'it''s 50'",
        ] {
            assert_eq!(parse_auth(sql).to_string(), sql);
        }
    }

    #[test]
    fn parse_alter_role_settings() {
        for sql in [
            "ALTER ROLE reader SET max_rows = 30000",
            "ALTER ROLE reader SET MAX_ROWS TO '30000'",
        ] {
            match parse_auth(sql) {
                AuthStatement::SetRoleSetting { role, key, value } => {
                    assert_eq!(role, "reader");
                    assert_eq!(key, "max_rows");
                    assert_eq!(value, "30000");
                }
                other => panic!("unexpected for `{sql}`: {other:?}"),
            }
        }
        match parse_auth("ALTER ROLE reader SET WMS.Max_Tiles = 'gold tier'") {
            AuthStatement::SetRoleSetting { key, value, .. } => {
                assert_eq!(key, "wms.max_tiles");
                assert_eq!(value, "gold tier");
            }
            other => panic!("unexpected: {other:?}"),
        }
        assert!(matches!(
            parse_auth("ALTER ROLE reader RESET max_rows"),
            AuthStatement::ResetRoleSetting { role, key } if role == "reader" && key == "max_rows"
        ));
        for (sql, message) in [
            (
                "ALTER ROLE reader SET \"bad key\" = 1",
                "invalid role setting key",
            ),
            (
                "ALTER ROLE reader RENAME TO writer",
                "expected SET or RESET",
            ),
            ("ALTER ROLE reader SET max_rows 1", "expected `TO`"),
        ] {
            let error = AuthStatement::parse(sql).unwrap_err();
            assert!(error.to_string().contains(message), "{sql}: {error}");
        }
    }
}
