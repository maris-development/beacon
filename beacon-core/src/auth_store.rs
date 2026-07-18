//! Managed-table–backed persistence for the auth subsystem (users, roles, grants).
//!
//! The durable truth for auth state lives in four internal managed tables in the `db://` tables
//! store, so it survives a restart; [`beacon_auth`]'s in-memory `InMemoryUserStore`/`RoleProvider`
//! are the hot read path, hydrated from these tables at startup and written through on every
//! mutation. This mirrors the schema of the removed SQLite backend:
//!
//! - `__beacon_users(username, password_hash)`
//! - `__beacon_user_roles(username, role)`
//! - `__beacon_roles(name)`
//! - `__beacon_role_rules(role, kind, privilege, target_type, target_value)`  (`kind` ∈ grant|deny)
//!
//! # Reaching the session without a cycle
//!
//! The store runs its SQL through the runtime's `SessionContext`, but the [`AuthContext`] that owns
//! it is registered as a *session config extension*. A strong `Arc<SessionContext>` here would close
//! a `SessionContext -> config ext -> AuthContext -> store -> SessionContext` cycle that leaks the
//! session — and the redb `beacon.db` exclusive lock — for the process lifetime. So the store holds
//! the planner's [`SessionCell`] (a late-filled `Weak`) and upgrades it per call, exactly like the
//! statement execution nodes. `tests/tables_store_lock_release.rs` pins that the lock still releases.
//!
//! # Injection safety
//!
//! Usernames and path patterns are attacker-influenced, and every value is interpolated into SQL, so
//! [`quote_literal`] escapes single quotes on **every** interpolated value. A missed one is an
//! injection.

use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
    sync::Arc,
};

use arrow::{array::Array, record_batch::RecordBatch};
use async_trait::async_trait;
use beacon_auth::{
    decode_target, encode_target, hash_password, rule_kind, Privilege, PrivilegeRule, Role,
    RoleStore, StoredUser, UserDirectory, UserRecord,
};
use datafusion::{prelude::SessionContext, sql::parser::DFParserBuilder};
use futures::TryStreamExt;

use crate::statement_plan::SessionCell;

// The four internal tables. Their names are under `beacon_datafusion_ext::table_ext`'s
// `INTERNAL_TABLE_PREFIX`, which is what hides them from user-facing catalog listings and gates them
// to the super-user (see `statement_plan::authz`).
const USERS_TABLE: &str = "__beacon_users";
const USER_ROLES_TABLE: &str = "__beacon_user_roles";
const ROLES_TABLE: &str = "__beacon_roles";
const ROLE_RULES_TABLE: &str = "__beacon_role_rules";

/// SQL-managed durable backend for auth state, addressing the internal managed tables through the
/// runtime session. Implements both [`UserDirectory`] and [`RoleStore`]: the in-memory working
/// copies write through to it and hydrate from it.
pub(crate) struct TablesAuthStore {
    /// Late-filled weak handle to the session, upgraded per call. A `Weak` (not an `Arc`) breaks the
    /// config-extension cycle described in the module docs.
    session: SessionCell,
}

impl std::fmt::Debug for TablesAuthStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TablesAuthStore").finish_non_exhaustive()
    }
}

/// Escapes single quotes so `value` is safe to embed inside a single-quoted SQL literal. Applied to
/// **every** interpolated value — a missed call is an injection.
fn quote_literal(value: &str) -> String {
    value.replace('\'', "''")
}

/// Reads a Utf8/Utf8View/LargeUtf8 cell as an owned `String` (empty for NULL). Managed-table scans
/// may return either the plain or the view string layout, so all are handled.
fn string_at(array: &dyn Array, row: usize) -> anyhow::Result<String> {
    use arrow::array::{LargeStringArray, StringArray, StringViewArray};
    if array.is_null(row) {
        return Ok(String::new());
    }
    if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = array.as_any().downcast_ref::<StringViewArray>() {
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(a.value(row).to_string());
    }
    anyhow::bail!(
        "auth store: unexpected column type {:?} where a string was expected",
        array.data_type()
    )
}

impl TablesAuthStore {
    pub(crate) fn new(session: SessionCell) -> Self {
        Self { session }
    }

    /// Upgrades the weak session handle. Fails if the session has already been torn down (never
    /// during a live runtime: the store is only used while the runtime is alive).
    fn session(&self) -> anyhow::Result<Arc<SessionContext>> {
        self.session
            .get()
            .and_then(|weak| weak.upgrade())
            .ok_or_else(|| anyhow::anyhow!("auth store: beacon session context is unavailable"))
    }

    /// Runs one SQL statement through the same lowering + execution path as `run_query`, so managed
    /// `DELETE`/`UPDATE` are rewritten to the copy-on-write node (a plain `session.sql` would not).
    /// Collects and returns the resulting batches (empty for side-effecting statements).
    async fn run(&self, sql: String) -> anyhow::Result<Vec<RecordBatch>> {
        let session = self.session()?;
        // Scoped so the (`!Send`) parser is dropped before the first `.await`, keeping this future
        // `Send` as the `async_trait` methods require.
        let statement = {
            let mut parser = DFParserBuilder::new(sql.as_str()).build()?;
            parser.parse_statement()?
        };
        let plan = crate::statement_plan::lower_df_statement(&session, statement).await?;
        let stream = crate::statement_plan::execute_statement_plan(&session, plan).await?;
        let batches = stream.try_collect::<Vec<_>>().await?;
        Ok(batches)
    }

    /// Creates the four internal tables if they do not already exist. `CREATE TABLE IF NOT EXISTS`
    /// is idempotent, so this is safe to run on every start (the tables are rebuilt from their
    /// persisted definitions by `init_tables` before this runs).
    pub(crate) async fn ensure_tables(&self) -> anyhow::Result<()> {
        for ddl in [
            format!(
                "CREATE TABLE IF NOT EXISTS {USERS_TABLE} (username VARCHAR, password_hash VARCHAR)"
            ),
            format!("CREATE TABLE IF NOT EXISTS {USER_ROLES_TABLE} (username VARCHAR, role VARCHAR)"),
            format!("CREATE TABLE IF NOT EXISTS {ROLES_TABLE} (name VARCHAR)"),
            format!(
                "CREATE TABLE IF NOT EXISTS {ROLE_RULES_TABLE} \
                 (role VARCHAR, kind VARCHAR, privilege VARCHAR, target_type VARCHAR, target_value VARCHAR)"
            ),
        ] {
            self.run(ddl).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl UserDirectory for TablesAuthStore {
    async fn create_user(&self, username: &str, password: &str) -> anyhow::Result<()> {
        // The durable store hashes independently of the in-memory copy: both verify the same
        // password. CREATE USER is rare and off every hot path, so Argon2's cost is irrelevant here.
        let password_hash = hash_password(password)?;
        self.run(format!(
            "INSERT INTO {USERS_TABLE} (username, password_hash) VALUES ('{}', '{}')",
            quote_literal(username),
            quote_literal(&password_hash),
        ))
        .await?;
        Ok(())
    }

    async fn drop_user(&self, username: &str) -> anyhow::Result<()> {
        let username = quote_literal(username);
        self.run(format!(
            "DELETE FROM {USERS_TABLE} WHERE username = '{username}'"
        ))
        .await?;
        self.run(format!(
            "DELETE FROM {USER_ROLES_TABLE} WHERE username = '{username}'"
        ))
        .await?;
        Ok(())
    }

    async fn grant_role(&self, username: &str, role: &str) -> anyhow::Result<()> {
        self.run(format!(
            "INSERT INTO {USER_ROLES_TABLE} (username, role) VALUES ('{}', '{}')",
            quote_literal(username),
            quote_literal(role),
        ))
        .await?;
        Ok(())
    }

    async fn revoke_role(&self, username: &str, role: &str) -> anyhow::Result<()> {
        self.run(format!(
            "DELETE FROM {USER_ROLES_TABLE} WHERE username = '{}' AND role = '{}'",
            quote_literal(username),
            quote_literal(role),
        ))
        .await?;
        Ok(())
    }

    async fn user_exists(&self, username: &str) -> bool {
        let sql = format!(
            "SELECT username FROM {USERS_TABLE} WHERE username = '{}' LIMIT 1",
            quote_literal(username),
        );
        match self.run(sql).await {
            Ok(batches) => batches.iter().map(|b| b.num_rows()).sum::<usize>() > 0,
            Err(_) => false,
        }
    }

    async fn list_users(&self) -> anyhow::Result<Vec<UserRecord>> {
        Ok(self
            .load_users()
            .await?
            .into_iter()
            .map(|user| UserRecord {
                username: user.username,
                roles: user.roles,
            })
            .collect())
    }

    async fn load_users(&self) -> anyhow::Result<Vec<StoredUser>> {
        let user_rows = self
            .run(format!("SELECT username, password_hash FROM {USERS_TABLE}"))
            .await?;
        let role_rows = self
            .run(format!("SELECT username, role FROM {USER_ROLES_TABLE}"))
            .await?;

        // Ordered by username so the hydrated set is deterministic (matches the in-memory copy).
        let mut users: BTreeMap<String, (String, Vec<String>)> = BTreeMap::new();
        for batch in &user_rows {
            let usernames = batch.column(0);
            let hashes = batch.column(1);
            for row in 0..batch.num_rows() {
                let username = string_at(usernames.as_ref(), row)?;
                let hash = string_at(hashes.as_ref(), row)?;
                users.entry(username).or_default().0 = hash;
            }
        }
        for batch in &role_rows {
            let usernames = batch.column(0);
            let roles = batch.column(1);
            for row in 0..batch.num_rows() {
                let username = string_at(usernames.as_ref(), row)?;
                let role = string_at(roles.as_ref(), row)?;
                // A role assignment for an unknown user cannot happen while both tables are written
                // through together; ignore it defensively rather than fabricate a user.
                if let Some(entry) = users.get_mut(&username) {
                    entry.1.push(role);
                }
            }
        }

        Ok(users
            .into_iter()
            .map(|(username, (password_hash, mut roles))| {
                roles.sort();
                StoredUser {
                    username,
                    password_hash,
                    roles,
                }
            })
            .collect())
    }
}

#[async_trait]
impl RoleStore for TablesAuthStore {
    async fn load_roles(&self) -> anyhow::Result<HashMap<String, Role>> {
        let mut roles: HashMap<String, Role> = HashMap::new();

        let name_rows = self.run(format!("SELECT name FROM {ROLES_TABLE}")).await?;
        for batch in &name_rows {
            let names = batch.column(0);
            for row in 0..batch.num_rows() {
                let name = string_at(names.as_ref(), row)?;
                roles.entry(name.clone()).or_insert_with(|| Role::new(name));
            }
        }

        let rule_rows = self
            .run(format!(
                "SELECT role, kind, privilege, target_type, target_value FROM {ROLE_RULES_TABLE}"
            ))
            .await?;
        for batch in &rule_rows {
            let role_col = batch.column(0);
            let kind_col = batch.column(1);
            let privilege_col = batch.column(2);
            let target_type_col = batch.column(3);
            let target_value_col = batch.column(4);
            for row in 0..batch.num_rows() {
                let role = string_at(role_col.as_ref(), row)?;
                let kind = string_at(kind_col.as_ref(), row)?;
                let privilege = Privilege::from_str(&string_at(privilege_col.as_ref(), row)?)
                    .map_err(|err| anyhow::anyhow!(err))?;
                let target_type = string_at(target_type_col.as_ref(), row)?;
                let target_value = string_at(target_value_col.as_ref(), row)?;
                let target = decode_target(&target_type, target_value)?;
                let rule = PrivilegeRule::new(privilege, target);

                let entry = roles
                    .entry(role.clone())
                    .or_insert_with(|| Role::new(role));
                if kind == rule_kind(true) {
                    entry.denies.insert(rule);
                } else {
                    entry.grants.insert(rule);
                }
            }
        }

        Ok(roles)
    }

    async fn persist_create_role(&self, name: &str) -> anyhow::Result<()> {
        self.run(format!(
            "INSERT INTO {ROLES_TABLE} (name) VALUES ('{}')",
            quote_literal(name),
        ))
        .await?;
        Ok(())
    }

    async fn persist_drop_role(&self, name: &str) -> anyhow::Result<()> {
        let name = quote_literal(name);
        self.run(format!("DELETE FROM {ROLES_TABLE} WHERE name = '{name}'"))
            .await?;
        self.run(format!(
            "DELETE FROM {ROLE_RULES_TABLE} WHERE role = '{name}'"
        ))
        .await?;
        Ok(())
    }

    async fn persist_insert_rule(
        &self,
        role: &str,
        is_deny: bool,
        rule: &PrivilegeRule,
    ) -> anyhow::Result<()> {
        let (target_type, target_value) = encode_target(&rule.target);
        self.run(format!(
            "INSERT INTO {ROLE_RULES_TABLE} (role, kind, privilege, target_type, target_value) \
             VALUES ('{}', '{}', '{}', '{}', '{}')",
            quote_literal(role),
            quote_literal(rule_kind(is_deny)),
            quote_literal(&rule.privilege.to_string()),
            quote_literal(target_type),
            quote_literal(&target_value),
        ))
        .await?;
        Ok(())
    }

    async fn persist_remove_rule(
        &self,
        role: &str,
        is_deny: bool,
        rule: &PrivilegeRule,
    ) -> anyhow::Result<()> {
        let (target_type, target_value) = encode_target(&rule.target);
        self.run(format!(
            "DELETE FROM {ROLE_RULES_TABLE} \
             WHERE role = '{}' AND kind = '{}' AND privilege = '{}' \
             AND target_type = '{}' AND target_value = '{}'",
            quote_literal(role),
            quote_literal(rule_kind(is_deny)),
            quote_literal(&rule.privilege.to_string()),
            quote_literal(target_type),
            quote_literal(&target_value),
        ))
        .await?;
        Ok(())
    }
}
