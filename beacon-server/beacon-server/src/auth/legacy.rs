//! The one-time import of the auth state from the internal tables of `beacon.db`.
//!
//! Earlier versions kept users, roles and rules in five managed tables of the engine:
//!
//! - `__beacon_users(username, password_hash)`
//! - `__beacon_user_roles(username, role)`
//! - `__beacon_roles(name)`
//! - `__beacon_role_rules(role, kind, privilege, target_type, target_value)`
//! - `__beacon_role_settings(role, key, value)`
//!
//! The server now keeps that state in its own file. On the first start with no such file,
//! [`import`] copies the content of the tables to it. The tables stay in `beacon.db`, so an
//! operator can go back to an earlier version.

use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;

use arrow::array::{Array, AsArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use beacon_auth::{
    decode_target, normalize_setting, rule_kind, FileAuthStore, Privilege, PrivilegeRule, Role,
    StoredUser,
};
use beacon_core::plan_validator::{allow_all, AllowAll};
use beacon_core::query::Query;
use beacon_core::runtime::Runtime;
use futures::TryStreamExt;

use super::statement::unquoted_table_rule_name;

const USERS_TABLE: &str = "__beacon_users";
const USER_ROLES_TABLE: &str = "__beacon_user_roles";
const ROLES_TABLE: &str = "__beacon_roles";
const ROLE_RULES_TABLE: &str = "__beacon_role_rules";
const ROLE_SETTINGS_TABLE: &str = "__beacon_role_settings";

/// Copies the auth state of the internal tables to `store`.
///
/// # Arguments
///
/// * `runtime` - The engine that holds the tables
/// * `store` - The empty auth store of the server
///
/// # Returns
///
/// `true` when the tables held a user or a role, and `store` now holds that state.
///
/// # Errors
///
/// Returns an error when a table cannot be read or when `store` cannot be written.
pub(super) async fn import(runtime: &Runtime, store: &FileAuthStore) -> anyhow::Result<bool> {
    let users = load_users(runtime).await?;
    let roles = load_roles(runtime).await?;
    if users.is_empty() && roles.is_empty() {
        return Ok(false);
    }
    tracing::info!(
        users = users.len(),
        roles = roles.len(),
        store = %store.path().display(),
        "imported the auth state from the internal tables of beacon.db"
    );
    store.import(users, roles).await?;
    Ok(true)
}

/// The rows of `table` as strings, one inner vector for each row. A null gives an empty string.
///
/// A table that does not exist gives no rows: a database of an earlier version can lack a
/// table, and a new database has none of them.
async fn rows(runtime: &Runtime, table: &str, columns: &str) -> anyhow::Result<Vec<Vec<String>>> {
    if let Err(error) = runtime.table_arrow_schema(table, &AllowAll).await {
        tracing::debug!(%table, %error, "no internal auth table to import");
        return Ok(Vec::new());
    }
    let batches: Vec<RecordBatch> = runtime
        .run_query(
            Query::sql(format!("SELECT {columns} FROM {table}")),
            allow_all(),
        )
        .await?
        .into_record_stream()?
        .try_collect()
        .await?;

    let mut rows = Vec::with_capacity(batches.iter().map(RecordBatch::num_rows).sum());
    for batch in &batches {
        // A scan can give a plain, a large or a view string array. The cast gives one layout.
        let columns = batch
            .columns()
            .iter()
            .map(|column| arrow::compute::cast(column, &DataType::Utf8))
            .collect::<Result<Vec<_>, _>>()?;
        rows.extend((0..batch.num_rows()).map(|row| {
            columns
                .iter()
                .map(|column| {
                    let values = column.as_string::<i32>();
                    if values.is_null(row) {
                        String::new()
                    } else {
                        values.value(row).to_string()
                    }
                })
                .collect()
        }));
    }
    Ok(rows)
}

/// Moves the fields out of a row with exactly `N` columns.
fn fields<const N: usize>(row: Vec<String>) -> anyhow::Result<[String; N]> {
    row.try_into()
        .map_err(|row: Vec<String>| anyhow::anyhow!("expected {N} columns, found {}", row.len()))
}

async fn load_users(runtime: &Runtime) -> anyhow::Result<Vec<StoredUser>> {
    // Sorted by username, so the import is deterministic.
    let mut users: BTreeMap<String, (String, Vec<String>)> = BTreeMap::new();
    for row in rows(runtime, USERS_TABLE, "username, password_hash").await? {
        let [username, password_hash] = fields(row)?;
        users.entry(username).or_default().0 = password_hash;
    }
    for row in rows(runtime, USER_ROLES_TABLE, "username, role").await? {
        let [username, role] = fields(row)?;
        // A role of an unknown user is skipped. It does not create a user.
        if let Some(user) = users.get_mut(&username) {
            user.1.push(role);
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

async fn load_roles(runtime: &Runtime) -> anyhow::Result<HashMap<String, Role>> {
    let mut roles: HashMap<String, Role> = HashMap::new();
    for row in rows(runtime, ROLES_TABLE, "name").await? {
        let [name] = fields(row)?;
        roles.entry(name.clone()).or_insert_with(|| Role::new(name));
    }

    let rule_columns = "role, kind, privilege, target_type, target_value";
    for row in rows(runtime, ROLE_RULES_TABLE, rule_columns).await? {
        let [role, kind, privilege, target_type, target_value] = fields(row)?;
        let privilege = Privilege::from_str(&privilege).map_err(|e| anyhow::anyhow!(e))?;
        // An earlier version stored a table name with its SQL quotes.
        let target_value = (target_type == "table")
            .then(|| unquoted_table_rule_name(&target_value))
            .flatten()
            .unwrap_or(target_value);
        let rule = PrivilegeRule::new(privilege, decode_target(&target_type, target_value)?);
        let entry = roles.entry(role.clone()).or_insert_with(|| Role::new(role));
        if kind == rule_kind(true) {
            entry.denies.insert(rule);
        } else {
            entry.grants.insert(rule);
        }
    }

    // `key` is a keyword in a column list, so it has quotes.
    for row in rows(runtime, ROLE_SETTINGS_TABLE, "role, \"key\", value").await? {
        let [role, key, value] = fields(row)?;
        // A setting that fails the check is skipped, so the other roles still load.
        match normalize_setting(&key, &value) {
            Ok((key, value)) => {
                roles
                    .entry(role.clone())
                    .or_insert_with(|| Role::new(role))
                    .settings
                    .insert(key, value);
            }
            Err(error) => {
                tracing::warn!(%role, %key, %value, ?error, "skipped a role setting");
            }
        }
    }
    Ok(roles)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fields_needs_the_exact_column_count() {
        let [a, b] = fields(vec!["x".to_string(), "y".to_string()]).unwrap();
        assert_eq!((a.as_str(), b.as_str()), ("x", "y"));

        let error = fields::<2>(vec!["x".to_string()]).unwrap_err();
        assert!(error.to_string().contains("expected 2 columns, found 1"));
    }
}
