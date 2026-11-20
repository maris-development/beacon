//! `beacon.system.users`, `beacon.system.roles` and `beacon.system.role_settings` — the auth
//! directory as SQL.
//!
//! Authentication itself is not expressible as SQL: verifying a credential is
//! what *produces* the identity a query runs as, so it necessarily precedes any
//! query. Everything around it is SQL — `CREATE USER`, `CREATE ROLE`,
//! `GRANT`/`DENY` already are, and these tables complete the set by making the
//! directory readable.
//!
//! The server owns the tables and adds them to the `system` schema of the engine.
//! All are super-user-only, gated unconditionally in
//! [`RoleValidator`](super::RoleValidator) —
//! the same treatment the `__beacon_*` tables get, and for the same reason: grant
//! enforcement is off by default, so a gate that depended on it would expose the
//! directory on a default server. No password material is exposed here at all:
//! the hash never leaves the auth subsystem.

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use beacon_auth::{AuthContext, PrivilegeRule, PrivilegeTarget, Role};
use beacon_core::runtime::Runtime;
use beacon_core::{Snapshot, SystemTable};
use datafusion::common::Result as DFResult;

/// Adds `beacon.system.users`, `beacon.system.roles` and `beacon.system.role_settings` to the
/// engine, with `auth` as their source.
///
/// # Errors
///
/// Returns an error when the `system` schema already holds one of the tables.
pub(super) fn register(runtime: &Runtime, auth: &Arc<AuthContext>) -> anyhow::Result<()> {
    // `Arc::clone` copies the pointer, not the auth state.
    runtime.register_system_table("users", Arc::new(users_table(Arc::clone(auth))))?;
    runtime.register_system_table("roles", Arc::new(roles_table(Arc::clone(auth))))?;
    runtime.register_system_table(
        "role_settings",
        Arc::new(role_settings_table(Arc::clone(auth))),
    )
}

fn users_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("username", DataType::Utf8, false),
        // JSON array: a user's role list is variable length.
        Field::new("roles", DataType::Utf8, false),
    ]))
}

fn roles_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("role_name", DataType::Utf8, false),
        // JSON arrays of privilege rules, rendered through their `Display`.
        Field::new("grants", DataType::Utf8, false),
        Field::new("denies", DataType::Utf8, false),
        // JSON object of the role's settings, by key.
        Field::new("settings", DataType::Utf8, false),
    ]))
}

fn role_settings_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("role_name", DataType::Utf8, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
    ]))
}

/// `beacon.system.users` — usernames and their roles. Never the password hash.
fn users_table(auth: Arc<AuthContext>) -> SystemTable {
    let snapshot: Snapshot = Arc::new(move || {
        let auth = auth.clone();
        Box::pin(async move {
            // A directory that cannot enumerate (e.g. a pure OIDC provider)
            // yields no rows rather than an error.
            let mut users = auth.list_users().await.unwrap_or_default();
            users.sort_by(|left, right| left.username.cmp(&right.username));

            let usernames: Vec<&str> = users.iter().map(|u| u.username.as_str()).collect();
            let roles: Vec<String> = users
                .iter()
                .map(|u| serde_json::to_string(&u.roles).unwrap_or_else(|_| "[]".to_string()))
                .collect();

            let columns: Vec<ArrayRef> = vec![
                Arc::new(StringArray::from(usernames)),
                Arc::new(StringArray::from(roles)),
            ];
            Ok(RecordBatch::try_new(users_schema(), columns)?) as DFResult<RecordBatch>
        })
    });

    SystemTable::new(users_schema(), snapshot)
}

/// `beacon.system.roles` — roles with their grant and deny rules.
fn roles_table(auth: Arc<AuthContext>) -> SystemTable {
    let snapshot: Snapshot = Arc::new(move || {
        let auth = auth.clone();
        Box::pin(async move {
            // `list_roles` is already sorted by name.
            let roles = auth.list_roles();

            let names: Vec<&str> = roles.iter().map(|r| r.name.as_str()).collect();
            let grants: Vec<String> = roles.iter().map(|r| rules_json(&r.grants)).collect();
            let denies: Vec<String> = roles.iter().map(|r| rules_json(&r.denies)).collect();
            let settings: Vec<String> = roles.iter().map(settings_json).collect();

            let columns: Vec<ArrayRef> = vec![
                Arc::new(StringArray::from(names)),
                Arc::new(StringArray::from(grants)),
                Arc::new(StringArray::from(denies)),
                Arc::new(StringArray::from(settings)),
            ];
            Ok(RecordBatch::try_new(roles_schema(), columns)?) as DFResult<RecordBatch>
        })
    });

    SystemTable::new(roles_schema(), snapshot)
}

/// `beacon.system.role_settings` — one row for each key-value setting of a role, set with
/// `ALTER ROLE <role> SET <key> = <value>`.
fn role_settings_table(auth: Arc<AuthContext>) -> SystemTable {
    let snapshot: Snapshot = Arc::new(move || {
        let auth = auth.clone();
        Box::pin(async move {
            // Roles come sorted by name and settings by key, so the scan is deterministic.
            let roles = auth.list_roles();
            let rows: Vec<(&str, &str, &str)> = roles
                .iter()
                .flat_map(|role| {
                    role.settings
                        .iter()
                        .map(|(key, value)| (role.name.as_str(), key.as_str(), value.as_str()))
                })
                .collect();

            let columns: Vec<ArrayRef> = vec![
                Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.0))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.2))),
            ];
            Ok(RecordBatch::try_new(role_settings_schema(), columns)?) as DFResult<RecordBatch>
        })
    });

    SystemTable::new(role_settings_schema(), snapshot)
}

/// Render the settings of a role as a JSON object, e.g. `{"wms.max_tiles":"500"}`.
fn settings_json(role: &Role) -> String {
    serde_json::to_string(&role.settings).unwrap_or_else(|_| "{}".to_string())
}

/// Render a rule set as a sorted JSON array of
/// `{privilege, target_type, target_value}` objects.
///
/// The shape mirrors the `AuthRuleView` API contract so a consumer can
/// deserialize a row straight into it. Sorted because the underlying set has no
/// stable order and a scan must be deterministic.
fn rules_json(rules: &std::collections::HashSet<PrivilegeRule>) -> String {
    let mut rendered: Vec<serde_json::Value> = rules
        .iter()
        .map(|rule| {
            // `None` and `All` both mean "every target for this privilege".
            let (target_type, target_value) = match &rule.target {
                None | Some(PrivilegeTarget::All) => ("all", None),
                Some(PrivilegeTarget::Table(t)) => ("table", Some(t.clone())),
                Some(PrivilegeTarget::Path(p)) => ("path", Some(p.clone())),
            };
            serde_json::json!({
                "privilege": rule.privilege.to_string(),
                "target_type": target_type,
                "target_value": target_value,
            })
        })
        .collect();
    rendered.sort_by_key(|value| value.to_string());
    serde_json::to_string(&rendered).unwrap_or_else(|_| "[]".to_string())
}
