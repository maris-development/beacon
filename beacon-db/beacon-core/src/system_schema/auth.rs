//! `beacon.system.users` and `beacon.system.roles` — the auth directory as SQL.
//!
//! Authentication itself is not expressible as SQL: verifying a credential is
//! what *produces* the identity a query runs as, so it necessarily precedes any
//! query (see [`Runtime::authenticate`](crate::runtime::Runtime::authenticate)).
//! Everything around it is SQL — `CREATE USER`, `CREATE ROLE`, `GRANT`/`DENY`
//! already are, and these two tables complete the set by making the directory
//! readable.
//!
//! Both are super-user-only, gated unconditionally in
//! [`authorize_logical_plan`](crate::statement_plan::authorize_logical_plan) —
//! the same treatment the `__beacon_*` tables get, and for the same reason: grant
//! enforcement is off by default, so a gate that depended on it would expose the
//! directory on a default runtime. No password material is exposed here at all:
//! the hash never leaves the auth subsystem.

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::common::Result as DFResult;

use super::table::{Snapshot, SystemTable};

/// Table names in `beacon.system` that expose the auth directory. Reading either
/// requires super-user, always.
pub const AUTH_TABLES: [&str; 2] = ["users", "roles"];

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
    ]))
}

/// `beacon.system.users` — usernames and their roles. Never the password hash.
pub(super) fn users_table(auth: Arc<beacon_auth::AuthContext>) -> SystemTable {
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
pub(super) fn roles_table(auth: Arc<beacon_auth::AuthContext>) -> SystemTable {
    let snapshot: Snapshot = Arc::new(move || {
        let auth = auth.clone();
        Box::pin(async move {
            // `list_roles` is already sorted by name.
            let roles = auth.list_roles();

            let names: Vec<&str> = roles.iter().map(|r| r.name.as_str()).collect();
            let grants: Vec<String> = roles.iter().map(|r| rules_json(&r.grants)).collect();
            let denies: Vec<String> = roles.iter().map(|r| rules_json(&r.denies)).collect();

            let columns: Vec<ArrayRef> = vec![
                Arc::new(StringArray::from(names)),
                Arc::new(StringArray::from(grants)),
                Arc::new(StringArray::from(denies)),
            ];
            Ok(RecordBatch::try_new(roles_schema(), columns)?) as DFResult<RecordBatch>
        })
    });

    SystemTable::new(roles_schema(), snapshot)
}

/// Render a rule set as a sorted JSON array of
/// `{privilege, target_type, target_value}` objects.
///
/// The shape mirrors the `AuthRuleView` API contract so a consumer can
/// deserialize a row straight into it. Sorted because the underlying set has no
/// stable order and a scan must be deterministic.
fn rules_json(rules: &std::collections::HashSet<beacon_auth::PrivilegeRule>) -> String {
    let mut rendered: Vec<serde_json::Value> = rules
        .iter()
        .map(|rule| {
            // `None` and `All` both mean "every target for this privilege".
            let (target_type, target_value) = match &rule.target {
                None | Some(beacon_auth::PrivilegeTarget::All) => ("all", None),
                Some(beacon_auth::PrivilegeTarget::Table(t)) => ("table", Some(t.clone())),
                Some(beacon_auth::PrivilegeTarget::Path(p)) => ("path", Some(p.clone())),
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
