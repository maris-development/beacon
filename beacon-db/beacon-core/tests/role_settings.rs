//! Role settings, end to end: `ALTER ROLE … SET <key> = <value>` stores any
//! key-value pair, `beacon.system.role_settings` reads them back, and they survive
//! a restart.

mod common;

use beacon_core::{AuthIdentity, Credential};
use common::{restartable_runtime, runtime_with, TestRuntime};

/// Every `(role, key, value)` row of `beacon.system.role_settings`, in scan order.
async fn setting_rows(rt: &TestRuntime) -> Vec<(String, String, String)> {
    let batches = rt
        .sql("SELECT role_name, key, value FROM beacon.system.role_settings")
        .await;
    let (roles, keys, values) = (
        common::column_strings(&batches, 0),
        common::column_strings(&batches, 1),
        common::column_strings(&batches, 2),
    );
    roles
        .into_iter()
        .zip(keys)
        .zip(values)
        .map(|((role, key), value)| (role, key, value))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn any_key_value_pair_reads_back_by_role_and_survives_a_restart() {
    let rt = restartable_runtime("role-settings-kv", |builder| builder).await;
    rt.sql("CREATE ROLE gold").await;
    rt.sql("CREATE ROLE silver").await;
    rt.sql("ALTER ROLE gold SET WMS.Max_Tiles = 500").await;
    rt.sql("ALTER ROLE gold SET tier = 'Gold ''plus'''").await;
    rt.sql("ALTER ROLE silver SET wms.max_tiles TO 50").await;

    let row =
        |role: &str, key: &str, value: &str| (role.to_string(), key.to_string(), value.to_string());
    let expected = vec![
        row("gold", "tier", "Gold 'plus'"),
        row("gold", "wms.max_tiles", "500"),
        row("silver", "wms.max_tiles", "50"),
    ];
    assert_eq!(setting_rows(&rt).await, expected);

    let rt = rt.restart().await;
    assert_eq!(setting_rows(&rt).await, expected, "the settings persist");

    // Code reads a key through the auth context, by role or for all roles of a user.
    let auth = rt.runtime.auth();
    assert_eq!(
        auth.role_setting("gold", "wms.max_tiles").as_deref(),
        Some("500")
    );
    let roles = ["silver", "gold"].map(String::from);
    assert_eq!(
        auth.role_settings_for(&roles, "wms.max_tiles"),
        vec![
            ("silver".to_string(), "50".to_string()),
            ("gold".to_string(), "500".to_string())
        ]
    );

    rt.sql("ALTER ROLE gold RESET tier").await;
    rt.sql("DROP ROLE silver").await;
    let rt = rt.restart().await;
    assert_eq!(
        setting_rows(&rt).await,
        vec![row("gold", "wms.max_tiles", "500")],
        "a reset and a dropped role remove their rows"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_the_super_user_changes_a_role_setting() {
    let rt = runtime_with("role-settings-authz", |builder| builder).await;
    rt.sql("CREATE ROLE reader").await;
    rt.sql("CREATE USER alice WITH PASSWORD 'pw'").await;
    rt.sql("GRANT ROLE reader TO USER alice").await;
    let alice: AuthIdentity = rt
        .runtime
        .authenticate(&Credential::basic("alice", "pw"))
        .await
        .expect("alice should authenticate");

    rt.try_sql_as("ALTER ROLE reader SET tier = 'gold'", alice)
        .await
        .expect_err("a role user must not change its own role");
    let error = rt
        .try_sql("ALTER ROLE ghost SET tier = 'gold'")
        .await
        .expect_err("the role must exist");
    assert!(format!("{error:#}").contains("does not exist"), "{error:#}");
    let error = rt
        .try_sql("ALTER ROLE reader SET \"bad key\" = 1")
        .await
        .expect_err("the key must be letters, digits, '_' or '.'");
    assert!(
        format!("{error:#}").contains("invalid role setting key"),
        "{error:#}"
    );
}
