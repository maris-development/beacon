//! Sessions live in the internal `__beacon_sessions` table, so they survive a restart.

mod common;

use std::sync::{Arc, Mutex};

use beacon_core::{AuthIdentity, Credential};
use common::{restartable_runtime, total_rows, ADMIN_PASSWORD, ADMIN_USERNAME};

async fn session_rows(rt: &common::TestRuntime, filter: &str) -> usize {
    total_rows(
        &rt.sql_as(
            &format!("SELECT token_hash FROM __beacon_sessions WHERE {filter}"),
            AuthIdentity::system(),
        )
        .await,
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn a_session_survives_a_restart() {
    let rt = restartable_runtime("session-restart", |b| b).await;
    let issued = rt
        .runtime
        .auth()
        .create_session(ADMIN_USERNAME, ADMIN_PASSWORD, true)
        .await
        .unwrap();

    let rt = rt.restart().await;
    let identity = rt
        .runtime
        .authenticate(&Credential::session(&issued.token))
        .await
        .expect("the session should survive a restart");
    assert!(identity.is_super_user);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_load_drops_expired_rows() {
    let rt = restartable_runtime("session-expired", |b| b).await;
    rt.sql_as(
        "INSERT INTO __beacon_sessions VALUES ('deadbeef', 'test-admin', 'x', false, 0, 1)",
        AuthIdentity::system(),
    )
    .await;

    let rt = rt.restart().await;
    assert_eq!(session_rows(&rt, "token_hash = 'deadbeef'").await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_user_removes_the_stored_sessions() {
    let rt = restartable_runtime("session-drop-user", |b| b).await;
    rt.sql_as("CREATE USER alice WITH PASSWORD 'pw'", AuthIdentity::system())
        .await;
    let issued = rt
        .runtime
        .auth()
        .create_session("alice", "pw", false)
        .await
        .unwrap();
    rt.sql_as("DROP USER alice", AuthIdentity::system()).await;

    let rt = rt.restart().await;
    assert_eq!(session_rows(&rt, "username = 'alice'").await, 0);
    assert!(rt
        .runtime
        .authenticate(&Credential::session(&issued.token))
        .await
        .is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_super_user_password_ends_old_sessions() {
    let password = Arc::new(Mutex::new(ADMIN_PASSWORD.to_string()));
    let current = password.clone();
    let rt = restartable_runtime("session-new-password", move |b| {
        b.with_admin_credentials(ADMIN_USERNAME.to_string(), current.lock().unwrap().clone())
    })
    .await;
    let issued = rt
        .runtime
        .auth()
        .create_session(ADMIN_USERNAME, ADMIN_PASSWORD, true)
        .await
        .unwrap();

    *password.lock().unwrap() = "a-new-password".to_string();
    let rt = rt.restart().await;
    assert!(rt
        .runtime
        .authenticate(&Credential::session(&issued.token))
        .await
        .is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_only_runtime_keeps_sessions_in_memory() {
    let rt = restartable_runtime("session-read-only", |b| b.with_read_only(true)).await;
    let issued = rt
        .runtime
        .auth()
        .create_session(ADMIN_USERNAME, ADMIN_PASSWORD, false)
        .await
        .expect("a read-only runtime should still issue sessions");
    assert!(rt
        .runtime
        .authenticate(&Credential::session(&issued.token))
        .await
        .is_ok());
    assert_eq!(session_rows(&rt, "true").await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn parallel_sign_ins_all_persist() {
    let rt = restartable_runtime("session-parallel", |b| b).await;
    let auth = rt.runtime.auth().clone();
    let handles: Vec<_> = (0..10)
        .map(|_| {
            let auth = auth.clone();
            tokio::spawn(async move {
                auth.create_session(ADMIN_USERNAME, ADMIN_PASSWORD, false)
                    .await
                    .unwrap()
                    .token
            })
        })
        .collect();
    let mut tokens = Vec::new();
    for handle in handles {
        tokens.push(handle.await.unwrap());
    }
    drop(auth);

    let rt = rt.restart().await;
    for token in tokens {
        assert!(rt
            .runtime
            .authenticate(&Credential::session(&token))
            .await
            .is_ok());
    }
}
