//! `AuthContext` sessions: sign-in, identity resolution and revocation.

use std::{collections::HashMap, sync::Arc};

use beacon_auth::{
    AuthContext, BasicAuthProvider, Credential, SessionError, SessionPersistence, SessionStore,
    SessionTtls, StoredSession,
};

const ADMIN: &str = "admin";
const ADMIN_PASSWORD: &str = "admin-pw";

/// Shared rows, so a second context can load what the first one wrote.
#[derive(Debug, Default)]
struct SharedRows(parking_lot::Mutex<HashMap<String, StoredSession>>);

#[async_trait::async_trait]
impl SessionPersistence for SharedRows {
    async fn insert_session(&self, session: &StoredSession) -> anyhow::Result<()> {
        self.0.lock().insert(session.token_hash.clone(), session.clone());
        Ok(())
    }
    async fn update_session_expiry(&self, token_hash: &str, expires_at: i64) -> anyhow::Result<()> {
        if let Some(row) = self.0.lock().get_mut(token_hash) {
            row.expires_at = expires_at;
        }
        Ok(())
    }
    async fn delete_sessions(&self, token_hashes: &[String]) -> anyhow::Result<()> {
        let mut rows = self.0.lock();
        for hash in token_hashes {
            rows.remove(hash);
        }
        Ok(())
    }
    async fn delete_user_sessions(&self, username: &str) -> anyhow::Result<()> {
        self.0.lock().retain(|_, row| row.username != username);
        Ok(())
    }
    async fn load_sessions(&self) -> anyhow::Result<Vec<StoredSession>> {
        Ok(self.0.lock().values().cloned().collect())
    }
}

fn context_with(rows: Arc<SharedRows>, admin: &str, password: &str) -> AuthContext {
    let mut ctx = AuthContext::new(Arc::new(BasicAuthProvider::new()));
    ctx.set_super_user(admin, password);
    ctx.set_sessions(SessionStore::with_persistence(SessionTtls::default(), rows));
    ctx
}

fn context() -> AuthContext {
    context_with(Arc::new(SharedRows::default()), ADMIN, ADMIN_PASSWORD)
}

#[tokio::test]
async fn super_user_session_authenticates_as_super_user() {
    let ctx = context();
    let issued = ctx.create_session(ADMIN, ADMIN_PASSWORD, false).await.unwrap();
    assert!(issued.identity.is_super_user);
    let identity = ctx.authenticate(&Credential::session(&issued.token)).await.unwrap();
    assert!(identity.is_super_user);
    assert_eq!(identity.username, ADMIN);
}

#[tokio::test]
async fn stored_user_session_resolves_current_roles() {
    let ctx = context();
    ctx.create_role("reader").await.unwrap();
    ctx.create_user("alice", "pw").await.unwrap();
    let issued = ctx.create_session("alice", "pw", false).await.unwrap();
    assert!(!issued.identity.is_super_user);

    // A role granted after sign-in shows on the next request.
    ctx.grant_role_to_user("alice", "reader").await.unwrap();
    let identity = ctx.authenticate(&Credential::session(&issued.token)).await.unwrap();
    assert_eq!(identity.roles, vec!["reader".to_string()]);
    assert!(!identity.is_super_user);
}

#[tokio::test]
async fn wrong_password_is_invalid_credentials() {
    let ctx = context();
    ctx.create_user("alice", "pw").await.unwrap();
    assert!(matches!(
        ctx.create_session("alice", "nope", false).await,
        Err(SessionError::InvalidCredentials)
    ));
}

#[tokio::test]
async fn the_anonymous_user_cannot_sign_in() {
    let mut ctx = context();
    ctx.set_anonymous_user("anonymous");
    ctx.create_user("anonymous", "").await.unwrap();
    assert!(matches!(
        ctx.create_session("anonymous", "", false).await,
        Err(SessionError::InvalidCredentials)
    ));
}

#[tokio::test]
async fn an_ended_session_is_rejected() {
    let ctx = context();
    let issued = ctx.create_session(ADMIN, ADMIN_PASSWORD, false).await.unwrap();
    ctx.end_session(&issued.token).await.unwrap();
    assert!(ctx.authenticate(&Credential::session(&issued.token)).await.is_err());
}

#[tokio::test]
async fn an_unknown_session_token_is_rejected() {
    let ctx = context();
    assert!(ctx
        .authenticate(&Credential::session("bst_unknown"))
        .await
        .is_err());
}

#[tokio::test]
async fn dropped_user_session_is_rejected() {
    let rows = Arc::new(SharedRows::default());
    let ctx = context_with(rows.clone(), ADMIN, ADMIN_PASSWORD);
    ctx.create_user("alice", "pw").await.unwrap();
    let issued = ctx.create_session("alice", "pw", false).await.unwrap();
    ctx.drop_user("alice").await.unwrap();
    assert!(ctx.authenticate(&Credential::session(&issued.token)).await.is_err());
    assert!(rows.0.lock().is_empty());
}

#[tokio::test]
async fn a_recreated_user_does_not_inherit_old_sessions() {
    let ctx = context();
    ctx.create_user("alice", "pw").await.unwrap();
    let issued = ctx.create_session("alice", "pw", false).await.unwrap();

    // Drop alice in the directory only, so her session row stays behind.
    let directory = ctx.auth_provider().user_directory().unwrap();
    directory.drop_user("alice").await.unwrap();

    ctx.create_user("alice", "new-pw").await.unwrap();
    assert!(ctx.authenticate(&Credential::session(&issued.token)).await.is_err());
}

#[tokio::test]
async fn changed_super_user_password_rejects_old_session() {
    let rows = Arc::new(SharedRows::default());
    let before = context_with(rows.clone(), ADMIN, ADMIN_PASSWORD);
    let issued = before.create_session(ADMIN, ADMIN_PASSWORD, true).await.unwrap();

    let after = context_with(rows.clone(), ADMIN, "new-pw");
    after.hydrate().await.unwrap();
    assert!(after.authenticate(&Credential::session(&issued.token)).await.is_err());
    assert!(rows.0.lock().is_empty(), "the mismatched session is deleted");
}

#[tokio::test]
async fn renamed_super_user_rejects_old_session() {
    let rows = Arc::new(SharedRows::default());
    let before = context_with(rows.clone(), ADMIN, ADMIN_PASSWORD);
    let issued = before.create_session(ADMIN, ADMIN_PASSWORD, false).await.unwrap();

    // A stored user now has the old super-user name. The old token must not become that user.
    let after = context_with(rows.clone(), "root", ADMIN_PASSWORD);
    after.hydrate().await.unwrap();
    after.create_user(ADMIN, "pw").await.unwrap();
    assert!(after.authenticate(&Credential::session(&issued.token)).await.is_err());
}

#[tokio::test]
async fn remember_flag_comes_back_in_session_info() {
    let ctx = context();
    let issued = ctx.create_session(ADMIN, ADMIN_PASSWORD, true).await.unwrap();
    assert!(issued.remember);
    let info = ctx.session_info(&issued.token).await.unwrap();
    assert!(info.remember);
    assert!(info.identity.is_super_user);
}
