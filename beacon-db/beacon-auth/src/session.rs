//! Server-issued session tokens: a client signs in one time and then sends the token, not the password.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use aws_lc_rs::{
    digest,
    rand::{SecureRandom, SystemRandom},
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use parking_lot::RwLock;

use crate::context::AuthIdentity;

/// Why a sign-in failed.
#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    /// Wrong username or password. The caller must not learn which.
    #[error("invalid credentials")]
    InvalidCredentials,
    #[error(transparent)]
    Storage(#[from] anyhow::Error),
}

/// A new session. `token` goes to the client one time and is never stored.
#[derive(Debug, Clone)]
pub struct IssuedSession {
    pub token: String,
    pub identity: AuthIdentity,
    pub remember: bool,
    pub expires_at: i64,
}

/// The state of a live session, after its expiry has slid.
#[derive(Debug, Clone)]
pub struct SessionInfo {
    pub identity: AuthIdentity,
    pub remember: bool,
    pub expires_at: i64,
}

/// Prefix of every session token. It routes a `Bearer` value to the session store, not to OIDC.
pub const SESSION_TOKEN_PREFIX: &str = "bst_";

const TOKEN_BYTES: usize = 32;

const FAILED_SIGN_IN_DELAY_MIN_MS: u64 = 500;
const FAILED_SIGN_IN_DELAY_SPAN_MS: u64 = 1000;

/// Makes a new random session token.
pub(crate) fn generate_token() -> anyhow::Result<String> {
    let mut bytes = [0u8; TOKEN_BYTES];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| anyhow::anyhow!("the system RNG failed"))?;
    Ok(format!("{SESSION_TOKEN_PREFIX}{}", URL_SAFE_NO_PAD.encode(bytes)))
}

fn sha256_hex(input: &[u8]) -> String {
    hex(digest::digest(&digest::SHA256, input).as_ref())
}

/// The value stored for a token. The token itself is never stored.
pub(crate) fn hash_token(token: &str) -> String {
    sha256_hex(token.as_bytes())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Fingerprint of the configured super-user credential. A new username or password changes it.
///
/// Slow on purpose (Argon2id), because it is stored in the database.
pub(crate) fn super_user_fingerprint(username: &str, password: &str) -> String {
    let input = format!("{FINGERPRINT_SALT}\0{username}\0{password}");
    let mut output = [0u8; 32];
    // The fixed salt and output length are valid, so only an input above 4 GiB can fail.
    argon2::Argon2::default()
        .hash_password_into(input.as_bytes(), FINGERPRINT_SALT.as_bytes(), &mut output)
        .expect("Argon2id with a fixed salt and output length does not fail");
    hex(&output)
}

/// A fixed salt keeps the fingerprint the same for the same credential.
const FINGERPRINT_SALT: &str = "beacon-session-v1";

/// A random wait for a failed sign-in, from 500 ms to 1500 ms.
pub fn failed_sign_in_delay() -> Duration {
    let mut bytes = [0u8; 8];
    // A failed RNG gives the minimum delay, which still slows the caller.
    let random = SystemRandom::new()
        .fill(&mut bytes)
        .map(|_| u64::from_le_bytes(bytes))
        .unwrap_or(0);
    Duration::from_millis(
        FAILED_SIGN_IN_DELAY_MIN_MS + random % (FAILED_SIGN_IN_DELAY_SPAN_MS + 1),
    )
}

/// Whether a slid expiry must be written: the stored expiry is less than half the TTL away.
pub(crate) fn needs_persist(persisted_expires_at: i64, now: i64, ttl_secs: i64) -> bool {
    persisted_expires_at - now < ttl_secs / 2
}

/// The idle TTLs of a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionTtls {
    pub normal: Duration,
    /// `Duration::ZERO` turns "stay signed in" off.
    pub remember: Duration,
}

impl Default for SessionTtls {
    fn default() -> Self {
        Self {
            normal: Duration::from_secs(8 * 60 * 60),
            remember: Duration::from_secs(30 * 24 * 60 * 60),
        }
    }
}

/// One session, in the shape of a `__beacon_sessions` row. Times are Unix seconds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredSession {
    pub token_hash: String,
    pub username: String,
    /// Empty for a stored user. For the super-user, an Argon2id hash of its username and password.
    pub credential_fingerprint: String,
    pub remember: bool,
    pub created_at: i64,
    pub expires_at: i64,
}

/// The durable backend a [`SessionStore`] writes through to.
#[async_trait::async_trait]
pub trait SessionPersistence: std::fmt::Debug + Send + Sync {
    async fn insert_session(&self, session: &StoredSession) -> anyhow::Result<()>;
    async fn update_session_expiry(&self, token_hash: &str, expires_at: i64) -> anyhow::Result<()>;
    async fn delete_sessions(&self, token_hashes: &[String]) -> anyhow::Result<()>;
    async fn delete_user_sessions(&self, username: &str) -> anyhow::Result<()>;
    async fn load_sessions(&self) -> anyhow::Result<Vec<StoredSession>>;
}

type Clock = Arc<dyn Fn() -> i64 + Send + Sync>;

fn system_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or(0)
}

#[derive(Debug, Clone)]
struct Entry {
    /// `expires_at` here is the live, slid value.
    session: StoredSession,
    persisted_expires_at: i64,
}

/// In-memory sessions by token hash, written through to an optional [`SessionPersistence`].
pub struct SessionStore {
    ttls: SessionTtls,
    sessions: RwLock<HashMap<String, Entry>>,
    persistence: Option<Arc<dyn SessionPersistence>>,
    clock: Clock,
}

impl std::fmt::Debug for SessionStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionStore")
            .field("ttls", &self.ttls)
            .finish_non_exhaustive()
    }
}

impl SessionStore {
    pub fn new(ttls: SessionTtls) -> Self {
        Self {
            ttls,
            sessions: RwLock::new(HashMap::new()),
            persistence: None,
            clock: Arc::new(system_now),
        }
    }

    pub fn with_persistence(ttls: SessionTtls, persistence: Arc<dyn SessionPersistence>) -> Self {
        Self {
            persistence: Some(persistence),
            ..Self::new(ttls)
        }
    }

    #[cfg(test)]
    fn with_clock(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }

    fn remember_enabled(&self) -> bool {
        !self.ttls.remember.is_zero()
    }

    fn ttl_secs(&self, remember: bool) -> i64 {
        let ttl = if remember && self.remember_enabled() {
            self.ttls.remember
        } else {
            self.ttls.normal
        };
        ttl.as_secs() as i64
    }

    /// Replaces the in-memory sessions with the live rows, and deletes the expired rows.
    pub async fn hydrate(&self) -> anyhow::Result<()> {
        let Some(persistence) = &self.persistence else {
            return Ok(());
        };
        let now = (self.clock)();
        let (live, expired): (Vec<_>, Vec<_>) = persistence
            .load_sessions()
            .await?
            .into_iter()
            .partition(|session| session.expires_at > now);
        if !expired.is_empty() {
            let hashes: Vec<String> = expired.into_iter().map(|s| s.token_hash).collect();
            persistence.delete_sessions(&hashes).await?;
        }
        *self.sessions.write() = live
            .into_iter()
            .map(|session| {
                (
                    session.token_hash.clone(),
                    Entry {
                        persisted_expires_at: session.expires_at,
                        session,
                    },
                )
            })
            .collect();
        Ok(())
    }

    /// Issues a session. The row is stored before the token is returned.
    pub async fn create(
        &self,
        username: &str,
        credential_fingerprint: &str,
        remember: bool,
    ) -> anyhow::Result<(String, StoredSession)> {
        self.sweep().await;
        let remember = remember && self.remember_enabled();
        let now = (self.clock)();
        let token = generate_token()?;
        let session = StoredSession {
            token_hash: hash_token(&token),
            username: username.to_string(),
            credential_fingerprint: credential_fingerprint.to_string(),
            remember,
            created_at: now,
            expires_at: now + self.ttl_secs(remember),
        };
        if let Some(persistence) = &self.persistence {
            persistence.insert_session(&session).await?;
        }
        self.sessions.write().insert(
            session.token_hash.clone(),
            Entry {
                persisted_expires_at: session.expires_at,
                session: session.clone(),
            },
        );
        Ok((token, session))
    }

    /// Finds a live session and slides its expiry. `None` for an unknown or expired token.
    pub fn touch(&self, token: &str) -> Option<StoredSession> {
        let token_hash = hash_token(token);
        let now = (self.clock)();
        let mut sessions = self.sessions.write();
        if sessions.get(&token_hash)?.session.expires_at <= now {
            sessions.remove(&token_hash);
            drop(sessions);
            self.spawn_delete(vec![token_hash]);
            return None;
        }
        let entry = sessions.get_mut(&token_hash)?;
        let ttl = self.ttl_secs(entry.session.remember);
        entry.session.expires_at = now + ttl;
        let persist = needs_persist(entry.persisted_expires_at, now, ttl);
        if persist {
            // Set under the lock, so a parallel touch sees it and does not write again.
            entry.persisted_expires_at = entry.session.expires_at;
        }
        let session = entry.session.clone();
        drop(sessions);
        if persist {
            self.spawn_update(&session);
        }
        Some(session)
    }

    /// Ends the session of `token`.
    pub async fn remove(&self, token: &str) -> anyhow::Result<()> {
        self.remove_hash(&hash_token(token)).await
    }

    /// Ends the session stored as `token_hash`.
    pub async fn remove_hash(&self, token_hash: &str) -> anyhow::Result<()> {
        self.sessions.write().remove(token_hash);
        if let Some(persistence) = &self.persistence {
            persistence.delete_sessions(&[token_hash.to_string()]).await?;
        }
        Ok(())
    }

    /// Ends every session of `username`.
    pub async fn remove_user(&self, username: &str) -> anyhow::Result<()> {
        self.sessions
            .write()
            .retain(|_, entry| entry.session.username != username);
        if let Some(persistence) = &self.persistence {
            persistence.delete_user_sessions(username).await?;
        }
        Ok(())
    }

    /// Deletes expired sessions. Selected from memory: a persisted expiry can lag the live one.
    async fn sweep(&self) {
        let now = (self.clock)();
        let mut expired = Vec::new();
        self.sessions.write().retain(|hash, entry| {
            let live = entry.session.expires_at > now;
            if !live {
                expired.push(hash.clone());
            }
            live
        });
        if expired.is_empty() {
            return;
        }
        if let Some(persistence) = &self.persistence {
            if let Err(error) = persistence.delete_sessions(&expired).await {
                tracing::warn!(?error, "could not delete expired sessions");
            }
        }
    }

    fn spawn_update(&self, session: &StoredSession) {
        let Some(persistence) = self.persistence.clone() else {
            return;
        };
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let (token_hash, expires_at) = (session.token_hash.clone(), session.expires_at);
        handle.spawn(async move {
            if let Err(error) = persistence.update_session_expiry(&token_hash, expires_at).await {
                tracing::warn!(?error, "could not persist a session expiry");
            }
        });
    }

    fn spawn_delete(&self, token_hashes: Vec<String>) {
        let Some(persistence) = self.persistence.clone() else {
            return;
        };
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        handle.spawn(async move {
            if let Err(error) = persistence.delete_sessions(&token_hashes).await {
                tracing::warn!(?error, "could not delete an expired session");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_token_has_the_prefix_and_fixed_length() {
        let token = generate_token().unwrap();
        assert!(token.starts_with(SESSION_TOKEN_PREFIX));
        assert_eq!(token.len(), 47);
        assert_ne!(token, generate_token().unwrap());
    }

    #[test]
    fn a_token_hash_is_stable_hex_and_not_the_token() {
        let hash = hash_token("bst_abc");
        assert_eq!(hash, hash_token("bst_abc"));
        assert_eq!(hash.len(), 64);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(hash, hash_token("bst_abd"));
    }

    #[test]
    fn the_super_user_fingerprint_follows_username_and_password() {
        let base = super_user_fingerprint("admin", "pw");
        assert_eq!(base, super_user_fingerprint("admin", "pw"));
        assert_ne!(base, super_user_fingerprint("admin", "pw2"));
        assert_ne!(base, super_user_fingerprint("root", "pw"));
        assert_eq!(base.len(), 64);
        assert!(base.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
        assert_ne!(base, sha256_hex(b"beacon-session-v1\0admin\0pw"));
    }

    #[test]
    fn the_failed_sign_in_delay_stays_in_range() {
        for _ in 0..200 {
            let delay = failed_sign_in_delay().as_millis();
            assert!((500..=1500).contains(&delay), "delay {delay} ms is out of range");
        }
    }

    #[test]
    fn persist_only_below_half_the_ttl() {
        // TTL 100: half is 50.
        assert!(!needs_persist(160, 100, 100));
        assert!(!needs_persist(150, 100, 100));
        assert!(needs_persist(149, 100, 100));
    }

    use std::{
        collections::HashMap,
        sync::{
            atomic::{AtomicBool, AtomicI64, Ordering},
            Arc,
        },
    };

    /// Persistence double: keeps rows in a map and counts expiry updates.
    #[derive(Debug, Default)]
    struct FakePersistence {
        rows: parking_lot::Mutex<HashMap<String, StoredSession>>,
        updates: parking_lot::Mutex<Vec<(String, i64)>>,
        fail_insert: AtomicBool,
    }

    #[async_trait::async_trait]
    impl SessionPersistence for FakePersistence {
        async fn insert_session(&self, session: &StoredSession) -> anyhow::Result<()> {
            if self.fail_insert.load(Ordering::SeqCst) {
                anyhow::bail!("insert failed");
            }
            self.rows.lock().insert(session.token_hash.clone(), session.clone());
            Ok(())
        }
        async fn update_session_expiry(&self, token_hash: &str, expires_at: i64) -> anyhow::Result<()> {
            self.updates.lock().push((token_hash.to_string(), expires_at));
            if let Some(row) = self.rows.lock().get_mut(token_hash) {
                row.expires_at = expires_at;
            }
            Ok(())
        }
        async fn delete_sessions(&self, token_hashes: &[String]) -> anyhow::Result<()> {
            let mut rows = self.rows.lock();
            for hash in token_hashes {
                rows.remove(hash);
            }
            Ok(())
        }
        async fn delete_user_sessions(&self, username: &str) -> anyhow::Result<()> {
            self.rows.lock().retain(|_, row| row.username != username);
            Ok(())
        }
        async fn load_sessions(&self) -> anyhow::Result<Vec<StoredSession>> {
            Ok(self.rows.lock().values().cloned().collect())
        }
    }

    const TTLS: SessionTtls = SessionTtls {
        normal: Duration::from_secs(100),
        remember: Duration::from_secs(1000),
    };

    fn store_at(
        ttls: SessionTtls,
        persistence: Arc<FakePersistence>,
    ) -> (SessionStore, Arc<AtomicI64>) {
        let now = Arc::new(AtomicI64::new(1000));
        let clock = now.clone();
        let store = SessionStore::with_persistence(ttls, persistence)
            .with_clock(Arc::new(move || clock.load(Ordering::SeqCst)));
        (store, now)
    }

    /// Lets spawned persist tasks run.
    async fn settle() {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn create_persists_the_hash_and_not_the_token() {
        let persistence = Arc::new(FakePersistence::default());
        let (store, _) = store_at(TTLS, persistence.clone());
        let (token, session) = store.create("alice", "", false).await.unwrap();
        assert_eq!(session.expires_at, 1100);
        let rows = persistence.rows.lock();
        let row = rows.get(&hash_token(&token)).expect("the row is stored by hash");
        assert_eq!(row.username, "alice");
        assert!(!rows.contains_key(&token));
    }

    #[tokio::test]
    async fn a_failed_insert_issues_no_session() {
        let persistence = Arc::new(FakePersistence::default());
        persistence.fail_insert.store(true, Ordering::SeqCst);
        let (store, _) = store_at(TTLS, persistence);
        assert!(store.create("alice", "", false).await.is_err());
        assert!(store.sessions.read().is_empty());
    }

    #[tokio::test]
    async fn touch_slides_the_expiry() {
        let (store, now) = store_at(TTLS, Arc::new(FakePersistence::default()));
        let (token, _) = store.create("alice", "", false).await.unwrap();
        now.store(1030, Ordering::SeqCst);
        assert_eq!(store.touch(&token).unwrap().expires_at, 1130);
    }

    #[tokio::test]
    async fn an_expired_session_is_gone_from_memory_and_table() {
        let persistence = Arc::new(FakePersistence::default());
        let (store, now) = store_at(TTLS, persistence.clone());
        let (token, _) = store.create("alice", "", false).await.unwrap();
        now.store(1100, Ordering::SeqCst);
        assert!(store.touch(&token).is_none());
        settle().await;
        assert!(persistence.rows.lock().is_empty());
        assert!(store.touch(&token).is_none());
    }

    #[tokio::test]
    async fn remember_uses_the_long_ttl() {
        let (store, _) = store_at(TTLS, Arc::new(FakePersistence::default()));
        let (_, session) = store.create("alice", "", true).await.unwrap();
        assert!(session.remember);
        assert_eq!(session.expires_at, 2000);
    }

    #[tokio::test]
    async fn remember_is_ignored_when_its_ttl_is_zero() {
        let ttls = SessionTtls { normal: Duration::from_secs(100), remember: Duration::ZERO };
        let (store, _) = store_at(ttls, Arc::new(FakePersistence::default()));
        let (_, session) = store.create("alice", "", true).await.unwrap();
        assert!(!session.remember);
        assert_eq!(session.expires_at, 1100);
    }

    #[tokio::test]
    async fn the_expiry_persists_only_below_half_the_ttl() {
        let persistence = Arc::new(FakePersistence::default());
        let (store, now) = store_at(TTLS, persistence.clone());
        let (token, _) = store.create("alice", "", false).await.unwrap();

        // Persisted 1100, now 1040: 60 left of 100, no write.
        now.store(1040, Ordering::SeqCst);
        store.touch(&token).unwrap();
        settle().await;
        assert!(persistence.updates.lock().is_empty());

        // Persisted 1100, now 1060: 40 left, write 1160.
        now.store(1060, Ordering::SeqCst);
        store.touch(&token).unwrap();
        settle().await;
        assert_eq!(*persistence.updates.lock(), vec![(hash_token(&token), 1160)]);

        // Persisted 1160, now 1061: 99 left, no write.
        now.store(1061, Ordering::SeqCst);
        store.touch(&token).unwrap();
        settle().await;
        assert_eq!(persistence.updates.lock().len(), 1);
    }

    #[tokio::test]
    async fn concurrent_touches_persist_once() {
        let persistence = Arc::new(FakePersistence::default());
        let (store, now) = store_at(TTLS, persistence.clone());
        let store = Arc::new(store);
        let (token, _) = store.create("alice", "", false).await.unwrap();
        now.store(1060, Ordering::SeqCst);

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let (store, token) = (store.clone(), token.clone());
                tokio::spawn(async move { store.touch(&token).is_some() })
            })
            .collect();
        for handle in handles {
            assert!(handle.await.unwrap());
        }
        settle().await;
        assert_eq!(persistence.updates.lock().len(), 1);
    }

    #[tokio::test]
    async fn hydrate_drops_expired_rows() {
        let persistence = Arc::new(FakePersistence::default());
        for (hash, expires_at) in [("live", 1500), ("dead", 900)] {
            persistence.rows.lock().insert(
                hash.to_string(),
                StoredSession {
                    token_hash: hash.to_string(),
                    username: "alice".to_string(),
                    credential_fingerprint: String::new(),
                    remember: false,
                    created_at: 0,
                    expires_at,
                },
            );
        }
        let (store, _) = store_at(TTLS, persistence.clone());
        store.hydrate().await.unwrap();
        assert!(store.sessions.read().contains_key("live"));
        assert!(!store.sessions.read().contains_key("dead"));
        assert!(!persistence.rows.lock().contains_key("dead"));
    }

    #[tokio::test]
    async fn remove_user_removes_only_that_user() {
        let persistence = Arc::new(FakePersistence::default());
        let (store, _) = store_at(TTLS, persistence.clone());
        let (alice, _) = store.create("alice", "", false).await.unwrap();
        let (bob, _) = store.create("bob", "", false).await.unwrap();
        store.remove_user("alice").await.unwrap();
        assert!(store.touch(&alice).is_none());
        assert!(store.touch(&bob).is_some());
        assert_eq!(persistence.rows.lock().len(), 1);
    }

    #[tokio::test]
    async fn remove_ends_one_session() {
        let persistence = Arc::new(FakePersistence::default());
        let (store, _) = store_at(TTLS, persistence.clone());
        let (token, _) = store.create("alice", "", false).await.unwrap();
        store.remove(&token).await.unwrap();
        assert!(store.touch(&token).is_none());
        assert!(persistence.rows.lock().is_empty());
    }

    #[tokio::test]
    async fn create_sweeps_expired_sessions() {
        let persistence = Arc::new(FakePersistence::default());
        let (store, now) = store_at(TTLS, persistence.clone());
        let (old, _) = store.create("alice", "", false).await.unwrap();
        now.store(1200, Ordering::SeqCst);
        store.create("bob", "", false).await.unwrap();
        assert!(!persistence.rows.lock().contains_key(&hash_token(&old)));
        assert_eq!(store.sessions.read().len(), 1);
    }
}
