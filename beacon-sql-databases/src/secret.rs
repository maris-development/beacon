//! At-rest encryption for external-database credentials.
//!
//! Credentials are supplied inline in `CREATE EXTERNAL TABLE … OPTIONS (…)`,
//! but the table definition is persisted to `table.json` and reloaded at
//! startup. To avoid writing plaintext secrets to disk, the credential is
//! encrypted eagerly at creation time with the deployment's master key
//! (`BEACON_SECRETS_KEY`) and only decrypted in-memory when the provider is
//! built. The serialized form holds ciphertext only.

use anyhow::{anyhow, Context};
use base64::Engine as _;
use chacha20poly1305::{
    aead::{Aead, AeadCore, KeyInit, OsRng},
    Key, XChaCha20Poly1305, XNonce,
};
use secrecy::SecretString;

const B64: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::STANDARD;

/// An encrypted secret as persisted in a table definition. Carries only
/// ciphertext and a nonce — never plaintext. `Debug` is redacted so the secret
/// cannot leak through tracing of the enclosing definition.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct EncryptedSecret {
    /// Base64 XChaCha20-Poly1305 ciphertext (includes the auth tag).
    ciphertext: String,
    /// Base64 24-byte nonce.
    nonce: String,
}

impl std::fmt::Debug for EncryptedSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("EncryptedSecret(***)")
    }
}

impl EncryptedSecret {
    /// Encrypt `plaintext` under the 32-byte master `key`.
    pub fn encrypt(plaintext: &str, key: &[u8; 32]) -> anyhow::Result<Self> {
        let cipher = XChaCha20Poly1305::new(Key::from_slice(key));
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        let ciphertext = cipher
            .encrypt(&nonce, plaintext.as_bytes())
            .map_err(|e| anyhow!("failed to encrypt secret: {e}"))?;
        Ok(Self {
            ciphertext: B64.encode(ciphertext),
            nonce: B64.encode(nonce),
        })
    }

    /// Decrypt the secret under the 32-byte master `key`. Fails if the key is
    /// wrong or the ciphertext has been tampered with (AEAD auth failure).
    pub fn decrypt(&self, key: &[u8; 32]) -> anyhow::Result<SecretString> {
        let cipher = XChaCha20Poly1305::new(Key::from_slice(key));
        let nonce = B64
            .decode(&self.nonce)
            .context("secret nonce is not valid base64")?;
        // `XNonce::from_slice` panics on the wrong length, and the nonce comes
        // from an on-disk (operator-editable) `table.json` — validate first.
        anyhow::ensure!(
            nonce.len() == 24,
            "secret nonce must be 24 bytes, got {}",
            nonce.len()
        );
        let ciphertext = B64
            .decode(&self.ciphertext)
            .context("secret ciphertext is not valid base64")?;
        let plaintext = cipher
            .decrypt(XNonce::from_slice(&nonce), ciphertext.as_ref())
            .map_err(|_| anyhow!("failed to decrypt secret (wrong BEACON_SECRETS_KEY?)"))?;
        let plaintext = String::from_utf8(plaintext).context("decrypted secret is not UTF-8")?;
        Ok(SecretString::from(plaintext))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::ExposeSecret as _;

    #[test]
    fn round_trips_under_the_same_key() {
        let key = [7u8; 32];
        let enc = EncryptedSecret::encrypt("hunter2", &key).unwrap();
        assert_eq!(enc.decrypt(&key).unwrap().expose_secret(), "hunter2");
    }

    #[test]
    fn fails_under_a_different_key() {
        let enc = EncryptedSecret::encrypt("hunter2", &[1u8; 32]).unwrap();
        assert!(enc.decrypt(&[2u8; 32]).is_err());
    }

    #[test]
    fn rejects_a_malformed_nonce_without_panicking() {
        // Simulate a hand-edited / corrupted table.json with a too-short nonce.
        let enc: EncryptedSecret =
            serde_json::from_str(r#"{"ciphertext":"AAAA","nonce":"AAAA"}"#).unwrap();
        let err = enc.decrypt(&[0u8; 32]).unwrap_err();
        assert!(err.to_string().contains("nonce"));
    }

    #[test]
    fn serialized_form_contains_no_plaintext() {
        let enc = EncryptedSecret::encrypt("super-secret-password", &[3u8; 32]).unwrap();
        let json = serde_json::to_string(&enc).unwrap();
        assert!(!json.contains("super-secret-password"));
        assert!(json.contains("ciphertext"));
        assert!(json.contains("nonce"));
    }

    #[test]
    fn debug_is_redacted() {
        let enc = EncryptedSecret::encrypt("p", &[4u8; 32]).unwrap();
        assert_eq!(format!("{enc:?}"), "EncryptedSecret(***)");
    }
}
