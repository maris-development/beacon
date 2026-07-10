//! Argon2 password hashing helpers.
//!
//! Passwords are stored as Argon2 PHC strings (salt + parameters embedded), never in plain text.

use argon2::{
    password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};

/// Hashes a plaintext password into an Argon2 PHC string.
pub fn hash_password(password: &str) -> anyhow::Result<String> {
    let salt = SaltString::generate(&mut OsRng);
    let hash = Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map_err(|err| anyhow::anyhow!("failed to hash password: {err}"))?;
    Ok(hash.to_string())
}

/// Verifies a plaintext password against a previously stored Argon2 PHC string.
pub fn verify_password(stored_hash: &str, candidate: &str) -> bool {
    let Ok(parsed) = PasswordHash::new(stored_hash) else {
        return false;
    };
    Argon2::default()
        .verify_password(candidate.as_bytes(), &parsed)
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_then_verify_roundtrip() {
        let hash = hash_password("s3cret").unwrap();
        assert!(verify_password(&hash, "s3cret"));
        assert!(!verify_password(&hash, "wrong"));
    }

    #[test]
    fn hashes_are_salted_and_not_plaintext() {
        let a = hash_password("same").unwrap();
        let b = hash_password("same").unwrap();
        assert_ne!(
            a, b,
            "salt should make identical passwords hash differently"
        );
        assert!(!a.contains("same"));
    }

    #[test]
    fn malformed_hash_does_not_verify() {
        assert!(!verify_password("not-a-phc-string", "whatever"));
    }
}
