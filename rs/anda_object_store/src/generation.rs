//! Generation identities and opaque compare-and-swap tokens.

use base64::{Engine, prelude::BASE64_URL_SAFE};
use rand::RngExt;
use sha3::Digest;
use std::sync::atomic::{AtomicU64, Ordering};

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub(crate) fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub(crate) fn new_generation() -> String {
    generation_at(unix_ms(), rand::rng().random())
}

fn generation_at(ms: u64, salt: u128) -> String {
    // Unique within a process even after clock rollback or RNG repetition.
    // Across processes the random 128-bit component provides isolation.
    let sequence = SEQUENCE
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
        .expect("generation sequence exhausted");
    format!("{ms:016x}-{salt:032x}-{sequence:016x}")
}

pub(crate) fn generation_timestamp_ms(value: &str) -> Option<u64> {
    let mut fields = value.split('-');
    let timestamp = fields.next()?;
    let salt = fields.next()?;
    let sequence = fields.next();
    let hex = |value: &str, len| value.len() == len && value.bytes().all(|b| b.is_ascii_hexdigit());
    if !hex(timestamp, 16) || fields.next().is_some() {
        return None;
    }
    match sequence {
        Some(sequence) if hex(salt, 32) && hex(sequence, 16) => {}
        None if hex(salt, 8) => {}
        _ => return None,
    }
    u64::from_str_radix(timestamp, 16).ok()
}

/// ETags identify commits, not payload contents. Older payload-derived tokens
/// remain valid opaque strings when read from existing metadata.
pub(crate) fn commit_e_tag(generation: &str) -> String {
    let mut hash = sha3::Sha3_256::new();
    hash.update(b"anda_object_store.commit.v2:");
    hash.update(generation.as_bytes());
    BASE64_URL_SAFE.encode(hash.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_randomness_and_clock_rollback_do_not_reuse_identity() {
        let a = generation_at(10, 0);
        let b = generation_at(10, 0);
        let c = generation_at(9, 0);
        assert_ne!(a, b);
        assert_ne!(b, c);
        assert_eq!(generation_timestamp_ms(&a), Some(10));
        assert_eq!(
            generation_timestamp_ms("0000000000000001-deadbeef"),
            Some(1)
        );
        for value in [
            "0000000000000001-not-hex!",
            "0000000000000001-00000000/child",
            "0000000000000001-00000000-extra",
        ] {
            assert_eq!(generation_timestamp_ms(value), None);
        }
    }
}
