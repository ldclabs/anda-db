//! # Mnemonic strength (Spec §59.1, Profile §6.1)
//!
//! Decay is computed, not written. `MnemonicState.memory_strength` is the last
//! explicitly written base, `last_metabolized_at` its anchor and
//! `strength_policy` a pinned policy artifact; the read-only member
//! `effective_strength` is derived from the three when a read is evaluated
//! and never written back. Idle memory costs no writes and no Change
//! Envelopes.
//!
//! This engine knows one policy, the standard `kip:strength-half-life-30d`.
//! A missing base, anchor or pin, a policy it does not know, or a pin whose
//! digest is not that artifact's leaves the value `null` — unknown — and
//! never falls back to another policy or a default such as `0.5`.

use anda_kip::{Json, Map};
use std::sync::LazyLock;

/// A half-life policy, read from its artifact.
struct HalfLife {
    policy_id: String,
    content_digest: String,
    half_life_ms: f64,
}

static STANDARD: LazyLock<Option<HalfLife>> = LazyLock::new(|| {
    let artifact: Json = serde_json::from_str(crate::profiles::STRENGTH_HALF_LIFE_30D).ok()?;
    (artifact["method"]["kind"] == "half_life").then_some(())?;
    Some(HalfLife {
        policy_id: artifact["policy_id"].as_str()?.to_string(),
        content_digest: artifact["integrity"]["content_digest"]
            .as_str()?
            .to_string(),
        half_life_ms: artifact["method"]["half_life_ms"].as_f64()?,
    })
});

/// Whether a Facet key names the Profile's `MnemonicState`, in any version.
fn is_mnemonic_state(key: &str) -> bool {
    key.starts_with(crate::profiles::COGNITIVE_MEMORY_ID)
        && key.ends_with("/MnemonicState")
        && crate::schema::lineage_of(key)
            == format!("{}/MnemonicState", crate::profiles::COGNITIVE_MEMORY_ID)
}

/// `effective_strength` for one `MnemonicState` at the instant `now`.
pub(crate) fn effective(state: &Map<String, Json>, now: &str) -> Option<f64> {
    let policy = STANDARD.as_ref()?;
    let base = state.get("memory_strength")?.as_f64()?;
    let anchor = crate::time::parse(state.get("last_metabolized_at")?.as_str()?).ok()?;
    let pin = state.get("strength_policy")?;
    if pin["artifact_ref"].as_str() != Some(policy.policy_id.as_str())
        || pin["content_digest"].as_str() != Some(policy.content_digest.as_str())
    {
        return None;
    }
    let now = crate::time::parse(now).ok()?;
    // Before its anchor the value is the base.
    let elapsed = (now - anchor).num_milliseconds().max(0) as f64;
    Some(base * (-elapsed / policy.half_life_ms).exp2())
}

/// Adds the computed `effective_strength` to every `MnemonicState` Facet of a
/// rendered view, evaluated at `now` — the read's own instant, never its
/// `FOR TIME` (Profile §6.1).
pub(crate) fn compute(view: &mut Json, now: &str) {
    let Some(facets) = view.get_mut("facets").and_then(Json::as_object_mut) else {
        return;
    };
    for (key, state) in facets.iter_mut() {
        if !is_mnemonic_state(key) {
            continue;
        }
        if let Some(state) = state.as_object_mut() {
            let value = effective(state, now).map_or(Json::Null, Json::from);
            state.insert("effective_strength".to_string(), value);
        }
    }
}

/// Removes what [`compute`] added: a computed member never leaves the read
/// that evaluated it, so an export carries the state it is computed from and
/// the importer computes its own (§18.2).
pub(crate) fn strip(view: &mut Json) {
    let Some(facets) = view.get_mut("facets").and_then(Json::as_object_mut) else {
        return;
    };
    for (key, state) in facets.iter_mut() {
        if is_mnemonic_state(key)
            && let Some(state) = state.as_object_mut()
        {
            state.remove("effective_strength");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(value: Json) -> Map<String, Json> {
        value.as_object().cloned().unwrap()
    }

    fn pin() -> Json {
        serde_json::json!({
            "artifact_ref": "kip:strength-half-life-30d",
            "content_digest": "sha256:a50a89b83f937c97cabf0f8371cccfd326f4fdd438b6d7b9ead507d77927b227",
        })
    }

    #[test]
    fn one_half_life_halves_the_base() {
        let s = state(serde_json::json!({
            "memory_strength": 0.8,
            "last_metabolized_at": "2026-01-01T00:00:00.000Z",
            "strength_policy": pin(),
        }));
        let value = effective(&s, "2026-01-31T00:00:00.000Z").unwrap();
        assert!((value - 0.4).abs() < 1e-12, "{value}");
        // Before its anchor it is the base.
        assert_eq!(effective(&s, "2025-06-01T00:00:00.000Z"), Some(0.8));
    }

    #[test]
    fn a_missing_or_foreign_pin_is_unknown() {
        let mut s = state(serde_json::json!({
            "memory_strength": 0.8,
            "last_metabolized_at": "2026-01-01T00:00:00.000Z",
        }));
        assert_eq!(effective(&s, "2026-02-01T00:00:00.000Z"), None);
        let mut other = pin();
        other["content_digest"] = Json::from("sha256:00");
        s.insert("strength_policy".into(), other);
        assert_eq!(effective(&s, "2026-02-01T00:00:00.000Z"), None);
        s.insert("strength_policy".into(), pin());
        s.remove("last_metabolized_at");
        assert_eq!(effective(&s, "2026-02-01T00:00:00.000Z"), None);
    }
}
