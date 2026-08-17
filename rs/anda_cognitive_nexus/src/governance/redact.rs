//! # What a permitted read is allowed to contain
//!
//! Two narrowings, applied to the rendered view before anything reads a field
//! from it:
//!
//! ```text
//! field mask       a Grant may allow `read` over some members only (§109)
//! raw origin       `_system.origin` needs its own permission (§110)
//! ```
//!
//! ## Why the mask is applied at load, not at projection
//!
//! A redacted field has to be invisible to `FILTER` and `ORDER BY` as well as
//! to the projection list. Otherwise
//!
//! ```text
//! FIND(?c) WHERE { ?c CONCEPT {type: "Person"} FILTER(?c.attributes.salary > 200000) }
//! ```
//!
//! answers the question the mask was meant to refuse — the rows come back
//! without the field, and their *membership* is the disclosure. So the view
//! cached for one query is redacted once, on the way in, and every later stage
//! reads the same narrowed object.
//!
//! ## Why identity survives every mask
//!
//! An element that reached this point is one the caller may read. Stripping its
//! `id` or `kind` would produce a row nothing can join on or cite, which is not
//! a safer answer — it is an unusable one. A mask narrows *content*; membership
//! was already decided by the visibility check.

use anda_kip::Json;

use super::rows::AuthorityConstraints;

/// The members a mask never removes.
///
/// `id` and `kind` are how a caller refers to what it just read, and
/// `space_id` is what tells it which Brain answered. A Grant that listed only
/// `name` still means "name, of a thing you can name back".
const ALWAYS_VISIBLE: &[&str] = &["id", "kind", "space_id"];

/// Narrows a rendered element view to what this decision permits.
///
/// `may_read_origin` comes from the `read_raw_origin` permission rather than
/// from the field mask, because engine origin is a different disclosure from
/// content: it names the Principal that wrote the element and the channel it
/// arrived on, which is operational information about the deployment rather
/// than about the memory (§110).
pub fn apply(view: &mut Json, constraints: &AuthorityConstraints, may_read_origin: bool) {
    if !may_read_origin && let Some(system) = view.get_mut("_system") {
        redact_origin(system);
    }
    if constraints.fields.is_empty() {
        return;
    }
    let Some(object) = view.as_object_mut() else {
        return;
    };
    object.retain(|key, _| {
        ALWAYS_VISIBLE.contains(&key.as_str())
            || constraints.fields.iter().any(|allowed| allowed == key)
    });
}

/// Replaces engine origin with the fact that there was one.
///
/// Removing `origin` entirely would say "this element has no recorded origin",
/// which is a claim — and a false one, since every element here has one. What
/// is withheld is *whose*: the reader learns the write was attributed, not to
/// whom (§110).
fn redact_origin(system: &mut Json) {
    let Some(object) = system.as_object_mut() else {
        return;
    };
    if !object.contains_key("origin") {
        return;
    }
    object.insert(
        "origin".to_string(),
        serde_json::json!({"redacted": "read_raw_origin"}),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn element() -> Json {
        serde_json::json!({
            "id": "C-1",
            "kind": "concept",
            "space_id": "kip:space:default",
            "name": "Alice",
            "attributes": {"salary": 210000},
            "_system": {
                "version": 1,
                "origin": {"principal_id": "kip:principal:agent", "channel": "mcp"}
            }
        })
    }

    #[test]
    fn an_unrestricted_decision_changes_nothing() {
        let mut view = element();
        apply(&mut view, &AuthorityConstraints::default(), true);
        assert_eq!(view, element());
    }

    #[test]
    fn a_field_mask_keeps_identity_and_drops_the_rest() {
        let mut view = element();
        apply(
            &mut view,
            &AuthorityConstraints {
                fields: vec!["name".into()],
                ..Default::default()
            },
            true,
        );
        assert_eq!(view["name"], "Alice");
        assert_eq!(view["id"], "C-1");
        assert_eq!(view["kind"], "concept");
        assert!(view.get("attributes").is_none());
        // Even `_system` goes, unless the mask names it: a version counter is
        // content the Grant did not allow.
        assert!(view.get("_system").is_none());
    }

    #[test]
    fn origin_is_withheld_rather_than_erased() {
        // Removing it would say "no origin was recorded", which is false for
        // every element in this engine.
        let mut view = element();
        apply(&mut view, &AuthorityConstraints::default(), false);
        assert_eq!(view["_system"]["origin"]["redacted"], "read_raw_origin");
        assert!(view["_system"]["origin"].get("principal_id").is_none());
        assert_eq!(view["_system"]["version"], 1);
    }

    #[test]
    fn a_mask_that_names_system_still_hides_origin_without_the_permission() {
        // The two narrowings are independent: a Grant that allows reading
        // `_system` has not thereby allowed reading who wrote the element.
        let mut view = element();
        apply(
            &mut view,
            &AuthorityConstraints {
                fields: vec!["_system".into()],
                ..Default::default()
            },
            false,
        );
        assert_eq!(view["_system"]["origin"]["redacted"], "read_raw_origin");
        assert!(view.get("name").is_none());
    }
}
