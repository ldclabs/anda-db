//! # Cognitive Capsules
//!
//! A Capsule is the portable form of cognition: a digest-bearing artifact that
//! carries elements, the exact schema they are bound to, and where they came
//! from. It is the baseline interoperability model — `export → policy → capsule
//! → import` — rather than cross-Space graph traversal (Spec §6.3).
//!
//! ## What import must not do
//!
//! **Not reuse the source's ids.** Imported records get destination ids. An
//! element's id is Nexus-local (§7.1), and honouring a foreign one would let
//! an arriving Capsule claim an identity the destination already uses.
//!
//! **Not inherit the source's engine origin** (§2.5, §20.13). `_system.origin`
//! records what *this* runtime observed. The source's origin is preserved as
//! import provenance instead — a claim about where the data came from, which is
//! exactly what it is.
//!
//! **Not activate schema** (§41.3). A Capsule may name the packages its
//! records are bound to; it may not decide that this Space trusts them. Import
//! validates that the schema is available and refuses otherwise, because
//! importing records whose types cannot be resolved would store cognition with
//! no recoverable meaning.
//!
//! **Not treat a legacy export as a Capsule** (§103.8). Nothing here reads
//! KIP 1.x `UPSERT` scripts.

use anda_kip::{
    Capsule, CapsuleIntegrity, CapsuleKind, CapsuleManifest, CapsulePayload, CapsuleRecords,
    CapsuleSource, ElementKind, ExternalRef, ExternalRefKind, Json, KipError, KipErrorCode, Map,
    SchemaDependency,
};
use std::collections::{BTreeMap, BTreeSet};

pub mod merge;

use crate::id::ElementId;
use crate::kql::Context;

/// The format tag this engine writes and accepts.
pub const FORMAT: &str = "KIP-Cognitive-Capsule";

/// How far a referential closure walks.
///
/// A Capsule that referenced elements it did not carry would import as a graph
/// full of dangling edges, so the closure is on by default; the depth bound is
/// what stops one root from dragging in a whole Space.
pub const DEFAULT_DEPTH: usize = 3;

/// Builds a Capsule from a set of root elements.
pub async fn export(
    cx: &mut Context<'_>,
    roots: Vec<ElementId>,
    options: &Map<String, Json>,
) -> Result<Capsule, KipError> {
    let depth = options
        .get("provenance_depth")
        .and_then(Json::as_u64)
        .map(|d| d as usize)
        .unwrap_or(DEFAULT_DEPTH);
    // §40.3's vocabulary, spelled the way §40.3 spells it. An engine that
    // invented its own words for the same three shapes would make a Capsule's
    // own manifest unreadable to the destination that has to decide whether to
    // trust it.
    let closure = match options.get("closure").and_then(Json::as_str) {
        None => Closure::Referential,
        Some("referential") => Closure::Referential,
        Some("closed") => Closure::Closed,
        Some("selective") => Closure::Selective,
        Some(other) => {
            return Err(KipError::unsupported_capability(format!(
                "§40.3 declares a closure as \"closed\", \"referential\" or \"selective\"; \
                 this Capsule asks for {other:?}"
            )));
        }
    };
    // A proof profile promises a signature, and this engine holds no signing
    // keys. Emitting an unsigned Capsule under a profile that names one would
    // put the claim in the manifest and nothing behind it (§37.8).
    if let Some(profile) = options.get("proof_profile")
        && !profile.is_null()
    {
        return Err(KipError::unsupported_capability(format!(
            "this engine signs nothing, so it cannot produce a Capsule under the proof profile \
             {profile}; an exported Capsule is unsigned and says so"
        )));
    }
    let include_schema = options
        .get("include_schema")
        .and_then(Json::as_bool)
        .unwrap_or(true);
    if options.get("include_blobs").and_then(Json::as_bool) == Some(true) {
        return Err(KipError::unsupported_capability(
            "this engine stores no blobs, so it cannot include them in a Capsule",
        ));
    }

    let ids = match closure {
        Closure::Selective => roots.iter().copied().collect::<BTreeSet<_>>(),
        Closure::Referential | Closure::Closed => expand(cx, &roots, depth).await?,
    };

    let mut records = CapsuleRecords::default();
    let mut schema_refs: BTreeSet<String> = BTreeSet::new();
    for id in &ids {
        let Some(_) = cx.load(*id).await? else {
            continue;
        };
        // The redacted view, for the same reason SEARCH uses it: a field the
        // caller may not read must not leave the Space in a Capsule either
        // (§20.9). Elements it may not read at all were already dropped by
        // `load`, which is what makes the manifest's `partial` honest.
        // No fallback to the raw renderer: `load` caches a redacted view for
        // every element it admits, so an absent one means the element was not
        // admitted — and rendering it here would export exactly the fields the
        // redaction removed.
        let Some(rendered) = cx.cached_view(*id) else {
            continue;
        };
        collect_schema_refs(&rendered, &mut schema_refs);
        let rendered = rendered.as_ref().clone();
        match id.kind {
            ElementKind::Concept => records.concepts.push(rendered),
            ElementKind::Proposition => records.propositions.push(rendered),
            ElementKind::Assertion => records.assertions.push(rendered),
            ElementKind::Evidence => records.evidence.push(rendered),
            ElementKind::Activity => records.activities.push(rendered),
        }
    }

    // §40.1: what the records reference but do not carry is *declared*, not
    // dropped. A Capsule missing an edge and saying nothing imports as a graph
    // the destination believes is whole — an Assertion whose Evidence is
    // silently gone reads as an unsupported claim rather than a partial
    // import.
    let mut external_refs = Vec::new();
    for id in &ids {
        let Some(element) = cx.load(*id).await? else {
            continue;
        };
        for referenced in element.references() {
            if ids.contains(&referenced) {
                continue;
            }
            external_refs.push(ExternalRef {
                reference: referenced.to_string(),
                kind: ExternalRefKind::SourceElement,
                identity: Some(serde_json::json!({"id": referenced.to_string()})),
                reason: Some(format!(
                    "outside the {} closure of this export",
                    closure.as_str()
                )),
            });
        }
    }
    external_refs.sort_by(|a, b| a.reference.cmp(&b.reference));
    external_refs.dedup_by(|a, b| a.reference == b.reference);
    // A `closed` Capsule promises self-containment, so it fails rather than
    // shipping the promise with a hole in it. §40.3 names the three shapes so
    // a destination can tell them apart; one that claimed `closed` and carried
    // ExternalRefs would make the word mean nothing.
    if closure == Closure::Closed && !external_refs.is_empty() {
        return Err(KipError::constraint_violation(format!(
            "a \"closed\" Capsule carries everything it references, and this export would leave \
             {} reference(s) outside it — the first is {}. Raise `provenance_depth`, widen the \
             roots, or ask for a \"referential\" closure, which declares what it does not carry",
            external_refs.len(),
            external_refs[0].reference
        )));
    }

    let space = cx.store.get_space(&cx.space).await?;
    let payload = CapsulePayload {
        manifest: CapsuleManifest {
            kind: CapsuleKind::Snapshot,
            created_at: Some(crate::time::now()),
            // `partial` unless the closure ran and nothing was dropped: a
            // Capsule that claimed completeness it does not have would import
            // as a graph the destination believes is whole.
            completeness: Some(closure.completeness().to_string()),
            closure: Some(serde_json::json!({"mode": closure.as_str(), "provenance_depth": depth})),
        },
        source: CapsuleSource {
            nexus_id: Some(cx.store.db.name().to_string()),
            space_ref: Some(space.space_id.clone()),
            snapshot_seq: Some(space.seq),
            base_seq: None,
            target_seq: Some(space.seq),
            schema_environment_version: Some(cx.env.version),
        },
        // §20.4: the exact refs travel with the records. A Capsule that
        // exported local names would arrive meaning whatever the destination
        // happens to call them.
        schema: if include_schema {
            schema_dependencies(cx, &schema_refs)
        } else {
            vec![]
        },
        records,
        external_refs,
        blobs: vec![],
        handling: None,
        extensions: Map::new(),
    };

    let digest = payload_digest(&payload)?;
    Ok(Capsule::new(
        payload,
        CapsuleIntegrity {
            content_digest: digest,
            // No proofs: this engine signs nothing, and an empty proof list is
            // an honest "unsigned" rather than a claim of provenance.
            proofs: vec![],
        },
    ))
}

/// How much of the graph around the roots a Capsule carries (§40.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Closure {
    /// Self-contained: everything the records reference travels with them, and
    /// an export that would leave a dangling edge fails instead.
    Closed,
    /// The references are walked to the declared depth; whatever falls outside
    /// is declared as an `ExternalRef` rather than dropped.
    Referential,
    /// The roots and nothing else. Honest about being a selection.
    Selective,
}

impl Closure {
    fn as_str(self) -> &'static str {
        match self {
            Closure::Closed => "closed",
            Closure::Referential => "referential",
            Closure::Selective => "selective",
        }
    }

    fn completeness(self) -> &'static str {
        match self {
            Closure::Closed => "closed",
            Closure::Referential => "referential_closure",
            Closure::Selective => "roots_only",
        }
    }
}

/// Walks the referential closure out from the roots.
async fn expand(
    cx: &mut Context<'_>,
    roots: &[ElementId],
    depth: usize,
) -> Result<BTreeSet<ElementId>, KipError> {
    let mut seen: BTreeSet<ElementId> = roots.iter().copied().collect();
    let mut frontier: Vec<ElementId> = roots.to_vec();

    for _ in 0..depth {
        let mut next = Vec::new();
        for id in std::mem::take(&mut frontier) {
            let Some(element) = cx.load(id).await? else {
                continue;
            };
            for referenced in element.references() {
                if seen.insert(referenced) {
                    next.push(referenced);
                }
            }
        }
        if next.is_empty() {
            break;
        }
        cx.charge(next.len())?;
        frontier = next;
    }
    Ok(seen)
}

fn collect_schema_refs(rendered: &Json, into: &mut BTreeSet<String>) {
    for key in ["schema_ref", "predicate_ref"] {
        if let Some(reference) = rendered.get(key).and_then(Json::as_str) {
            into.insert(reference.to_string());
        }
    }
    for key in ["facets", "structural"] {
        if let Some(map) = rendered.get(key).and_then(Json::as_object) {
            into.extend(map.keys().cloned());
        }
    }
}

fn schema_dependencies(cx: &Context<'_>, refs: &BTreeSet<String>) -> Vec<SchemaDependency> {
    let mut packages: BTreeMap<String, SchemaDependency> = BTreeMap::new();
    for reference in refs {
        let Ok(symbol) = reference.parse::<crate::schema::SymbolRef>() else {
            continue;
        };
        let package_ref = symbol.package.to_string();
        packages
            .entry(package_ref.clone())
            .or_insert_with(|| SchemaDependency {
                package: symbol.package.package_id.clone(),
                version: symbol.package.version.to_string(),
                // The digest the destination checks the artifact against, when
                // it has the artifact at all.
                digest: cx
                    .env
                    .artifact(&package_ref)
                    .and_then(|a| a.integrity.as_ref())
                    .map(|integrity| integrity.content_digest.clone()),
            });
    }
    packages.into_values().collect()
}

/// The content digest over a Capsule payload (§37.7).
///
/// Canonicalized by [`anda_kip::canonical_json`] — RFC 8785, keys ordered by
/// UTF-16 code units — rather than by this engine's own encoder. Portable
/// artifact identity is cryptographic: two implementations that disagree about
/// which bytes a Capsule *is* produce different digests for the same
/// cognition, and every `VERIFY CAPSULE` across that boundary then fails for a
/// reason neither side can see. The Schema Package registry keeps its own
/// engine-local digest, which is correct — a package artifact never crosses
/// between engines under this digest, and a Capsule always does.
pub fn payload_digest(payload: &CapsulePayload) -> Result<String, KipError> {
    let value = serde_json::to_value(payload)
        .map_err(|err| KipError::internal_error(format!("a Capsule failed to encode: {err}")))?;
    let canonical = anda_kip::canonical_json(&value);
    use sha3::{Digest, Sha3_256};
    Ok(format!(
        "{DIGEST_PROFILE}:{}",
        hex::encode(Sha3_256::digest(canonical.as_bytes()))
    ))
}

/// What an import did, or would do.
#[derive(Debug, Default)]
pub struct ImportReport {
    /// Source element id → destination element id.
    pub mapping: BTreeMap<String, String>,
    /// Elements written, by kind.
    pub counts: BTreeMap<String, usize>,
    /// Non-fatal caveats.
    pub warnings: Vec<String>,
}

impl ImportReport {
    /// The report body a caller receives.
    pub fn to_json(&self, dry_run: bool) -> Json {
        serde_json::json!({
            "imported": !dry_run,
            "counts": self.counts,
            "identity_map": self.mapping,
            "warnings": self.warnings,
        })
    }
}

/// The digest algorithm this engine computes over a Capsule payload.
pub const DIGEST_PROFILE: &str = "sha3-256";

/// Refuses a Capsule digested under an algorithm this engine cannot compute.
///
/// Reported as an unsupported profile rather than as a digest mismatch, and
/// the difference matters: `DigestMismatch` says *this artifact was modified*,
/// which is an accusation. An artifact written by an engine that hashes its
/// canonical bytes differently is intact and unreadable here, and telling an
/// operator it was tampered with would send them hunting for an attacker that
/// does not exist (§86.4).
fn check_digest_profile(declared: &str) -> Result<(), KipError> {
    let profile = declared.split_once(':').map(|(algorithm, _)| algorithm);
    match profile {
        Some(DIGEST_PROFILE) => Ok(()),
        Some(other) => Err(KipError::unsupported_capability(format!(
            "this Capsule is digested under {other:?} and this engine computes {DIGEST_PROFILE:?} \
             over RFC 8785 canonical JSON; it cannot check the artifact's integrity, which is not \
             the same as finding it corrupt"
        ))),
        None => Err(KipError::new(
            KipErrorCode::CapsuleValidationFailed,
            format!(
                "this Capsule's content digest {declared:?} names no algorithm; a digest whose \
                 profile is unstated cannot be checked"
            ),
        )),
    }
}

/// Checks a Capsule's frame and digest without importing it.
///
/// Integrity, not legality: this says the artifact is what it claims to be, and
/// says nothing about whether its records would be accepted.
pub fn verify(capsule: &Capsule) -> Result<Json, KipError> {
    capsule.validate_frame()?;
    check_digest_profile(&capsule.integrity.content_digest)?;
    let recomputed = payload_digest(&capsule.payload)?;
    let matches = recomputed == capsule.integrity.content_digest;
    if !matches {
        return Err(KipError::new(
            KipErrorCode::DigestMismatch,
            format!(
                "this Capsule declares the digest {} and its payload digests to {recomputed}; it \
                 was modified after it was written",
                capsule.integrity.content_digest
            ),
        ));
    }
    Ok(serde_json::json!({
        "valid": true,
        "content_digest": recomputed,
        "digest_profile": "sha3-256 over RFC 8785 canonical JSON (§37.7)",
        // An unsigned Capsule proves nothing about who wrote it. Saying so is
        // the difference between "intact" and "trustworthy".
        "signed": !capsule.integrity.proofs.is_empty(),
        "records": capsule.payload.records.len(),
        "note": "a matching digest means the artifact is intact, not that its claims are true",
    }))
}

/// Imports a Capsule into a Space.
///
/// Two-phase by necessity: every schema reference is resolved before anything
/// is written (§41.2), because a half-imported graph bound to types the
/// destination cannot resolve is cognition with no recoverable meaning.
pub async fn import(
    nexus: &crate::CognitiveNexus,
    capsule: &Capsule,
    space_id: &str,
    dry_run: bool,
    auth: crate::governance::AuthContext,
    isolate: bool,
) -> Result<ImportReport, KipError> {
    capsule.validate_frame()?;
    let mut report = ImportReport::default();

    // Integrity first (§41.2: VERIFY → VALIDATE → PREVIEW → import). A
    // modified artifact must not reach identity resolution: everything after
    // this point trusts the record ids to mean what the digest covers.
    check_digest_profile(&capsule.integrity.content_digest)?;
    let digest = payload_digest(&capsule.payload)?;
    if digest != capsule.integrity.content_digest {
        return Err(KipError::new(
            KipErrorCode::DigestMismatch,
            format!(
                "this Capsule declares the digest {} and its payload digests to {digest}; it was \
                 modified after it was written, or written by an engine using a different \
                 canonicalization",
                capsule.integrity.content_digest
            ),
        ));
    }

    let env = nexus.store.schema_environment(space_id).await?;
    for dependency in &capsule.payload.schema {
        let package_ref = format!("{}@{}", dependency.package, dependency.version);
        let Some(artifact) = env.artifact(&package_ref) else {
            // Refused, not downgraded: importing records whose types cannot be
            // resolved would store cognition nobody can read back. Activating
            // the package on the Capsule's say-so is exactly what §88 forbids.
            return Err(KipError::new(
                KipErrorCode::SchemaPackageUnavailable,
                format!(
                    "this Capsule's records are bound to {package_ref}, which is not in this \
                     Space's Schema Environment; install and activate it first — an import \
                     cannot activate schema on the artifact's own say-so"
                ),
            ));
        };
        if let (Some(declared), Some(installed)) = (
            dependency.digest.as_ref(),
            artifact
                .integrity
                .as_ref()
                .map(|integrity| &integrity.content_digest),
        ) && declared != installed
        {
            return Err(KipError::new(
                KipErrorCode::DigestMismatch,
                format!(
                    "this Capsule was written against a {package_ref} whose digest was \
                     {declared}, and this Space has {installed}; the same version means the same \
                     content, so one of them is not what it claims"
                ),
            ));
        }
    }

    if capsule.integrity.proofs.is_empty() {
        report.warnings.push(
            "this Capsule is unsigned: its stated source is a claim the destination cannot check"
                .to_string(),
        );
    }
    // §39.5: none of the source's trust travels with its records. This engine
    // has no trust model to apply either, and says so rather than letting the
    // absence read as acceptance.
    report.warnings.push(
        "imported records carry no trust from their source, and this engine has no trust model \
         to apply in its place: what arrived is a record of what somebody else claimed"
            .to_string(),
    );

    if dry_run {
        return merge::preview(&nexus.store, capsule, space_id, &digest, report).await;
    }
    merge::merge(
        &nexus.store,
        capsule,
        space_id,
        &digest,
        report,
        auth,
        isolate,
    )
    .await
}

/// Parses a Capsule artifact.
pub fn parse(source: &str) -> Result<Capsule, KipError> {
    let capsule: Capsule = serde_json::from_str(source).map_err(|err| {
        KipError::new(
            KipErrorCode::ArtifactParseError,
            format!("this is not a readable Cognitive Capsule: {err}"),
        )
    })?;
    capsule.validate_frame()?;
    Ok(capsule)
}

/// Reports what a Capsule artifact contains, without importing it (§63.3).
///
/// Inspection rather than verification: this is the manifest, the source
/// identity, the schema it was written against and how much of each kind it
/// carries. `VERIFY CAPSULE` is what checks the digest, and the two are kept
/// apart on purpose — describing an artifact must not read as vouching for it.
///
/// It answers from the parsed artifact alone. Nothing here touches the Space,
/// so an operator can look at a Capsule before deciding whether this Brain
/// should see it at all.
pub fn describe(source: &str) -> Result<Json, KipError> {
    let capsule = parse(source)?;
    let payload = &capsule.payload;
    Ok(serde_json::json!({
        "format": capsule.format,
        "manifest": payload.manifest,
        "source": payload.source,
        "schema": payload.schema,
        "counts": {
            "concept": payload.records.concepts.len(),
            "proposition": payload.records.propositions.len(),
            "assertion": payload.records.assertions.len(),
            "evidence": payload.records.evidence.len(),
            "activity": payload.records.activities.len(),
        },
        "external_refs": payload.external_refs.len(),
        "blobs": payload.blobs.len(),
        "integrity": {
            "content_digest": capsule.integrity.content_digest,
            // Stated separately from the digest, because they answer different
            // questions: the digest says the bytes are intact, a signature
            // would say who stood behind them, and neither says the claims are
            // true (§37.8).
            "signed": !capsule.integrity.proofs.is_empty(),
        },
        "note": "this describes the artifact; VERIFY CAPSULE checks its digest, and neither \
                 makes its claims true",
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_modified_capsule_fails_its_own_digest() {
        let payload = CapsulePayload {
            manifest: CapsuleManifest {
                kind: CapsuleKind::Snapshot,
                created_at: Some("2026-08-16T00:00:00.000Z".into()),
                completeness: Some("roots_only".into()),
                closure: None,
            },
            records: CapsuleRecords {
                concepts: vec![serde_json::json!({"id": "C-1", "name": "Alice"})],
                ..Default::default()
            },
            ..Default::default()
        };
        let digest = payload_digest(&payload).unwrap();
        let capsule = Capsule::new(
            payload.clone(),
            CapsuleIntegrity {
                content_digest: digest,
                proofs: vec![],
            },
        );
        let report = verify(&capsule).unwrap();
        assert_eq!(report["valid"], true);
        // Intact is not trustworthy, and the report says which one it means.
        assert_eq!(report["signed"], false);
        assert!(
            report["note"]
                .as_str()
                .unwrap()
                .contains("not that its claims are true")
        );

        let mut tampered = capsule.clone();
        tampered.payload.records.concepts[0]["name"] = Json::from("Mallory");
        let err = verify(&tampered).unwrap_err();
        assert_eq!(err.name(), "DigestMismatch");
    }

    /// The cross-engine pin for the Capsule content digest.
    ///
    /// `ts/kip-do`'s `test/foundation.test.ts` has the same literal over the
    /// same value. A Capsule is the one artifact that leaves this engine and
    /// is checked by another, so the canonicalization *and* the algorithm are
    /// part of the contract — and if either side drifts, one of these two
    /// tests goes red instead of every cross-engine `VERIFY CAPSULE` failing
    /// for a reason neither side can see.
    ///
    /// The members are deliberately out of order: canonicalization is what has
    /// to agree, not the writer's key order.
    #[test]
    fn a_capsule_payload_digests_the_same_in_both_engines() {
        let payload = serde_json::json!({
            "records": {"concepts": [{"id": "C-1", "kind": "concept", "name": "Alice"}]},
            "manifest": {"kind": "snapshot", "completeness": "referential_closure"},
            "source": {"space_ref": "kip:space:default", "snapshot_seq": 3},
        });
        let canonical = anda_kip::canonical_json(&payload);
        assert_eq!(
            canonical,
            r#"{"manifest":{"completeness":"referential_closure","kind":"snapshot"},"records":{"concepts":[{"id":"C-1","kind":"concept","name":"Alice"}]},"source":{"snapshot_seq":3,"space_ref":"kip:space:default"}}"#
        );
        use sha3::{Digest, Sha3_256};
        assert_eq!(
            hex::encode(Sha3_256::digest(canonical.as_bytes())),
            "fa8db2155f56bf075fe25e59dda1ba01c9a87500baa77d7f3d5c79af6558e567"
        );
    }

    #[test]
    fn the_digest_covers_the_payload_and_not_the_proofs() {
        // A signature is added after the content is frozen, so adding one must
        // not invalidate the digest it signs.
        let payload = CapsulePayload::default();
        let digest = payload_digest(&payload).unwrap();
        let mut capsule = Capsule::new(
            payload,
            CapsuleIntegrity {
                content_digest: digest.clone(),
                proofs: vec![],
            },
        );
        capsule.integrity.proofs.push(anda_kip::CapsuleProof {
            proof_type: "signature".into(),
            suite: None,
            verification_method: None,
            signature: Some("...".into()),
        });
        assert_eq!(payload_digest(&capsule.payload).unwrap(), digest);
    }
}
