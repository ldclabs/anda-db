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
    let mut omitted = Vec::new();
    let mut included = BTreeSet::new();
    let mut source_control = Map::new();
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
        let mut rendered = rendered.as_ref().clone();
        if let Some(object) = rendered.as_object_mut() {
            object.remove("canonical_subject");
            object.remove("canonical_object");
        }
        crate::projection::strength::strip(&mut rendered);
        if let Some(governance) = rendered["governance"].as_object_mut() {
            let extra: Map<String, Json> = governance
                .iter()
                .filter(|(k, _)| {
                    !matches!(
                        k.as_str(),
                        "classification" | "authority_class" | "policy_ref"
                    )
                })
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            governance.retain(|k, _| {
                matches!(
                    k.as_str(),
                    "classification" | "authority_class" | "policy_ref"
                )
            });
            if !extra.is_empty() {
                source_control.insert(id.to_string(), Json::Object(extra));
            }
        }
        if crate::schema::contracts::validate_value(
            &serde_json::json!({"$ref":"urn:kip:2.0:schema:element"}),
            &rendered,
        )
        .is_err()
        {
            if closure == Closure::Closed {
                return Err(KipError::constraint_violation(
                    "closed Capsule requires complete, visible canonical element fields",
                ));
            }
            omitted.push(ExternalRef {
                reference: id.to_string(),
                kind: if rendered["_system"]["origin"]["redacted"] == true
                    || rendered.get("_system").is_none()
                {
                    ExternalRefKind::Redacted
                } else {
                    ExternalRefKind::Unavailable
                },
                identity: Some(serde_json::json!({"id": id.to_string()})),
            });
            continue;
        }
        included.insert(*id);
        records.0.push(rendered);
    }

    // §40.1: what the records reference but do not carry is *declared*, not
    // dropped. A Capsule missing an edge and saying nothing imports as a graph
    // the destination believes is whole — an Assertion whose Evidence is
    // silently gone reads as an unsupported claim rather than a partial
    // import.
    let mut external_refs = omitted;
    for id in &ids {
        let Some(element) = cx.load(*id).await? else {
            continue;
        };
        for referenced in element.references() {
            if included.contains(&referenced) {
                continue;
            }
            external_refs.push(ExternalRef {
                reference: referenced.to_string(),
                kind: ExternalRefKind::SourceElement,
                identity: Some(serde_json::json!({"id": referenced.to_string()})),
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
            roots: ids.iter().map(ToString::to_string).collect(),
            base_seq: None,
            target_seq: None,
            closure: closure.as_str().into(),
        },
        source: CapsuleSource {
            space_ref: Some(space.space_id.clone()),
            snapshot_seq: Some(space.seq),
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
        changes: None,
        external_refs,
        blobs: BTreeMap::new(),
        handling: anda_kip::CapsuleHandling {
            extra: if source_control.is_empty() {
                Map::new()
            } else {
                Map::from_iter([("anda/source_control".into(), Json::Object(source_control))])
            },
        },
    };

    let digest = payload_digest(&payload)?;
    Ok(Capsule::new(
        payload,
        CapsuleIntegrity {
            digest_profile: "kip-jcs-safe-v1".into(),
            content_digest: digest,
            // No proofs: this engine signs nothing, and an empty proof list is
            // an honest "unsigned" rather than a claim of provenance.
            proofs: vec![],
            covers: None,
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
                    .and_then(|a| package_digest(a).ok()),
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
    let canonical = anda_kip::try_canonical_json(
        &serde_json::json!({"format": anda_kip::CAPSULE_FORMAT, "format_version": anda_kip::CAPSULE_VERSION, "payload": value}),
    )?;
    use sha2::{Digest, Sha256};
    Ok(format!(
        "{DIGEST_PROFILE}:{}",
        hex::encode(Sha256::digest(canonical.as_bytes()))
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
pub const DIGEST_PROFILE: &str = "sha256";

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
        "digest_profile": "kip-jcs-safe-v1",
        // An unsigned Capsule proves nothing about who wrote it. Saying so is
        // the difference between "intact" and "trustworthy".
        "signed": !capsule.integrity.proofs.is_empty(),
        "records": capsule.payload.records.len(),
        "note": "a matching digest means the artifact is intact, not that its claims are true",
    }))
}

/// One source draft symbol an import maps (Capsule companion §41.7, Spec
/// §20.16): `from` is the source's exact reference, `to` a destination symbol
/// of the same `kind` (`ConceptType` or `PredicateType`) — one of the
/// destination's own draft symbols or an installed package's.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct SymbolMapping {
    pub kind: String,
    pub from: String,
    pub to: String,
}

/// Rewrites the source draft symbols a Capsule's records use onto the
/// destination symbols `symbols` maps them to, or `None` when it uses none.
///
/// A source's `kip://local/...` vocabulary stays source-namespaced: it is
/// never matched by name against the destination's draft symbols, and an
/// unmapped one fails `SchemaPackageUnavailable` naming every unmapped symbol.
fn map_draft_symbols(
    env: &crate::schema::SchemaEnvironment,
    capsule: &Capsule,
    symbols: &[SymbolMapping],
) -> Result<Option<Capsule>, KipError> {
    use crate::schema::symbol::SymbolKind;
    let prefix = format!("{}/", anda_kip::DRAFT_PACKAGE_REF);
    let slots = [
        ("schema_ref", SymbolKind::ConceptType),
        ("predicate_ref", SymbolKind::PredicateType),
    ];
    let mut mapped = capsule.clone();
    let mut changed = false;
    let mut unmapped: BTreeSet<String> = BTreeSet::new();
    for record in &mut mapped.payload.records.0 {
        for (field, kind) in slots {
            let Some(from) = record.get(field).and_then(Json::as_str) else {
                continue;
            };
            if !from.starts_with(&prefix) {
                continue;
            }
            let kind_name = crate::schema::env::draft_kind_name(kind);
            let Some(entry) = symbols
                .iter()
                .find(|entry| entry.kind == kind_name && entry.from == from)
            else {
                unmapped.insert(format!("{kind_name} {from}"));
                continue;
            };
            let to = env
                .resolve_symbol(kind, &entry.to, crate::schema::Intent::Write)?
                .to_string();
            record[field] = Json::String(to);
            changed = true;
        }
    }
    if !unmapped.is_empty() {
        return Err(KipError::new(
            KipErrorCode::SchemaPackageUnavailable,
            format!(
                "this Capsule uses source draft symbols the import does not map: {}; a source's \
                 draft vocabulary is never matched by name, so map each to a destination symbol \
                 of the same kind (§20.16, §41.7)",
                unmapped.into_iter().collect::<Vec<_>>().join(", ")
            ),
        ));
    }
    Ok(changed.then_some(mapped))
}

/// Imports a Capsule into a Space.
///
/// Two-phase by necessity: every schema reference is resolved before anything
/// is written (§41.2), because a half-imported graph bound to types the
/// destination cannot resolve is cognition with no recoverable meaning.
/// `symbols` maps the source draft symbols the records use (§41.7).
pub async fn import(
    nexus: &crate::CognitiveNexus,
    capsule: &Capsule,
    space_id: &str,
    dry_run: bool,
    auth: crate::governance::AuthContext,
    isolate: bool,
    symbols: &[SymbolMapping],
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
        // The source's draft vocabulary is its own, whatever this Space's
        // draft holds; the records using it are mapped below (§20.16).
        if dependency.package == anda_kip::DRAFT_PACKAGE_ID {
            continue;
        }
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
        let installed_digest = package_digest(artifact)?;
        if let Some(declared) = &dependency.digest
            && declared != &installed_digest
        {
            return Err(KipError::new(
                KipErrorCode::DigestMismatch,
                format!(
                    "this Capsule was written against a {package_ref} whose digest was \
                     {declared}, and this Space has {installed_digest}; the same version means the same \
                     content, so one of them is not what it claims"
                ),
            ));
        }
    }

    let mapped = map_draft_symbols(&env, capsule, symbols)?;
    let capsule = mapped.as_ref().unwrap_or(capsule);

    if capsule.integrity.proofs.is_empty() {
        report.warnings.push(
            "this Capsule is unsigned: its stated source is a claim the destination cannot check"
                .to_string(),
        );
    }
    // §39.5: destination control state governs imported records.
    report.warnings.push(
        "imported records carry no source trust or local standing; destination policies govern their use"
            .to_string(),
    );

    if dry_run {
        return merge::preview(&nexus.store, &env, capsule, space_id, &digest, report).await;
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
    let value = anda_kip::parse_canonical_json(source)
        .map_err(|e| KipError::new(KipErrorCode::ArtifactParseError, e.message))?;
    let capsule: Capsule = serde_json::from_value(value.clone()).map_err(|err| {
        KipError::new(
            KipErrorCode::ArtifactParseError,
            format!("this is not a readable Cognitive Capsule: {err}"),
        )
    })?;
    capsule.validate_frame()?;
    crate::schema::contracts::validate_value(
        &serde_json::json!({"$ref":"urn:kip:2.0:schema:capsule"}),
        &value,
    )
    .map_err(|e| KipError::capsule_validation_failed(e.message))?;
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
            "concept": payload.records.by_kind(ElementKind::Concept).count(),
            "proposition": payload.records.by_kind(ElementKind::Proposition).count(),
            "assertion": payload.records.by_kind(ElementKind::Assertion).count(),
            "evidence": payload.records.by_kind(ElementKind::Evidence).count(),
            "activity": payload.records.by_kind(ElementKind::Activity).count(),
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

fn package_digest(package: &crate::schema::SchemaPackage) -> Result<String, KipError> {
    let mut value = package.artifact()?;
    if let Some(object) = value.as_object_mut() {
        object.remove("integrity");
    }
    crate::schema::contracts::digest(&value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_modified_capsule_fails_its_own_digest() {
        let payload = CapsulePayload {
            manifest: CapsuleManifest {
                kind: CapsuleKind::Snapshot,
                closure: "selective".into(),
                ..Default::default()
            },
            records: CapsuleRecords(vec![
                serde_json::json!({"id": "C-1", "kind":"concept", "name": "Alice"}),
            ]),
            ..Default::default()
        };
        let digest = payload_digest(&payload).unwrap();
        let capsule = Capsule::new(
            payload.clone(),
            CapsuleIntegrity {
                digest_profile: "kip-jcs-safe-v1".into(),
                content_digest: digest,
                proofs: vec![],
                covers: None,
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
        tampered.payload.records.0[0]["name"] = Json::from("Mallory");
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
        use sha2::{Digest, Sha256};
        assert_eq!(
            hex::encode(Sha256::digest(canonical.as_bytes())),
            "7dc21a89f1745504bba876135b251f50788665c2e1926be12ca6f0186904d4e8"
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
                digest_profile: "kip-jcs-safe-v1".into(),
                content_digest: digest.clone(),
                proofs: vec![],
                covers: None,
            },
        );
        capsule.integrity.proofs.push(
            serde_json::json!({"type":"signature","signature":"..."})
                .as_object()
                .unwrap()
                .clone(),
        );
        assert_eq!(payload_digest(&capsule.payload).unwrap(), digest);
    }
}
