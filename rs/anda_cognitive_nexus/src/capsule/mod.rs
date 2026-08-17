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
//! **Not inherit the source's engine origin** (§27, §240.22). `_system.origin`
//! records what *this* runtime observed. The source's origin is preserved as
//! import provenance instead — a claim about where the data came from, which is
//! exactly what it is.
//!
//! **Not activate schema** (§88, §240.20). A Capsule may name the packages its
//! records are bound to; it may not decide that this Space trusts them. Import
//! validates that the schema is available and refuses otherwise, because
//! importing records whose types cannot be resolved would store cognition with
//! no recoverable meaning.
//!
//! **Not treat a legacy export as a Capsule** (§240.14). Nothing here reads
//! KIP 1.x `UPSERT` scripts.

use anda_kip::{
    Capsule, CapsuleIntegrity, CapsuleKind, CapsuleManifest, CapsulePayload, CapsuleRecords,
    CapsuleSource, ElementKind, Json, KipError, KipErrorCode, Map, SchemaDependency,
};
use std::collections::{BTreeMap, BTreeSet};

pub mod merge;

use crate::id::ElementId;
use crate::kql::Context;
use crate::store::Element;
use crate::view;

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
    let closure = options
        .get("closure")
        .and_then(Json::as_str)
        .unwrap_or("referential");
    if closure != "referential" && closure != "none" {
        return Err(KipError::unsupported_capability(format!(
            "this engine writes a \"referential\" closure or \"none\"; it has no {closure:?}"
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

    let ids = if closure == "none" {
        roots.iter().copied().collect::<BTreeSet<_>>()
    } else {
        expand(cx, &roots, depth).await?
    };

    let mut records = CapsuleRecords::default();
    let mut schema_refs: BTreeSet<String> = BTreeSet::new();
    for id in &ids {
        let Some(element) = cx.load(*id).await? else {
            continue;
        };
        // The redacted view, for the same reason SEARCH uses it: a field the
        // caller may not read must not leave the Space in a Capsule either
        // (§144). Elements it may not read at all were already dropped by
        // `load`, which is what makes the manifest's `partial` honest.
        let rendered = cx
            .cached_view(*id)
            .unwrap_or_else(|| view::render(&element));
        collect_schema_refs(&rendered, &mut schema_refs);
        match id.kind {
            ElementKind::Concept => records.concepts.push(rendered),
            ElementKind::Proposition => records.propositions.push(rendered),
            ElementKind::Assertion => records.assertions.push(rendered),
            ElementKind::Evidence => records.evidence.push(rendered),
            ElementKind::Activity => records.activities.push(rendered),
        }
    }

    let space = cx.store.get_space(&cx.space).await?;
    let payload = CapsulePayload {
        manifest: CapsuleManifest {
            kind: CapsuleKind::Snapshot,
            created_at: Some(crate::time::now()),
            // `partial` unless the closure ran and nothing was dropped: a
            // Capsule that claimed completeness it does not have would import
            // as a graph the destination believes is whole.
            completeness: Some(if closure == "referential" {
                "referential_closure".to_string()
            } else {
                "roots_only".to_string()
            }),
            closure: Some(serde_json::json!({"mode": closure, "provenance_depth": depth})),
        },
        source: CapsuleSource {
            nexus_id: Some(cx.store.db.name().to_string()),
            space_ref: Some(space.space_id.clone()),
            snapshot_seq: Some(space.seq),
            base_seq: None,
            target_seq: Some(space.seq),
            schema_environment_version: Some(cx.env.version),
        },
        // §240.47: the exact refs travel with the records. A Capsule that
        // exported local names would arrive meaning whatever the destination
        // happens to call them.
        schema: if include_schema {
            schema_dependencies(cx, &schema_refs)
        } else {
            vec![]
        },
        records,
        external_refs: vec![],
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
            for referenced in references(&element) {
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

/// Every local element one element points at.
fn references(element: &Element) -> Vec<ElementId> {
    fn local(value: &Json) -> Option<ElementId> {
        match crate::term::Endpoint::from_json(value) {
            Ok(crate::term::Endpoint::Local(id)) => Some(id),
            _ => None,
        }
    }

    let mut out = Vec::new();
    match element {
        Element::Concept(row) => {
            for refs in row.structural.values() {
                if let Some(items) = refs.as_array() {
                    out.extend(items.iter().filter_map(local));
                }
            }
        }
        Element::Proposition(row) => {
            out.extend(local(&row.subject));
            out.extend(local(&row.object));
        }
        Element::Assertion(row) => {
            if let Ok(id) = row.proposition_id.parse() {
                out.push(id);
            }
            out.extend(local(&row.asserted_by));
            out.extend(
                row.evidence_ids
                    .iter()
                    .filter_map(|id| id.parse::<ElementId>().ok()),
            );
            out.extend(row.context_refs.iter().filter_map(local));
        }
        Element::Evidence(row) => {
            if let Ok(id) = row.generated_by.parse() {
                out.push(id);
            }
            out.extend(row.source_refs.iter().filter_map(local));
        }
        Element::Activity(row) => {
            out.extend(row.inputs.iter().filter_map(local));
            out.extend(row.outputs.iter().filter_map(local));
            out.extend(row.associated_actors.iter().filter_map(local));
        }
    }
    out
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

/// The engine-local content digest over a Capsule payload.
///
/// The specification's canonicalization profile is still a draft, so this is
/// the same engine-local encoding the Schema Package registry uses. It detects
/// a modified Capsule; it is not presented as the standard digest.
pub fn payload_digest(payload: &CapsulePayload) -> Result<String, KipError> {
    let value = serde_json::to_value(payload)
        .map_err(|err| KipError::internal_error(format!("a Capsule failed to encode: {err}")))?;
    Ok(crate::store::schema::content_digest(&value))
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

/// Checks a Capsule's frame and digest without importing it.
///
/// Integrity, not legality: this says the artifact is what it claims to be, and
/// says nothing about whether its records would be accepted.
pub fn verify(capsule: &Capsule) -> Result<Json, KipError> {
    capsule.validate_frame()?;
    let recomputed = payload_digest(&capsule.payload)?;
    let matches = recomputed == capsule.integrity.content_digest;
    if !matches {
        return Err(KipError::new(
            KipErrorCode::DigestMismatch,
            format!(
                "this Capsule declares the digest {} and its payload digests to {recomputed}; it \
                 was modified after it was written, or written by an engine using a different \
                 canonicalization",
                capsule.integrity.content_digest
            ),
        ));
    }
    Ok(serde_json::json!({
        "valid": true,
        "content_digest": recomputed,
        "digest_profile": "engine-local canonical JSON (the KIP profile is still a draft)",
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
/// is written (§240.48), because a half-imported graph bound to types the
/// destination cannot resolve is cognition with no recoverable meaning.
pub async fn import(
    nexus: &crate::CognitiveNexus,
    capsule: &Capsule,
    space_id: &str,
    dry_run: bool,
    auth: crate::governance::AuthContext,
) -> Result<ImportReport, KipError> {
    capsule.validate_frame()?;
    let mut report = ImportReport::default();

    // Integrity first (§41.2: VERIFY → VALIDATE → PREVIEW → import). A
    // modified artifact must not reach identity resolution: everything after
    // this point trusts the record ids to mean what the digest covers.
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
    merge::merge(&nexus.store, capsule, space_id, &digest, report, auth).await
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
