//! # Installing packages and activating environments
//!
//! Two operations that are deliberately not the same one (Spec §20.12):
//!
//! ```text
//! install   the artifact is available locally, and inert
//! activate  Governance decides it may resolve symbols in this Space
//! ```
//!
//! An imported Capsule can carry a package; it cannot activate one (§88). That
//! separation is what stops a piece of arriving data from redefining what the
//! Space's existing data means.

use anda_db_schema::Fv;
use anda_kip::{Json, KipError, KipErrorCode};
use std::{collections::BTreeMap, sync::Arc};

use super::{Store, eq_field, rows::*};
use crate::error::db_error;
use crate::schema::env::{CORE_PACKAGE, CORE_PACKAGE_REF, SchemaEnvironment, SchemaLock};
use crate::schema::package::SchemaPackage;
use crate::schema::symbol::PackageRef;
use crate::time;

/// Canonical JSON: object keys sorted, no insignificant whitespace.
///
/// The specification's own canonicalization profile is still a draft
/// (`kip-draft-canonical-json-v1`), so this is an **engine-local** encoding
/// used for one purpose: detecting that an already-installed package reference
/// came back with different content. It is not presented as the spec digest,
/// and the artifact's declared digest is stored verbatim beside it rather than
/// being checked against this one.
fn canonical_json(value: &Json, out: &mut String) {
    match value {
        Json::Object(map) => {
            // `serde_json::Map` may preserve insertion order depending on
            // build features, so the sort is explicit rather than assumed.
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_unstable();
            out.push('{');
            for (index, key) in keys.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&Json::String((*key).clone()).to_string());
                out.push(':');
                canonical_json(&map[*key], out);
            }
            out.push('}');
        }
        Json::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                canonical_json(item, out);
            }
            out.push(']');
        }
        scalar => out.push_str(&scalar.to_string()),
    }
}

/// The engine-local content digest of an artifact.
pub fn content_digest(artifact: &Json) -> String {
    use sha3::{Digest, Sha3_256};

    let mut canonical = String::new();
    canonical_json(artifact, &mut canonical);
    format!(
        "sha3-256:{}",
        hex::encode(Sha3_256::digest(canonical.as_bytes()))
    )
}

/// Refuses a `functional_by` declaration §20.15 does not admit: any value but
/// `"object_type"`, together with `functional: true`, or over an object that
/// is not declared as Concepts — a Literal has no Concept Type to partition by.
pub(crate) fn check_functional_by(package: &SchemaPackage) -> Result<(), KipError> {
    for (name, def) in &package.definitions.predicates {
        let Some(by) = &def.functional_by else {
            continue;
        };
        let concepts = !def.object.concept_types.is_empty()
            || (!def.object.kinds.is_empty() && def.object.kinds.iter().all(|k| k == "Concept"));
        if by != "object_type"
            || def.functional
            || !def.object.literal_datatypes().is_empty()
            || !concepts
        {
            return Err(KipError::constraint_violation(format!(
                "predicate {name}: functional_by must be \"object_type\", never with \
                 functional: true, and its object must be declared as Concepts (§20.15)"
            )));
        }
    }
    Ok(())
}

/// Refuses a package that would shadow a reserved Core symbol (§20.13).
///
/// `kip://core` is implicitly active in every Schema Environment and cannot be
/// deactivated, replaced, or shadowed. A package defining a Concept type named
/// `Assertion`, or a structural field named `evidence`, would put two meanings
/// behind one word in the same resolution scope — and the reader that resolved
/// it to the wrong one would have no way to notice.
///
/// The two namespaces are checked apart: `Assertion` shadows as a type name,
/// `inputs` as a structural field. A Predicate named `source` is a claim about
/// origin and shadows nothing.
fn reject_core_shadowing(package: &SchemaPackage) -> Result<(), KipError> {
    use crate::schema::symbol::SymbolKind;

    let refuse = |kind: SymbolKind, name: &str, detail: &str| {
        Err(KipError::new(
            KipErrorCode::ConstraintViolation,
            format!(
                "this package defines the {} `{name}`, which is a reserved Core symbol \
                 (§20.13){detail}: `kip://core` is implicitly active in every Schema \
                 Environment and cannot be shadowed. Rename it, or address the Core symbol \
                 you meant",
                kind.section()
            ),
        ))
    };

    // A type, Facet or Enum resolves by name alone, so any of the Core element
    // kinds is a shadow wherever it appears.
    for kind in [SymbolKind::ConceptType, SymbolKind::Facet, SymbolKind::Enum] {
        for name in package.symbols(kind) {
            if anda_kip::CORE_ELEMENT_KINDS.contains(&name) {
                return refuse(kind, name, "");
            }
        }
    }

    // A structural field resolves by *source kind and* name (§8.2): the Core
    // plane is reached through the source element's own kind. So a Profile
    // field named `evidence` shadows only where an Assertion could carry it —
    // on a Concept, which owns no Core structural field, the two planes stay
    // apart and the name is free.
    for (name, def) in &package.definitions.structural_fields {
        let Some(owner) = anda_kip::core_structural_owner(name) else {
            continue;
        };
        let unconstrained = def.source.kinds.is_empty() && def.source.concept_types.is_empty();
        if unconstrained || def.source.kinds.iter().any(|kind| kind == owner) {
            return refuse(
                SymbolKind::StructuralField,
                name,
                &format!(
                    " on {owner}, whose `{name}` the protocol itself defines; a source that \
                     admits {owner} — or one that constrains nothing, which admits every kind —"
                ),
            );
        }
    }

    Ok(())
}

impl Store {
    /// Installs a Schema Package artifact, or confirms it is already installed.
    ///
    /// Installing does not activate (§20.12, §41.3). Re-installing the same
    /// reference with different content is refused: `package_id + version`
    /// identifies one immutable content forever, and silently accepting a
    /// replacement is the same-version replacement attack of §20.11 — every
    /// element already bound to that reference would change meaning with no
    /// transaction recording it.
    pub async fn install_package(
        &self,
        package: &SchemaPackage,
        source: &str,
    ) -> Result<PackageRef, KipError> {
        let package_ref = package.package_ref()?;
        reject_core_shadowing(package)?;
        check_functional_by(package)?;
        let artifact = package.artifact()?;
        crate::schema::contracts::verify_artifact(&artifact)?;
        let digest = content_digest(&artifact);

        if let Some(existing) = self.find_package_row(&package_ref.to_string()).await? {
            if existing.content_digest != digest {
                return Err(KipError::new(
                    KipErrorCode::DigestMismatch,
                    format!(
                        "{package_ref} is already installed with content {}, and the artifact \
                         offered now digests to {digest}; a published package version is \
                         immutable, so this is an integrity failure rather than an upgrade",
                        existing.content_digest
                    ),
                ));
            }
            return Ok(package_ref);
        }

        let row = SchemaPackageRow {
            _id: 0,
            package_ref: package_ref.to_string(),
            package_id: package_ref.package_id.clone(),
            version: package_ref.version.to_string(),
            content_digest: digest,
            declared_digest: package
                .integrity
                .as_ref()
                .map(|integrity| integrity.content_digest.clone())
                .unwrap_or_default(),
            artifact,
            installed_at: time::now(),
            source: source.to_string(),
        };
        self.schema_packages()
            .add_from(&row)
            .await
            .map_err(db_error)?;
        Ok(package_ref)
    }

    /// A resolved environment this Nexus has already built, if it has.
    fn cached_environment(&self, space_id: &str, version: u64) -> Option<SchemaEnvironment> {
        self.environments
            .read()
            .get(&(space_id.to_string(), version))
            .cloned()
    }

    fn remember_environment(&self, space_id: &str, version: u64, env: &SchemaEnvironment) {
        self.environments
            .write()
            .insert((space_id.to_string(), version), env.clone());
    }

    /// Every installed artifact, keyed by canonical reference.
    pub async fn installed_packages(
        &self,
    ) -> Result<BTreeMap<String, Arc<SchemaPackage>>, KipError> {
        let collection = self.schema_packages();
        // A Schema Environment resolves against the complete installed set, so
        // this enumerates every row rather than a filtered view. Every package
        // id is non-empty, which makes this range the whole collection.
        let ids = collection
            .query_all_ids(anda_db::query::Filter::Field((
                "package_id".to_string(),
                anda_db::query::RangeQuery::Gt(Fv::Text(String::new())),
            )))
            .await
            .map_err(db_error)?;

        let mut packages = BTreeMap::new();
        for id in ids {
            let row: SchemaPackageRow = collection.get_as(id).await.map_err(db_error)?;
            let package: SchemaPackage =
                SchemaPackage::parse(&row.artifact.to_string()).map_err(|err| {
                    KipError::new(
                        KipErrorCode::ArtifactParseError,
                        format!("installed package {} is unreadable: {err}", row.package_ref),
                    )
                })?;
            packages.insert(row.package_ref, Arc::new(package));
        }
        Ok(packages)
    }

    async fn find_package_row(
        &self,
        package_ref: &str,
    ) -> Result<Option<SchemaPackageRow>, KipError> {
        let collection = self.schema_packages();
        let ids = collection
            .query_all_ids(eq_field("package_ref", Fv::Text(package_ref.to_string())))
            .await
            .map_err(db_error)?;
        match ids.first() {
            None => Ok(None),
            Some(id) => Ok(Some(collection.get_as(*id).await.map_err(db_error)?)),
        }
    }

    /// Activates a Schema Lock, minting the next environment version.
    ///
    /// Atomic at the environment boundary (§20.9): the lock is resolved in
    /// full before anything is written, so a lock naming an uninstalled
    /// package fails without leaving the Space half-upgraded.
    ///
    /// Rolling defaults back later does not erase data written under the newer
    /// schema (§20.4) — persisted elements carry exact references, so they
    /// keep resolving through whichever environment version they were written
    /// under.
    pub async fn activate_schema(
        &self,
        space_id: &str,
        lock: SchemaLock,
    ) -> Result<SchemaEnvironment, KipError> {
        let space = self.get_space(space_id).await?;
        let mut lock = lock;
        // The draft package and its promotions are Space history, not a host's
        // to drop (§20.16): an activation carries them forward.
        let current = self
            .schema_environment_at(space_id, space.schema_environment_version)
            .await?;
        lock.retain_space_local(&current.lock);
        if !lock.draft.is_empty()
            && lock.states.get(anda_kip::DRAFT_PACKAGE_ID)
                != Some(&crate::schema::PackageState::Active)
        {
            return Err(KipError::constraint_violation(format!(
                "{} is the Space's draft vocabulary and stays active once defined (§20.16)",
                anda_kip::DRAFT_PACKAGE_REF
            )));
        }
        let available = self.installed_packages().await?;
        let version = space.schema_environment_version.saturating_add(1);
        // Resolve first: an environment that cannot be resolved must not
        // become the Space's current one.
        let environment = SchemaEnvironment::resolve(version, lock.clone(), &available)?;
        let first_activation = version == 1 && space.seq == 0;
        let context = if first_activation {
            None
        } else {
            Some(
                self.begin_transaction(
                    space_id,
                    serde_json::json!({
                        "principal_id": "kip:principal:system",
                        "channel": "governance",
                    }),
                )
                .await?,
            )
        };

        let row = SchemaEnvRow {
            _id: 0,
            space: space_id.to_string(),
            version,
            lock: serde_json::to_value(&lock).map_err(|err| {
                KipError::internal_error(format!("a Schema Lock failed to encode: {err}"))
            })?,
            created_at: time::now(),
            tx_id: context
                .as_ref()
                .map(|cx| cx.tx_id.clone())
                .unwrap_or_default(),
        };
        self.schema_envs().add_from(&row).await.map_err(db_error)?;

        let mut updated_space = self.get_space(space_id).await?;
        updated_space.schema_environment_version = version;
        self.put_space(&updated_space).await?;
        if let Some(cx) = context {
            self.journal(
                &cx,
                super::space::JournalEntry {
                    status: "committed".to_string(),
                    transaction_class: "governance".to_string(),
                    schema_environment_version: version,
                    // A new environment is a `schema` control change (§36.1).
                    result: serde_json::json!({
                        "schema_environment_version": version,
                        "control_changes": [{"kind": "schema", "version": cx.seq.to_string()}],
                    }),
                    ..Default::default()
                },
            )
            .await?;
        }

        Ok(environment)
    }

    /// Adds one symbol to the Space's draft vocabulary (§20.16).
    ///
    /// `definition` is the `DEFINE` body with its parameters bound. Only
    /// adds: a name that already names a symbol of this kind anywhere in the
    /// environment fails `SchemaSymbolConflict`, even when the definition is
    /// identical — a retry is deduplicated by its idempotency key, never by
    /// content. The definition persists with its endpoint types resolved to
    /// exact references, and commits as its own governance transaction that
    /// advances `schema_environment_version` and publishes a `schema` control
    /// change. A dry run checks everything and writes nothing.
    ///
    /// Returns the result `{ref, schema_environment_version}` and, unless it
    /// was a dry run, the journalled transaction.
    pub async fn define_draft_symbol(
        &self,
        space_id: &str,
        kind: anda_kip::DefineKind,
        name: &str,
        definition: Json,
        entry: super::space::JournalEntry,
        dry_run: bool,
    ) -> Result<(Json, Option<TransactionRow>), KipError> {
        use crate::schema::symbol::{SymbolKind, SymbolRef};
        use crate::schema::{Intent, PackageState};

        let symbol_kind = match kind {
            anda_kip::DefineKind::Predicate => SymbolKind::PredicateType,
            anda_kip::DefineKind::ConceptType => SymbolKind::ConceptType,
        };
        let reference = anda_kip::draft_symbol_ref(name);
        if name.is_empty()
            || crate::schema::symbol::is_qualified(name)
            || reference
                .parse::<SymbolRef>()
                .map(|s| s.name)
                .ok()
                .as_deref()
                != Some(name)
        {
            return Err(KipError::invalid_identifier(format!(
                "{name:?} is not a symbol name DEFINE can add; a draft symbol is named by a bare \
                 local name, without a package or `/` (§20.16)"
            )));
        }
        anda_kip::check_draft_definition(kind, &definition)?;
        let env = self.schema_environment(space_id).await?;
        if let Some(existing) = env.name_taken(symbol_kind, name) {
            return Err(KipError::new(
                KipErrorCode::SchemaSymbolConflict,
                format!(
                    "{name:?} already names {existing}; DEFINE only adds, and a draft symbol never \
                     shadows another (§20.16)"
                ),
            )
            .with_hint(
                "use the existing symbol, or define the new meaning under another name".to_string(),
            ));
        }

        // Endpoint types persist as exact references, like any package's.
        let mut definition = definition;
        if symbol_kind == SymbolKind::PredicateType {
            for side in ["subject", "object"] {
                let Some(types) = definition
                    .get_mut(side)
                    .and_then(|endpoint| endpoint.get_mut("concept_types"))
                    .and_then(Json::as_array_mut)
                else {
                    continue;
                };
                for ty in types.iter_mut() {
                    let local = ty.as_str().ok_or_else(|| {
                        KipError::constraint_violation(format!(
                            "{side}.concept_types names Concept Types as strings"
                        ))
                    })?;
                    *ty = Json::String(
                        env.resolve_symbol(SymbolKind::ConceptType, local, Intent::Read)?
                            .to_string(),
                    );
                }
            }
        } else {
            let attributes = definition
                .as_object_mut()
                .map(|members| {
                    members
                        .entry("attributes")
                        .or_insert_with(|| serde_json::json!({}))
                })
                .and_then(Json::as_object_mut);
            if let Some(attributes) = attributes {
                attributes.insert("open".into(), Json::Bool(true));
                attributes
                    .entry("fields")
                    .or_insert_with(|| serde_json::json!({}));
            }
        }
        if let Some(members) = definition.as_object_mut() {
            members.insert("ref".into(), Json::String(reference.clone()));
            members.insert(
                "kind".into(),
                Json::from(crate::schema::env::draft_kind_name(symbol_kind)),
            );
        }

        let mut lock = env.lock.clone();
        match symbol_kind {
            SymbolKind::PredicateType => lock.draft.predicates.insert(name.into(), definition),
            _ => lock.draft.concept_types.insert(name.into(), definition),
        };
        lock.packages.insert(
            anda_kip::DRAFT_PACKAGE_ID.into(),
            anda_kip::DRAFT_PACKAGE_VERSION.into(),
        );
        lock.states
            .insert(anda_kip::DRAFT_PACKAGE_ID.into(), PackageState::Active);
        let package = lock.draft.package().map_err(|err| {
            KipError::constraint_violation(format!(
                "the definition is not a valid {symbol_kind} definition: {}",
                err.message
            ))
        })?;
        check_functional_by(&package)?;
        let version = env.version.saturating_add(1);
        SchemaEnvironment::resolve(version, lock.clone(), &self.installed_packages().await?)?;

        let result = serde_json::json!({"ref": reference, "schema_environment_version": version});
        if dry_run {
            return Ok((result, None));
        }
        let row = self
            .commit_environment(space_id, lock, entry, result)
            .await?;
        Ok((row.result.clone(), Some(row)))
    }

    /// Promotes a draft symbol (§20.16): a Schema migration that maps its
    /// lineage onto a symbol of the same kind in an installed package, with
    /// the rename semantics of §20.14. Elements written under the draft keep
    /// their exact reference and are read through the target's lineage from
    /// the new environment version on; an `AS OF` read before it does not see
    /// the mapping. A draft symbol is promoted at most once.
    pub async fn promote_draft_symbol(
        &self,
        space_id: &str,
        kind: crate::schema::symbol::SymbolKind,
        from: &str,
        to: &str,
        origin: Json,
    ) -> Result<TransactionRow, KipError> {
        use crate::schema::symbol::SymbolKind;
        use crate::schema::{Intent, lineage_of};

        if !matches!(kind, SymbolKind::ConceptType | SymbolKind::PredicateType) {
            return Err(KipError::constraint_violation(
                "only Concept Types and Predicates are drafted, and so promoted (§20.16)",
            ));
        }
        let env = self.schema_environment(space_id).await?;
        let prefix = format!("{}/", anda_kip::DRAFT_PACKAGE_REF);
        let name = from.strip_prefix(&prefix).unwrap_or(from);
        let drafted = match kind {
            SymbolKind::PredicateType => env.lock.draft.predicates.contains_key(name),
            _ => env.lock.draft.concept_types.contains_key(name),
        };
        if !drafted {
            return Err(KipError::new(
                KipErrorCode::SchemaSymbolNotFound,
                format!("this Space's draft vocabulary defines no {kind} named {name:?}"),
            ));
        }
        let from_ref = anda_kip::draft_symbol_ref(name);
        let kind_name = crate::schema::env::draft_kind_name(kind);
        let from_lineage = lineage_of(&from_ref);
        if env
            .lock
            .lineage_maps
            .iter()
            .any(|map| map.kind == kind_name && map.from == from_lineage)
        {
            return Err(KipError::constraint_violation(format!(
                "{from_ref} was already promoted; a draft symbol is promoted at most once (§20.16)"
            )));
        }
        let target = if crate::schema::symbol::is_qualified(to) {
            to.parse::<crate::schema::SymbolRef>()?
        } else {
            // The draft's own local name would resolve to itself.
            let mut candidates = Vec::new();
            for (package_id, version) in &env.lock.packages {
                let package_ref = format!("{package_id}@{version}");
                if package_ref == anda_kip::DRAFT_PACKAGE_REF {
                    continue;
                }
                if let Some(artifact) = env.artifact(&package_ref)
                    && env.state(package_id).answers_local_names()
                    && artifact.defines(kind, to)
                {
                    candidates.push(artifact.symbol_ref(to)?);
                }
            }
            match candidates.len() {
                1 => candidates.remove(0),
                0 => {
                    return Err(KipError::new(
                        KipErrorCode::SchemaSymbolNotFound,
                        format!("no installed package defines the {kind} {to:?} in this Space"),
                    ));
                }
                _ => {
                    return Err(KipError::new(
                        KipErrorCode::SchemaSymbolAmbiguous,
                        format!(
                            "the {kind} {to:?} is defined by more than one package; name the \
                             target by its exact reference"
                        ),
                    ));
                }
            }
        };
        if target.package.package_id == anda_kip::DRAFT_PACKAGE_ID {
            return Err(KipError::constraint_violation(
                "a draft symbol is promoted to a symbol of an installed package, never to another \
                 draft symbol (§20.16)",
            ));
        }
        let target = env.resolve_symbol(kind, &target.to_string(), Intent::Read)?;
        let mut lock = env.lock.clone();
        lock.lineage_maps.push(crate::schema::env::LineageMap {
            kind: kind_name.to_string(),
            from: from_lineage.clone(),
            to: lineage_of(&target.to_string()),
        });
        let version = env.version.saturating_add(1);
        SchemaEnvironment::resolve(version, lock.clone(), &self.installed_packages().await?)?;
        self.commit_environment(
            space_id,
            lock,
            super::space::JournalEntry {
                origin,
                ..Default::default()
            },
            serde_json::json!({
                "promoted": {"kind": kind_name, "from": from_lineage, "to": lineage_of(&target.to_string())},
                "schema_environment_version": version,
            }),
        )
        .await
    }

    /// Commits a new Schema Environment version as its own governance
    /// transaction: the environment row, the Space's version, and a journal
    /// entry whose result carries the `schema` control change (§36.1). It
    /// writes no element, so the entry's Receipt origin is all the origin it
    /// records.
    pub(crate) async fn commit_environment(
        &self,
        space_id: &str,
        lock: SchemaLock,
        entry: super::space::JournalEntry,
        mut result: Json,
    ) -> Result<TransactionRow, KipError> {
        let space = self.get_space(space_id).await?;
        let version = space.schema_environment_version.saturating_add(1);
        let cx = self
            .begin_transaction(space_id, entry.origin.clone())
            .await?;
        let row = SchemaEnvRow {
            _id: 0,
            space: space_id.to_string(),
            version,
            lock: serde_json::to_value(&lock).map_err(|err| {
                KipError::internal_error(format!("a Schema Lock failed to encode: {err}"))
            })?,
            created_at: cx.at.clone(),
            tx_id: cx.tx_id.clone(),
        };
        self.schema_envs().add_from(&row).await.map_err(db_error)?;
        let mut updated_space = self.get_space(space_id).await?;
        updated_space.schema_environment_version = version;
        self.put_space(&updated_space).await?;
        result["control_changes"] =
            serde_json::json!([{"kind": "schema", "version": cx.seq.to_string()}]);
        self.journal(
            &cx,
            super::space::JournalEntry {
                status: "committed".to_string(),
                transaction_class: "governance".to_string(),
                schema_environment_version: version,
                result,
                ..entry
            },
        )
        .await
    }

    /// The Space's current Schema Environment.
    pub async fn schema_environment(&self, space_id: &str) -> Result<SchemaEnvironment, KipError> {
        let space = self.get_space(space_id).await?;
        self.schema_environment_at(space_id, space.schema_environment_version)
            .await
    }

    /// The Schema Environment as it was at one version.
    ///
    /// This is what `AS OF` reads against and what a transaction receipt names
    /// (§20.9, §33.2): reconstructing a historical read under today's schema
    /// would answer a question nobody asked.
    pub async fn schema_environment_at(
        &self,
        space_id: &str,
        version: u64,
    ) -> Result<SchemaEnvironment, KipError> {
        // Version 0 is the environment a Space has before anything is
        // activated: Core, and nothing else.
        if version == 0 {
            return Ok(SchemaEnvironment::core_only());
        }
        if let Some(cached) = self.cached_environment(space_id, version) {
            return Ok(cached);
        }
        let collection = self.schema_envs();
        let ids = collection
            .query_all_ids(anda_db::query::Filter::And(vec![
                Box::new(eq_field("space", Fv::Text(space_id.to_string()))),
                Box::new(eq_field("version", Fv::U64(version))),
            ]))
            .await
            .map_err(db_error)?;
        let id = ids.first().ok_or_else(|| {
            KipError::new(
                KipErrorCode::HistoricalSchemaUnavailable,
                format!(
                    "this Nexus has no Schema Environment version {version} for Space \
                     {space_id:?}"
                ),
            )
        })?;
        let row: SchemaEnvRow = collection.get_as(*id).await.map_err(db_error)?;
        let lock: SchemaLock = serde_json::from_value(row.lock).map_err(|err| {
            KipError::internal_error(format!("a stored Schema Lock is unreadable: {err}"))
        })?;
        let available = self.installed_packages().await?;
        let environment = SchemaEnvironment::resolve(row.version, lock, &available)?;
        self.remember_environment(space_id, version, &environment);
        Ok(environment)
    }

    /// Installs the built-in Core package if it is not already present.
    ///
    /// Core is foundational rather than optional (§20.13), so a Space that has
    /// never activated anything still resolves Core symbols; installing the
    /// artifact makes it introspectable through META alongside every other
    /// package.
    pub async fn install_core_package(&self) -> Result<PackageRef, KipError> {
        if let Some(_existing) = self.find_package_row(&CORE_PACKAGE_REF.to_string()).await? {
            return Ok(CORE_PACKAGE_REF.clone());
        }
        self.install_package(&CORE_PACKAGE, "built-in").await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn the_digest_ignores_key_order_and_nothing_else() {
        let a = json!({"b": 1, "a": [1, 2, {"y": 1, "x": 2}]});
        let b = json!({"a": [1, 2, {"x": 2, "y": 1}], "b": 1});
        assert_eq!(content_digest(&a), content_digest(&b));

        // Order inside an array is content, not presentation.
        assert_ne!(
            content_digest(&json!({"a": [1, 2]})),
            content_digest(&json!({"a": [2, 1]}))
        );
        // So is a value.
        assert_ne!(
            content_digest(&json!({"a": 1})),
            content_digest(&json!({"a": 2}))
        );
        // And so is a key that exists at all.
        assert_ne!(
            content_digest(&json!({"a": 1})),
            content_digest(&json!({"a": 1, "b": null}))
        );
    }
}
