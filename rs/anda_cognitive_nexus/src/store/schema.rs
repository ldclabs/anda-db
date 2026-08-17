//! # Installing packages and activating environments
//!
//! Two operations that are deliberately not the same one (Spec §240.18):
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

impl Store {
    /// Installs a Schema Package artifact, or confirms it is already installed.
    ///
    /// Installing does not activate (§240.18, §240.20). Re-installing the same
    /// reference with different content is refused: `package_id + version`
    /// identifies one immutable content forever, and silently accepting a
    /// replacement is the same-version replacement attack of §150 — every
    /// element already bound to that reference would change meaning with no
    /// transaction recording it.
    pub async fn install_package(
        &self,
        package: &SchemaPackage,
        source: &str,
    ) -> Result<PackageRef, KipError> {
        let package_ref = package.package_ref()?;
        let artifact = serde_json::to_value(package).map_err(|err| {
            KipError::internal_error(format!("a parsed package failed to re-encode: {err}"))
        })?;
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
            let package: SchemaPackage = serde_json::from_value(row.artifact).map_err(|err| {
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
    /// Atomic at the environment boundary (§240.43): the lock is resolved in
    /// full before anything is written, so a lock naming an uninstalled
    /// package fails without leaving the Space half-upgraded.
    ///
    /// Rolling defaults back later does not erase data written under the newer
    /// schema (§240.44) — persisted elements carry exact references, so they
    /// keep resolving through whichever environment version they were written
    /// under.
    pub async fn activate_schema(
        &self,
        space_id: &str,
        lock: SchemaLock,
        tx_id: &str,
    ) -> Result<SchemaEnvironment, KipError> {
        let space = self.get_space(space_id).await?;
        let available = self.installed_packages().await?;
        let version = space.schema_environment_version.saturating_add(1);
        // Resolve first: an environment that cannot be resolved must not
        // become the Space's current one.
        let environment = SchemaEnvironment::resolve(version, lock.clone(), &available)?;

        let row = SchemaEnvRow {
            _id: 0,
            space: space_id.to_string(),
            version,
            lock: serde_json::to_value(&lock).map_err(|err| {
                KipError::internal_error(format!("a Schema Lock failed to encode: {err}"))
            })?,
            created_at: time::now(),
            tx_id: tx_id.to_string(),
        };
        self.schema_envs().add_from(&row).await.map_err(db_error)?;

        let mut fields = BTreeMap::new();
        fields.insert("schema_environment_version".to_string(), Fv::U64(version));
        self.spaces()
            .update(space._id, fields)
            .await
            .map_err(db_error)?;

        Ok(environment)
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
    /// (§144, §145): reconstructing a historical read under today's schema
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
        SchemaEnvironment::resolve(row.version, lock, &available)
    }

    /// Installs the built-in Core package if it is not already present.
    ///
    /// Core is foundational rather than optional (§158), so a Space that has
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
