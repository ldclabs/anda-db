//! What a migration would do, worked out without doing it.
//!
//! A migration that runs at startup is convenient and irreversible in the same
//! breath. This is the part an operator runs first, on a copy or on the live
//! database, to see the shape of the answer before committing to it: how many
//! elements, which vocabulary gets published, what would fail, and — the part
//! worth the most — which of their rows the ambiguous 1.x fields actually
//! appear on.
//!
//! It writes nothing. Not the staging area either, because a dry run that
//! staged would leave the database different for having been asked.
//!
//! ## Why the inventory matters more than the counts
//!
//! The migration guide is emphatic that `confidence`, `access_level` and
//! `author` cannot be mapped mechanically: 1.x deployments used them for
//! different things, and only the operator knows which (§12, §13, §21). That
//! advice is unactionable in the abstract — it becomes actionable when you can
//! see that this deployment has 12 propositions carrying confidence between
//! 0.4 and 0.9, three distinct `access_level` values, and an `author` that
//! names a real Concept 40 times out of 51.

use anda_db::database::AndaDB;
use anda_kip::{Json, KipError};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use super::package::Vocabulary;
use super::stage::{self, LegacyKind, LegacyRow};

/// What a migration would produce, and what it would have to be told.
#[derive(Clone, Debug, Default)]
pub struct MigrationPlan {
    /// 1.x Concept rows.
    pub concepts: usize,
    /// 1.x Proposition rows, before the fan-out.
    pub proposition_rows: usize,
    /// 2.0 Propositions after one tuple per predicate.
    pub propositions: usize,
    /// 2.0 Assertions, one per migrated tuple.
    pub assertions: usize,
    /// The Concept types that would go into the generated package.
    pub concept_types: BTreeSet<String>,
    /// The predicates that would go into it.
    pub predicates: BTreeSet<String>,
    /// Things that would stop the migration, stated as what to fix.
    pub blockers: Vec<String>,
    /// Legacy `confidence` values, and where they sit.
    pub confidence: Option<ConfidenceRange>,
    /// Distinct legacy `access_level` values, with counts.
    pub access_levels: BTreeMap<String, usize>,
    /// Distinct legacy `author` values, with counts.
    pub authors: BTreeMap<String, usize>,
    /// How many `author` values name exactly one migrated Concept.
    pub authors_resolvable: usize,
}

/// The spread of legacy `confidence` in one deployment.
#[derive(Clone, Copy, Debug)]
pub struct ConfidenceRange {
    /// How many tuples carry one.
    pub count: usize,
    /// The lowest.
    pub min: f64,
    /// The highest.
    pub max: f64,
}

impl MigrationPlan {
    /// Whether the migration would run to completion.
    pub fn is_runnable(&self) -> bool {
        self.blockers.is_empty()
    }
}

/// Works out what migrating this database would do. Writes nothing.
///
/// `Ok(None)` means there is no 1.x layout here — a fresh database, or one
/// already migrated.
pub async fn plan(db: &Arc<AndaDB>) -> Result<Option<MigrationPlan>, KipError> {
    // A dry run after an interrupted migration should describe the work that
    // is actually left, which by then lives in staging rather than under the
    // 1.x names.
    let rows = match stage::read_live_v1(db).await? {
        Some(rows) => Some(rows),
        None => match stage::open(db).await? {
            Some(staging) if !stage::is_complete(&staging).await? => Some((
                stage::rows(&staging, LegacyKind::Concept).await?,
                stage::rows(&staging, LegacyKind::Proposition).await?,
            )),
            _ => None,
        },
    };
    let Some((concepts, propositions)) = rows else {
        return Ok(None);
    };
    if concepts.is_empty() && propositions.is_empty() {
        return Ok(None);
    }
    Ok(Some(build(&concepts, &propositions)))
}

fn build(concepts: &[LegacyRow], propositions: &[LegacyRow]) -> MigrationPlan {
    let vocabulary = Vocabulary::scan(concepts, propositions);
    let mut plan = MigrationPlan {
        concepts: concepts.len(),
        proposition_rows: propositions.len(),
        concept_types: vocabulary.concept_types.clone(),
        predicates: vocabulary.predicates.clone(),
        ..Default::default()
    };

    // Names are how an `author` string is matched back to a speaker, and a
    // name shared by two Concepts identifies neither.
    let mut by_name: BTreeMap<&str, usize> = BTreeMap::new();
    for row in concepts {
        if let Some(name) = row.doc.get("name").and_then(Json::as_str) {
            *by_name.entry(name).or_default() += 1;
        }
        if row
            .doc
            .get("type")
            .and_then(Json::as_str)
            .filter(|name| !name.is_empty())
            .is_none()
        {
            plan.blockers.push(format!(
                "1.x Concept {} has no type, so it has no symbol to resolve to. Give it one in \
                 the source database, or accept that it cannot be migrated.",
                row.legacy_id
            ));
        }
        collect_legacy(&row.doc, &mut plan, None);
    }

    // The fan-out, and the endpoint graph it has to close over.
    let available: BTreeSet<String> = concepts
        .iter()
        .map(|row| format!("C:{}", row.legacy_id))
        .collect();
    let mut tuples: BTreeSet<String> = BTreeSet::new();
    for row in propositions {
        if let Some(predicates) = row.doc.get("predicates").and_then(Json::as_array) {
            for predicate in predicates.iter().filter_map(Json::as_str) {
                tuples.insert(format!("P:{}:{predicate}", row.legacy_id));
                plan.propositions += 1;
                plan.assertions += 1;
                let properties = row.doc.get("properties").and_then(|p| p.get(predicate));
                if let Some(properties) = properties {
                    collect_legacy(properties, &mut plan, Some(&by_name));
                }
            }
        }
    }

    for row in propositions {
        for slot in ["subject", "object"] {
            let Some(reference) = row.doc.get(slot).and_then(Json::as_str) else {
                plan.blockers
                    .push(format!("1.x Proposition {} has no {slot}.", row.legacy_id));
                continue;
            };
            let known = if reference.starts_with("C:") {
                available.contains(reference)
            } else {
                tuples.contains(reference)
            };
            if !known {
                plan.blockers.push(format!(
                    "1.x Proposition {} points its {slot} at {reference:?}, which nothing in this \
                     database provides. Migrating it would invent a graph the old one did not have.",
                    row.legacy_id
                ));
            }
        }
    }
    plan
}

/// Reads the ambiguous legacy fields out of a 1.x `metadata` map.
fn collect_legacy(doc: &Json, plan: &mut MigrationPlan, by_name: Option<&BTreeMap<&str, usize>>) {
    let Some(metadata) = doc.get("metadata").and_then(Json::as_object) else {
        return;
    };
    if let Some(value) = metadata.get("confidence").and_then(Json::as_f64) {
        let range = plan.confidence.get_or_insert(ConfidenceRange {
            count: 0,
            min: value,
            max: value,
        });
        range.count += 1;
        range.min = range.min.min(value);
        range.max = range.max.max(value);
    }
    if let Some(level) = metadata.get("access_level").and_then(Json::as_str) {
        *plan.access_levels.entry(level.to_string()).or_default() += 1;
    }
    if let Some(author) = metadata.get("author").and_then(Json::as_str) {
        *plan.authors.entry(author.to_string()).or_default() += 1;
        if by_name.is_some_and(|names| names.get(author) == Some(&1)) {
            plan.authors_resolvable += 1;
        }
    }
}

impl fmt::Display for MigrationPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "KIP 1.x → 2.0 migration plan (nothing has been written)")?;
        writeln!(f)?;
        writeln!(f, "  Concepts            {}", self.concepts)?;
        writeln!(
            f,
            "  Proposition rows    {}  → {} tuple(s) after the fan-out",
            self.proposition_rows, self.propositions
        )?;
        writeln!(
            f,
            "  Assertions          {}  (mode \"imported\", one per tuple)",
            self.assertions
        )?;
        writeln!(f)?;
        writeln!(
            f,
            "  Generated package   {} — {} type(s), {} predicate(s)",
            super::legacy_package_ref(),
            self.concept_types.len(),
            self.predicates.len()
        )?;
        if !self.concept_types.is_empty() {
            writeln!(
                f,
                "    types             {}",
                self.concept_types
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            )?;
        }
        if !self.predicates.is_empty() {
            writeln!(
                f,
                "    predicates        {}",
                self.predicates
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            )?;
        }

        writeln!(f)?;
        writeln!(f, "  Decisions this data actually needs:")?;
        match self.confidence {
            Some(range) => writeln!(
                f,
                "    confidence        {} tuple(s), {:.2}–{:.2}. Carried onto the Assertion and \
                 also kept verbatim. If it meant staleness or importance rather than truth, it \
                 belongs in MnemonicState instead (§13).",
                range.count, range.min, range.max
            )?,
            None => writeln!(f, "    confidence        none present")?,
        }
        if self.access_levels.is_empty() {
            writeln!(f, "    access_level      none present")?;
        } else {
            let values: Vec<String> = self
                .access_levels
                .iter()
                .map(|(value, count)| format!("{value}×{count}"))
                .collect();
            writeln!(
                f,
                "    access_level      {}. Kept as a legacy attribute, NOT promoted to a \
                 classification: 1.x annotated where 2.0 enforces (§21).",
                values.join(", ")
            )?;
        }
        if self.authors.is_empty() {
            writeln!(f, "    author            none present")?;
        } else {
            writeln!(
                f,
                "    author            {} distinct, {} occurrence(s) naming exactly one Concept \
                 and becoming asserted_by. The rest stay attributes rather than becoming \
                 speakers (§12).",
                self.authors.len(),
                self.authors_resolvable
            )?;
        }

        writeln!(f)?;
        if self.blockers.is_empty() {
            writeln!(f, "  No blockers. This migration would run to completion.")?;
        } else {
            writeln!(
                f,
                "  {} blocker(s) — migration would refuse:",
                self.blockers.len()
            )?;
            for blocker in &self.blockers {
                writeln!(f, "    - {blocker}")?;
            }
        }
        Ok(())
    }
}
