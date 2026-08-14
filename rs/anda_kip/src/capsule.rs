//! # KIP Genesis Capsules
//!
//! Static, version-controlled KIP source for the Bootstrapping Model defined in
//! the KIP specification. Loading these capsules brings a fresh Cognitive Nexus
//! up to a self-describing state where every legal concept type and proposition
//! predicate is itself a queryable concept node.
//!
//! - [`GENESIS_KIP`] defines the meta-types `$ConceptType` / `$PropositionType`,
//!   the `Domain` type, and the `CoreSchema` core domain.
//! - The `*_KIP` concept-type constants define the standard concept types
//!   (`Person`, `Event`, `Insight`, `Preference`, `Commitment`, `SleepTask`,
//!   and the Experience-learning triad `Experience` / `ExperienceStep` /
//!   `Skill` added in KIP v1.0-RC11).
//! - The `*_PROP_KIP` constants define the shared predicates that used to ride
//!   along inside `Event.kip` (`involves`, `mentions`, `consolidated_to`,
//!   `derived_from` — now widened to Experience) plus the Experience-specific
//!   relations (`has_step`, `caused_by`, `derived_insight`, `compiled_to`).
//!   They ship as standalone capsules and must be loaded *after* the concept
//!   types they reference.
//! - [`PERSON_SELF_KIP`] / [`PERSON_SYSTEM_KIP`] materialize the system actors
//!   `$self` (waking persona) and `$system` (sleeping persona).
//!
//! The string constants below name the well-known concept type / predicate names
//! that executors must guard against accidental modification (see the protected
//! scope rules in KIP §4.2.4 and `KIP_3004`).

/// The absolute root type of all knowledge concepts.
pub static META_CONCEPT_TYPE: &str = "$ConceptType";

/// The absolute root type of all knowledge propositions.
pub static META_PROPOSITION_TYPE: &str = "$PropositionType";

/// The agent itself: {type: "Person", name: "$self"}
pub static META_SELF_NAME: &str = "$self";

/// The system itself: {type: "Person", name: "$system"}
pub static META_SYSTEM_NAME: &str = "$system";

/// The type identifier for domain entities.
pub static DOMAIN_TYPE: &str = "Domain";

/// The type identifier for event entities.
pub static EVENT_TYPE: &str = "Event";

/// The type identifier for person entities.
pub static PERSON_TYPE: &str = "Person";

/// The type identifier for Insight entities.
pub static INSIGHT_TYPE: &str = "Insight";

/// The type identifier for event entities.
pub static SLEEP_TASK_TYPE: &str = "SleepTask";

/// The type identifier for preference entities.
pub static PREFERENCE_TYPE: &str = "Preference";

/// The type identifier for commitment entities (prospective memory).
pub static COMMITMENT_TYPE: &str = "Commitment";

/// The type identifier for experience entities (goal-directed trajectories).
pub static EXPERIENCE_TYPE: &str = "Experience";

/// The type identifier for the ordered steps inside an [`EXPERIENCE_TYPE`].
pub static EXPERIENCE_STEP_TYPE: &str = "ExperienceStep";

/// The type identifier for skill entities (procedural memory).
pub static SKILL_TYPE: &str = "Skill";

/// The predicate type for domain membership relationships.
pub static BELONGS_TO_DOMAIN_TYPE: &str = "belongs_to_domain";

/// The predicate type linking an Event or Experience to a participating Person.
pub static INVOLVES_TYPE: &str = "involves";

/// The predicate type linking an Event or Experience to a referenced concept.
pub static MENTIONS_TYPE: &str = "mentions";

/// The predicate type linking an Event or Experience to knowledge extracted
/// from it.
pub static CONSOLIDATED_TO_TYPE: &str = "consolidated_to";

/// The inverse provenance predicate type of [`CONSOLIDATED_TO_TYPE`].
pub static DERIVED_FROM_TYPE: &str = "derived_from";

/// The predicate type linking an Experience to one of its steps.
pub static HAS_STEP_TYPE: &str = "has_step";

/// The predicate type asserting causality between two experience steps
/// (effect → cause).
pub static CAUSED_BY_TYPE: &str = "caused_by";

/// The predicate type linking an Experience to an Insight extracted from it.
pub static DERIVED_INSIGHT_TYPE: &str = "derived_insight";

/// The predicate type linking an Experience to the Skill compiled from it.
pub static COMPILED_TO_TYPE: &str = "compiled_to";

/// The genesis capsule containing the initial state of the Cognitive Nexus.
pub static GENESIS_KIP: &str = include_str!("../capsules/Genesis.kip");

/// The Event type definition capsule.
pub static EVENT_KIP: &str = include_str!("../capsules/Event.kip");

/// The Insight type definition capsule.
pub static INSIGHT_KIP: &str = include_str!("../capsules/Insight.kip");

/// The Person type definition capsule.
pub static PERSON_KIP: &str = include_str!("../capsules/Person.kip");

/// The Preference type definition capsule.
pub static PREFERENCE_KIP: &str = include_str!("../capsules/Preference.kip");

/// The Commitment type definition capsule (prospective memory:
/// promises, reminders, follow-ups, deadlines).
pub static COMMITMENT_KIP: &str = include_str!("../capsules/Commitment.kip");

/// The SleepTask type definition capsule.
pub static SLEEP_TASK_KIP: &str = include_str!("../capsules/SleepTask.kip");

/// The Experience type definition capsule (goal-directed trajectory,
/// KIP v1.0-RC11).
pub static EXPERIENCE_KIP: &str = include_str!("../capsules/Experience.kip");

/// The ExperienceStep type definition capsule (one ordered unit of a
/// trajectory, KIP v1.0-RC11).
pub static EXPERIENCE_STEP_KIP: &str = include_str!("../capsules/ExperienceStep.kip");

/// The Skill type definition capsule (procedural memory, KIP v1.0-RC11).
pub static SKILL_KIP: &str = include_str!("../capsules/Skill.kip");

/// The `involves` predicate capsule: Event | Experience → Person.
pub static INVOLVES_PROP_KIP: &str = include_str!("../capsules/involves.kip");

/// The `mentions` predicate capsule: Event | Experience → any concept.
pub static MENTIONS_PROP_KIP: &str = include_str!("../capsules/mentions.kip");

/// The `consolidated_to` predicate capsule: Event | Experience → semantic
/// knowledge.
pub static CONSOLIDATED_TO_PROP_KIP: &str = include_str!("../capsules/consolidated_to.kip");

/// The `derived_from` predicate capsule: consolidated knowledge or Skill →
/// source evidence.
pub static DERIVED_FROM_PROP_KIP: &str = include_str!("../capsules/derived_from.kip");

/// The `has_step` predicate capsule: Experience → ExperienceStep.
pub static HAS_STEP_PROP_KIP: &str = include_str!("../capsules/has_step.kip");

/// The `caused_by` predicate capsule: ExperienceStep → ExperienceStep.
pub static CAUSED_BY_PROP_KIP: &str = include_str!("../capsules/caused_by.kip");

/// The `derived_insight` predicate capsule: Experience → Insight.
pub static DERIVED_INSIGHT_PROP_KIP: &str = include_str!("../capsules/derived_insight.kip");

/// The `compiled_to` predicate capsule: Experience → Skill.
pub static COMPILED_TO_PROP_KIP: &str = include_str!("../capsules/compiled_to.kip");

/// The $self capsule representing the agent itself (should replace $self_reserved_principal_id).
pub static PERSON_SELF_KIP: &str = include_str!("../capsules/persons/self.kip");

/// The $system capsule representing the system itself.
pub static PERSON_SYSTEM_KIP: &str = include_str!("../capsules/persons/system.kip");

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse_kip;

    /// Every bundled capsule must parse. Listing them by name here is what
    /// makes a newly added `.kip` file that was never wired into a `*_KIP`
    /// constant visible: the test below cross-checks this list against the
    /// `capsules/` directory.
    const ALL_CAPSULES: &[(&str, &str)] = &[
        ("Genesis.kip", GENESIS_KIP),
        ("Event.kip", EVENT_KIP),
        ("Insight.kip", INSIGHT_KIP),
        ("Person.kip", PERSON_KIP),
        ("Preference.kip", PREFERENCE_KIP),
        ("Commitment.kip", COMMITMENT_KIP),
        ("SleepTask.kip", SLEEP_TASK_KIP),
        ("Experience.kip", EXPERIENCE_KIP),
        ("ExperienceStep.kip", EXPERIENCE_STEP_KIP),
        ("Skill.kip", SKILL_KIP),
        ("involves.kip", INVOLVES_PROP_KIP),
        ("mentions.kip", MENTIONS_PROP_KIP),
        ("consolidated_to.kip", CONSOLIDATED_TO_PROP_KIP),
        ("derived_from.kip", DERIVED_FROM_PROP_KIP),
        ("has_step.kip", HAS_STEP_PROP_KIP),
        ("caused_by.kip", CAUSED_BY_PROP_KIP),
        ("derived_insight.kip", DERIVED_INSIGHT_PROP_KIP),
        ("compiled_to.kip", COMPILED_TO_PROP_KIP),
        ("persons/self.kip", PERSON_SELF_KIP),
        ("persons/system.kip", PERSON_SYSTEM_KIP),
    ];

    #[test]
    fn test_capsule() {
        for (name, source) in ALL_CAPSULES {
            let parsed =
                parse_kip(source).unwrap_or_else(|err| panic!("Failed to parse {name}: {err}"));
            println!("{name}: {parsed:#?}");
        }
    }

    /// Guards against a capsule file landing in `capsules/` without a matching
    /// `*_KIP` constant — the failure mode that silently leaves a predicate
    /// undefined in a freshly bootstrapped nexus.
    #[test]
    fn every_capsule_file_is_bundled() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/capsules");
        let mut found: Vec<String> = Vec::new();
        for entry in std::fs::read_dir(dir).expect("capsules dir") {
            let entry = entry.expect("dir entry");
            let name = entry.file_name().to_string_lossy().into_owned();
            if entry.file_type().expect("file type").is_dir() {
                for sub in std::fs::read_dir(entry.path()).expect("capsules subdir") {
                    let sub = sub.expect("dir entry");
                    found.push(format!("{name}/{}", sub.file_name().to_string_lossy()));
                }
            } else if name.ends_with(".kip") {
                found.push(name);
            }
        }
        found.sort();

        let mut bundled: Vec<String> = ALL_CAPSULES.iter().map(|(n, _)| n.to_string()).collect();
        bundled.sort();
        assert_eq!(found, bundled, "capsules/ and ALL_CAPSULES disagree");
    }
}
