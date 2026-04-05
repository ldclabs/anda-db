//! # KIP Genesis Capsules
//!
/// The absolute root type of all knowledge concepts.
pub static META_CONCEPT_TYPE: &str = "$ConceptType";

/// The absolute root type of all knowledge propositions.
pub static META_PROPOSITION_TYPE: &str = "$PropositionType";

/// The agent itself: {type: "Person", name: "$self"}
pub static META_SELF_NAME: &str = "$self";

/// The system itself: {type: "System", name: "$system"}
pub static META_SYSTEM_NAME: &str = "$system";

/// The type identifier for domain entities.
pub static DOMAIN_TYPE: &str = "Domain";

/// The type identifier for event entities.
pub static EVENT_TYPE: &str = "Event";

pub static PERSON_TYPE: &str = "Person";

/// The type identifier for Insight entities.
pub static INSIGHT_TYPE: &str = "Insight";

/// The type identifier for event entities.
pub static SLEEP_TASK_TYPE: &str = "SleepTask";

/// The type identifier for preference entities.
pub static PREFERENCE_TYPE: &str = "Preference";

/// The predicate type for domain membership relationships.
pub static BELONGS_TO_DOMAIN_TYPE: &str = "belongs_to_domain";

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

/// The SleepTask type definition capsule.
pub static SLEEP_TASK_KIP: &str = include_str!("../capsules/SleepTask.kip");

/// The $self capsule representing the agent itself (should replace $self_reserved_principal_id).
pub static PERSON_SELF_KIP: &str = include_str!("../capsules/persons/self.kip");

/// The $system capsule representing the system itself.
pub static PERSON_SYSTEM_KIP: &str = include_str!("../capsules/persons/system.kip");

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse_kip;

    #[test]
    fn test_capsule() {
        let genesis = parse_kip(GENESIS_KIP).expect("Failed to parse Genesis capsule");
        println!("Genesis Capsule: {:#?}", genesis);

        let person_type = parse_kip(PERSON_KIP).expect("Failed to parse Person type capsule");
        println!("Person Type Capsule: {:#?}", person_type);

        let person_self = parse_kip(PERSON_SELF_KIP).expect("Failed to parse Self person capsule");
        println!("Self Capsule: {:#?}", person_self);

        let person_system =
            parse_kip(PERSON_SYSTEM_KIP).expect("Failed to parse System person capsule");
        println!("System Capsule: {:#?}", person_system);
    }
}
