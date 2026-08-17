//! # What each command asks for
//!
//! One table, from parsed command to the permissions it needs. It lives here
//! rather than being scattered through the executors so that adding a clause
//! cannot quietly add an ungoverned write path: a new [`MutationClause`] that
//! is not in [`kml_permissions`] fails to compile the match.
//!
//! ## Read this table as the security surface
//!
//! Three groupings are deliberate and easy to get wrong in the other direction:
//!
//! - **`EXPORT CAPSULE` asks for `export`, not `read`.** A caller who may read
//!   every element in a Space still may not package them and take them away
//!   (§78, and the `Read ≠ Export` equation in §271).
//! - **A historical read asks for `read_history` on top of `read`.** What the
//!   Brain contained in January is a different disclosure from what it contains
//!   now — it can include elements since archived, and origins since revoked.
//! - **`DESCRIBE PROTOCOL` and friends ask for nothing.** They describe the
//!   engine, not the Space. Gating them would mean an unauthorized caller could
//!   not discover *how to authenticate*.

use anda_kip::{DescribeTarget, KmlStatement, KqlQuery, MetaCommand, MutationClause, WhereClause};

use super::Permission;

/// What a KQL query needs.
pub fn kql_permissions(query: &KqlQuery) -> Vec<Permission> {
    let mut needed = vec![Permission::Read];
    if query.as_of.is_some() {
        needed.push(Permission::ReadHistory);
    }
    if query.where_clauses.iter().any(projects_belief) {
        needed.push(Permission::Project);
    }
    needed
}

fn projects_belief(clause: &WhereClause) -> bool {
    match clause {
        WhereClause::Belief { .. } | WhereClause::BeliefSlot { .. } => true,
        WhereClause::Not(clauses)
        | WhereClause::Optional(clauses)
        | WhereClause::Union(clauses) => clauses.iter().any(projects_belief),
        _ => false,
    }
}

/// What a META command needs.
pub fn meta_permissions(command: &MetaCommand) -> Vec<Permission> {
    match command {
        MetaCommand::Describe(target) => describe_permissions(target),
        MetaCommand::List(_) => vec![Permission::Discover],
        MetaCommand::Search(_) => vec![Permission::Search],
        // Legality, not disclosure: `VALIDATE` answers whether a command would
        // be accepted by the schema, which is what `DESCRIBE TYPE` already
        // tells a caller who may discover the Space at all.
        MetaCommand::Validate(_) => vec![Permission::Discover],
        // A preview computes an effect over real state, so it discloses what a
        // read would. It is not a write and does not ask for one.
        MetaCommand::Preview(_) => vec![Permission::Read],
        // Verification runs entirely on the artifact the caller supplied.
        MetaCommand::Verify { .. } => Vec::new(),
        MetaCommand::History(_) | MetaCommand::Changes(_) => {
            vec![Permission::Read, Permission::ReadHistory]
        }
        MetaCommand::Snapshot { .. } => vec![Permission::ReadHistory],
        MetaCommand::ExportCapsule(_) => vec![Permission::Export],
    }
}

fn describe_permissions(target: &DescribeTarget) -> Vec<Permission> {
    match target {
        // About the engine, not about the Space.
        DescribeTarget::Protocol
        | DescribeTarget::Capabilities
        | DescribeTarget::Error(_)
        | DescribeTarget::Compatibility { .. }
        | DescribeTarget::ProjectionCapability
        | DescribeTarget::EpistemicPolicy { .. } => Vec::new(),
        // About the caller itself. §266: an Agent must be able to learn what it
        // may do without first being permitted to do it.
        DescribeTarget::ExecutionContext | DescribeTarget::Access { .. } => Vec::new(),
        DescribeTarget::Trust { .. } => vec![Permission::Read],
        DescribeTarget::Transaction(_)
        | DescribeTarget::TransactionByIdempotencyKey(_)
        | DescribeTarget::Snapshot { .. } => vec![Permission::ReadHistory],
        DescribeTarget::SchemaEnvironment { as_of: Some(_) } => {
            vec![Permission::Discover, Permission::ReadHistory]
        }
        _ => vec![Permission::Discover],
    }
}

/// What a KML statement needs: the union over its clauses.
pub fn kml_permissions(statement: &KmlStatement) -> Vec<Permission> {
    let mut needed: Vec<Permission> = Vec::new();
    for clause in &statement.clauses {
        for permission in clause_permissions(clause) {
            if !needed.contains(&permission) {
                needed.push(permission);
            }
        }
    }
    needed
}

/// What one clause needs.
///
/// Exhaustive on purpose: a new clause has to be given a permission before it
/// compiles, so an ungoverned write cannot arrive by omission.
pub fn clause_permissions(clause: &MutationClause) -> Vec<Permission> {
    match clause {
        MutationClause::CreateConcept(_)
        | MutationClause::EnsureProposition(_)
        | MutationClause::CreateEvidence(_)
        | MutationClause::CreateActivity(_) => vec![Permission::Create],
        // An upsert either creates or changes, and the caller cannot know
        // which in advance — so it asks for both rather than for whichever
        // turned out to happen.
        MutationClause::UpsertConcept(_) => vec![Permission::Create, Permission::Update],
        // The Assertion permission family is refined per Assertion in the write
        // path: recording another actor's claim and speaking as that actor are
        // different permissions, and which one applies depends on `asserted_by`
        // (§17, §18). `assert` is the floor.
        MutationClause::CreateAssertion(_) => vec![Permission::Assert],
        MutationClause::Update(_) => vec![Permission::Update],
        MutationClause::RetractAssertion(_) => vec![Permission::RetractOwn],
        MutationClause::SupersedeAssertion(_) => vec![Permission::SupersedeOwn],
        // Correcting Evidence is a maintenance act on an immutable record: it
        // writes a new record and links it, never edits the old one.
        MutationClause::CorrectEvidence(_) => vec![Permission::Create, Permission::Maintain],
        MutationClause::TransitionActivity(_) => vec![Permission::Update],
        MutationClause::SetRetention(_) => vec![Permission::ManageRetention],
        MutationClause::Archive(_) => vec![Permission::Archive],
        MutationClause::Tombstone(_) => vec![Permission::Tombstone],
        MutationClause::Purge(_) => vec![Permission::Purge],
        MutationClause::MergeConcept(_) => vec![Permission::MergeIdentity, Permission::Maintain],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anda_kip::{Command, parse_kip};

    fn kql(source: &str) -> KqlQuery {
        match parse_kip(source).unwrap() {
            Command::Kql(query) => query,
            other => panic!("expected KQL, got {other:?}"),
        }
    }

    fn meta(source: &str) -> MetaCommand {
        match parse_kip(source).unwrap() {
            Command::Meta(command) => command,
            other => panic!("expected META, got {other:?}"),
        }
    }

    fn kml(source: &str) -> KmlStatement {
        match parse_kip(source).unwrap() {
            Command::Kml(statement) => statement,
            other => panic!("expected KML, got {other:?}"),
        }
    }

    #[test]
    fn an_ordinary_query_asks_only_to_read() {
        assert_eq!(
            kql_permissions(&kql(r#"FIND(?c) WHERE { ?c CONCEPT {type: "Person"} }"#)),
            vec![Permission::Read]
        );
    }

    #[test]
    fn reading_the_past_is_a_separate_disclosure() {
        let needed = kql_permissions(&kql(
            r#"FIND(?c) WHERE { ?c CONCEPT {type: "Person"} } AS OF SEQ 3"#,
        ));
        assert!(needed.contains(&Permission::ReadHistory));
    }

    #[test]
    fn a_projection_asks_for_project_as_well_as_read() {
        let needed = kql_permissions(&kql(r#"FIND(?b) WHERE { ?b BELIEF (?s, "likes", ?o) }"#));
        assert!(needed.contains(&Permission::Read));
        assert!(needed.contains(&Permission::Project));
    }

    #[test]
    fn a_projection_nested_in_a_union_is_still_a_projection() {
        let needed = kql_permissions(&kql(
            r#"FIND(?b) WHERE { UNION { ?b BELIEF (?s, "likes", ?o) } }"#,
        ));
        assert!(needed.contains(&Permission::Project));
    }

    #[test]
    fn export_is_not_read() {
        // §271: the equation this whole table exists to preserve.
        let needed = meta_permissions(&meta(
            r#"EXPORT CAPSULE :out WHERE { ?c CONCEPT {type: "T"} }"#,
        ));
        assert_eq!(needed, vec![Permission::Export]);
        assert!(!needed.contains(&Permission::Read));
    }

    #[test]
    fn describing_the_engine_needs_no_authority() {
        // Otherwise an unauthorized caller could not learn how to become one.
        for command in ["DESCRIBE PROTOCOL", "DESCRIBE CAPABILITIES"] {
            assert!(meta_permissions(&meta(command)).is_empty(), "{command}");
        }
        assert!(meta_permissions(&meta("DESCRIBE EXECUTION CONTEXT")).is_empty());
    }

    #[test]
    fn describing_the_space_needs_discovery() {
        assert_eq!(
            meta_permissions(&meta("DESCRIBE PRIMER")),
            vec![Permission::Discover]
        );
        assert_eq!(
            meta_permissions(&meta("LIST TYPES")),
            vec![Permission::Discover]
        );
    }

    #[test]
    fn removal_and_erasure_ask_for_different_things() {
        // §271 again: logical removal is not erasure.
        assert_eq!(
            kml_permissions(&kml("TOMBSTONE :x")),
            vec![Permission::Tombstone]
        );
        assert_eq!(
            kml_permissions(&kml(r#"PURGE :x CONFIRM "PURGE""#)),
            vec![Permission::Purge]
        );
    }

    #[test]
    fn a_multi_clause_statement_asks_for_the_union() {
        let needed = kml_permissions(&kml(r#"MUTATE {
                CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }
                ARCHIVE :old
            }"#));
        assert!(needed.contains(&Permission::Create));
        assert!(needed.contains(&Permission::Archive));
    }
}
