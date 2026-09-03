/**
 * The KIP 2.0 cross-engine conformance fixtures — GENERATED FILE, DO NOT EDIT.
 *
 * Source of truth: `fixtures/kip-conformance-2.0/*.json`, which the Rust
 * engine's `tests/conformance.rs` reads from disk. Regenerate with
 * `pnpm run codegen:fixtures`.
 */

/** One expectation: a result to match, or the registry code to fail with. */
export interface Expectation {
  result?: unknown
  error?: string
}

export interface Case {
  name: string
  command: string
  params?: Record<string, unknown>
  expect: Expectation
  /** Whether the order of a top-level result array is part of the contract. */
  ordered?: boolean
  /**
   * Extra request-envelope members, merged over the ones the harness builds.
   *
   * Most behaviour is decided by the command, but some of it is decided by the
   * envelope around the command — ingest, execution.idempotency_key — and
   * those are cross-engine contracts too.
   */
  envelope?: Record<string, unknown>
  /**
   * The normative conformance vectors this case pins, by their §27 short names
   * (CORE-001, KML-031, …) — the ones §102's invariant registry names. Read by
   * the Rust harness's coverage report; the
   * TypeScript harness carries them so the two run the same fixture file.
   */
  vectors?: string[]
}

export interface Fixture {
  name: string
  description: string
  /** Extra Schema Package artifacts to install and activate, inline. */
  packages?: unknown[]
  setup?: string[]
  cases: Case[]
}

export const FIXTURES: readonly Fixture[] = [
  {
    "name": "consequence",
    "description": "The consequence channel: what the world did after the Brain acted, and what a Skill's standing is spent from. Outcome Evidence (Spec §15.7) carries an OutcomeRecord Facet — the graded index over an untouched payload — and cognition subscribes to a stream by task family rather than by reference. What an engine owes here is the Profile's schema discipline: the scoring handle a Skill cannot be compiled without, the four lifecycle states, a graded index its subject cannot rewrite, and the one guarded statement (Appendix F.6) a lifecycle verdict executes as. The verdict rule itself is Brain policy; that it lands as one recomputable transition is not.",
    "setup": [
      "MUTATE {\n  CREATE CONCEPT ?skill {\n    TYPE \"Skill\"\n    NAME \"Deploy behind a pre-flight migration check\"\n    SET ATTRIBUTES {\n      skill_class: \"workflow\",\n      task_family: \"deploy/pre-flight\",\n      summary: \"Dry-run the migration before the deploy\",\n      procedure: \"1. dry-run the migration 2. deploy 3. verify\",\n      status: \"proposed\"\n    }\n    SET FACET \"MnemonicState\" {utility: 0.5}\n  }\n}",
      "MUTATE {\n  CREATE EVIDENCE ?win {\n    SET FIELDS {\n      evidence_class: \"outcome\",\n      payload: \"deploy 41: the pre-flight check caught the drift, rollout clean\",\n      media_type: \"text/plain\",\n      observed_at: \"2026-08-20T09:00:00Z\"\n    }\n    SET FACET \"OutcomeRecord\" {task_family: \"deploy/pre-flight\", outcome_status: \"success\", magnitude: 0.8}\n  }\n  CREATE EVIDENCE ?loss {\n    SET FIELDS {\n      evidence_class: \"outcome\",\n      payload: \"deploy 42: pre-flight passed, rollout still failed on a stale replica\",\n      media_type: \"text/plain\",\n      observed_at: \"2026-08-21T09:00:00Z\"\n    }\n    SET FACET \"OutcomeRecord\" {task_family: \"deploy/pre-flight\", outcome_status: \"failure\"}\n  }\n  CREATE ACTIVITY ?observed {\n    SET FIELDS {activity_class: \"outcome_observation\", status: \"completed\"}\n    SET STRUCTURAL {\n      (\"outputs\", ?win)\n      (\"outputs\", ?loss)\n    }\n  }\n}"
    ],
    "cases": [
      {
        "name": "the channel is a graded index over Evidence, keyed by the family it grades",
        "command": "FIND(?e.facets[\"OutcomeRecord\"].task_family, ?e.facets[\"OutcomeRecord\"].outcome_status) WHERE { ?e EVIDENCE {evidence_class: \"outcome\"} }",
        "expect": {
          "result": [
            [
              "deploy/pre-flight",
              "success"
            ],
            [
              "deploy/pre-flight",
              "failure"
            ]
          ]
        },
        "vectors": [
          "X-016"
        ]
      },
      {
        "name": "an optional grade member may be absent without the grade being incomplete",
        "command": "FIND(?e.facets[\"OutcomeRecord\"].magnitude) WHERE { ?e EVIDENCE {evidence_class: \"outcome\"} }",
        "expect": {
          "result": [
            0.8,
            null
          ]
        }
      },
      {
        "name": "a Skill must name the outcome stream that could prove it wrong",
        "command": "MUTATE {\n  CREATE CONCEPT ?s {\n    TYPE \"Skill\"\n    NAME \"Be careful\"\n    SET ATTRIBUTES {skill_class: \"heuristic\", summary: \"Think first\", procedure: \"think\", status: \"proposed\"}\n  }\n}",
        "expect": {
          "error": "ConstraintViolation"
        }
      },
      {
        "name": "the retired lifecycle vocabulary is not a lifecycle state",
        "command": "MUTATE {\n  CREATE CONCEPT ?s {\n    TYPE \"Skill\"\n    NAME \"Deploy on Fridays\"\n    SET ATTRIBUTES {skill_class: \"workflow\", task_family: \"deploy/pre-flight\", summary: \"Ship it\", procedure: \"ship\", status: \"validated\"}\n  }\n}",
        "expect": {
          "error": "ConstraintViolation"
        }
      },
      {
        "name": "a grade is not rewritable by the cognition it grades",
        "command": "UPDATE \"E-1\" SET FACET \"OutcomeRecord\" {outcome_status: \"failure\"}",
        "expect": {
          "error": "ConstraintViolation"
        },
        "vectors": [
          "GOV-026",
          "X-017"
        ]
      },
      {
        "name": "and erasing a grade is rewriting it to absent",
        "command": "UPDATE \"E-1\" UNSET FACET \"OutcomeRecord\" {outcome_status}",
        "expect": {
          "error": "ConstraintViolation"
        }
      },
      {
        "name": "the graded index is closed: an instrument cannot smuggle a verdict into it",
        "command": "UPDATE \"E-1\" SET FACET \"OutcomeRecord\" {verdict: \"promote\"}",
        "expect": {
          "error": "ConstraintViolation"
        },
        "vectors": [
          "X-016"
        ]
      },
      {
        "name": "an optional grade member may still be established after the fact, once",
        "command": "MUTATE {\n  UPDATE \"E-2\"\n  SET FACET \"OutcomeRecord\" {magnitude: 0.25}\n}",
        "expect": {}
      },
      {
        "name": "establishing it is not a licence to revise it",
        "command": "UPDATE \"E-2\" SET FACET \"OutcomeRecord\" {magnitude: 0.9}",
        "expect": {
          "error": "ConstraintViolation"
        }
      },
      {
        "name": "a lifecycle move is one guarded statement: the verdict Activity and the transition commit together",
        "command": "MUTATE {\n  CREATE ACTIVITY ?verdict {\n    SET FIELDS {\n      activity_class: \"lifecycle_verdict\",\n      status: \"completed\",\n      parameters_digest: \"sha3-256:ru1e\"\n    }\n    SET STRUCTURAL {\n      (\"inputs\", \"E-1\")\n      (\"inputs\", \"E-2\")\n      (\"outputs\", \"C-1\")\n    }\n  }\n  UPDATE \"C-1\"\n  SET ATTRIBUTES {status: \"trialed\"}\n  SET FACET \"GradingState\" {success_count: 1, failure_count: 1, graded_count: 2}\n  EXPECT VERSION 1\n}",
        "expect": {}
      },
      {
        "name": "standing is what the verdict moved, the tallies count graded outcomes, and the admission bet stays on MnemonicState",
        "command": "FIND(?s.attributes.status, ?s.facets[\"GradingState\"].graded_count, ?s.facets[\"MnemonicState\"].utility) WHERE { ?s CONCEPT {type: \"Skill\"} }",
        "expect": {
          "result": [
            [
              "trialed",
              2,
              0.5
            ]
          ]
        }
      },
      {
        "name": "replaying the same verdict against the version it already consumed is refused",
        "command": "MUTATE {\n  UPDATE \"C-1\"\n  SET ATTRIBUTES {status: \"adopted\"}\n  EXPECT VERSION 1\n}",
        "expect": {
          "error": "VersionConflict"
        }
      },
      {
        "name": "the verdict is recomputable: its rule and the outcomes it read are still on the record",
        "command": "FIND(?v.parameters_digest, ?v.inputs) WHERE { ?v ACTIVITY {activity_class: \"lifecycle_verdict\"} }",
        "expect": {
          "result": [
            [
              "sha3-256:ru1e",
              [
                {
                  "id": "E:<1>"
                },
                {
                  "id": "E:<2>"
                }
              ]
            ]
          ]
        }
      },
      {
        "name": "the lifecycle enum is not only a creation-time contract",
        "command": "UPDATE \"C-1\" SET ATTRIBUTES {status: \"validated\"}",
        "expect": {
          "error": "ConstraintViolation"
        }
      },
      {
        "name": "a refused UPDATE leaves the standing it failed to move",
        "command": "FIND(?s.attributes.status) WHERE { ?s CONCEPT {type: \"Skill\"} }",
        "expect": {
          "result": [
            "trialed"
          ]
        }
      },
      {
        "name": "nor is the scoring handle: UPDATE cannot unset what the type requires",
        "command": "UPDATE \"C-1\" UNSET ATTRIBUTES {task_family}",
        "expect": {
          "error": "ConstraintViolation"
        }
      },
      {
        "name": "nor is a declared type",
        "command": "UPDATE \"C-1\" SET ATTRIBUTES {task_family: 7}",
        "expect": {
          "error": "ConstraintViolation"
        }
      },
      {
        "name": "a statement is judged by the state it ends in, not by the order of its clauses",
        "command": "MUTATE {\n  UPDATE \"C-1\"\n  UNSET ATTRIBUTES {summary}\n  SET ATTRIBUTES {summary: \"Dry-run the migration, then deploy\"}\n}",
        "expect": {}
      },
      {
        "name": "and the transition the verdict licensed still goes through",
        "command": "MUTATE {\n  UPDATE \"C-1\"\n  SET ATTRIBUTES {status: \"adopted\"}\n}",
        "expect": {}
      },
      {
        "name": "the Skill that came out the other side",
        "command": "FIND(?s.attributes.status, ?s.attributes.summary) WHERE { ?s CONCEPT {type: \"Skill\"} }",
        "expect": {
          "result": [
            [
              "adopted",
              "Dry-run the migration, then deploy"
            ]
          ]
        }
      },
      {
        "name": "an instrumented run records the identity the grading joined on",
        "command": "MUTATE {\n  CREATE CONCEPT ?run {\n    TYPE \"GradedRun\"\n    NAME \"deploy 42\"\n    SET ATTRIBUTES {run_id: \"deploy-42\", note: \"stale replica\"}\n  }\n}",
        "expect": {}
      },
      {
        "name": "what a verdict actually graded cannot be relabelled afterwards",
        "command": "UPDATE ?r SET ATTRIBUTES {run_id: \"deploy-99\"} WHERE { ?r CONCEPT {type: \"GradedRun\"} }",
        "expect": {
          "error": "ConstraintViolation"
        }
      },
      {
        "name": "what was only ever commentary still moves",
        "command": "MUTATE {\n  UPDATE ?r\n  SET ATTRIBUTES {note: \"stale replica, since drained\"}\n  WHERE { ?r CONCEPT {type: \"GradedRun\"} }\n}",
        "expect": {}
      },
      {
        "name": "the run, after the one revision its type allowed",
        "command": "FIND(?r.attributes.run_id, ?r.attributes.note) WHERE { ?r CONCEPT {type: \"GradedRun\"} }",
        "expect": {
          "result": [
            [
              "deploy-42",
              "stale replica, since drained"
            ]
          ]
        }
      },
      {
        "name": "the graded index belongs on the Evidence, not on the cognition it grades",
        "command": "MUTATE {\n  UPDATE \"C-1\"\n  SET FACET \"OutcomeRecord\" {task_family: \"deploy/pre-flight\", outcome_status: \"success\"}\n}",
        "expect": {
          "error": "ConstraintViolation"
        }
      },
      {
        "name": "and the tallies belong on the Skill, not on the outcome that moved them",
        "command": "MUTATE {\n  UPDATE \"E-1\"\n  SET FACET \"GradingState\" {graded_count: 1}\n}",
        "expect": {
          "error": "ConstraintViolation"
        }
      }
    ],
    "packages": [
      {
        "format": "KIP-Schema-Package",
        "manifest": {
          "package_id": "kip://test/instrumentation",
          "version": "1.0.0"
        },
        "definitions": {
          "concept_types": {
            "GradedRun": {
              "kind": "ConceptType",
              "description": "One instrumented run an outcome stream graded. `run_id` is immutable because a verdict binds to what it actually graded: relabelling the run afterwards would silently move a grade onto something else.",
              "attributes": {
                "open": false,
                "fields": {
                  "run_id": {
                    "type": "string",
                    "required": true,
                    "mutable": false
                  },
                  "note": {
                    "type": "string",
                    "required": false,
                    "mutable": true
                  }
                }
              }
            }
          }
        }
      }
    ]
  },
  {
    "name": "core-truth-neutrality",
    "description": "The distinction the version exists for: a Proposition existing is not the Proposition being true. A tuple carries no confidence, the same tuple resolves to one Proposition, and a raw read reports claims rather than beliefs.",
    "setup": [
      "MUTATE {\n  CREATE CONCEPT ?alice { TYPE \"Person\" NAME \"Alice\" }\n  CREATE CONCEPT ?dark { TYPE \"Preference\" NAME \"Dark\" }\n  ENSURE PROPOSITION ?p (?alice, \"prefers\", ?dark)\n  CREATE EVIDENCE ?e {\n    SET FIELDS { evidence_class: \"user_statement\", payload: \"I prefer dark mode\" }\n  }\n  CREATE ASSERTION ?a {\n    SET FIELDS { proposition: ?p, asserted_by: ?alice, stance: \"support\", mode: \"stated\", confidence: 0.9 }\n    SET STRUCTURAL { (\"evidence\", ?e) { role: \"support\" } }\n  }\n}",
      "CREATE EVIDENCE ?e {\n  CLIENT KEY \"message:42:evidence\"\n  SET FIELDS { evidence_class: \"user_statement\", payload: \"I prefer dark mode\" }\n}",
      "CREATE EVIDENCE ?e {\n  CLIENT KEY \"message:42:evidence\"\n  SET FIELDS { evidence_class: \"user_statement\", payload: \"I prefer dark mode\" }\n}"
    ],
    "cases": [
      {
        "name": "a Proposition carries no confidence",
        "command": "FIND(?p.confidence) WHERE { ?p PROPOSITION (?s, \"prefers\", ?o) }",
        "expect": {
          "result": [
            null
          ]
        },
        "vectors": [
          "CORE-001"
        ]
      },
      {
        "name": "the Assertion about it does",
        "command": "FIND(?a.confidence, ?a.stance, ?a.mode) WHERE { ?a ASSERTION {} }",
        "expect": {
          "result": [
            [
              0.9,
              "support",
              "stated"
            ]
          ]
        },
        "vectors": [
          "CORE-006"
        ]
      },
      {
        "name": "a local predicate name is persisted as its exact symbol",
        "command": "FIND(?p.predicate_ref) WHERE { ?p PROPOSITION (?s, \"prefers\", ?o) }",
        "expect": {
          "result": [
            "kip://profiles/cognitive-memory@2.0.0/prefers"
          ]
        }
      },
      {
        "name": "a Concept's type is persisted as its exact symbol",
        "command": "FIND(?c.schema_ref) WHERE { ?c CONCEPT {name: \"Alice\"} }",
        "expect": {
          "result": [
            "kip://profiles/cognitive-memory@2.0.0/Person"
          ]
        }
      },
      {
        "name": "ENSURE resolves the same tuple rather than duplicating it",
        "command": "FIND(COUNT(?p)) WHERE { ?p PROPOSITION (?s, \"prefers\", ?o) }",
        "expect": {
          "result": [
            1
          ]
        },
        "vectors": [
          "CORE-002"
        ]
      },
      {
        "name": "a new element starts at version 1",
        "command": "FIND(?c._system.version) WHERE { ?c CONCEPT {name: \"Alice\"} }",
        "expect": {
          "result": [
            1
          ]
        }
      },
      {
        "name": "an unknown type is refused, never invented",
        "command": "CREATE CONCEPT ?x { TYPE \"Spaceship\" NAME \"Enterprise\" }",
        "expect": {
          "error": "SchemaSymbolNotFound"
        }
      },
      {
        "name": "an Assertion's epistemic payload cannot be edited",
        "command": "UPDATE ?a SET FIELDS { confidence: 0.1 } WHERE { ?a ASSERTION {} }",
        "expect": {
          "error": "InvalidSyntax"
        },
        "vectors": [
          "CORE-007"
        ]
      },
      {
        "name": "a reference slot carries the name and the shape the Specification fixes",
        "command": "FIND(?a.proposition.id, ?a.asserted_by.id) WHERE { ?a ASSERTION {} }",
        "expect": {
          "result": [
            [
              "P:<1>",
              "C:<2>"
            ]
          ]
        }
      },
      {
        "name": "a citation is {id, role}, under the slot named evidence",
        "command": "FIND(?a.evidence) WHERE { ?a ASSERTION {} }",
        "expect": {
          "result": [
            [
              {
                "id": "E:<1>",
                "role": "support"
              }
            ]
          ]
        }
      },
      {
        "name": "a Proposition carries no author-writable attribute bag",
        "command": "UPDATE ?p SET ATTRIBUTES { note: \"about the tuple\" } WHERE { ?p PROPOSITION (?s, \"prefers\", ?o) }",
        "expect": {
          "error": "ImmutableField"
        }
      },
      {
        "name": "a stance the Core Package does not name is refused, bound parameter or not",
        "command": "MUTATE {\n  ENSURE PROPOSITION ?p (:alice, \"prefers\", :dark)\n  CREATE ASSERTION ?a {\n    SET FIELDS { proposition: ?p, asserted_by: :alice, stance: :stance, mode: \"stated\" }\n  }\n}",
        "params": {
          "alice": {
            "id": "C-1"
          },
          "dark": {
            "id": "C-2"
          },
          "stance": "maybe"
        },
        "expect": {
          "error": "ConstraintViolation"
        }
      },
      {
        "name": "confidence is epistemic support in [0, 1] at both ends",
        "command": "MUTATE {\n  ENSURE PROPOSITION ?p (:alice, \"prefers\", :dark)\n  CREATE ASSERTION ?a {\n    SET FIELDS { proposition: ?p, asserted_by: :alice, stance: \"support\", mode: \"stated\", confidence: :c }\n  }\n}",
        "params": {
          "alice": {
            "id": "C-1"
          },
          "dark": {
            "id": "C-2"
          },
          "c": -0.5
        },
        "expect": {
          "error": "ConstraintViolation"
        }
      },
      {
        "name": "an Evidence citation role comes from the Core registry",
        "command": "MUTATE {\n  ENSURE PROPOSITION ?p (:alice, \"prefers\", :dark)\n  CREATE EVIDENCE ?e { SET FIELDS { evidence_class: \"user_statement\", payload: \"x\" } }\n  CREATE ASSERTION ?a {\n    SET FIELDS { proposition: ?p, asserted_by: :alice, stance: \"support\", mode: \"stated\" }\n    SET STRUCTURAL { (\"evidence\", ?e) { role: :role } }\n  }\n}",
        "params": {
          "alice": {
            "id": "C-1"
          },
          "dark": {
            "id": "C-2"
          },
          "role": "vouches"
        },
        "expect": {
          "error": "ConstraintViolation"
        }
      },
      {
        "name": "and so does an Activity status, whether it is written or bound",
        "command": "CREATE ACTIVITY ?a { SET FIELDS {activity_class: \"test\", status: :s} }",
        "params": {
          "s": "banana"
        },
        "expect": {
          "error": "ConstraintViolation"
        }
      },
      {
        "name": "a CLIENT KEY makes a resend a retry rather than a second creation",
        "command": "FIND(COUNT(?e)) WHERE { ?e EVIDENCE {evidence_class: \"user_statement\"} }",
        "expect": {
          "result": [
            2
          ]
        }
      }
    ]
  },
  {
    "name": "derivation",
    "description": "What a Space can find out about cognition it built on something else, and what byte destruction may and may not take with it. LIST DEPENDENTS walks provenance in the derived direction so a revised root's downstream artifacts can be reviewed instead of guessed at (Spec §57.5, §63.5); reachability is topology, not a verdict. PURGE PAYLOAD destroys Evidence bytes while the record, its digest, its citations and its provenance role survive (§60.6) — the data-minimization instrument, which is a different promise from element purge.",
    "setup": [
      "MUTATE {\n  CREATE CONCEPT ?event {\n    TYPE \"Event\"\n    NAME \"Migration meeting\"\n    SET ATTRIBUTES {summary: \"The team agreed to migrate on Friday\"}\n  }\n  CREATE CONCEPT ?insight {\n    TYPE \"Insight\"\n    NAME \"Migrations need a rollback plan\"\n    SET ATTRIBUTES {summary: \"Every migration ships with a rollback\"}\n  }\n  CREATE ACTIVITY ?consolidate {\n    SET FIELDS {activity_class: \"semantic_consolidation\", status: \"completed\"}\n    SET STRUCTURAL {\n      (\"inputs\", ?event)\n      (\"outputs\", ?insight)\n    }\n  }\n}",
      "MUTATE {\n  CREATE CONCEPT ?skill {\n    TYPE \"Skill\"\n    NAME \"Plan a migration\"\n    SET ATTRIBUTES {\n      skill_class: \"workflow\",\n      task_family: \"migration/rollback\",\n      summary: \"Write the rollback first\",\n      procedure: \"1. write the rollback 2. migrate\",\n      status: \"proposed\"\n    }\n  }\n  CREATE ACTIVITY ?compile {\n    SET FIELDS {activity_class: \"procedural_consolidation\", status: \"completed\"}\n    SET STRUCTURAL {\n      (\"inputs\", \"C-2\")\n      (\"outputs\", ?skill)\n    }\n  }\n}",
      "MUTATE {\n  CREATE CONCEPT ?alice { TYPE \"Person\" NAME \"Alice\" }\n  CREATE CONCEPT ?dark { TYPE \"Preference\" NAME \"Dark\" }\n  ENSURE PROPOSITION ?p (?alice, \"prefers\", ?dark)\n  CREATE EVIDENCE ?e {\n    SET FIELDS {\n      evidence_class: \"user_statement\",\n      payload: \"I prefer dark mode, and my address is 12 Elm Street.\",\n      content_digest: \"sha3-256:d1ge5t\",\n      media_type: \"text/plain\",\n      observed_at: \"2026-08-16T09:00:00Z\"\n    }\n  }\n  CREATE ASSERTION ?a {\n    SET FIELDS { proposition: ?p, asserted_by: ?alice, stance: \"support\", mode: \"stated\", confidence: 0.9 }\n    SET STRUCTURAL { (\"evidence\", ?e) {role: \"support\"} }\n  }\n}"
    ],
    "cases": [
      {
        "name": "one hop of provenance runs inputs to outputs, and names the Activity it came through",
        "command": "LIST DEPENDENTS \"C-1\"",
        "ordered": true,
        "expect": {
          "result": [
            {
              "id": "C:<1>",
              "kind": "concept",
              "distance": 1,
              "via": {
                "activity": "X:<2>"
              }
            }
          ]
        },
        "vectors": [
          "META-025"
        ]
      },
      {
        "name": "DEPTH is what turns one hop into the closure",
        "command": "LIST DEPENDENTS \"C-1\" DEPTH 2",
        "ordered": true,
        "expect": {
          "result": [
            {
              "id": "C:<1>",
              "kind": "concept",
              "distance": 1,
              "via": {
                "activity": "X:<2>"
              }
            },
            {
              "id": "C:<3>",
              "kind": "concept",
              "distance": 2,
              "via": {
                "activity": "X:<4>"
              }
            }
          ]
        }
      },
      {
        "name": "the closure pages like every other LIST",
        "command": "LIST DEPENDENTS \"C-1\" DEPTH 2 LIMIT 1",
        "ordered": true,
        "expect": {
          "result": [
            {
              "id": "C:<1>",
              "kind": "concept",
              "distance": 1,
              "via": {
                "activity": "X:<2>"
              }
            }
          ]
        }
      },
      {
        "name": "reachability is topology, not a verdict: the listed artifact is untouched",
        "command": "FIND(?c._system.state, ?c._system.version) WHERE { ?c CONCEPT {id: \"C-2\"} }",
        "expect": {
          "result": [
            [
              "active",
              1
            ]
          ]
        },
        "vectors": [
          "EPI-028"
        ]
      },
      {
        "name": "a transformation that recorded no Activity lineage is not discoverable",
        "command": "LIST DEPENDENTS \"C-3\" DEPTH 4",
        "expect": {
          "result": []
        }
      },
      {
        "name": "an unknown root is answered exactly as an absent one",
        "command": "LIST DEPENDENTS \"C-999\"",
        "expect": {
          "result": []
        }
      },
      {
        "name": "DEPTH 0 asks for the element itself, which is not one of its dependents",
        "command": "LIST DEPENDENTS \"C-1\" DEPTH 0",
        "expect": {
          "error": "ConstraintViolation"
        }
      },
      {
        "name": "a DEPTH that is not a non-negative integer is a type error, not a bound",
        "command": "LIST DEPENDENTS \"C-1\" DEPTH :n",
        "params": {
          "n": -1
        },
        "expect": {
          "error": "TypeMismatch"
        }
      },
      {
        "name": "and neither is a word that happens to sit where a number goes",
        "command": "LIST DEPENDENTS \"C-1\" DEPTH :n",
        "params": {
          "n": "deep"
        },
        "expect": {
          "error": "TypeMismatch"
        }
      },
      {
        "name": "the root operand is a string, not whatever coerces to one",
        "command": "LIST DEPENDENTS :root",
        "params": {
          "root": 1
        },
        "expect": {
          "error": "TypeMismatch"
        }
      },
      {
        "name": "a payload purge is confirmed with the exact literal, or it is not a purge",
        "command": "PURGE PAYLOAD \"E-1\" CONFIRM \"purge\"",
        "expect": {
          "error": "InvalidSyntax"
        }
      },
      {
        "name": "only Evidence has a payload to purge",
        "command": "PURGE PAYLOAD \"C-1\" CONFIRM \"PURGE\"",
        "expect": {
          "error": "ConstraintViolation"
        }
      },
      {
        "name": "and the refusal erased nothing on the way to refusing",
        "command": "FIND(?e.payload.mode) WHERE { ?e EVIDENCE {} }",
        "expect": {
          "result": [
            "inline"
          ]
        }
      },
      {
        "name": "a payload purge destroys the bytes",
        "command": "PURGE PAYLOAD \"E-1\" CONFIRM \"PURGE\"",
        "expect": {},
        "vectors": [
          "KML-034"
        ]
      },
      {
        "name": "the payload reports that it was purged, rather than reporting nothing",
        "command": "FIND(?e.payload) WHERE { ?e EVIDENCE {} }",
        "expect": {
          "result": [
            {
              "mode": "purged"
            }
          ]
        }
      },
      {
        "name": "the record survives with its digest, class and observation time",
        "command": "FIND(?e.content_digest, ?e.evidence_class, ?e.media_type, ?e.observed_at, ?e._system.state) WHERE { ?e EVIDENCE {} }",
        "expect": {
          "result": [
            [
              "sha3-256:d1ge5t",
              "user_statement",
              "text/plain",
              "2026-08-16T09:00:00.000Z",
              "active"
            ]
          ]
        },
        "vectors": [
          "KML-034"
        ]
      },
      {
        "name": "and so does the citation that pointed at it",
        "command": "FIND(?a.evidence) WHERE { ?a ASSERTION {} }",
        "expect": {
          "result": [
            [
              {
                "id": "E:<1>",
                "role": "support"
              }
            ]
          ]
        }
      },
      {
        "name": "purging an already-purged payload is a no-op, not an error",
        "command": "PURGE PAYLOAD \"E-1\" CONFIRM \"PURGE\"",
        "expect": {}
      }
    ]
  },
  {
    "name": "epistemic-projection",
    "description": "Belief is projected from the Assertions on record. Silence is insufficient and never rejection; repetition is not corroboration; material disagreement is contested rather than decided.",
    "packages": [
      {
        "format": "KIP-Schema-Package",
        "manifest": {
          "package_id": "kip://conformance/status",
          "version": "1.0.0"
        },
        "definitions": {
          "concept_types": {
            "Service": {
              "kind": "ConceptType",
              "description": "A service."
            },
            "Status": {
              "kind": "ConceptType",
              "description": "A status value."
            }
          },
          "predicates": {
            "status": {
              "kind": "PredicateType",
              "description": "Single-valued current status.",
              "functional": true
            }
          }
        }
      }
    ],
    "setup": [
      "MUTATE {\n  CREATE CONCEPT ?alice { TYPE \"Person\" NAME \"Alice\" }\n  CREATE CONCEPT ?bob { TYPE \"Person\" NAME \"Bob\" }\n  CREATE CONCEPT ?carol { TYPE \"Person\" NAME \"Carol\" }\n  CREATE CONCEPT ?quiet { TYPE \"Preference\" NAME \"Quiet\" }\n  CREATE CONCEPT ?loud { TYPE \"Preference\" NAME \"Loud\" }\n  ENSURE PROPOSITION ?unspoken (?alice, \"prefers\", ?quiet)\n  ENSURE PROPOSITION ?repeated (?alice, \"prefers\", ?loud)\n}",
      "MUTATE {\n  CREATE CONCEPT ?svc { TYPE \"Service\" NAME \"api\" }\n  CREATE CONCEPT ?healthy { TYPE \"Status\" NAME \"healthy\" }\n  CREATE CONCEPT ?degraded { TYPE \"Status\" NAME \"degraded\" }\n  ENSURE PROPOSITION ?ok (?svc, \"status\", ?healthy)\n  ENSURE PROPOSITION ?bad (?svc, \"status\", ?degraded)\n}",
      "MUTATE {\n  CREATE CONCEPT ?dave { TYPE \"Person\" NAME \"Dave\" }\n  CREATE CONCEPT ?warm { TYPE \"Preference\" NAME \"Warm\" }\n  ENSURE PROPOSITION ?p (?dave, \"prefers\", ?warm)\n  CREATE EVIDENCE ?seen { SET FIELDS { evidence_class: \"observation\", payload: \"one observation\" } }\n  CREATE ASSERTION ?a1 {\n    SET FIELDS { proposition: ?p, asserted_by: ?dave, stance: \"support\", mode: \"stated\", confidence: 0.6 }\n    SET STRUCTURAL { (\"evidence\", ?seen) { role: \"support\" } }\n  }\n  CREATE ASSERTION ?a2 {\n    SET FIELDS { proposition: ?p, asserted_by: ?dave, stance: \"support\", mode: \"stated\", confidence: 0.6 }\n    SET STRUCTURAL { (\"evidence\", ?seen) { role: \"support\" } }\n  }\n}"
    ],
    "cases": [
      {
        "name": "nothing on record is insufficient, not rejected",
        "command": "FIND(?b.status) WHERE {\n  ?s CONCEPT {name: \"Alice\"}\n  ?o CONCEPT {name: \"Quiet\"}\n  ?p PROPOSITION (?s, \"prefers\", ?o)\n  ?b BELIEF (?p)\n}",
        "expect": {
          "result": [
            "insufficient"
          ]
        },
        "vectors": [
          "EPI-001",
          "EPI-025"
        ]
      },
      {
        "name": "an unsupported Proposition has no support and no opposition",
        "command": "FIND(?b.support.score, ?b.opposition.score) WHERE {\n  ?s CONCEPT {name: \"Alice\"}\n  ?o CONCEPT {name: \"Quiet\"}\n  ?p PROPOSITION (?s, \"prefers\", ?o)\n  ?b BELIEF (?p)\n}",
        "expect": {
          "result": [
            [
              0,
              0
            ]
          ]
        }
      },
      {
        "name": "a projection declares that its score is not a probability",
        "command": "FIND(?b.support.score_semantics) WHERE {\n  ?s CONCEPT {name: \"Alice\"}\n  ?o CONCEPT {name: \"Quiet\"}\n  ?p PROPOSITION (?s, \"prefers\", ?o)\n  ?b BELIEF (?p)\n}",
        "expect": {
          "result": [
            "normalized_support_not_probability"
          ]
        },
        "vectors": [
          "EPI-007"
        ]
      },
      {
        "name": "a projection reports the policy it ran under",
        "command": "FIND(?b.policy.id) WHERE {\n  ?s CONCEPT {name: \"Alice\"}\n  ?o CONCEPT {name: \"Quiet\"}\n  ?p PROPOSITION (?s, \"prefers\", ?o)\n  ?b BELIEF (?p)\n}",
        "expect": {
          "result": [
            "kip:policy:baseline"
          ]
        }
      },
      {
        "name": "BELIEF over an unbound variable is refused, not guessed",
        "command": "FIND(?b) WHERE { ?b BELIEF (?nothing) }",
        "expect": {
          "error": "ProjectionTargetUnbound"
        }
      },
      {
        "name": "BELIEF SLOT over an unbound subject is refused",
        "command": "FIND(?slot) WHERE { ?slot BELIEF SLOT (?anything, \"prefers\") }",
        "expect": {
          "error": "ProjectionTargetUnbounded"
        }
      },
      {
        "name": "an unknown epistemic policy is named rather than defaulted",
        "command": "FIND(?b) WHERE {\n  ?s CONCEPT {name: \"Alice\"}\n  ?o CONCEPT {name: \"Quiet\"}\n  ?p PROPOSITION (?s, \"prefers\", ?o)\n  ?b BELIEF (?p)\n} WITH EPISTEMIC {policy: \"strict\"}",
        "expect": {
          "error": "ProjectionPolicyUnavailable"
        }
      },
      {
        "name": "repetition is one voice: a side reports its roots, not its rows",
        "command": "FIND(?b.support.root_groups) WHERE {\n  ?s CONCEPT {name: \"Dave\"}\n  ?o CONCEPT {name: \"Warm\"}\n  ?p PROPOSITION (?s, \"prefers\", ?o)\n  ?b BELIEF (?p)\n}",
        "expect": {
          "result": [
            [
              {
                "actors": [
                  {
                    "id": "C:<1>"
                  }
                ],
                "assertion_ids": [
                  "A:<2>",
                  "A:<3>"
                ],
                "contribution": 0.6,
                "evidence": [
                  "E:<4>"
                ]
              }
            ]
          ]
        },
        "vectors": [
          "EPI-015"
        ]
      },
      {
        "name": "two claims relaying one observation score as one",
        "command": "FIND(?b.support.score) WHERE {\n  ?s CONCEPT {name: \"Dave\"}\n  ?o CONCEPT {name: \"Warm\"}\n  ?p PROPOSITION (?s, \"prefers\", ?o)\n  ?b BELIEF (?p)\n}",
        "expect": {
          "result": [
            0.6
          ]
        },
        "vectors": [
          "EPI-018"
        ]
      },
      {
        "name": "a fully grounded BELIEF about a Proposition nobody created still answers",
        "command": "FIND(?b.status, ?b.proposition_id) WHERE { ?b BELIEF (:bob, \"prefers\", :quiet) }",
        "params": {
          "bob": {
            "id": "C-2"
          },
          "quiet": {
            "id": "C-4"
          }
        },
        "expect": {
          "result": [
            [
              "insufficient",
              null
            ]
          ]
        }
      },
      {
        "name": "explanation: none returns no ledger rather than an empty one",
        "command": "FIND(?b.explanation) WHERE {\n  ?s CONCEPT {name: \"Dave\"}\n  ?o CONCEPT {name: \"Warm\"}\n  ?p PROPOSITION (?s, \"prefers\", ?o)\n  ?b BELIEF (?p)\n} WITH EPISTEMIC { explanation: \"none\" }",
        "expect": {
          "result": [
            null
          ]
        }
      },
      {
        "name": "a setting WITH EPISTEMIC does not implement is refused, never ignored",
        "command": "FIND(?b.status) WHERE {\n  ?s CONCEPT {name: \"Dave\"}\n  ?o CONCEPT {name: \"Warm\"}\n  ?p PROPOSITION (?s, \"prefers\", ?o)\n  ?b BELIEF (?p)\n} WITH EPISTEMIC { curiosity: 0.5 }",
        "expect": {
          "error": "SchemaFieldNotFound"
        }
      },
      {
        "name": "BELIEF SLOT takes a subject an earlier pattern bound",
        "command": "FIND(?slot.status) WHERE {\n  ?s CONCEPT {name: \"Alice\"}\n  ?slot BELIEF SLOT (?s, \"prefers\")\n}",
        "expect": {
          "result": [
            "insufficient"
          ]
        }
      },
      {
        "name": "a bound BELIEF SLOT stays joined to the row it came from",
        "command": "FIND(?s.name, ?slot.status) WHERE {\n  ?s CONCEPT {type: \"Person\", name: \"Alice\"}\n  ?slot BELIEF SLOT (?s, \"prefers\")\n}",
        "expect": {
          "result": [
            [
              "Alice",
              "insufficient"
            ]
          ]
        }
      },
      {
        "name": "a BELIEF triple stays joined to the row its endpoints came from",
        "command": "FIND(?s.name, ?o.name, ?b.status) WHERE {\n  ?s CONCEPT {name: \"Alice\"}\n  ?o CONCEPT {name: \"Quiet\"}\n  ?b BELIEF (?s, \"prefers\", ?o)\n}",
        "expect": {
          "result": [
            [
              "Alice",
              "Quiet",
              "insufficient"
            ]
          ]
        }
      },
      {
        "name": "a BELIEF triple with an open object answers one belief per Proposition",
        "command": "FIND(?o.name) WHERE {\n  ?s CONCEPT {name: \"Alice\"}\n  ?b BELIEF (?s, \"prefers\", ?o)\n}",
        "expect": {
          "result": [
            "Loud",
            "Quiet"
          ]
        }
      },
      {
        "name": "a BELIEF SLOT subject bound to a Literal answers insufficient, and keeps its row",
        "command": "FIND(?n, ?slot.status) WHERE {\n  ?c CONCEPT {type: \"Person\", name: ?n}\n  FILTER(?n == \"Carol\")\n  ?slot BELIEF SLOT (?n, \"prefers\")\n}",
        "expect": {
          "result": [
            [
              "Carol",
              "insufficient"
            ]
          ]
        }
      }
    ]
  },
  {
    "name": "governance",
    "description": "Governance is a protected control plane, and these are the properties that make it one rather than a description of one. An engine claiming KIP 2.0 Governance conformance has to keep every distinction here: cognitive content that describes authority acquires none, an element's Governance block is unreachable from any mutation, a derived artifact records what it came from so its influence-authority ceiling can never be raised past it, and erasure refuses by default while anything still points at the target.",
    "setup": [
      "MUTATE {\n  CREATE CONCEPT ?alice { TYPE \"Person\" NAME \"Alice\" SET FIELDS {key: \"person:alice\"} }\n  CREATE CONCEPT ?dark { TYPE \"Preference\" NAME \"Dark\" }\n  CREATE CONCEPT ?admin { TYPE \"Person\" NAME \"Administrator\" SET ATTRIBUTES {authority: \"executable\", trust: 1.0} }\n  CREATE EVIDENCE ?secret { SET FIELDS {evidence_class: \"Document\", payload: \"an observation\"} }\n  ENSURE PROPOSITION ?p (?alice, \"prefers\", ?dark)\n  CREATE ASSERTION ?a {\n    SET FIELDS { proposition: ?p, asserted_by: ?alice, stance: \"support\", mode: \"inferred\", confidence: 0.6 }\n    SET STRUCTURAL { (\"evidence\", ?secret) {role: \"support\"} }\n  }\n}"
    ],
    "cases": [
      {
        "name": "an element's Governance block is not an author-writable field",
        "command": "UPDATE ?alice SET FIELDS { governance: {classification: \"public\"} } WHERE { ?alice CONCEPT {key: \"person:alice\"} }",
        "expect": {
          "error": "InvalidSyntax"
        },
        "vectors": [
          "GOV-018"
        ]
      },
      {
        "name": "and neither is it writable at creation",
        "command": "CREATE CONCEPT ?c { TYPE \"Person\" NAME \"Bob\" SET FIELDS {governance: {classification: \"public\"}} }",
        "expect": {
          "error": "InvalidSyntax"
        }
      },
      {
        "name": "content claiming an authority class has an ordinary attribute; Governance reports descriptive, the class every element has until Governance raises it",
        "command": "FIND(?c.attributes.authority, ?c.governance.authority_class) WHERE { ?c CONCEPT {name: \"Administrator\"} }",
        "expect": {
          "result": [
            [
              "executable",
              "descriptive"
            ]
          ]
        },
        "vectors": [
          "GOV-005",
          "GOV-018"
        ]
      },
      {
        "name": "an unclassified element does not thereby read as public",
        "command": "FIND(?c.governance.classification) WHERE { ?c CONCEPT {key: \"person:alice\"} }",
        "expect": {
          "result": [
            null
          ]
        }
      },
      {
        "name": "a claim citing Evidence records what it was derived from",
        "command": "FIND(?a.governance.authority_lineage) WHERE { ?a ASSERTION {} }",
        "expect": {
          "result": [
            [
              "E:<1>"
            ]
          ]
        },
        "vectors": [
          "GOV-006"
        ]
      },
      {
        "name": "erasure refuses by default while anything still references the target",
        "command": "PURGE ?alice WHERE { ?alice CONCEPT {key: \"person:alice\"} } CONFIRM \"PURGE\"",
        "expect": {
          "error": "PurgeDenied"
        }
      },
      {
        "name": "an unknown reference policy is refused rather than quietly defaulted",
        "command": "PURGE ?c WHERE { ?c CONCEPT {name: \"Administrator\"} } REFERENCE POLICY \"delete_everything\" CONFIRM \"PURGE\"",
        "expect": {
          "error": "ConstraintViolation"
        }
      },
      {
        "name": "an unreferenced element erases, leaving an identity stub",
        "command": "PURGE ?c WHERE { ?c CONCEPT {name: \"Administrator\"} } CONFIRM \"PURGE\"",
        "expect": {}
      },
      {
        "name": "the stub keeps its identity and carries a digest instead of content",
        "command": "FIND(?c.id, ?c.name, ?c.governance.purged) WHERE { ?c CONCEPT {state: \"purged\"} }",
        "expect": {
          "result": [
            [
              "C:<1>",
              null,
              true
            ]
          ]
        }
      },
      {
        "name": "and the content is gone from the past as well as from the present",
        "command": "FIND(?c.name) WHERE { ?c CONCEPT {type: \"Person\"} } AS OF SEQ 1",
        "expect": {
          "result": [
            "Alice"
          ]
        }
      }
    ]
  },
  {
    "name": "history",
    "description": "Two independent time axes. FOR TIME asks what was true then; AS OF asks what this Brain held then. A coordinate keeps what was later corrected, retracted or archived, because the record of what was once believed is the point. And the chronology itself is reported in transition envelopes (§36.1): §68.1 defines HISTORY as transition chronology and §36.2 defines a transition as one envelope, so HISTORY ELEMENT, HISTORY SPACE and CHANGES are the same unit asked for over different ranges — which is what lets a consumer deduplicate on space_id + space_seq + tx_id (§36.3). Each entry of `changes` is the normative shape of schemas/kip-change-envelope.schema.json: op, kind, id, new_version, old_version where the element existed, state {from, to} for a lifecycle move, refs.proposition on an Assertion entry — names and versions, never values.",
    "setup": [
      "MUTATE {\n  CREATE CONCEPT ?alice { TYPE \"Person\" NAME \"Alice\" }\n  CREATE CONCEPT ?dark { TYPE \"Preference\" NAME \"Dark\" }\n  ENSURE PROPOSITION ?p (?alice, \"prefers\", ?dark)\n  CREATE ASSERTION ?a {\n    SET FIELDS { proposition: ?p, asserted_by: ?alice, stance: \"support\", mode: \"stated\", confidence: 0.9 }\n  }\n}"
    ],
    "cases": [
      {
        "name": "the claim is active at the coordinate its transaction produced (seq 1)",
        "command": "FIND(?a.lifecycle.status, ?a.confidence) WHERE { ?a ASSERTION {} } AS OF SEQ 1",
        "expect": {
          "result": [
            [
              "active",
              0.9
            ]
          ]
        }
      },
      {
        "name": "retracting it changes the present",
        "command": "TRANSITION ?a TO \"retracted\" WHERE { ?a ASSERTION {} }",
        "expect": {}
      },
      {
        "name": "the present says retracted",
        "command": "FIND(?a.lifecycle.status) WHERE { ?a ASSERTION {} }",
        "expect": {
          "result": [
            "retracted"
          ]
        }
      },
      {
        "name": "and the earlier coordinate still says active: history is not rewritten",
        "command": "FIND(?a.lifecycle.status) WHERE { ?a ASSERTION {} } AS OF SEQ 1",
        "expect": {
          "result": [
            "active"
          ]
        }
      },
      {
        "name": "a coordinate before anything existed is empty, not an error",
        "command": "FIND(COUNT(?c)) WHERE { ?c CONCEPT {} } AS OF SEQ 0",
        "expect": {
          "result": [
            0
          ]
        }
      },
      {
        "name": "a coordinate the Space has not reached is refused, never rounded to the present",
        "command": "DESCRIBE SNAPSHOT AS OF SEQ 9999",
        "expect": {
          "error": "HistoricalSnapshotUnavailable"
        }
      },
      {
        "name": "an unknown transaction names no coordinate",
        "command": "DESCRIBE TRANSACTION \"kip:space:default#9999\"",
        "expect": {
          "error": "TransactionUnknown"
        }
      },
      {
        "name": "an element's chronology is transition envelopes, narrowed to that element",
        "command": "HISTORY ELEMENT \"A-1\"",
        "ordered": true,
        "expect": {
          "result": [
            {
              "kip": "2.0",
              "space_id": "kip:space:default",
              "space_seq": 1,
              "transaction_class": "cognitive",
              "schema_environment_version": 1,
              "changes": [
                {
                  "op": "create",
                  "kind": "assertion",
                  "id": "A:<1>",
                  "new_version": 1,
                  "refs": {
                    "proposition": "P:<2>"
                  }
                }
              ],
              "extensions": {
                "anda/transition": {
                  "snapshot_seq": 0,
                  "status": "committed"
                }
              }
            },
            {
              "kip": "2.0",
              "space_id": "kip:space:default",
              "space_seq": 2,
              "transaction_class": "cognitive",
              "schema_environment_version": 1,
              "changes": [
                {
                  "op": "lifecycle",
                  "kind": "assertion",
                  "id": "A:<1>",
                  "old_version": 1,
                  "new_version": 2,
                  "state": {
                    "from": "active",
                    "to": "retracted"
                  },
                  "refs": {
                    "proposition": "P:<2>"
                  },
                  "touched": [
                    "fields.retracted_at",
                    "fields.status"
                  ]
                }
              ],
              "extensions": {
                "anda/transition": {
                  "snapshot_seq": 1,
                  "status": "committed"
                }
              }
            }
          ]
        }
      },
      {
        "name": "a Space's chronology is the same envelope, unnarrowed",
        "command": "HISTORY SPACE FROM SEQ 2 TO SEQ 2",
        "ordered": true,
        "expect": {
          "result": [
            {
              "kip": "2.0",
              "space_id": "kip:space:default",
              "space_seq": 2,
              "transaction_class": "cognitive",
              "schema_environment_version": 1,
              "changes": [
                {
                  "op": "lifecycle",
                  "kind": "assertion",
                  "id": "A:<1>",
                  "old_version": 1,
                  "new_version": 2,
                  "state": {
                    "from": "active",
                    "to": "retracted"
                  },
                  "refs": {
                    "proposition": "P:<2>"
                  },
                  "touched": [
                    "fields.retracted_at",
                    "fields.status"
                  ]
                }
              ],
              "extensions": {
                "anda/transition": {
                  "snapshot_seq": 1,
                  "status": "committed"
                }
              }
            }
          ]
        }
      },
      {
        "name": "the change stream is the same envelope again, so one transition arrives whole",
        "command": "CHANGES AFTER SEQ 1",
        "ordered": true,
        "expect": {
          "result": [
            {
              "kip": "2.0",
              "space_id": "kip:space:default",
              "space_seq": 2,
              "transaction_class": "cognitive",
              "schema_environment_version": 1,
              "changes": [
                {
                  "op": "lifecycle",
                  "kind": "assertion",
                  "id": "A:<1>",
                  "old_version": 1,
                  "new_version": 2,
                  "state": {
                    "from": "active",
                    "to": "retracted"
                  },
                  "refs": {
                    "proposition": "P:<2>"
                  },
                  "touched": [
                    "fields.retracted_at",
                    "fields.status"
                  ]
                }
              ],
              "extensions": {
                "anda/transition": {
                  "snapshot_seq": 1,
                  "status": "committed"
                }
              }
            }
          ]
        }
      },
      {
        "name": "a caught-up follower is handed no envelopes",
        "command": "CHANGES AFTER SEQ 99",
        "expect": {
          "result": []
        }
      },
      {
        "name": "the history of an element that is not there is a refusal, not an empty chronology",
        "command": "HISTORY ELEMENT \"C-999\"",
        "expect": {
          "error": "NotFoundOrNotVisible"
        }
      },
      {
        "name": "a write under an idempotency key is recoverable by it",
        "command": "CREATE CONCEPT ?c { TYPE \"Person\" NAME \"Recoverable\" }",
        "envelope": {
          "execution": {
            "mode": "independent",
            "idempotency_key": "history:recover"
          }
        },
        "expect": {}
      },
      {
        "name": "and the description is the envelope shape plus whether it committed",
        "command": "DESCRIBE TRANSACTION BY IDEMPOTENCY KEY \"history:recover\"",
        "expect": {
          "result": {
            "kip": "2.0",
            "space_id": "kip:space:default",
            "space_seq": 3,
            "snapshot_seq": 2,
            "status": "committed",
            "transaction_class": "cognitive",
            "schema_environment_version": 1,
            "extensions": {
              "anda/transition": {
                "snapshot_seq": 2,
                "status": "committed"
              }
            },
            "changes": [
              {
                "op": "create",
                "kind": "concept",
                "id": "C:<1>",
                "new_version": 1,
                "schema_ref": "kip://profiles/cognitive-memory@2.0.0/Person"
              }
            ]
          }
        }
      }
    ]
  },
  {
    "name": "lifecycle",
    "description": "Nothing is rewritten and nothing is erased. An Assertion's epistemic payload is immutable, so correcting a claim records a new one and supersedes the old; an element that leaves ordinary recall keeps resolving as a reference.",
    "setup": [
      "MUTATE {\n  CREATE CONCEPT ?alice { TYPE \"Person\" NAME \"Alice\" }\n  CREATE CONCEPT ?dark { TYPE \"Preference\" NAME \"Dark\" }\n  ENSURE PROPOSITION ?p (?alice, \"prefers\", ?dark)\n  CREATE ASSERTION ?old {\n    SET FIELDS { proposition: ?p, asserted_by: ?alice, stance: \"support\", mode: \"stated\", confidence: 0.9 }\n  }\n}"
    ],
    "cases": [
      {
        "name": "a claim starts active",
        "command": "FIND(?a.lifecycle.status) WHERE { ?a ASSERTION {} }",
        "expect": {
          "result": [
            "active"
          ]
        }
      },
      {
        "name": "correcting a claim is a new Assertion plus supersession, never an edit",
        "command": "MUTATE {\n  ASSERT ?new (:alice, \"prefers\", :dark) { by: :alice, mode: \"stated\", confidence: 0.4 }\n    SUPERSEDING :old\n}",
        "params": {},
        "expect": {
          "error": "InvalidRequestEnvelope"
        },
        "vectors": [
          "CORE-008",
          "KML-017"
        ]
      },
      {
        "name": "an Assertion's stance cannot be rewritten in place",
        "command": "UPDATE ?a SET FIELDS { stance: \"reject\" } WHERE { ?a ASSERTION {} }",
        "expect": {
          "error": "InvalidSyntax"
        },
        "vectors": [
          "CORE-007"
        ]
      },
      {
        "name": "an archived element leaves ordinary recall",
        "command": "FIND(COUNT(?c)) WHERE { ?c CONCEPT {type: \"Person\"} }",
        "expect": {
          "result": [
            1
          ]
        }
      },
      {
        "name": "the record of what was claimed survives being questioned",
        "command": "FIND(?a.confidence, ?a.stance) WHERE { ?a ASSERTION {} }",
        "expect": {
          "result": [
            [
              0.9,
              "support"
            ]
          ]
        },
        "vectors": [
          "X-018"
        ]
      },
      {
        "name": "an element referenced by a tuple still resolves after archiving",
        "command": "FIND(COUNT(?p)) WHERE { ?p PROPOSITION (?s, \"prefers\", ?o) }",
        "expect": {
          "result": [
            1
          ]
        }
      }
    ]
  },
  {
    "name": "merge-identity",
    "description": "Non-destructive identity consolidation (§11). The merged-away Concept stays addressable and keeps forwarding; the history that referenced it keeps referencing it (§11.2); ordinary new writes land on the identity that survived (§11.3); and no merge may make canonical resolution — following `merged_into` to its fixpoint — cycle (§11.1).",
    "setup": [
      "MUTATE {\n  CREATE CONCEPT ?a { TYPE \"Person\" NAME \"Al\" SET FIELDS {key: \"al\"} }\n  CREATE CONCEPT ?b { TYPE \"Person\" NAME \"Alice\" SET FIELDS {key: \"alice\"} }\n  CREATE CONCEPT ?dark { TYPE \"Preference\" NAME \"Dark\" SET FIELDS {key: \"dark\"} }\n  ENSURE PROPOSITION ?old (?a, \"prefers\", ?dark)\n}",
      "MERGE CONCEPT ?source INTO ?target WHERE {\n  ?source CONCEPT {key: \"al\"}\n  ?target CONCEPT {key: \"alice\"}\n}",
      "MUTATE {\n  UPSERT CONCEPT ?a { MATCH {type: \"Person\", key: \"al\"} }\n  UPSERT CONCEPT ?dark { MATCH {type: \"Preference\", key: \"dark\"} }\n  ENSURE PROPOSITION ?new (?a, \"prefers\", ?dark)\n}",
      "MUTATE {\n  UPSERT CONCEPT ?a { MATCH {type: \"Person\", key: \"al\"} }\n  UPSERT CONCEPT ?dark { MATCH {type: \"Preference\", key: \"dark\"} }\n  ENSURE PROPOSITION ?again (?a, \"prefers\", ?dark)\n}"
    ],
    "cases": [
      {
        "name": "the merged-away Concept keeps its own name and leaves ordinary recall",
        "command": "FIND(?c.name, ?c._system.state) WHERE { ?c CONCEPT {key: \"al\", state: \"merged\"} }",
        "expect": {
          "result": [
            [
              "Al",
              "merged"
            ]
          ]
        },
        "vectors": [
          "CORE-020"
        ]
      },
      {
        "name": "it forwards to the identity that survived",
        "command": "FIND(?merged.key) WHERE {\n  ?c CONCEPT {key: \"al\", state: \"merged\"}\n  ?merged CONCEPT {id: ?target}\n  FILTER(?target == ?c.merged_into)\n}",
        "expect": {
          "result": [
            "alice"
          ]
        }
      },
      {
        "name": "history keeps referring to what it referred to",
        "command": "FIND(?s.key) WHERE { ?p PROPOSITION (?s, \"prefers\", ?o) }",
        "expect": {
          "result": [
            "al",
            "alice"
          ]
        },
        "vectors": [
          "CORE-021",
          "HIST-008"
        ]
      },
      {
        "name": "two post-merge writes resolve to one canonical Proposition on the survivor",
        "command": "FIND(COUNT(?p)) WHERE { ?p PROPOSITION (?s, \"prefers\", ?o) }",
        "expect": {
          "result": [
            2
          ]
        }
      },
      {
        "name": "re-sending the same merge is a no_effect, not a conflict",
        "command": "MERGE CONCEPT ?source INTO ?target WHERE {\n  ?source CONCEPT {key: \"al\", state: \"merged\"}\n  ?target CONCEPT {key: \"alice\"}\n}",
        "expect": {
          "result": null
        }
      },
      {
        "name": "merging back would make canonical resolution cycle",
        "command": "MERGE CONCEPT ?source INTO ?target WHERE {\n  ?source CONCEPT {key: \"alice\"}\n  ?target CONCEPT {key: \"al\", state: \"merged\"}\n}",
        "expect": {
          "error": "IdentityMergeConflict"
        }
      },
      {
        "name": "a Concept cannot be merged into itself",
        "command": "MERGE CONCEPT ?source INTO ?target WHERE {\n  ?source CONCEPT {key: \"alice\"}\n  ?target CONCEPT {key: \"alice\"}\n}",
        "expect": {
          "error": "IdentityMergeConflict"
        }
      }
    ]
  },
  {
    "name": "meta-shapes",
    "description": "The shape a META read answers in, which is a contract and not an engine choice. A client that talks to both engines parses one answer, and a divergence here is the most expensive kind to find: a reader written for one shape gets an empty result from the other, and an empty result reads as an empty Space rather than as a wrong shape — no error, no clue. So the `LIST` families pin their row shape and their order here rather than in either engine's own tests, which is where the two drifted apart in the first place. The Primer is not pinned whole here: most of it is deployment fact — a Space's name, its sequence, how many packages a host installed — and the fixtures are for behaviour, not for one engine's bootstrap. Its key structure is asserted in each engine's own tests instead.",
    "packages": [
      {
        "format": "KIP-Schema-Package",
        "manifest": {
          "package_id": "kip://test/meta-shapes",
          "version": "1.0.0"
        },
        "definitions": {
          "concept_types": {
            "Widget": {
              "kind": "ConceptType",
              "description": "Something to list."
            }
          },
          "predicates": {
            "fits_in": {
              "kind": "PredicateType",
              "description": "Where a Widget fits.",
              "subject": {
                "kinds": [
                  "Concept"
                ]
              },
              "object": {
                "kinds": [
                  "Concept"
                ]
              }
            }
          },
          "facets": {
            "Fit": {
              "kind": "FacetDefinition",
              "description": "How well it fits.",
              "fields": {
                "snug": {
                  "type": "boolean",
                  "required": false,
                  "mutable": true
                }
              }
            }
          },
          "structural_fields": {
            "packed_with": {
              "kind": "StructuralFieldDefinition",
              "description": "Other Widgets in the same box.",
              "source": {
                "kinds": [
                  "Concept"
                ]
              },
              "target": {
                "kinds": [
                  "Concept"
                ]
              },
              "ordered": false
            }
          }
        }
      }
    ],
    "setup": [],
    "cases": [
      {
        "name": "a listed symbol is a row, not a bare reference",
        "command": "LIST PREDICATES",
        "ordered": true,
        "expect": {
          "result": [
            {
              "ref": "kip://profiles/cognitive-memory@2.0.0/caused_by",
              "local_name": "caused_by",
              "package_ref": "kip://profiles/cognitive-memory@2.0.0",
              "status": "active"
            },
            {
              "ref": "kip://profiles/cognitive-memory@2.0.0/prefers",
              "local_name": "prefers",
              "package_ref": "kip://profiles/cognitive-memory@2.0.0",
              "status": "active"
            },
            {
              "ref": "kip://profiles/cognitive-memory@2.0.0/same_as",
              "local_name": "same_as",
              "package_ref": "kip://profiles/cognitive-memory@2.0.0",
              "status": "active"
            },
            {
              "ref": "kip://test/meta-shapes@1.0.0/fits_in",
              "local_name": "fits_in",
              "package_ref": "kip://test/meta-shapes@1.0.0",
              "status": "active"
            }
          ]
        }
      },
      {
        "name": "the row carries both names because they answer different questions",
        "command": "LIST FACETS",
        "ordered": true,
        "expect": {
          "result": [
            {
              "ref": "kip://profiles/cognitive-memory@2.0.0/DecisionRecord",
              "local_name": "DecisionRecord",
              "package_ref": "kip://profiles/cognitive-memory@2.0.0",
              "status": "active"
            },
            {
              "ref": "kip://profiles/cognitive-memory@2.0.0/DerivationState",
              "local_name": "DerivationState",
              "package_ref": "kip://profiles/cognitive-memory@2.0.0",
              "status": "active"
            },
            {
              "ref": "kip://profiles/cognitive-memory@2.0.0/GradingState",
              "local_name": "GradingState",
              "package_ref": "kip://profiles/cognitive-memory@2.0.0",
              "status": "active"
            },
            {
              "ref": "kip://profiles/cognitive-memory@2.0.0/MnemonicState",
              "local_name": "MnemonicState",
              "package_ref": "kip://profiles/cognitive-memory@2.0.0",
              "status": "active"
            },
            {
              "ref": "kip://profiles/cognitive-memory@2.0.0/OutcomeRecord",
              "local_name": "OutcomeRecord",
              "package_ref": "kip://profiles/cognitive-memory@2.0.0",
              "status": "active"
            },
            {
              "ref": "kip://profiles/cognitive-memory@2.0.0/TrialState",
              "local_name": "TrialState",
              "package_ref": "kip://profiles/cognitive-memory@2.0.0",
              "status": "active"
            },
            {
              "ref": "kip://test/meta-shapes@1.0.0/Fit",
              "local_name": "Fit",
              "package_ref": "kip://test/meta-shapes@1.0.0",
              "status": "active"
            }
          ]
        }
      },
      {
        "name": "the list is ordered by ref, so a LIMIT cuts the same rows twice",
        "command": "LIST PREDICATES LIMIT 1",
        "ordered": true,
        "expect": {
          "result": [
            {
              "ref": "kip://profiles/cognitive-memory@2.0.0/caused_by",
              "local_name": "caused_by",
              "package_ref": "kip://profiles/cognitive-memory@2.0.0",
              "status": "active"
            }
          ]
        }
      },
      {
        "name": "a policy list carries the policy, in the wire names both engines write",
        "command": "LIST EPISTEMIC POLICIES",
        "ordered": true,
        "expect": {
          "result": [
            {
              "id": "kip:policy:baseline",
              "version": 1,
              "eligible_modes": [
                "observed",
                "stated",
                "inferred",
                "imported"
              ],
              "accept_threshold": 0.7,
              "material_threshold": 0.3,
              "unstated_confidence_weight": 0.5,
              "conflict_set_expansion": true,
              "notes": [
                "mode gates eligibility and never weights a claim: a mode does not grant trust",
                "corroboration groups are counted once; repetition is not evidence"
              ]
            },
            {
              "id": "kip:policy:forecast",
              "version": 1,
              "eligible_modes": [
                "predicted",
                "inferred"
              ],
              "accept_threshold": 0.7,
              "material_threshold": 0.3,
              "unstated_confidence_weight": 0.5,
              "conflict_set_expansion": true,
              "notes": [
                "mode gates eligibility and never weights a claim: a mode does not grant trust",
                "corroboration groups are counted once; repetition is not evidence"
              ]
            }
          ]
        }
      },
      {
        "name": "and DESCRIBE answers about one in exactly the same shape",
        "command": "DESCRIBE EPISTEMIC POLICY \"kip:policy:forecast\"",
        "expect": {
          "result": {
            "id": "kip:policy:forecast",
            "version": 1,
            "eligible_modes": [
              "predicted",
              "inferred"
            ],
            "accept_threshold": 0.7,
            "material_threshold": 0.3,
            "unstated_confidence_weight": 0.5,
            "conflict_set_expansion": true,
            "notes": [
              "mode gates eligibility and never weights a claim: a mode does not grant trust",
              "corroboration groups are counted once; repetition is not evidence"
            ]
          }
        }
      }
    ]
  },
  {
    "name": "mutation-selection",
    "description": "A mutation may choose what it acts on. The judgement calls an engine has to make here are what this fixture pins down: UPDATE reaches mutable state and nothing else, a bounded sweep takes a documented order, a selection block reads the transaction's starting state, and a merge consolidates identity without copying or erasing anything.",
    "setup": [
      "MUTATE {\n  CREATE CONCEPT ?alice { TYPE \"Person\" NAME \"Alice\" SET FIELDS {key: \"person:alice\"} }\n  CREATE CONCEPT ?dark { TYPE \"Preference\" NAME \"Dark\" }\n  CREATE CONCEPT ?e1 { TYPE \"Experience\" NAME \"First\" SET ATTRIBUTES {goal: \"learn\", outcome_status: \"success\"} SET FACET \"MnemonicState\" {memory_strength: 0.8, salience: 0.5} }\n  CREATE CONCEPT ?e2 { TYPE \"Experience\" NAME \"Second\" SET ATTRIBUTES {goal: \"learn\", outcome_status: \"failure\"} SET FACET \"MnemonicState\" {memory_strength: 0.4, salience: 0.5} }\n  CREATE CONCEPT ?e3 { TYPE \"Experience\" NAME \"Third\" SET ATTRIBUTES {goal: \"rest\", outcome_status: \"success\"} SET FACET \"MnemonicState\" {memory_strength: 0.2, salience: 0.5} }\n  ENSURE PROPOSITION ?p (?alice, \"prefers\", ?dark)\n  CREATE ASSERTION ?a {\n    SET FIELDS { proposition: ?p, asserted_by: ?alice, stance: \"support\", mode: \"stated\", confidence: 0.9 }\n  }\n}"
    ],
    "cases": [
      {
        "name": "a sweep decays a Facet member by reading the target's own value",
        "command": "UPDATE ?m SET FACET \"MnemonicState\" { memory_strength: MUL(?m.facets[\"MnemonicState\"].memory_strength, 0.5) } WHERE { ?m CONCEPT {type: \"Experience\"} } LIMIT 2",
        "expect": {},
        "vectors": [
          "CORE-018",
          "X-011"
        ]
      },
      {
        "name": "LIMIT cuts in ascending element id, so the same sweep twice takes the same elements",
        "command": "FIND(?m.name, ?m.facets[\"MnemonicState\"].memory_strength) WHERE { ?m CONCEPT {type: \"Experience\"} } ORDER BY ?m.name",
        "ordered": true,
        "expect": {
          "result": [
            [
              "First",
              0.4
            ],
            [
              "Second",
              0.2
            ],
            [
              "Third",
              0.2
            ]
          ]
        }
      },
      {
        "name": "a Facet assignment merges members rather than replacing the Facet",
        "command": "FIND(?m.facets[\"MnemonicState\"].salience) WHERE { ?m CONCEPT {name: \"First\"} }",
        "expect": {
          "result": [
            0.5
          ]
        }
      },
      {
        "name": "UPDATE does not reach an Assertion's epistemic payload",
        "command": "UPDATE ?a SET FIELDS { name: \"relabelled\" } WHERE { ?a ASSERTION {} }",
        "expect": {
          "error": "EpistemicRevisionRequired"
        }
      },
      {
        "name": "a selection block that matches nothing changes nothing, and does not create",
        "command": "UPDATE ?m SET ATTRIBUTES { outcome_status: \"aborted\" } WHERE { ?m CONCEPT {type: \"Experience\"} FILTER(?m.attributes.goal == \"no such goal\") }",
        "expect": {}
      },
      {
        "name": "a selection block reads the state the transaction started from",
        "command": "MUTATE {\n  CREATE CONCEPT ?fresh { TYPE \"Experience\" NAME \"Fourth\" SET ATTRIBUTES {goal: \"learn\", outcome_status: \"success\"} }\n  TRANSITION ?m TO \"archived\" WHERE { ?m CONCEPT {type: \"Experience\"} }\n}",
        "expect": {}
      },
      {
        "name": "so the Concept the same transaction created is still in recall",
        "command": "FIND(?m.name) WHERE { ?m CONCEPT {type: \"Experience\"} }",
        "expect": {
          "result": [
            "Fourth"
          ]
        }
      },
      {
        "name": "a duplicate of the same person, recorded separately",
        "command": "UPSERT CONCEPT ?dup { MATCH {type: \"Person\", key: \"person:alice-duplicate\"} SET FIELDS {name: \"Alice\"} }",
        "expect": {}
      },
      {
        "name": "the upsert created the type its MATCH declared",
        "command": "FIND(?p.name) WHERE { ?p CONCEPT {type: \"Person\", key: \"person:alice-duplicate\"} }",
        "expect": {
          "result": [
            "Alice"
          ]
        }
      },
      {
        "name": "a merge never picks an identity by description: two Concepts share the name",
        "command": "MERGE CONCEPT ?source INTO ?target WHERE { ?source CONCEPT {key: \"person:alice-duplicate\"} ?target CONCEPT {name: \"Alice\"} }",
        "expect": {
          "error": "IdentitySelectorRequired"
        }
      },
      {
        "name": "named by stable identity instead, the merge consolidates them",
        "command": "MERGE CONCEPT ?source INTO ?target WHERE { ?source CONCEPT {key: \"person:alice-duplicate\"} ?target CONCEPT {key: \"person:alice\"} }",
        "expect": {}
      },
      {
        "name": "a logical key is identity within its type",
        "command": "UPSERT CONCEPT ?a { MATCH {type: \"Person\", key: \"shared:label\"} SET FIELDS {name: \"A person\"} }",
        "expect": {}
      },
      {
        "name": "so another type may carry the same key, and it is a second identity",
        "command": "UPSERT CONCEPT ?b { MATCH {type: \"Preference\", key: \"shared:label\"} SET FIELDS {name: \"A preference\"} }",
        "expect": {}
      },
      {
        "name": "the key alone now names two Concepts, and is not resolved arbitrarily",
        "command": "UPSERT CONCEPT ?c { MATCH {key: \"shared:label\"} SET FIELDS {name: \"?\"} }",
        "expect": {
          "error": "IdentityConflict"
        }
      },
      {
        "name": "an upsert declaring no type cannot create a Concept it would have to invent one for",
        "command": "UPSERT CONCEPT ?x { MATCH {key: \"person:nobody\"} SET FIELDS {name: \"Nobody\"} }",
        "expect": {
          "error": "SchemaSymbolNotFound"
        }
      },
      {
        "name": "an upsert by id resolves only, so an id nothing carries is a failure and not a create",
        "command": "UPSERT CONCEPT ?x { MATCH {type: \"Person\", id: \"C-99999\"} SET FIELDS {name: \"Nobody\"} }",
        "expect": {
          "error": "NotFoundOrNotVisible"
        }
      },
      {
        "name": "a MATCH member of the wrong type is refused rather than read as absent",
        "command": "UPSERT CONCEPT ?x { MATCH {type: 42, key: \"person:nobody\"} SET FIELDS {name: \"Nobody\"} }",
        "expect": {
          "error": "TypeMismatch"
        }
      },
      {
        "name": "MATCH selects; it does not seed the Concept it creates",
        "command": "UPSERT CONCEPT ?x { MATCH {type: \"Person\", key: \"person:unnamed\", name: \"Ignored\"} }",
        "expect": {}
      },
      {
        "name": "so grounding state arrives through SET FIELDS and nowhere else",
        "command": "FIND(?p.key) WHERE { ?p CONCEPT {type: \"Person\", name: \"Ignored\"} }",
        "expect": {
          "result": []
        }
      },
      {
        "name": "a name is not identity, and the language refuses it before an engine sees it",
        "command": "UPSERT CONCEPT ?c { MATCH {type: \"Person\", name: \"Alice\"} SET FIELDS {name: \"Alice B\"} }",
        "expect": {
          "error": "InvalidSyntax"
        }
      },
      {
        "name": "and `_system` is engine state no mutation may name",
        "command": "UPDATE \"person:alice\" SET FIELDS {_system: {version: 99}}",
        "expect": {
          "error": "InvalidSyntax"
        }
      },
      {
        "name": "a WHERE on a directly named target is a guard, and a guard that holds lets the write through",
        "command": "UPDATE ?t SET FIELDS {name: \"Alice Guarded\"} WHERE { ?t CONCEPT {key: \"person:alice\"} }",
        "expect": {}
      },
      {
        "name": "a guard that finds nothing makes the statement do nothing, and is not an error",
        "command": "UPDATE ?t SET FIELDS {name: \"Never\"} WHERE { ?t CONCEPT {key: \"person:alice\"}  FILTER(?t.name == \"Nobody\") }",
        "expect": {}
      },
      {
        "name": "so the guarded write landed and the unguarded one did not",
        "command": "FIND(?c.name) WHERE { ?c CONCEPT {key: \"person:alice\"} }",
        "expect": {
          "result": [
            "Alice Guarded"
          ]
        }
      },
      {
        "name": "PURGE refuses by default while anything still references the target",
        "command": "PURGE ?alice WHERE { ?alice CONCEPT {key: \"person:alice\"} } CONFIRM \"PURGE\"",
        "expect": {
          "error": "PurgeDenied"
        }
      },
      {
        "name": "an unreferenced element erases, leaving an identity stub rather than a hole",
        "command": "PURGE ?e WHERE { ?e CONCEPT {name: \"Third\", state: \"archived\"} } CONFIRM \"PURGE\"",
        "expect": {}
      },
      {
        "name": "the stub keeps identity and a digest, and carries none of the content",
        "command": "FIND(?e.name, ?e.governance.purged) WHERE { ?e CONCEPT {state: \"purged\"} }",
        "expect": {
          "result": [
            [
              null,
              true
            ]
          ]
        }
      }
    ]
  },
  {
    "name": "reads",
    "description": "The read language: joins on shared variables, OPTIONAL pads rather than drops, NOT asks about the record and never about the world, typed comparison, nulls last, deterministic paging.",
    "setup": [
      "MUTATE {\n  CREATE CONCEPT ?alice { TYPE \"Person\" NAME \"Alice\" SET ATTRIBUTES { display_name: \"Alice A\" } }\n  CREATE CONCEPT ?bob { TYPE \"Person\" NAME \"Bob\" }\n  CREATE CONCEPT ?dark { TYPE \"Preference\" NAME \"Dark\" }\n  ENSURE PROPOSITION ?p (?alice, \"prefers\", ?dark)\n}"
    ],
    "cases": [
      {
        "name": "a pattern finds by type",
        "command": "FIND(?c.name) WHERE { ?c CONCEPT {type: \"Person\"} } ORDER BY ?c.name",
        "ordered": true,
        "expect": {
          "result": [
            "Alice",
            "Bob"
          ]
        }
      },
      {
        "name": "a tuple pattern binds both ends and joins on them",
        "command": "FIND(?person.name, ?thing.name) WHERE { ?p PROPOSITION (?person, \"prefers\", ?thing) }",
        "expect": {
          "result": [
            [
              "Alice",
              "Dark"
            ]
          ]
        }
      },
      {
        "name": "NOT keeps what the pattern could not extend",
        "command": "FIND(?c.name) WHERE {\n  ?c CONCEPT {type: \"Person\"}\n  NOT { ?p PROPOSITION (?c, \"prefers\", ?o) }\n}",
        "expect": {
          "result": [
            "Bob"
          ]
        },
        "vectors": [
          "KQL-009"
        ]
      },
      {
        "name": "OPTIONAL pads rather than drops",
        "command": "FIND(COUNT(?c)) WHERE {\n  ?c CONCEPT {type: \"Person\"}\n  OPTIONAL { ?p PROPOSITION (?c, \"prefers\", ?o) }\n}",
        "expect": {
          "result": [
            2
          ]
        }
      },
      {
        "name": "UNION widens rather than filtering",
        "command": "FIND(?c.name) WHERE {\n  ?c CONCEPT {name: \"Alice\"}\n  UNION { ?c CONCEPT {name: \"Dark\"} }\n} ORDER BY ?c.name",
        "ordered": true,
        "expect": {
          "result": [
            "Alice",
            "Dark"
          ]
        }
      },
      {
        "name": "a comparison between unlike types decides nothing",
        "command": "FIND(?c.name) WHERE { ?c CONCEPT {type: \"Person\"} FILTER(?c.name > 5) }",
        "expect": {
          "result": []
        }
      },
      {
        "name": "nulls sort last under ASC",
        "command": "FIND(?c.name) WHERE { ?c CONCEPT {type: \"Person\"} } ORDER BY ?c.attributes.display_name ASC",
        "ordered": true,
        "expect": {
          "result": [
            "Alice",
            "Bob"
          ]
        }
      },
      {
        "name": "nulls sort last under DESC too, because absent is not a large value",
        "command": "FIND(?c.name) WHERE { ?c CONCEPT {type: \"Person\"} } ORDER BY ?c.attributes.display_name DESC",
        "ordered": true,
        "expect": {
          "result": [
            "Alice",
            "Bob"
          ]
        }
      },
      {
        "name": "a missing attribute reads as null, not as an error",
        "command": "FIND(?c.attributes.display_name) WHERE { ?c CONCEPT {name: \"Bob\"} }",
        "expect": {
          "result": [
            null
          ]
        }
      },
      {
        "name": "COUNT over nothing is zero, and zero is not a falsehood",
        "command": "FIND(COUNT(?c)) WHERE { ?c CONCEPT {name: \"Nobody\"} }",
        "expect": {
          "result": [
            0
          ]
        },
        "vectors": [
          "KQL-013"
        ]
      },
      {
        "name": "an archived element is out of recall by default",
        "command": "FIND(COUNT(?c)) WHERE { ?c CONCEPT {type: \"Person\", state: \"archived\"} }",
        "expect": {
          "result": [
            0
          ]
        }
      },
      {
        "name": "a bare variable projects the whole element",
        "command": "FIND(?c.id, ?c.kind) WHERE { ?c CONCEPT {name: \"Bob\"} }",
        "expect": {
          "result": [
            [
              "C:<1>",
              "concept"
            ]
          ]
        }
      },
      {
        "name": "a cursor a caller invented is malformed, whatever it is spelled as",
        "command": "FIND(?c.name) WHERE { ?c CONCEPT {} } LIMIT 1 CURSOR :c",
        "params": {
          "c": "not-a-cursor"
        },
        "expect": {
          "error": "CursorInvalid"
        },
        "vectors": [
          "KQL-017"
        ]
      },
      {
        "name": "and a number is the same refusal: a cursor is opaque, never a position a caller can type",
        "command": "FIND(?c.name) WHERE { ?c CONCEPT {} } LIMIT 1 CURSOR :c",
        "params": {
          "c": 1
        },
        "expect": {
          "error": "CursorInvalid"
        },
        "vectors": [
          "KQL-018"
        ]
      },
      {
        "name": "grouping is implicit: the non-aggregated projected expressions are the key",
        "command": "FIND(?c.name, COUNT(?p)) WHERE { ?c CONCEPT {type: \"Person\"}  ?p (?c, \"prefers\", ?o) } ORDER BY ?c.name",
        "ordered": true,
        "expect": {
          "result": [
            [
              "Alice",
              1
            ]
          ]
        }
      },
      {
        "name": "and a FIND of aggregates alone is one global group",
        "command": "FIND(COUNT(?c), COUNT(DISTINCT ?c.name)) WHERE { ?c CONCEPT {type: \"Person\"} }",
        "expect": {
          "result": [
            [
              2,
              2
            ]
          ]
        }
      },
      {
        "name": "ORDER BY may name an aggregate the caller did not project",
        "command": "FIND(?c.name) WHERE { ?c CONCEPT {type: \"Person\"} } ORDER BY COUNT(?c) DESC, ?c.name",
        "ordered": true,
        "expect": {
          "result": [
            "Alice",
            "Bob"
          ]
        }
      },
      {
        "name": "but a sort key that varies inside a group has no value to sort by",
        "command": "FIND(?c.name, COUNT(?c)) WHERE { ?c CONCEPT {type: \"Person\"} } ORDER BY ?c.attributes.display_name",
        "expect": {
          "error": "ConstraintViolation"
        }
      },
      {
        "name": "a bounded aggregate takes groups in a repeatable order, so the cut is the same twice",
        "command": "FIND(?c.name, COUNT(?c)) WHERE { ?c CONCEPT {type: \"Person\"} } ORDER BY ?c.name LIMIT 1",
        "ordered": true,
        "expect": {
          "result": [
            [
              "Alice",
              1
            ]
          ]
        }
      }
    ]
  },
  {
    "name": "request-envelope",
    "description": "Two things the envelope decides rather than the command (§71, §26, §33). `ingest` mints Evidence from the payload the transport carried, so the observation never passes through model-generated command text — the fidelity risk §88.12 names, where a model retyping what it saw truncates or paraphrases it and the record then says the source said something it did not. `execution.idempotency_key` makes a lost response recoverable: a timeout is not an abort, so a resend replays the outcome the first attempt produced rather than writing a second time. Both are envelope contracts, so both are pinned through the envelope. `requires` is the third: §67.4 fixes the capability names, so a fail-fast precondition written once must get the same answer from either engine — including for an entry whose value is a detail object rather than a bare `true`, and for a name no registry knows, which fails exactly as an unsupported one does. And an `ingest` block is minted inside the request's transaction, so a request that carries only reads opens no scope to mint into: refused, because minting nothing while answering `succeeded` leaves the caller believing the observation was recorded.",
    "setup": [
      "MUTATE {\n  CREATE CONCEPT ?alice { TYPE \"Person\" NAME \"Alice\" SET FIELDS {canonical_id: \"urn:x:alice\"} }\n  CREATE CONCEPT ?dark { TYPE \"Preference\" NAME \"Dark\" }\n  ENSURE PROPOSITION ?p (?alice, \"prefers\", ?dark)\n}"
    ],
    "cases": [
      {
        "name": "an ingested payload reaches a command without passing through its text",
        "command": "CREATE ASSERTION ?a {\n  SET FIELDS { proposition: \"P-1\", asserted_by: \"C-1\", stance: \"support\", mode: \"observed\" }\n  SET STRUCTURAL { (\"evidence\", :msg) {role: \"support\"} }\n}",
        "envelope": {
          "ingest": {
            "evidence": [
              {
                "key": "msg",
                "evidence_class": "user_statement",
                "payload": "I prefer   dark mode.",
                "media_type": "text/plain",
                "source_actor": {
                  "id": "C-1"
                }
              }
            ]
          }
        },
        "expect": {},
        "vectors": [
          "RT-031"
        ]
      },
      {
        "name": "and arrives byte for byte, whitespace and all",
        "command": "FIND(?e.payload.inline) WHERE { ?e EVIDENCE {} }",
        "expect": {
          "result": [
            "I prefer   dark mode."
          ]
        },
        "vectors": [
          "RT-031"
        ]
      },
      {
        "name": "the source actor is resolved to something a reader can follow",
        "command": "FIND(?c.name) WHERE { STRUCTURAL (?e, \"source\", ?c) ?c CONCEPT {} }",
        "expect": {
          "result": [
            "Alice"
          ]
        }
      },
      {
        "name": "an ingest key a request parameter already claims is refused",
        "command": "CREATE CONCEPT ?c { TYPE \"Person\" NAME \"Bob\" }",
        "params": {
          "msg": "a plain value"
        },
        "envelope": {
          "ingest": {
            "evidence": [
              {
                "key": "msg",
                "evidence_class": "user_statement",
                "payload": "hi"
              }
            ]
          }
        },
        "expect": {
          "error": "InvalidRequestEnvelope"
        }
      },
      {
        "name": "an ingest entry that names neither a payload nor a handle is refused",
        "command": "CREATE CONCEPT ?c { TYPE \"Person\" NAME \"Bob\" }",
        "envelope": {
          "ingest": {
            "evidence": [
              {
                "key": "msg",
                "evidence_class": "user_statement"
              }
            ]
          }
        },
        "expect": {
          "error": "InvalidRequestEnvelope"
        }
      },
      {
        "name": "a source actor that resolves to nothing is refused rather than stored as a name",
        "command": "CREATE CONCEPT ?c { TYPE \"Person\" NAME \"Bob\" }",
        "envelope": {
          "ingest": {
            "evidence": [
              {
                "key": "msg",
                "evidence_class": "user_statement",
                "payload": "hi",
                "source_actor": {
                  "id": "C-999"
                }
              }
            ]
          }
        },
        "expect": {
          "error": "NotFoundOrNotVisible"
        }
      },
      {
        "name": "a first write under an idempotency key commits",
        "command": "CREATE CONCEPT ?c { TYPE \"Person\" NAME \"Cass\" }",
        "envelope": {
          "execution": {
            "mode": "independent",
            "idempotency_key": "key-1"
          }
        },
        "expect": {}
      },
      {
        "name": "a resend under the same key replays rather than writing again",
        "command": "CREATE CONCEPT ?c { TYPE \"Person\" NAME \"Cass\" }",
        "envelope": {
          "execution": {
            "mode": "independent",
            "idempotency_key": "key-1"
          }
        },
        "expect": {},
        "vectors": [
          "RT-015",
          "TX-015"
        ]
      },
      {
        "name": "so there is one Cass, not two",
        "command": "FIND(COUNT(?c)) WHERE { ?c CONCEPT {type: \"Person\", name: \"Cass\"} }",
        "expect": {
          "result": [
            1
          ]
        }
      },
      {
        "name": "a different key is a different write, which is why the caller chooses it",
        "command": "CREATE CONCEPT ?c { TYPE \"Person\" NAME \"Cass\" }",
        "envelope": {
          "execution": {
            "mode": "independent",
            "idempotency_key": "key-2"
          }
        },
        "expect": {}
      },
      {
        "name": "and that one landed",
        "command": "FIND(COUNT(?c)) WHERE { ?c CONCEPT {type: \"Person\", name: \"Cass\"} }",
        "expect": {
          "result": [
            2
          ]
        }
      },
      {
        "name": "the same key on different work is a caller bug, not a retry",
        "command": "CREATE CONCEPT ?x { TYPE \"Person\" NAME \"Someone Else\" }",
        "envelope": {
          "execution": {
            "mode": "independent",
            "idempotency_key": "key-1"
          }
        },
        "expect": {
          "error": "IdempotencyConflict"
        },
        "vectors": [
          "TX-014"
        ]
      },
      {
        "name": "so the key still names the work it was spent on",
        "command": "FIND(COUNT(?c)) WHERE { ?c CONCEPT {type: \"Person\", name: \"Someone Else\"} }",
        "expect": {
          "result": [
            0
          ]
        }
      },
      {
        "name": "an isolation this engine cannot provide is refused, never accepted by ignoring it",
        "command": "FIND(?c.name) WHERE { ?c CONCEPT {type: \"Person\"} } LIMIT 1",
        "envelope": {
          "execution": {
            "mode": "independent",
            "isolation": "snapshot"
          }
        },
        "expect": {
          "error": "UnsupportedIsolation"
        }
      },
      {
        "name": "and the one it does provide is accepted",
        "command": "FIND(COUNT(?c)) WHERE { ?c CONCEPT {type: \"Person\"} }",
        "envelope": {
          "execution": {
            "mode": "independent",
            "isolation": "serializable"
          }
        },
        "expect": {
          "result": [
            3
          ]
        }
      },
      {
        "name": "a capability name from the shared vocabulary answers rather than reporting itself unknown",
        "command": "FIND(COUNT(?c)) WHERE { ?c CONCEPT {type: \"Person\"} }",
        "envelope": {
          "requires": {
            "version_planes": true,
            "transition": true,
            "symbol_lineage": true
          }
        },
        "expect": {
          "result": [
            3
          ]
        }
      },
      {
        "name": "a source actor is an element reference, never a name: a bare string is refused at the envelope",
        "command": "CREATE CONCEPT ?c { TYPE \"Person\" NAME \"Bob\" }",
        "envelope": {
          "ingest": {
            "evidence": [
              {
                "key": "msg",
                "evidence_class": "user_statement",
                "payload": "hi",
                "source_actor": "urn:x:alice"
              }
            ]
          }
        },
        "expect": {
          "error": "InvalidRequestEnvelope"
        }
      },
      {
        "name": "a supported capability lets the request through",
        "command": "FIND(COUNT(?c)) WHERE { ?c CONCEPT {type: \"Person\"} }",
        "envelope": {
          "requires": {
            "change_stream": true
          }
        },
        "expect": {
          "result": [
            3
          ]
        },
        "vectors": [
          "RT-008"
        ]
      },
      {
        "name": "an entry that carries a detail object still answers as supported",
        "command": "FIND(COUNT(?c)) WHERE { ?c CONCEPT {type: \"Person\"} }",
        "envelope": {
          "requires": {
            "idempotency_retention": true
          }
        },
        "expect": {
          "result": [
            3
          ]
        }
      },
      {
        "name": "a capability the engine does not have is refused before the command runs",
        "command": "FIND(COUNT(?c)) WHERE { ?c CONCEPT {type: \"Person\"} }",
        "envelope": {
          "requires": {
            "semantic_search": true
          }
        },
        "expect": {
          "error": "UnsupportedCapability"
        }
      },
      {
        "name": "a name no registry knows fails the same way, never by passing unrecognized",
        "command": "FIND(COUNT(?c)) WHERE { ?c CONCEPT {type: \"Person\"} }",
        "envelope": {
          "requires": {
            "telepathy": true
          }
        },
        "expect": {
          "error": "UnsupportedCapability"
        }
      },
      {
        "name": "an ingest block on a read-only request is refused, never silently dropped",
        "command": "FIND(COUNT(?c)) WHERE { ?c CONCEPT {type: \"Person\"} }",
        "envelope": {
          "ingest": {
            "evidence": [
              {
                "key": "msg",
                "evidence_class": "user_statement",
                "payload": "I prefer dark mode."
              }
            ]
          }
        },
        "expect": {
          "error": "InvalidRequestEnvelope"
        }
      }
    ]
  },
  {
    "name": "retention",
    "description": "`SET RETENTION` writes storage lifecycle and nothing else (§19). The judgement calls pinned here are the ones an engine gets to make wrong quietly: the block replaces rather than patches, a member outside §19.1's shape is refused rather than stored and lost, and a lapsed `expires_at` changes what a sweep may collect without changing what recall returns — retention says how long the record is kept, never whether the claim still holds.",
    "setup": [
      "MUTATE {\n  CREATE CONCEPT ?alice { TYPE \"Person\" NAME \"Alice\" }\n  CREATE CONCEPT ?bob { TYPE \"Person\" NAME \"Bob\" }\n  CREATE CONCEPT ?dark { TYPE \"Preference\" NAME \"Dark\" }\n}"
    ],
    "cases": [
      {
        "name": "a retention block is written onto the element the target names",
        "command": "SET RETENTION ?c {retention_class: \"short\", expires_at: \"2030-01-01T00:00:00Z\"} WHERE { ?c CONCEPT {name: \"Alice\"} }",
        "expect": {}
      },
      {
        "name": "and reads back through the element's retention hook",
        "command": "FIND(?c.retention.retention_class) WHERE { ?c CONCEPT {name: \"Alice\"} }",
        "expect": {
          "result": [
            "short"
          ]
        }
      },
      {
        "name": "storage lifecycle is not content: the Concept still says what it said",
        "command": "FIND(?c.name) WHERE { ?c CONCEPT {name: \"Alice\"} }",
        "expect": {
          "result": [
            "Alice"
          ]
        }
      },
      {
        "name": "an element the selection block did not reach carries no retention",
        "command": "FIND(?c.retention.retention_class) WHERE { ?c CONCEPT {name: \"Bob\"} }",
        "expect": {
          "result": [
            null
          ]
        }
      },
      {
        "name": "a member outside the hook's shape is refused, not stored and lost",
        "command": "SET RETENTION ?c {retention_class: \"standard\", review_at: \"2030-01-01T00:00:00Z\"} WHERE { ?c CONCEPT {name: \"Alice\"} }",
        "expect": {
          "error": "SchemaFieldNotFound"
        }
      },
      {
        "name": "a retention_class that is not a string is refused",
        "command": "SET RETENTION ?c {retention_class: 7} WHERE { ?c CONCEPT {name: \"Alice\"} }",
        "expect": {
          "error": "TypeMismatch"
        }
      },
      {
        "name": "an expires_at that is not a timestamp is refused",
        "command": "SET RETENTION ?c {expires_at: \"whenever\"} WHERE { ?c CONCEPT {name: \"Alice\"} }",
        "expect": {
          "error": "TypeMismatch"
        }
      },
      {
        "name": "the refused blocks left the recorded one alone",
        "command": "FIND(?c.retention.retention_class) WHERE { ?c CONCEPT {name: \"Alice\"} }",
        "expect": {
          "result": [
            "short"
          ]
        }
      },
      {
        "name": "the block replaces rather than patches",
        "command": "SET RETENTION ?c {expires_at: \"2031-01-01T00:00:00Z\"} WHERE { ?c CONCEPT {name: \"Alice\"} }",
        "expect": {}
      },
      {
        "name": "so a member the new block omits is cleared, not carried forward",
        "command": "FIND(?c.retention.retention_class) WHERE { ?c CONCEPT {name: \"Alice\"} }",
        "expect": {
          "result": [
            null
          ]
        }
      },
      {
        "name": "a lapsed retention is a sweep's business, not recall's",
        "command": "SET RETENTION ?c {retention_class: \"short\", expires_at: \"2020-01-01T00:00:00Z\"} WHERE { ?c CONCEPT {name: \"Bob\"} }",
        "expect": {}
      },
      {
        "name": "the element is still active and still recalled until a Principal asks",
        "command": "FIND(?c.name) WHERE { ?c CONCEPT {name: \"Bob\"} }",
        "expect": {
          "result": [
            "Bob"
          ]
        }
      },
      {
        "name": "a direct target needs no selection block",
        "command": "SET RETENTION :target {retention_class: \"standard\"}",
        "params": {
          "target": "C-3"
        },
        "expect": {}
      },
      {
        "name": "and writes the block it was given",
        "command": "FIND(?c.retention.retention_class) WHERE { ?c CONCEPT {name: \"Dark\"} }",
        "expect": {
          "result": [
            "standard"
          ]
        }
      },
      {
        "name": "a bounded sweep takes as many as LIMIT allows and no more",
        "command": "SET RETENTION ?c {retention_class: \"capped\"} WHERE { ?c CONCEPT {type: \"Person\"} } LIMIT 1",
        "expect": {}
      },
      {
        "name": "LIMIT cuts in ascending element id, so the cut is repeatable",
        "command": "FIND(?c.name, ?c.retention.retention_class) WHERE { ?c CONCEPT {type: \"Person\"} } ORDER BY ?c.name",
        "ordered": true,
        "expect": {
          "result": [
            [
              "Alice",
              "capped"
            ],
            [
              "Bob",
              "short"
            ]
          ]
        }
      }
    ]
  },
  {
    "name": "schema-endpoints",
    "description": "What a Schema Package says may occupy each end of a tuple, what may sit at each end of a structural edge, and what kind of element may carry a Facet (§41–§44, §58, §62–§66). An endpoint spec is a contract about what a reference points at, so it is checked where the reference is written rather than where it is read. Two judgement calls decide the shape: an endpoint this engine cannot resolve is reported as unknown rather than as wrong — inventing a violation out of a failed lookup would refuse legitimate cross-Space data — and an element the same transaction just created is resolvable, or a block that mints a Concept and then points at it would look untyped to itself. A declaration reads the same in both directions: an element reference on an end that names only datatypes is refused exactly as a Literal is on an end that names kinds, and an end that names both accepts both. A Facet's `applicable_to` is judged the same way, against the carrier: a Facet declaring `concept_types` refuses a record, which cannot be a Concept of any type, and refuses a Concept of another type.",
    "packages": [
      {
        "format": "KIP-Schema-Package",
        "manifest": {
          "package_id": "kip://test/endpoints",
          "version": "1.0.0"
        },
        "definitions": {
          "concept_types": {
            "Tool": {
              "kind": "ConceptType",
              "description": "Something used."
            },
            "Room": {
              "kind": "ConceptType",
              "description": "Somewhere a Tool is kept."
            }
          },
          "predicates": {
            "kept_in": {
              "kind": "PredicateType",
              "description": "Where a Tool is kept. Both ends are element references of a declared type.",
              "subject": {
                "concept_types": [
                  "kip://test/endpoints@1.0.0/Tool"
                ]
              },
              "object": {
                "concept_types": [
                  "kip://test/endpoints@1.0.0/Room"
                ]
              }
            },
            "engraved": {
              "kind": "PredicateType",
              "description": "The text engraved on a Tool. The object is a Literal of a declared datatype.",
              "subject": {
                "kinds": [
                  "Concept"
                ]
              },
              "object": {
                "datatypes": [
                  "kip:string"
                ]
              }
            },
            "labelled": {
              "kind": "PredicateType",
              "description": "A label for a Tool: either a Concept that stands for one, or the text itself.",
              "subject": {
                "kinds": [
                  "Concept"
                ]
              },
              "object": {
                "kinds": [
                  "Concept"
                ],
                "datatypes": [
                  "kip:string"
                ]
              }
            }
          },
          "facets": {
            "Wear": {
              "kind": "FacetDefinition",
              "description": "How worn a Tool is. State about a Tool, and only a Tool.",
              "closed": true,
              "applicable_to": {
                "concept_types": [
                  "kip://test/endpoints@1.0.0/Tool"
                ]
              },
              "fields": {
                "level": {
                  "type": "number",
                  "required": false,
                  "mutable": true,
                  "minimum": 0,
                  "maximum": 1
                }
              }
            }
          },
          "structural_fields": {
            "kept_with": {
              "kind": "StructuralFieldDefinition",
              "description": "Other Tools kept alongside this one. Both ends are Tools, at most two, and no Tool twice.",
              "source": {
                "concept_types": [
                  "kip://test/endpoints@1.0.0/Tool"
                ]
              },
              "target": {
                "concept_types": [
                  "kip://test/endpoints@1.0.0/Tool"
                ]
              },
              "cardinality": {
                "min": 0,
                "max": 2
              },
              "ordered": false,
              "unique": true
            }
          }
        }
      }
    ],
    "setup": [
      "MUTATE {\n  CREATE CONCEPT ?hammer { TYPE \"Tool\" NAME \"Hammer\" }\n  CREATE CONCEPT ?shed { TYPE \"Room\" NAME \"Shed\" }\n}"
    ],
    "cases": [
      {
        "name": "a tuple whose ends are what the predicate declares",
        "command": "MUTATE {\n  ENSURE PROPOSITION ?p (:tool, \"kept_in\", :room)\n}",
        "params": {
          "tool": {
            "id": "C-1"
          },
          "room": {
            "id": "C-2"
          }
        },
        "expect": {},
        "vectors": [
          "SCHEMA-019"
        ]
      },
      {
        "name": "an object of the wrong Concept type is refused where the reference is written",
        "command": "MUTATE {\n  ENSURE PROPOSITION ?p (:tool, \"kept_in\", :tool)\n}",
        "params": {
          "tool": {
            "id": "C-1"
          }
        },
        "expect": {
          "error": "ConstraintViolation"
        }
      },
      {
        "name": "and so is a subject of the wrong Concept type",
        "command": "MUTATE {\n  ENSURE PROPOSITION ?p (:room, \"kept_in\", :room)\n}",
        "params": {
          "room": {
            "id": "C-2"
          }
        },
        "expect": {
          "error": "ConstraintViolation"
        }
      },
      {
        "name": "a Literal cannot occupy an end the schema declares an element reference",
        "command": "MUTATE {\n  ENSURE PROPOSITION ?p (:tool, \"kept_in\", \"the shed\")\n}",
        "params": {
          "tool": {
            "id": "C-1"
          }
        },
        "expect": {
          "error": "ConstraintViolation"
        }
      },
      {
        "name": "an end this Space cannot resolve is unknown, not wrong",
        "command": "MUTATE {\n  ENSURE PROPOSITION ?p (:tool, \"kept_in\", :elsewhere)\n}",
        "params": {
          "tool": {
            "id": "C-1"
          },
          "elsewhere": {
            "canonical_id": "urn:room:shed"
          }
        },
        "expect": {}
      },
      {
        "name": "a Literal of the declared datatype is what a datatype endpoint is for",
        "command": "MUTATE {\n  ENSURE PROPOSITION ?p (:tool, \"engraved\", \"MMXXVI\")\n}",
        "params": {
          "tool": {
            "id": "C-1"
          }
        },
        "expect": {}
      },
      {
        "name": "a Literal of another datatype is not",
        "command": "MUTATE {\n  ENSURE PROPOSITION ?p (:tool, \"engraved\", 2026)\n}",
        "params": {
          "tool": {
            "id": "C-1"
          }
        },
        "expect": {
          "error": "ConstraintViolation"
        }
      },
      {
        "name": "and an element reference is as wrong there as a Literal is on a reference end",
        "command": "MUTATE {\n  ENSURE PROPOSITION ?p (:tool, \"engraved\", :room)\n}",
        "params": {
          "tool": {
            "id": "C-1"
          },
          "room": {
            "id": "C-2"
          }
        },
        "expect": {
          "error": "ConstraintViolation"
        }
      },
      {
        "name": "an end that declares both forms accepts both",
        "command": "MUTATE {\n  ENSURE PROPOSITION ?a (:tool, \"labelled\", :room)\n  ENSURE PROPOSITION ?b (:tool, \"labelled\", \"the good one\")\n}",
        "params": {
          "tool": {
            "id": "C-1"
          },
          "room": {
            "id": "C-2"
          }
        },
        "expect": {}
      },
      {
        "name": "an element the same transaction just created is resolvable to itself",
        "command": "MUTATE {\n  CREATE CONCEPT ?chisel { TYPE \"Tool\" NAME \"Chisel\" }\n  CREATE CONCEPT ?loft { TYPE \"Room\" NAME \"Loft\" }\n  ENSURE PROPOSITION ?p (?chisel, \"kept_in\", ?loft)\n}",
        "expect": {}
      },
      {
        "name": "including when what it just created is the wrong type for the slot",
        "command": "MUTATE {\n  CREATE CONCEPT ?bench { TYPE \"Tool\" NAME \"Bench\" }\n  ENSURE PROPOSITION ?p (:tool, \"kept_in\", ?bench)\n}",
        "params": {
          "tool": {
            "id": "C-1"
          }
        },
        "expect": {
          "error": "ConstraintViolation"
        }
      },
      {
        "name": "a Facet is state about the kind of element it declares",
        "command": "MUTATE {\n  UPDATE \"C-1\"\n  SET FACET \"Wear\" {level: 0.4}\n}",
        "expect": {}
      },
      {
        "name": "and a record, which cannot be a Concept of any type, cannot carry it",
        "command": "MUTATE {\n  CREATE EVIDENCE ?e {\n    SET FIELDS {evidence_class: \"observation\", payload: \"the handle is splitting\"}\n    SET FACET \"Wear\" {level: 0.4}\n  }\n}",
        "expect": {
          "error": "ConstraintViolation"
        }
      },
      {
        "name": "and a Concept of another type cannot carry it either",
        "command": "MUTATE {\n  UPDATE \"C-2\"\n  SET FACET \"Wear\" {level: 0.4}\n}",
        "expect": {
          "error": "ConstraintViolation"
        }
      },
      {
        "name": "the tuples that survived",
        "command": "FIND(?s.name, ?o.name) WHERE { ?p PROPOSITION (?s, \"kept_in\", ?o) }",
        "expect": {
          "result": [
            [
              "Hammer",
              "Shed"
            ],
            [
              "Chisel",
              "Loft"
            ],
            [
              "Hammer",
              null
            ]
          ]
        }
      },
      {
        "name": "a structural edge is held to the ends its field declares",
        "command": "MUTATE {\n  CREATE CONCEPT ?saw { TYPE \"Tool\" NAME \"Saw\" }\n  CREATE CONCEPT ?plane { TYPE \"Tool\" NAME \"Plane\" }\n  UPDATE ?saw SET STRUCTURAL { (\"kept_with\", ?plane) }\n}",
        "expect": {}
      },
      {
        "name": "a target of the wrong Concept type is refused there too",
        "command": "MUTATE {\n  UPDATE \"C-1\"\n  SET STRUCTURAL { (\"kept_with\", :room) }\n}",
        "params": {
          "room": {
            "id": "C-2"
          }
        },
        "expect": {
          "error": "ConstraintViolation"
        }
      },
      {
        "name": "and so is a source the field was never about",
        "command": "MUTATE {\n  UPDATE \"C-2\"\n  SET STRUCTURAL { (\"kept_with\", :tool) }\n}",
        "params": {
          "tool": {
            "id": "C-1"
          }
        },
        "expect": {
          "error": "ConstraintViolation"
        }
      },
      {
        "name": "stating the same reference twice is one edge, not a duplicate",
        "command": "MUTATE {\n  CREATE CONCEPT ?awl {\n    TYPE \"Tool\"\n    NAME \"Awl\"\n    SET STRUCTURAL {\n      (\"kept_with\", :tool)\n      (\"kept_with\", :tool)\n    }\n  }\n}",
        "params": {
          "tool": {
            "id": "C-1"
          }
        },
        "expect": {}
      },
      {
        "name": "which is why a field declared unique is not violable by writing to it",
        "command": "FIND(?t.structural) WHERE { ?t CONCEPT {name: \"Awl\"} }",
        "expect": {
          "result": [
            {
              "kip://test/endpoints@1.0.0/kept_with": [
                {
                  "id": "C:<1>"
                }
              ]
            }
          ]
        }
      },
      {
        "name": "and more references than the declared cardinality permits is another",
        "command": "MUTATE {\n  CREATE CONCEPT ?rasp { TYPE \"Tool\" NAME \"Rasp\" }\n  CREATE CONCEPT ?file { TYPE \"Tool\" NAME \"File\" }\n  CREATE CONCEPT ?vice {\n    TYPE \"Tool\"\n    NAME \"Vice\"\n    SET STRUCTURAL {\n      (\"kept_with\", :tool)\n      (\"kept_with\", ?rasp)\n      (\"kept_with\", ?file)\n    }\n  }\n}",
        "params": {
          "tool": {
            "id": "C-1"
          }
        },
        "expect": {
          "error": "ConstraintViolation"
        }
      }
    ]
  },
  {
    "name": "structural-core-fields",
    "description": "`STRUCTURAL` reaches both structural planes (§8.2, §17). A Profile field is addressed by its resolved symbol; a Core field — an Assertion's `evidence` and `context`, an Evidence record's `source` and `generated_by`, an Activity's `inputs`, `outputs` and `associated_actors` — is addressed by its plain name. That is what gives \"which Assertions cite this Evidence\" a spelling. The two planes are told apart by name and never merged, so a Profile field cannot redefine what an Assertion cites, and a Core field reports no `index`: its order is storage order, not a declared position.",
    "packages": [
      {
        "format": "KIP-Schema-Package",
        "manifest": {
          "package_id": "kip://test/structural-core-probe",
          "version": "1.0.0"
        },
        "definitions": {
          "concept_types": {
            "Note": {
              "kind": "ConceptType",
              "description": "A note that cites other notes."
            }
          },
          "structural_fields": {
            "evidence": {
              "kind": "StructuralFieldDefinition",
              "description": "A Profile field that happens to share a Core field's name, to prove the two planes stay apart.",
              "source": {
                "concept_types": [
                  "kip://test/structural-core-probe@1.0.0/Note"
                ]
              },
              "target": {
                "kinds": [
                  "Concept"
                ]
              },
              "ordered": true
            },
            "reviewed": {
              "kind": "StructuralFieldDefinition",
              "description": "What an element was reviewed against. Carried by any kind, to prove a Profile field is not a Concept's alone.",
              "source": {
                "kinds": [
                  "Concept",
                  "Assertion",
                  "Activity",
                  "Evidence"
                ]
              },
              "target": {
                "kinds": [
                  "Concept"
                ]
              },
              "ordered": false
            }
          }
        }
      }
    ],
    "setup": [
      "MUTATE {\n  CREATE CONCEPT ?alice { TYPE \"Person\" NAME \"Alice\" }\n  CREATE CONCEPT ?dark { TYPE \"Preference\" NAME \"Dark\" }\n  CREATE CONCEPT ?note { TYPE \"Note\" NAME \"A note\" }\n  CREATE CONCEPT ?citing {\n    TYPE \"Note\"\n    NAME \"Citing note\"\n    SET STRUCTURAL { (\"evidence\", ?note) }\n  }\n  ENSURE PROPOSITION ?p (?alice, \"prefers\", ?dark)\n  CREATE EVIDENCE ?e {\n    SET FIELDS {\n      evidence_class: \"user_statement\",\n      payload: \"I prefer dark mode.\",\n      content_digest: \"sha3-256:d1ge5t\",\n      media_type: \"text/plain\",\n      observed_at: \"2026-08-16T09:00:00Z\"\n    }\n  }\n  CREATE ASSERTION ?a {\n    SET FIELDS { proposition: ?p, asserted_by: ?alice, stance: \"support\", mode: \"stated\", confidence: 0.9 }\n    SET STRUCTURAL { (\"evidence\", ?e) {role: \"support\"} }\n  }\n  CREATE ACTIVITY ?run {\n    SET FIELDS {activity_class: \"semantic_consolidation\", status: \"completed\"}\n    SET STRUCTURAL {\n      (\"inputs\", ?alice)\n      (\"outputs\", ?dark)\n      (\"associated_actors\", ?alice)\n      (\"reviewed\", ?note)\n    }\n  }\n}"
    ],
    "cases": [
      {
        "name": "which Assertions cite this Evidence has a spelling",
        "command": "FIND(?a.id) WHERE { ?a ASSERTION {} STRUCTURAL (?a, \"evidence\", ?e) ?e EVIDENCE {} }",
        "expect": {
          "result": [
            "A:<1>"
          ]
        }
      },
      {
        "name": "and it answers the other way round too",
        "command": "FIND(?e.evidence_class) WHERE { STRUCTURAL (:a, \"evidence\", ?e) ?e EVIDENCE {} }",
        "params": {
          "a": "A-1"
        },
        "expect": {
          "result": [
            "user_statement"
          ]
        }
      },
      {
        "name": "an Activity's inputs and outputs are the same kind of field",
        "command": "FIND(?c.name) WHERE { STRUCTURAL (?x, \"inputs\", ?c) ?c CONCEPT {} }",
        "expect": {
          "result": [
            "Alice"
          ]
        }
      },
      {
        "name": "so are its outputs",
        "command": "FIND(?c.name) WHERE { STRUCTURAL (?x, \"outputs\", ?c) ?c CONCEPT {} }",
        "expect": {
          "result": [
            "Dark"
          ]
        }
      },
      {
        "name": "a Core field an element does not carry matches nothing rather than erroring",
        "command": "FIND(?c) WHERE { STRUCTURAL (?e, \"source\", ?c) }",
        "expect": {
          "result": []
        }
      },
      {
        "name": "the bound edge names the Core field plainly, and reports no declared position",
        "command": "FIND(?edge.field, ?edge.index) WHERE { ?edge STRUCTURAL (:a, \"evidence\", ?e) }",
        "params": {
          "a": "A-1"
        },
        "expect": {
          "result": [
            [
              "evidence",
              null
            ]
          ]
        }
      },
      {
        "name": "a Profile field of the same name is a different edge, and keeps its symbol",
        "command": "FIND(?edge.field, ?edge.index) WHERE { ?edge STRUCTURAL (?n, \"evidence\", ?t) ?n CONCEPT {name: \"Citing note\"} }",
        "expect": {
          "result": [
            [
              "kip://test/structural-core-probe@1.0.0/evidence",
              0
            ]
          ]
        }
      },
      {
        "name": "a name that is neither a Core field nor a declared symbol is refused",
        "command": "FIND(?c) WHERE { STRUCTURAL (?x, \"not_a_field\", ?c) }",
        "expect": {
          "error": "SchemaSymbolNotFound"
        }
      },
      {
        "name": "a Profile field on a record is reachable from a bound source, not just from a Concept",
        "command": "FIND(?edge.field) WHERE { ?edge STRUCTURAL (:run, \"reviewed\", ?t) }",
        "params": {
          "run": "X-1"
        },
        "expect": {
          "result": [
            "kip://test/structural-core-probe@1.0.0/reviewed"
          ]
        }
      }
    ]
  },
  {
    "name": "transactions",
    "description": "A MUTATE block is one transaction, not a script that happens to run in order: everything in it commits or none of it does. A precondition that fails leaves the element exactly as the caller last saw it. EXPECT VERSION is the one guard, and it is always the trailing clause (Spec §52.8); there is no EXPECT STATE — TRANSITION validates the target's current lifecycle state itself and fails InvalidLifecycleTransition from the wrong one (§35.3, §52.5), while a move to the state already held is a no_effect rather than an error.",
    "setup": [
      "MUTATE {\n  CREATE CONCEPT ?alice { TYPE \"Person\" NAME \"Alice\" SET FIELDS {key: \"person:alice\"} }\n  CREATE CONCEPT ?dark { TYPE \"Preference\" NAME \"Dark\" }\n  ENSURE PROPOSITION ?p (?alice, \"prefers\", ?dark)\n  CREATE ASSERTION ?a {\n    SET FIELDS { proposition: ?p, asserted_by: ?alice, stance: \"support\", mode: \"stated\", confidence: 0.9 }\n  }\n}"
    ],
    "cases": [
      {
        "name": "a clause that fails undoes the clauses that already ran beside it",
        "command": "MUTATE {\n  CREATE CONCEPT ?ok { TYPE \"Person\" NAME \"Rollback probe\" }\n  CREATE CONCEPT ?bad { TYPE \"Spaceship\" NAME \"Serenity\" }\n}",
        "expect": {
          "error": "SchemaSymbolNotFound"
        },
        "vectors": [
          "TX-023"
        ]
      },
      {
        "name": "so the clause that succeeded left nothing behind",
        "command": "FIND(COUNT(?c)) WHERE { ?c CONCEPT {name: \"Rollback probe\"} }",
        "expect": {
          "result": [
            0
          ]
        }
      },
      {
        "name": "a stale EXPECT VERSION refuses the write",
        "command": "UPSERT CONCEPT ?p { MATCH {key: \"person:alice\"} SET FIELDS {name: \"Rewritten\"} } EXPECT VERSION 99",
        "expect": {
          "error": "VersionConflict"
        }
      },
      {
        "name": "and the refused write changed nothing",
        "command": "FIND(?c.name) WHERE { ?c CONCEPT {key: \"person:alice\"} }",
        "expect": {
          "result": [
            "Alice"
          ]
        }
      },
      {
        "name": "a selection block that matches nothing is a no-op, not an error",
        "command": "UPDATE ?c SET ATTRIBUTES { note: \"unreachable\" } WHERE { ?c CONCEPT {name: \"Nobody\"} }",
        "expect": {}
      },
      {
        "name": "TRANSITION validates the move against the target's kind: an Assertion has no Activity state to move to",
        "command": "TRANSITION ?a TO \"running\" WHERE { ?a ASSERTION {} }",
        "expect": {
          "error": "InvalidLifecycleTransition"
        }
      },
      {
        "name": "the claim it refused to touch is still active",
        "command": "FIND(?a.lifecycle.status) WHERE { ?a ASSERTION {} }",
        "expect": {
          "result": [
            "active"
          ]
        }
      },
      {
        "name": "the assertor's withdrawal is one TRANSITION, legal from active",
        "command": "TRANSITION ?a TO \"retracted\" WHERE { ?a ASSERTION {} }",
        "expect": {}
      },
      {
        "name": "and the claim is withdrawn rather than deleted",
        "command": "FIND(?a.lifecycle.status, ?a.confidence) WHERE { ?a ASSERTION {} }",
        "expect": {
          "result": [
            [
              "retracted",
              0.9
            ]
          ]
        }
      },
      {
        "name": "a move to the state the target already holds is no_effect, not an error",
        "command": "TRANSITION ?a TO \"retracted\" WHERE { ?a ASSERTION {} }",
        "expect": {}
      },
      {
        "name": "and a move that is not legal from retracted is refused",
        "command": "TRANSITION ?a TO \"superseded\" BY \"A-1\" WHERE { ?a ASSERTION {} }",
        "expect": {
          "error": "InvalidLifecycleTransition"
        }
      },
      {
        "name": "a stale guard on one version plane names the plane it refused",
        "command": "UPDATE ?c SET ATTRIBUTES { note: \"late\" } WHERE { ?c CONCEPT {key: \"person:alice\"} } EXPECT VERSION 99 OF ATTRIBUTES",
        "expect": {
          "error": "VersionConflict"
        }
      },
      {
        "name": "the same guard on two planes is refused as syntax, before anything runs",
        "command": "UPDATE ?c SET ATTRIBUTES { note: \"late\" } WHERE { ?c CONCEPT {key: \"person:alice\"} } EXPECT VERSION 1 OF ATTRIBUTES EXPECT VERSION 1 OF ATTRIBUTES",
        "expect": {
          "error": "InvalidSyntax"
        }
      },
      {
        "name": "a guard between the target and the actions is not where a mutation keeps its preconditions",
        "command": "UPDATE ?c EXPECT VERSION 1 SET ATTRIBUTES { note: \"late\" } WHERE { ?c CONCEPT {key: \"person:alice\"} }",
        "expect": {
          "error": "InvalidSyntax"
        }
      },
      {
        "name": "a CLIENT KEY retry writes nothing and hands back the element the first attempt made",
        "command": "MUTATE {\n  CREATE CONCEPT ?a { TYPE \"Person\" NAME \"Casey\" CLIENT KEY \"person:casey\" }\n}",
        "expect": {}
      },
      {
        "name": "so the same creation under the same key is a retry, not a second Casey",
        "command": "MUTATE {\n  CREATE CONCEPT ?a { TYPE \"Person\" NAME \"Casey\" CLIENT KEY \"person:casey\" }\n}",
        "expect": {}
      },
      {
        "name": "and there is one of them",
        "command": "FIND(COUNT(?c)) WHERE { ?c CONCEPT {type: \"Person\", name: \"Casey\"} }",
        "expect": {
          "result": [
            1
          ]
        }
      },
      {
        "name": "but a different creation under that key is a conflict, never a silent reuse",
        "command": "MUTATE {\n  CREATE CONCEPT ?a { TYPE \"Person\" NAME \"Someone Else\" CLIENT KEY \"person:casey\" }\n}",
        "expect": {
          "error": "ClientKeyConflict"
        }
      },
      {
        "name": "two clauses that give one target two final values have no answer but a refusal",
        "command": "MUTATE {\n  UPDATE ?x SET FIELDS {name: \"One\"} WHERE { ?x CONCEPT {key: \"person:alice\"} }\n  UPDATE ?y SET FIELDS {name: \"Two\"} WHERE { ?y CONCEPT {key: \"person:alice\"} }\n}",
        "expect": {
          "error": "DuplicateMutationTarget"
        }
      },
      {
        "name": "and the refused block changed nothing",
        "command": "FIND(?c.name) WHERE { ?c CONCEPT {key: \"person:alice\"} }",
        "expect": {
          "result": [
            "Alice"
          ]
        }
      },
      {
        "name": "two clauses that agree are not in conflict: a plan assembled from parts may say a thing twice",
        "command": "MUTATE {\n  UPDATE ?x SET FIELDS {name: \"Agreed\"} WHERE { ?x CONCEPT {key: \"person:alice\"} }\n  UPDATE ?y SET FIELDS {name: \"Agreed\"} WHERE { ?y CONCEPT {key: \"person:alice\"} }\n}",
        "expect": {}
      },
      {
        "name": "nor are two clauses writing different paths of one target",
        "command": "MUTATE {\n  UPDATE ?x SET FIELDS {name: \"Split\"} WHERE { ?x CONCEPT {key: \"person:alice\"} }\n  UPDATE ?y SET ATTRIBUTES {role: \"lead\"} WHERE { ?y CONCEPT {key: \"person:alice\"} }\n}",
        "expect": {}
      },
      {
        "name": "so the two writes that agreed and the two that did not overlap both landed",
        "command": "FIND(?c.name, ?c.attributes.role) WHERE { ?c CONCEPT {key: \"person:alice\"} }",
        "expect": {
          "result": [
            [
              "Split",
              "lead"
            ]
          ]
        }
      }
    ]
  },
  {
    "name": "tuple-endpoints",
    "description": "What may stand in a Proposition tuple's subject/object slot, and what happens to the ones an engine cannot resolve. The grammar's `term` admits an object pattern and a nested Proposition expression; only two spellings of an object pattern name an endpoint (§8.1, §8.2), and a term an engine cannot resolve has to be refused rather than treated as an open slot — an unconstrained endpoint silently matches every tuple under its predicate (§43.2).",
    "setup": [
      "MUTATE {\n  CREATE CONCEPT ?alice { TYPE \"Person\" NAME \"Alice\" }\n  CREATE CONCEPT ?bob { TYPE \"Person\" NAME \"Bob\" }\n  ENSURE PROPOSITION ?p (?alice, \"same_as\", {canonical_id: \"urn:x:alice\"})\n  ENSURE PROPOSITION ?q (?bob, \"same_as\", {canonical_id: \"urn:x:bob\"})\n}"
    ],
    "cases": [
      {
        "name": "an object endpoint written as {canonical_id: ...} names one endpoint, not every tuple",
        "command": "FIND(?s.name) WHERE { ?p PROPOSITION (?s, \"same_as\", {canonical_id: \"urn:x:alice\"}) }",
        "expect": {
          "result": [
            "Alice"
          ]
        }
      },
      {
        "name": "a canonical endpoint nobody wrote matches nothing rather than everything",
        "command": "FIND(?s.name) WHERE { ?p PROPOSITION (?s, \"same_as\", {canonical_id: \"urn:x:nobody\"}) }",
        "expect": {
          "result": []
        }
      },
      {
        "name": "an object endpoint that describes rather than names is refused",
        "command": "FIND(?s.name) WHERE { ?p PROPOSITION (?s, \"same_as\", {name: \"Alice\"}) }",
        "expect": {
          "error": "IdentitySelectorRequired"
        }
      },
      {
        "name": "an identity member that is itself a pattern is refused",
        "command": "FIND(?s.name) WHERE { ?p PROPOSITION (?s, \"same_as\", {canonical_id: ?whatever}) }",
        "expect": {
          "error": "IdentitySelectorRequired"
        }
      },
      {
        "name": "a nested Proposition endpoint is refused, never silently unconstrained",
        "command": "FIND(?s.name) WHERE { ?meta PROPOSITION (?s, \"same_as\", (id: :other)) }",
        "params": {
          "other": "P-1"
        },
        "expect": {
          "error": "UnsupportedCapability"
        }
      },
      {
        "name": "the same refusal on the mutation path",
        "command": "ENSURE PROPOSITION ?p (:subject, \"same_as\", (id: :other))",
        "params": {
          "subject": "C-1",
          "other": "P-1"
        },
        "expect": {
          "error": "UnsupportedCapability"
        }
      },
      {
        "name": "a mutation endpoint that describes rather than names is refused",
        "command": "ENSURE PROPOSITION ?p (:subject, \"same_as\", {name: \"Bob\"})",
        "params": {
          "subject": "C-1"
        },
        "expect": {
          "error": "IdentitySelectorRequired"
        }
      },
      {
        "name": "an identity endpoint carrying more than the identity is refused, never half-honoured",
        "command": "FIND(?s.name) WHERE { ?p PROPOSITION (?s, \"same_as\", {canonical_id: \"urn:x:alice\", name: \"Zed\"}) }",
        "expect": {
          "error": "IdentitySelectorRequired"
        }
      }
    ]
  }
] as unknown as Fixture[]

/** The total number of cases, so a silent shrink is visible. */
export const CASE_COUNT = 264
