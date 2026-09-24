/**
 * The KIP 2.0 cross-engine conformance fixtures — GENERATED FILE, DO NOT EDIT.
 *
 * Source of truth: `fixtures/kip-conformance-2.0/*.json`, which the Rust
 * engine's `tests/conformance.rs` reads from disk. Regenerate with
 * `pnpm run codegen:fixtures`.
 */

/**
 * One expectation: a result to match, members and rows the result must
 * contain, or the registry code to fail with. An empty expectation passes on
 * any result.
 */
export interface Expectation {
  result?: unknown
  result_contains?: unknown
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

/**
 * A setup step: a bare command, or one whose raw result is captured into
 * parameters (JSON Pointers) for later steps and every case of the fixture.
 */
export type Setup =
  | string
  | { command: string; params?: Record<string, unknown>; capture?: Record<string, string> }

export interface Fixture {
  name: string
  description: string
  /** `pending_engine` while no engine has verified the fixture. */
  status?: string
  /** Extra Schema Package artifacts to install and activate, inline. */
  packages?: unknown[]
  setup?: Setup[]
  cases: Case[]
}

export const FIXTURES: readonly Fixture[] = [
  {
    "name": "cognitive-consistency",
    "description": "d6e3a45 Core cognitive contracts: conflict-complete belief, context inclusion, half-open clocks, protected identity and portable numeric inputs.",
    "packages": [
      {
        "format": "KIP-Schema-Package",
        "manifest": {
          "package_id": "kip://test/consistency",
          "version": "1.0.0"
        },
        "definitions": {
          "predicates": {
            "position": {
              "kind": "PredicateType",
              "functional": true,
              "object": {
                "literal_types": [
                  "string"
                ]
              }
            },
            "scoped": {
              "kind": "PredicateType",
              "functional": true,
              "object": {
                "literal_types": [
                  "string"
                ]
              }
            },
            "temporal": {
              "kind": "PredicateType",
              "functional": true,
              "object": {
                "literal_types": [
                  "string"
                ]
              }
            },
            "likes": {
              "kind": "PredicateType",
              "functional": false,
              "object": {
                "literal_types": [
                  "string"
                ]
              }
            }
          }
        }
      }
    ],
    "setup": [
      "MUTATE {\n CREATE CONCEPT ?a { TYPE \"Person\" NAME \"Ada\" }\n CREATE CONCEPT ?work { TYPE \"Person\" NAME \"work\" }\n CREATE CONCEPT ?travel { TYPE \"Person\" NAME \"travel\" }\n ENSURE PROPOSITION ?home (?a, \"position\", \"home\")\n ENSURE PROPOSITION ?office (?a, \"position\", \"office\")\n ENSURE PROPOSITION ?tea (?a, \"likes\", \"tea\")\n ENSURE PROPOSITION ?coffee (?a, \"likes\", \"coffee\")\n ENSURE PROPOSITION ?w (?a, \"scoped\", \"work\")\n ENSURE PROPOSITION ?t (?a, \"scoped\", \"travel\")\n ENSURE PROPOSITION ?old (?a, \"temporal\", \"old\")\n ENSURE PROPOSITION ?new (?a, \"temporal\", \"new\")\n CREATE ASSERTION ?a1 { SET FIELDS { proposition: ?home, asserted_by: ?a, stance: \"support\", mode: \"stated\", confidence: 0.9 } }\n CREATE ASSERTION ?a2 { SET FIELDS { proposition: ?office, asserted_by: ?a, stance: \"support\", mode: \"stated\", confidence: 0.9 } }\n CREATE ASSERTION ?a3 { SET FIELDS { proposition: ?tea, asserted_by: ?a, stance: \"support\", mode: \"stated\", confidence: 0.9 } }\n CREATE ASSERTION ?a4 { SET FIELDS { proposition: ?coffee, asserted_by: ?a, stance: \"support\", mode: \"stated\", confidence: 0.9 } }\n CREATE ASSERTION ?a5 { SET FIELDS { proposition: ?w, asserted_by: ?a, stance: \"support\", mode: \"stated\", confidence: 0.9, context_refs: [?work] } }\n CREATE ASSERTION ?a6 { SET FIELDS { proposition: ?t, asserted_by: ?a, stance: \"support\", mode: \"stated\", confidence: 0.9, context_refs: [?travel] } }\n CREATE ASSERTION ?a7 { SET FIELDS { proposition: ?old, asserted_by: ?a, stance: \"support\", mode: \"stated\", confidence: 0.9, valid_time: { from: \"2026-09-01T00:00:00.000Z\", until: \"2026-09-07T00:00:00.000Z\" } } }\n CREATE ASSERTION ?a8 { SET FIELDS { proposition: ?new, asserted_by: ?a, stance: \"support\", mode: \"stated\", confidence: 0.9, valid_time: { from: \"2026-09-07T00:00:00.000Z\" } } }\n}"
    ],
    "cases": [
      {
        "name": "grounded conflict includes candidate diagnosis and visible slot opposition",
        "command": "FIND(?b.status, ?b.candidate_status, ?b.slot_status, ?b.conflict_reasons, ?b.leading) WHERE { ?a CONCEPT {name: \"Ada\"} ?b BELIEF (?a, \"position\", \"home\") }",
        "expect": {
          "result": [
            [
              "contested",
              "accepted",
              "contested",
              [
                "functional_value"
              ],
              "none"
            ]
          ]
        },
        "vectors": [
          "MEM-001"
        ]
      },
      {
        "name": "slot conflict has no accepted values",
        "command": "FIND(?b.status, ?b.accepted_values) WHERE { ?a CONCEPT {name: \"Ada\"} ?b BELIEF SLOT (?a, \"position\") }",
        "expect": {
          "result": [
            [
              "contested",
              []
            ]
          ]
        },
        "vectors": [
          "MEM-001"
        ]
      },
      {
        "name": "LIMIT never hides a competing conflict",
        "command": "FIND(?b.status) WHERE { ?a CONCEPT {name: \"Ada\"} ?b BELIEF (?a, \"position\", ?value) } LIMIT 1",
        "expect": {
          "result": [
            "contested"
          ]
        },
        "vectors": [
          "MEM-001"
        ]
      },
      {
        "name": "a multi-value preference slot remains accepted",
        "command": "FIND(?b.status) WHERE { ?a CONCEPT {name: \"Ada\"} ?b BELIEF SLOT (?a, \"likes\") }",
        "expect": {
          "result": [
            "accepted"
          ]
        },
        "vectors": [
          "MEM-001"
        ]
      },
      {
        "name": "scoped assertions cannot support context-free recall",
        "command": "FIND(?b.status) WHERE { ?a CONCEPT {name: \"Ada\"} ?b BELIEF (?a, \"scoped\", \"work\") }",
        "expect": {
          "result": [
            "insufficient"
          ]
        },
        "vectors": [
          "MEM-007"
        ]
      },
      {
        "name": "exact context includes the matching scoped claim",
        "command": "FIND(?b.status) WHERE { ?a CONCEPT {name: \"Ada\"} ?b BELIEF (?a, \"scoped\", \"work\") } WITH EPISTEMIC {context_refs: [\"C-2\"]}",
        "expect": {
          "result": [
            "accepted"
          ]
        },
        "vectors": [
          "MEM-007"
        ]
      },
      {
        "name": "a context-mismatched candidate is insufficient without exclusive completeness",
        "command": "FIND(?b.status) WHERE { ?a CONCEPT {name: \"Ada\"} ?b BELIEF (?a, \"scoped\", \"travel\") } WITH EPISTEMIC {context_refs: [\"C-2\"]}",
        "expect": {
          "result": [
            "insufficient"
          ]
        },
        "vectors": [
          "MEM-007"
        ]
      },
      {
        "name": "both explicit contexts disclose the conflict",
        "command": "FIND(?b.status) WHERE { ?a CONCEPT {name: \"Ada\"} ?b BELIEF (?a, \"scoped\", \"work\") } WITH EPISTEMIC {context_refs: [\"C-2\", \"C-3\"]}",
        "expect": {
          "result": [
            "contested"
          ]
        },
        "vectors": [
          "MEM-001"
        ]
      },
      {
        "name": "until is excluded exactly at the boundary: an expired value contributes nothing",
        "command": "FIND(?b.status) WHERE { ?a CONCEPT {name: \"Ada\"} ?b BELIEF (?a, \"temporal\", \"old\") } FOR TIME \"2026-09-07T00:00:00.000Z\"",
        "expect": {
          "result": [
            "insufficient"
          ]
        },
        "vectors": [
          "MEM-007"
        ]
      },
      {
        "name": "the value beginning at the boundary is eligible",
        "command": "FIND(?b.status) WHERE { ?a CONCEPT {name: \"Ada\"} ?b BELIEF (?a, \"temporal\", \"new\") } FOR TIME \"2026-09-07T00:00:00.000Z\"",
        "expect": {
          "result": [
            "accepted"
          ]
        },
        "vectors": [
          "MEM-007"
        ]
      },
      {
        "name": "basis preserves canonical time and the next known invalidation",
        "command": "FIND(?b.basis.valid_at, ?b.basis.next_invalid_at) WHERE { ?a CONCEPT {name: \"Ada\"} ?b BELIEF (?a, \"temporal\", \"old\") } FOR TIME \"2026-09-06T00:00:00.000Z\"",
        "expect": {
          "result": [
            [
              "2026-09-06T00:00:00.000Z",
              "2026-09-07T00:00:00.000Z"
            ]
          ]
        },
        "vectors": [
          "MEM-007"
        ]
      },
      {
        "name": "invalid half-open interval ('2026-09-07T00:00:00.000Z', '2026-09-07T00:00:00.000Z')",
        "command": "CREATE ASSERTION ?x { SET FIELDS { proposition: \"P-7\", asserted_by: \"C-1\", stance: \"support\", mode: \"stated\", valid_time: {\"from\": \"2026-09-07T00:00:00.000Z\", \"until\": \"2026-09-07T00:00:00.000Z\"} } }",
        "expect": {
          "error": "ConstraintViolation"
        },
        "vectors": [
          "MEM-007"
        ]
      },
      {
        "name": "invalid half-open interval ('2026-09-08T00:00:00.000Z', '2026-09-07T00:00:00.000Z')",
        "command": "CREATE ASSERTION ?x { SET FIELDS { proposition: \"P-7\", asserted_by: \"C-1\", stance: \"support\", mode: \"stated\", valid_time: {\"from\": \"2026-09-08T00:00:00.000Z\", \"until\": \"2026-09-07T00:00:00.000Z\"} } }",
        "expect": {
          "error": "ConstraintViolation"
        },
        "vectors": [
          "MEM-007"
        ]
      },
      {
        "name": "merged_into is protected identity state",
        "command": "UPDATE \"C-1\" SET FIELDS {merged_into: \"C-2\"}",
        "expect": {
          "error": "InvalidSyntax"
        },
        "vectors": [
          "MEM-007"
        ]
      },
      {
        "name": "portable numeric source rejects 9007199254740992",
        "command": "CREATE CONCEPT ?x { TYPE \"Person\" SET ATTRIBUTES {n: 9007199254740992} }",
        "expect": {
          "error": "InvalidSyntax"
        },
        "vectors": [
          "MEM-011"
        ]
      },
      {
        "name": "portable numeric source rejects 9007199254740993.0",
        "command": "CREATE CONCEPT ?x { TYPE \"Person\" SET ATTRIBUTES {n: 9007199254740993.0} }",
        "expect": {
          "error": "InvalidSyntax"
        },
        "vectors": [
          "MEM-011"
        ]
      },
      {
        "name": "portable numeric source rejects 9007199254740993e0",
        "command": "CREATE CONCEPT ?x { TYPE \"Person\" SET ATTRIBUTES {n: 9007199254740993e0} }",
        "expect": {
          "error": "InvalidSyntax"
        },
        "vectors": [
          "MEM-011"
        ]
      },
      {
        "name": "portable numeric source rejects 1e-400",
        "command": "CREATE CONCEPT ?x { TYPE \"Person\" SET ATTRIBUTES {n: 1e-400} }",
        "expect": {
          "error": "InvalidSyntax"
        },
        "vectors": [
          "MEM-011"
        ]
      },
      {
        "name": "unknown context is rejected without widening scope",
        "command": "FIND(?b.status) WHERE {?a CONCEPT {name:\"Ada\"} ?b BELIEF (?a,\"scoped\",\"work\")} WITH EPISTEMIC {context_refs:[\"C-999999\"]}",
        "expect": {
          "error": "NotFoundOrNotVisible"
        },
        "vectors": [
          "MEM-007"
        ]
      },
      {
        "name": "KIP strings reject unpaired Unicode escapes before execution",
        "command": "CREATE CONCEPT ?x {TYPE \"Person\" NAME \"\\ud800\"}",
        "expect": {
          "error": "InvalidSyntax"
        },
        "vectors": [
          "MEM-011"
        ]
      }
    ]
  },
  {
    "name": "consequence",
    "description": "The consequence channel: what the world did after the Brain acted, and what a Skill's standing is spent from. Outcome Evidence (Spec §15.7) carries an OutcomeRecord Facet — the graded index over an untouched payload — and cognition subscribes to a stream by task family rather than by reference. What an engine owes here is the Profile's schema discipline: the scoring handle a Skill cannot be compiled without, the four lifecycle states, a graded index its subject cannot rewrite, and the one guarded statement (Appendix F.6) a lifecycle verdict executes as. The verdict rule itself is Brain policy; that it lands as one recomputable transition is not.",
    "setup": [
      "MUTATE {\n  CREATE CONCEPT ?skill {\n    TYPE \"Skill\"\n    NAME \"Deploy behind a pre-flight migration check\"\n    SET ATTRIBUTES {\n      skill_class: \"workflow\",\n      summary: \"Dry-run the migration before the deploy\",\n      status: \"proposed\"\n    }\n    SET FACET \"MnemonicState\" {utility: 0.5}\n    SET STRUCTURAL {(\"current_revision\",?revision)}\n  }\n  CREATE CONCEPT ?revision {TYPE \"SkillRevision\" SET ATTRIBUTES {task_family:\"deploy/pre-flight\",procedure:\"dry-run migration before deploy\",behavior_digest:\"sha256:045d856aa6d929d266e7b68583ab860353254097da85f7f4969499ab535bb7b3\"} SET STRUCTURAL {(\"revision_of\",?skill)}}\n}",
      "MUTATE {\n  CREATE EVIDENCE ?win {\n    SET FIELDS {\n      evidence_class: \"outcome\",\n      payload: \"deploy 41: the pre-flight check caught the drift, rollout clean\",\n      media_type: \"text/plain\",\n      observed_at: \"2026-08-20T09:00:00.000Z\"\n    }\n    SET FACET \"OutcomeRecord\" {attempt_ref: null, metric: \"completion\", window: \"run\", terminal: true, observer_config_digest: \"sha256:0000000000000000000000000000000000000000000000000000000000000000\", observation_key: \"win\", task_family: \"deploy/pre-flight\", outcome_status: \"success\", magnitude: 0.8}\n  }\n  CREATE EVIDENCE ?loss {\n    SET FIELDS {\n      evidence_class: \"outcome\",\n      payload: \"deploy 42: pre-flight passed, rollout still failed on a stale replica\",\n      media_type: \"text/plain\",\n      observed_at: \"2026-08-21T09:00:00.000Z\"\n    }\n    SET FACET \"OutcomeRecord\" {attempt_ref: null, metric: \"completion\", window: \"run\", terminal: true, observer_config_digest: \"sha256:0000000000000000000000000000000000000000000000000000000000000000\", observation_key: \"loss\", task_family: \"deploy/pre-flight\", outcome_status: \"failure\"}\n  }\n  CREATE ACTIVITY ?observed {\n    SET FIELDS {activity_class: \"outcome_observation\", status: \"completed\"}\n    SET STRUCTURAL {\n      (\"outputs\", ?win)\n      (\"outputs\", ?loss)\n    }\n  }\n}"
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
        "name": "behavior belongs to a SkillRevision with a task_family",
        "command": "MUTATE {\n  CREATE CONCEPT ?s {\n    TYPE \"SkillRevision\"\n    NAME \"Be careful\"\n    SET ATTRIBUTES {skill_class: \"heuristic\", summary: \"Think first\", procedure: \"think\", status: \"proposed\"}\n  }\n}",
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
        "name": "an immutable OutcomeRecord cannot gain a retrospective measurement",
        "command": "MUTATE {\n  UPDATE \"E-2\"\n  SET FACET \"OutcomeRecord\" {magnitude: 0.25}\n}",
        "expect": {
          "error": "ImmutableField"
        }
      },
      {
        "name": "a second retrospective measurement is also refused",
        "command": "UPDATE \"E-2\" SET FACET \"OutcomeRecord\" {magnitude: 0.9}",
        "expect": {
          "error": "ImmutableField"
        }
      },
      {
        "name": "a verdict name and mutable tallies cannot establish validated learning",
        "command": "MUTATE {\n  CREATE ACTIVITY ?verdict {\n    SET FIELDS {\n      activity_class: \"lifecycle_verdict\",\n      status: \"completed\",\n      parameters_digest: \"sha3-256:ru1e\"\n    }\n    SET STRUCTURAL {\n      (\"inputs\", \"E-1\")\n      (\"inputs\", \"E-2\")\n      (\"outputs\", \"C-1\")\n    }\n  }\n  UPDATE \"C-1\"\n  SET ATTRIBUTES {status: \"trialed\"}\n  SET FACET \"GradingState\" {success_count: 1, failure_count: 1, graded_count: 2}\n  EXPECT VERSION 1\n}",
        "expect": {
          "error": "ConstraintViolation"
        }
      },
      {
        "name": "refused grading preserves unproven standing and independent utility",
        "command": "FIND(?s.attributes.status, ?s.facets[\"GradingState\"].graded_count, ?s.facets[\"MnemonicState\"].utility) WHERE { ?s CONCEPT {type: \"Skill\"} }",
        "expect": {
          "result": [
            [
              "proposed",
              null,
              0.5
            ]
          ]
        }
      },
      {
        "name": "an unvalidated adoption cannot bypass the learning contract",
        "command": "MUTATE {\n  UPDATE \"C-1\"\n  SET ATTRIBUTES {status: \"adopted\"}\n  EXPECT VERSION 1\n}",
        "expect": {
          "error": "ConstraintViolation"
        }
      },
      {
        "name": "a rejected grading transaction commits no verdict Activity",
        "command": "FIND(?v.parameters_digest, ?v.inputs) WHERE { ?v ACTIVITY {activity_class: \"lifecycle_verdict\"} }",
        "expect": {
          "result": []
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
            "proposed"
          ]
        }
      },
      {
        "name": "UPDATE cannot unset the required Skill class",
        "command": "UPDATE \"C-1\" UNSET ATTRIBUTES {skill_class}",
        "expect": {
          "error": "ConstraintViolation"
        }
      },
      {
        "name": "legacy behavior fields are not writable on a stable Skill",
        "command": "UPDATE \"C-1\" SET ATTRIBUTES {task_family: 7}",
        "expect": {
          "error": "ConstraintViolation"
        }
      },
      {
        "name": "conflicting removal and assignment cannot use clause order as a tie-break",
        "command": "MUTATE {\n  UPDATE \"C-1\"\n  UNSET ATTRIBUTES {summary}\n  SET ATTRIBUTES {summary: \"Dry-run the migration, then deploy\"}\n}",
        "expect": {
          "error": "DuplicateMutationTarget"
        }
      },
      {
        "name": "adoption cannot omit the lifecycle and cache version guards",
        "command": "MUTATE {\n  UPDATE \"C-1\"\n  SET ATTRIBUTES {status: \"adopted\"}\n}",
        "expect": {
          "error": "VersionConflict"
        }
      },
      {
        "name": "the Skill that came out the other side",
        "command": "FIND(?s.attributes.status, ?s.attributes.summary) WHERE { ?s CONCEPT {type: \"Skill\"} }",
        "expect": {
          "result": [
            [
              "proposed",
              "Dry-run the migration before the deploy"
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
      "MUTATE {\n  CREATE CONCEPT ?alice { TYPE \"Person\" NAME \"Alice\" }\n  CREATE CONCEPT ?dark { TYPE \"Option\" NAME \"Dark\" }\n  ENSURE PROPOSITION ?p (?alice, \"prefers\", ?dark)\n  CREATE EVIDENCE ?e {\n    SET FIELDS { evidence_class: \"user_statement\", payload: \"I prefer dark mode\" }\n  }\n  CREATE ASSERTION ?a {\n    SET FIELDS { proposition: ?p, asserted_by: ?alice, stance: \"support\", mode: \"stated\", confidence: 0.9 }\n    SET STRUCTURAL { (\"evidence\", ?e) { role: \"support\" } }\n  }\n}",
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
    ],
    "packages": [
      {
        "format": "KIP-Schema-Package",
        "manifest": {
          "package_id": "kip://conformance/options",
          "version": "1.0.0"
        },
        "definitions": {
          "concept_types": {
            "Option": {
              "kind": "ConceptType",
              "description": "A preference option used by these fixtures. A real Space types each option by its kind (Profile §5.5, §7), because prefers partitions by the option's Concept Type; one catch-all type is one partition."
            }
          }
        }
      }
    ]
  },
  {
    "name": "derivation",
    "description": "What a Space can find out about cognition it built on something else, and what byte destruction may and may not take with it. LIST DEPENDENTS walks provenance in the derived direction so a revised root's downstream artifacts can be reviewed instead of guessed at (Spec §57.5, §63.5); reachability is topology, not a verdict. PURGE PAYLOAD destroys Evidence bytes while the record, its digest, its citations and its provenance role survive (§60.6) — the data-minimization instrument, which is a different promise from element purge.",
    "setup": [
      "MUTATE {\n  CREATE CONCEPT ?event {\n    TYPE \"Event\"\n    NAME \"Migration meeting\"\n    SET ATTRIBUTES {summary: \"The team agreed to migrate on Friday\"}\n  }\n  CREATE CONCEPT ?insight {\n    TYPE \"Insight\"\n    NAME \"Migrations need a rollback plan\"\n    SET ATTRIBUTES {summary: \"Every migration ships with a rollback\"}\n  }\n  CREATE ACTIVITY ?consolidate {\n    SET FIELDS {activity_class: \"semantic_consolidation\", status: \"completed\"}\n    SET STRUCTURAL {\n      (\"inputs\", ?event)\n      (\"outputs\", ?insight)\n    }\n  }\n}",
      "MUTATE {\n  CREATE CONCEPT ?skill {\n    TYPE \"Insight\"\n    NAME \"Plan a migration\"\n    SET ATTRIBUTES {\n      summary: \"Write the rollback first\"\n    }\n  }\n  CREATE ACTIVITY ?compile {\n    SET FIELDS {activity_class: \"procedural_consolidation\", status: \"completed\"}\n    SET STRUCTURAL {\n      (\"inputs\", \"C-2\")\n      (\"outputs\", ?skill)\n    }\n  }\n}",
      "MUTATE {\n  CREATE CONCEPT ?alice { TYPE \"Person\" NAME \"Alice\" }\n  CREATE CONCEPT ?dark { TYPE \"Option\" NAME \"Dark\" }\n  ENSURE PROPOSITION ?p (?alice, \"prefers\", ?dark)\n  CREATE EVIDENCE ?e {\n    SET FIELDS {\n      evidence_class: \"user_statement\",\n      payload: \"I prefer dark mode, and my address is 12 Elm Street.\",\n      content_digest: \"sha3-256:d1ge5t\",\n      media_type: \"text/plain\",\n      observed_at: \"2026-08-16T09:00:00.000Z\"\n    }\n  }\n  CREATE ASSERTION ?a {\n    SET FIELDS { proposition: ?p, asserted_by: ?alice, stance: \"support\", mode: \"stated\", confidence: 0.9 }\n    SET STRUCTURAL { (\"evidence\", ?e) {role: \"support\"} }\n  }\n}"
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
    ],
    "packages": [
      {
        "format": "KIP-Schema-Package",
        "manifest": {
          "package_id": "kip://conformance/options",
          "version": "1.0.0"
        },
        "definitions": {
          "concept_types": {
            "Option": {
              "kind": "ConceptType",
              "description": "A preference option used by these fixtures. A real Space types each option by its kind (Profile §5.5, §7), because prefers partitions by the option's Concept Type; one catch-all type is one partition."
            }
          }
        }
      }
    ]
  },
  {
    "name": "draft-vocabulary",
    "description": "The Space's draft vocabulary (§20.16), for an engine that advertises draft_vocabulary. DEFINE adds a Predicate or a Concept Type at the fixed reference kip://local/draft@0.0.0, and the symbol resolves for the next operation: it is listed and described with its package, elements persist its exact reference, and a draft Predicate projects like any other. DEFINE only adds — a name of the same kind that already resolves fails SchemaSymbolConflict — never runs inside MUTATE, needs a description, and never claims authority over the data: no closed world, no exclusive-value completeness, no required attribute or Facet, no member outside its definition. Parameters bind before the checks. Every case requires draft_vocabulary through the request envelope (§71), so an engine without it skips the whole chain.",
    "setup": [
      "MUTATE {\n  CREATE CONCEPT ?ada { TYPE \"Person\" NAME \"Ada\" }\n  CREATE CONCEPT ?grace { TYPE \"Person\" NAME \"Grace\" }\n}",
      {
        "command": "FIND(?ada.id) WHERE { ?ada CONCEPT {name: \"Ada\"} }",
        "capture": {
          "ada": "/0"
        }
      }
    ],
    "cases": [
      {
        "name": "DEFINE adds a Predicate to the draft vocabulary",
        "command": "DEFINE PREDICATE \"mentors\" {\n  description: \"The subject mentors the object.\",\n  subject: {concept_types: [\"Person\"]},\n  object: {concept_types: [\"Person\"]}\n}",
        "envelope": {
          "requires": {
            "draft_vocabulary": true
          }
        },
        "expect": {
          "result_contains": {
            "ref": "kip://local/draft@0.0.0/mentors"
          }
        },
        "vectors": [
          "SCHEMA-022",
          "GOV-031"
        ]
      },
      {
        "name": "the draft symbol resolves for the next operation",
        "command": "FIND(?p) WHERE { ?p (?x, \"mentors\", ?y) }",
        "envelope": {
          "requires": {
            "draft_vocabulary": true
          }
        },
        "expect": {
          "result": []
        },
        "vectors": [
          "SCHEMA-022"
        ]
      },
      {
        "name": "the draft Predicate is listed under the draft package",
        "command": "LIST PREDICATES",
        "envelope": {
          "requires": {
            "draft_vocabulary": true
          }
        },
        "expect": {
          "result_contains": [
            {
              "ref": "kip://local/draft@0.0.0/mentors",
              "local_name": "mentors",
              "package_ref": "kip://local/draft@0.0.0",
              "status": "active"
            }
          ]
        },
        "vectors": [
          "SCHEMA-022"
        ]
      },
      {
        "name": "the draft package is an active package of the Space",
        "command": "LIST SCHEMA PACKAGES",
        "envelope": {
          "requires": {
            "draft_vocabulary": true
          }
        },
        "expect": {
          "result_contains": [
            {
              "package_ref": "kip://local/draft@0.0.0",
              "package_id": "kip://local/draft",
              "version": "0.0.0",
              "status": "active"
            }
          ]
        },
        "vectors": [
          "SCHEMA-022"
        ]
      },
      {
        "name": "DESCRIBE names the exact draft reference",
        "command": "DESCRIBE PREDICATE \"mentors\"",
        "envelope": {
          "requires": {
            "draft_vocabulary": true
          }
        },
        "expect": {
          "result_contains": {
            "ref": "kip://local/draft@0.0.0/mentors",
            "local_name": "mentors",
            "package_ref": "kip://local/draft@0.0.0",
            "definition": {
              "description": "The subject mentors the object."
            }
          }
        },
        "vectors": [
          "SCHEMA-022"
        ]
      },
      {
        "name": "DEFINE adds a Concept Type",
        "command": "DEFINE CONCEPT TYPE \"Instrument\" {\n  description: \"A musical instrument.\",\n  attributes: {open: true, fields: {family: {type: \"string\", description: \"strings, brass, percussion ...\"}}}\n}",
        "envelope": {
          "requires": {
            "draft_vocabulary": true
          }
        },
        "expect": {
          "result_contains": {
            "ref": "kip://local/draft@0.0.0/Instrument"
          }
        },
        "vectors": [
          "SCHEMA-022"
        ]
      },
      {
        "name": "an element of a draft type is written at once",
        "command": "MUTATE {\n  CREATE CONCEPT ?violin { TYPE \"Instrument\" NAME \"Violin\" SET ATTRIBUTES {family: \"strings\"} }\n}",
        "envelope": {
          "requires": {
            "draft_vocabulary": true
          }
        },
        "expect": {},
        "vectors": [
          "SCHEMA-022"
        ]
      },
      {
        "name": "the element persists the exact draft reference",
        "command": "FIND(?c.schema_ref) WHERE { ?c CONCEPT {type: \"Instrument\"} }",
        "envelope": {
          "requires": {
            "draft_vocabulary": true
          }
        },
        "expect": {
          "result": [
            "kip://local/draft@0.0.0/Instrument"
          ]
        },
        "vectors": [
          "SCHEMA-022"
        ]
      },
      {
        "name": "a draft Predicate can name a draft Concept Type",
        "command": "DEFINE PREDICATE \"main_instrument\" {\n  description: \"The instrument the subject mainly plays.\",\n  subject: {concept_types: [\"Person\"]},\n  object: {concept_types: [\"Instrument\"]},\n  functional: true\n}",
        "envelope": {
          "requires": {
            "draft_vocabulary": true
          }
        },
        "expect": {
          "result_contains": {
            "ref": "kip://local/draft@0.0.0/main_instrument"
          }
        },
        "vectors": [
          "SCHEMA-022"
        ]
      },
      {
        "name": "a claim over a draft Predicate is an ordinary Assertion",
        "command": "MUTATE {\n  CREATE CONCEPT ?cello { TYPE \"Instrument\" NAME \"Cello\" }\n  ASSERT (:ada, \"main_instrument\", ?cello) {\n    by: :ada, mode: \"stated\", at: \"2026-09-01T00:00:00.000Z\"\n  }\n}",
        "envelope": {
          "requires": {
            "draft_vocabulary": true
          }
        },
        "expect": {},
        "vectors": [
          "SCHEMA-022"
        ]
      },
      {
        "name": "and projects like any other",
        "command": "FIND(?b.status) WHERE { ?s CONCEPT {name: \"Ada\"} ?o CONCEPT {name: \"Cello\"} ?b BELIEF (?s, \"main_instrument\", ?o) } FOR TIME \"2026-09-20T00:00:00.000Z\"",
        "envelope": {
          "requires": {
            "draft_vocabulary": true
          }
        },
        "expect": {
          "result": [
            "accepted"
          ]
        },
        "vectors": [
          "SCHEMA-022"
        ]
      },
      {
        "name": "a name that already resolves fails SchemaSymbolConflict",
        "command": "DEFINE PREDICATE \"prefers\" {\n  description: \"already a Profile Predicate\"\n}",
        "envelope": {
          "requires": {
            "draft_vocabulary": true
          }
        },
        "expect": {
          "error": "SchemaSymbolConflict"
        },
        "vectors": [
          "SCHEMA-022"
        ]
      },
      {
        "name": "a draft symbol is defined once, even identically",
        "command": "DEFINE PREDICATE \"mentors\" {\n  description: \"The subject mentors the object.\",\n  subject: {concept_types: [\"Person\"]},\n  object: {concept_types: [\"Person\"]}\n}",
        "envelope": {
          "requires": {
            "draft_vocabulary": true
          }
        },
        "expect": {
          "error": "SchemaSymbolConflict"
        },
        "vectors": [
          "SCHEMA-022"
        ]
      },
      {
        "name": "a Concept Type an installed package names conflicts",
        "command": "DEFINE CONCEPT TYPE \"Person\" {\n  description: \"another person\"\n}",
        "envelope": {
          "requires": {
            "draft_vocabulary": true
          }
        },
        "expect": {
          "error": "SchemaSymbolConflict"
        },
        "vectors": [
          "SCHEMA-022"
        ]
      },
      {
        "name": "a draft Predicate cannot claim a closed world",
        "command": "DEFINE PREDICATE \"closed_relation\" {\n  description: \"authority over the data belongs to installed packages\",\n  open_world: false\n}",
        "envelope": {
          "requires": {
            "draft_vocabulary": true
          }
        },
        "expect": {
          "error": "ConstraintViolation"
        },
        "vectors": [
          "SCHEMA-022"
        ]
      },
      {
        "name": "a draft Predicate cannot claim exclusive-value completeness",
        "command": "DEFINE PREDICATE \"exclusive_relation\" {\n  description: \"one value excludes the others\",\n  functional: true,\n  complete: true\n}",
        "envelope": {
          "requires": {
            "draft_vocabulary": true
          }
        },
        "expect": {
          "error": "ConstraintViolation"
        },
        "vectors": [
          "SCHEMA-022"
        ]
      },
      {
        "name": "a definition needs a description",
        "command": "DEFINE PREDICATE \"unexplained\" {\n  subject: {concept_types: [\"Person\"]}\n}",
        "envelope": {
          "requires": {
            "draft_vocabulary": true
          }
        },
        "expect": {
          "error": "ConstraintViolation"
        },
        "vectors": [
          "SCHEMA-022"
        ]
      },
      {
        "name": "a member outside the definition is refused",
        "command": "DEFINE PREDICATE \"tagged\" {\n  description: \"carries an unknown member\",\n  cardinality: 3\n}",
        "envelope": {
          "requires": {
            "draft_vocabulary": true
          }
        },
        "expect": {
          "error": "ConstraintViolation"
        },
        "vectors": [
          "SCHEMA-022"
        ]
      },
      {
        "name": "a draft Concept Type declares no required attribute",
        "command": "DEFINE CONCEPT TYPE \"Genre\" {\n  description: \"A musical genre.\",\n  attributes: {fields: {label: {type: \"string\", required: true}}}\n}",
        "envelope": {
          "requires": {
            "draft_vocabulary": true
          }
        },
        "expect": {
          "error": "ConstraintViolation"
        },
        "vectors": [
          "SCHEMA-022"
        ]
      },
      {
        "name": "a draft Concept Type declares no Facets",
        "command": "DEFINE CONCEPT TYPE \"Venue\" {\n  description: \"A concert venue.\",\n  facets: {MnemonicState: {}}\n}",
        "envelope": {
          "requires": {
            "draft_vocabulary": true
          }
        },
        "expect": {
          "error": "ConstraintViolation"
        },
        "vectors": [
          "SCHEMA-022"
        ]
      },
      {
        "name": "DEFINE is never a clause of MUTATE",
        "command": "MUTATE {\n  DEFINE PREDICATE \"inside\" {description: \"not a clause\"}\n}",
        "envelope": {
          "requires": {
            "draft_vocabulary": true
          }
        },
        "expect": {
          "error": "InvalidSyntax"
        },
        "vectors": [
          "SCHEMA-022"
        ]
      },
      {
        "name": "a parameterized definition binds before it is checked",
        "command": "DEFINE PREDICATE :name {description: :description}",
        "params": {
          "name": "admires",
          "description": "The subject admires the object."
        },
        "envelope": {
          "requires": {
            "draft_vocabulary": true
          }
        },
        "expect": {
          "result_contains": {
            "ref": "kip://local/draft@0.0.0/admires"
          }
        },
        "vectors": [
          "SCHEMA-022"
        ]
      },
      {
        "name": "a parameter cannot smuggle in a closed world",
        "command": "DEFINE PREDICATE \"closed_by_parameter\" {description: \"bound later\", open_world: :open_world}",
        "params": {
          "open_world": false
        },
        "envelope": {
          "requires": {
            "draft_vocabulary": true
          }
        },
        "expect": {
          "error": "ConstraintViolation"
        },
        "vectors": [
          "SCHEMA-022"
        ]
      },
      {
        "name": "a Predicate and a Concept Type may share a name and exact reference",
        "command": "DEFINE PREDICATE \"Instrument\" {description: \"The subject uses the object as an instrument.\"}",
        "envelope": {
          "requires": {
            "draft_vocabulary": true
          }
        },
        "expect": {
          "result_contains": {
            "ref": "kip://local/draft@0.0.0/Instrument"
          }
        },
        "vectors": [
          "SCHEMA-022"
        ]
      },
      {
        "name": "same-named symbols queue separate reviews by kind and exact reference",
        "command": "MUTATE {\n  CREATE CONCEPT ?type_review {\n    TYPE \"SleepTask\" NAME \"Review ConceptType Instrument\"\n    CLIENT KEY \"review_schema:ConceptType:kip://local/draft@0.0.0/Instrument\"\n    SET ATTRIBUTES {task_class: \"review_schema\", summary: \"ConceptType kip://local/draft@0.0.0/Instrument\", status: \"pending\"}\n  }\n  CREATE CONCEPT ?predicate_review {\n    TYPE \"SleepTask\" NAME \"Review PredicateType Instrument\"\n    CLIENT KEY \"review_schema:PredicateType:kip://local/draft@0.0.0/Instrument\"\n    SET ATTRIBUTES {task_class: \"review_schema\", summary: \"PredicateType kip://local/draft@0.0.0/Instrument\", status: \"pending\"}\n  }\n}",
        "envelope": {
          "requires": {
            "draft_vocabulary": true
          }
        },
        "expect": {},
        "vectors": [
          "SCHEMA-022"
        ]
      },
      {
        "name": "retrying both reviews replays each creation independently",
        "command": "MUTATE {\n  CREATE CONCEPT ?type_review {\n    TYPE \"SleepTask\" NAME \"Review ConceptType Instrument\"\n    CLIENT KEY \"review_schema:ConceptType:kip://local/draft@0.0.0/Instrument\"\n    SET ATTRIBUTES {task_class: \"review_schema\", summary: \"ConceptType kip://local/draft@0.0.0/Instrument\", status: \"pending\"}\n  }\n  CREATE CONCEPT ?predicate_review {\n    TYPE \"SleepTask\" NAME \"Review PredicateType Instrument\"\n    CLIENT KEY \"review_schema:PredicateType:kip://local/draft@0.0.0/Instrument\"\n    SET ATTRIBUTES {task_class: \"review_schema\", summary: \"PredicateType kip://local/draft@0.0.0/Instrument\", status: \"pending\"}\n  }\n}",
        "envelope": {
          "requires": {
            "draft_vocabulary": true
          }
        },
        "expect": {},
        "vectors": [
          "SCHEMA-022"
        ]
      },
      {
        "name": "two reviews remain after retry, each identifying its symbol kind",
        "command": "FIND(?task.attributes.summary) WHERE { ?task CONCEPT {type: \"SleepTask\"} FILTER(?task.attributes.task_class == \"review_schema\") }",
        "envelope": {
          "requires": {
            "draft_vocabulary": true
          }
        },
        "expect": {
          "result": [
            "ConceptType kip://local/draft@0.0.0/Instrument",
            "PredicateType kip://local/draft@0.0.0/Instrument"
          ]
        },
        "vectors": [
          "SCHEMA-022"
        ]
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
      },
      {
        "format": "KIP-Schema-Package",
        "manifest": {
          "package_id": "kip://conformance/options",
          "version": "1.0.0"
        },
        "definitions": {
          "concept_types": {
            "Option": {
              "kind": "ConceptType",
              "description": "A preference option used by these fixtures. A real Space types each option by its kind (Profile §5.5, §7), because prefers partitions by the option's Concept Type; one catch-all type is one partition."
            }
          }
        }
      }
    ],
    "setup": [
      "MUTATE {\n  CREATE CONCEPT ?alice { TYPE \"Person\" NAME \"Alice\" }\n  CREATE CONCEPT ?bob { TYPE \"Person\" NAME \"Bob\" }\n  CREATE CONCEPT ?carol { TYPE \"Person\" NAME \"Carol\" }\n  CREATE CONCEPT ?quiet { TYPE \"Option\" NAME \"Quiet\" }\n  CREATE CONCEPT ?loud { TYPE \"Option\" NAME \"Loud\" }\n  ENSURE PROPOSITION ?unspoken (?alice, \"prefers\", ?quiet)\n  ENSURE PROPOSITION ?repeated (?alice, \"prefers\", ?loud)\n}",
      "MUTATE {\n  CREATE CONCEPT ?svc { TYPE \"Service\" NAME \"api\" }\n  CREATE CONCEPT ?healthy { TYPE \"Status\" NAME \"healthy\" }\n  CREATE CONCEPT ?degraded { TYPE \"Status\" NAME \"degraded\" }\n  ENSURE PROPOSITION ?ok (?svc, \"status\", ?healthy)\n  ENSURE PROPOSITION ?bad (?svc, \"status\", ?degraded)\n}",
      "MUTATE {\n  CREATE CONCEPT ?dave { TYPE \"Person\" NAME \"Dave\" }\n  CREATE CONCEPT ?warm { TYPE \"Option\" NAME \"Warm\" }\n  ENSURE PROPOSITION ?p (?dave, \"prefers\", ?warm)\n  CREATE EVIDENCE ?seen { SET FIELDS { evidence_class: \"observation\", payload: \"one observation\" } }\n  CREATE ASSERTION ?a1 {\n    SET FIELDS { proposition: ?p, asserted_by: ?dave, stance: \"support\", mode: \"stated\", confidence: 0.6 }\n    SET STRUCTURAL { (\"evidence\", ?seen) { role: \"support\" } }\n  }\n  CREATE ASSERTION ?a2 {\n    SET FIELDS { proposition: ?p, asserted_by: ?dave, stance: \"support\", mode: \"stated\", confidence: 0.6 }\n    SET STRUCTURAL { (\"evidence\", ?seen) { role: \"support\" } }\n  }\n}"
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
        "command": "FIND(?b.basis.policy.id) WHERE {\n  ?s CONCEPT {name: \"Alice\"}\n  ?o CONCEPT {name: \"Quiet\"}\n  ?p PROPOSITION (?s, \"prefers\", ?o)\n  ?b BELIEF (?p)\n}\nWITH EPISTEMIC {policy: \"kip:memory-default\"}",
        "expect": {
          "result": [
            "kip:memory-default"
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
      "MUTATE {\n  CREATE CONCEPT ?alice { TYPE \"Person\" NAME \"Alice\" SET FIELDS {key: \"person:alice\"} }\n  CREATE CONCEPT ?dark { TYPE \"Option\" NAME \"Dark\" }\n  CREATE CONCEPT ?admin { TYPE \"Person\" NAME \"Administrator\" SET ATTRIBUTES {authority: \"executable\", trust: 1.0} }\n  CREATE EVIDENCE ?secret { SET FIELDS {evidence_class: \"Document\", payload: \"an observation\"} }\n  ENSURE PROPOSITION ?p (?alice, \"prefers\", ?dark)\n  CREATE ASSERTION ?a {\n    SET FIELDS { proposition: ?p, asserted_by: ?alice, stance: \"support\", mode: \"inferred\", confidence: 0.6 }\n    SET STRUCTURAL { (\"evidence\", ?secret) {role: \"support\"} }\n  }\n}"
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
    ],
    "packages": [
      {
        "format": "KIP-Schema-Package",
        "manifest": {
          "package_id": "kip://conformance/options",
          "version": "1.0.0"
        },
        "definitions": {
          "concept_types": {
            "Option": {
              "kind": "ConceptType",
              "description": "A preference option used by these fixtures. A real Space types each option by its kind (Profile §5.5, §7), because prefers partitions by the option's Concept Type; one catch-all type is one partition."
            }
          }
        }
      }
    ]
  },
  {
    "name": "history",
    "description": "Two independent time axes. FOR TIME asks what was true then; AS OF asks what this Brain held then. A coordinate keeps what was later corrected, retracted or archived, because the record of what was once believed is the point. And the chronology itself is reported in transition envelopes (§36.1): §68.1 defines HISTORY as transition chronology and §36.2 defines a transition as one envelope, so HISTORY ELEMENT, HISTORY SPACE and CHANGES are the same unit asked for over different ranges — which is what lets a consumer deduplicate on space_id + space_seq + tx_id (§36.3). Each entry of `changes` is the normative shape of schemas/kip-change-envelope.schema.json: op, kind, id, new_version, old_version where the element existed, state {from, to} for a lifecycle move, refs.proposition on an Assertion entry — names and versions, never values.",
    "setup": [
      "MUTATE {\n  CREATE CONCEPT ?alice { TYPE \"Person\" NAME \"Alice\" }\n  CREATE CONCEPT ?dark { TYPE \"Option\" NAME \"Dark\" }\n  ENSURE PROPOSITION ?p (?alice, \"prefers\", ?dark)\n  CREATE ASSERTION ?a {\n    SET FIELDS { proposition: ?p, asserted_by: ?alice, stance: \"support\", mode: \"stated\", confidence: 0.9 }\n  }\n}"
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
              },
              "coverage": {
                "through_seq": 2,
                "complete": true
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
    ],
    "packages": [
      {
        "format": "KIP-Schema-Package",
        "manifest": {
          "package_id": "kip://conformance/options",
          "version": "1.0.0"
        },
        "definitions": {
          "concept_types": {
            "Option": {
              "kind": "ConceptType",
              "description": "A preference option used by these fixtures. A real Space types each option by its kind (Profile §5.5, §7), because prefers partitions by the option's Concept Type; one catch-all type is one partition."
            }
          }
        }
      }
    ]
  },
  {
    "name": "lifecycle",
    "description": "Nothing is rewritten and nothing is erased. An Assertion's epistemic payload is immutable, so correcting a claim records a new one and supersedes the old; an element that leaves ordinary recall keeps resolving as a reference.",
    "setup": [
      "MUTATE {\n  CREATE CONCEPT ?alice { TYPE \"Person\" NAME \"Alice\" }\n  CREATE CONCEPT ?dark { TYPE \"Option\" NAME \"Dark\" }\n  ENSURE PROPOSITION ?p (?alice, \"prefers\", ?dark)\n  CREATE ASSERTION ?old {\n    SET FIELDS { proposition: ?p, asserted_by: ?alice, stance: \"support\", mode: \"stated\", confidence: 0.9 }\n  }\n}"
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
          "error": "ReferenceError"
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
    ],
    "packages": [
      {
        "format": "KIP-Schema-Package",
        "manifest": {
          "package_id": "kip://conformance/options",
          "version": "1.0.0"
        },
        "definitions": {
          "concept_types": {
            "Option": {
              "kind": "ConceptType",
              "description": "A preference option used by these fixtures. A real Space types each option by its kind (Profile §5.5, §7), because prefers partitions by the option's Concept Type; one catch-all type is one partition."
            }
          }
        }
      }
    ]
  },
  {
    "name": "merge-identity",
    "description": "Non-destructive identity consolidation (§11). The merged-away Concept stays addressable and keeps forwarding; the history that referenced it keeps referencing it (§11.2); ordinary new writes land on the identity that survived (§11.3); and no merge may make canonical resolution — following `merged_into` to its fixpoint — cycle (§11.1).",
    "setup": [
      "MUTATE {\n  CREATE CONCEPT ?a { TYPE \"Person\" NAME \"Al\" SET FIELDS {key: \"al\"} }\n  CREATE CONCEPT ?b { TYPE \"Person\" NAME \"Alice\" SET FIELDS {key: \"alice\"} }\n  CREATE CONCEPT ?dark { TYPE \"Option\" NAME \"Dark\" SET FIELDS {key: \"dark\"} }\n  ENSURE PROPOSITION ?old (?a, \"prefers\", ?dark)\n}",
      "MERGE CONCEPT ?source INTO ?target WHERE {\n  ?source CONCEPT {key: \"al\"}\n  ?target CONCEPT {key: \"alice\"}\n}",
      "MUTATE {\n  UPSERT CONCEPT ?a { MATCH {type: \"Person\", key: \"al\"} }\n  UPSERT CONCEPT ?dark { MATCH {type: \"Option\", key: \"dark\"} }\n  ENSURE PROPOSITION ?new (?a, \"prefers\", ?dark)\n}",
      "MUTATE {\n  UPSERT CONCEPT ?a { MATCH {type: \"Person\", key: \"al\"} }\n  UPSERT CONCEPT ?dark { MATCH {type: \"Option\", key: \"dark\"} }\n  ENSURE PROPOSITION ?again (?a, \"prefers\", ?dark)\n}"
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
        "command": "FIND(?raw.key) WHERE { ?p PROPOSITION (?s, \"prefers\", ?o) ?raw CONCEPT {id: ?stored, state: ?state} FILTER(?stored == ?p.subject.id) }",
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
        "name": "merging a Concept into itself has no effect",
        "command": "MERGE CONCEPT ?source INTO ?target WHERE {\n  ?source CONCEPT {key: \"alice\"}\n  ?target CONCEPT {key: \"alice\"}\n}",
        "expect": {
          "result": null
        }
      }
    ],
    "packages": [
      {
        "format": "KIP-Schema-Package",
        "manifest": {
          "package_id": "kip://conformance/options",
          "version": "1.0.0"
        },
        "definitions": {
          "concept_types": {
            "Option": {
              "kind": "ConceptType",
              "description": "A preference option used by these fixtures. A real Space types each option by its kind (Profile §5.5, §7), because prefers partitions by the option's Concept Type; one catch-all type is one partition."
            }
          }
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
              "ref": "kip://profiles/cognitive-memory@2.0.0/AttemptRecord",
              "local_name": "AttemptRecord",
              "package_ref": "kip://profiles/cognitive-memory@2.0.0",
              "status": "active"
            },
            {
              "ref": "kip://profiles/cognitive-memory@2.0.0/CompressionRecord",
              "local_name": "CompressionRecord",
              "package_ref": "kip://profiles/cognitive-memory@2.0.0",
              "status": "active"
            },
            {
              "ref": "kip://profiles/cognitive-memory@2.0.0/DecisionRecord",
              "local_name": "DecisionRecord",
              "package_ref": "kip://profiles/cognitive-memory@2.0.0",
              "status": "active"
            },
            {
              "ref": "kip://profiles/cognitive-memory@2.0.0/DependencyBasis",
              "local_name": "DependencyBasis",
              "package_ref": "kip://profiles/cognitive-memory@2.0.0",
              "status": "active"
            },
            {
              "ref": "kip://profiles/cognitive-memory@2.0.0/EvaluationRecord",
              "local_name": "EvaluationRecord",
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
              "ref": "kip://profiles/cognitive-memory@2.0.0/LeaseState",
              "local_name": "LeaseState",
              "package_ref": "kip://profiles/cognitive-memory@2.0.0",
              "status": "active"
            },
            {
              "ref": "kip://profiles/cognitive-memory@2.0.0/MemoryScope",
              "local_name": "MemoryScope",
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
              "ref": "kip://profiles/cognitive-memory@2.0.0/ProcedureAssessment",
              "local_name": "ProcedureAssessment",
              "package_ref": "kip://profiles/cognitive-memory@2.0.0",
              "status": "active"
            },
            {
              "ref": "kip://profiles/cognitive-memory@2.0.0/RecallCoverage",
              "local_name": "RecallCoverage",
              "package_ref": "kip://profiles/cognitive-memory@2.0.0",
              "status": "active"
            },
            {
              "ref": "kip://profiles/cognitive-memory@2.0.0/RecordingRepair",
              "local_name": "RecordingRepair",
              "package_ref": "kip://profiles/cognitive-memory@2.0.0",
              "status": "active"
            },
            {
              "ref": "kip://profiles/cognitive-memory@2.0.0/RestoreReport",
              "local_name": "RestoreReport",
              "package_ref": "kip://profiles/cognitive-memory@2.0.0",
              "status": "active"
            },
            {
              "ref": "kip://profiles/cognitive-memory@2.0.0/TrialRecord",
              "local_name": "TrialRecord",
              "package_ref": "kip://profiles/cognitive-memory@2.0.0",
              "status": "active"
            },
            {
              "ref": "kip://profiles/cognitive-memory@2.0.0/WatchState",
              "local_name": "WatchState",
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
        "name": "policy introspection includes the standard memory policy and permits deployment policies",
        "command": "LIST EPISTEMIC POLICIES",
        "ordered": false,
        "expect": {
          "result_contains": [
            {
              "id": "kip:memory-default"
            }
          ]
        }
      },
      {
        "name": "the standard memory policy can be described without engine-specific thresholds",
        "command": "DESCRIBE EPISTEMIC POLICY \"kip:memory-default\"",
        "expect": {
          "result_contains": {
            "id": "kip:memory-default"
          }
        }
      }
    ]
  },
  {
    "name": "mnemonic-strength",
    "description": "Decay is computed, not written (§59.1, Profile §6.1): effective_strength is derived at read time from the base memory_strength, its anchor last_metabolized_at and the pinned strength_policy — here the standard kip:strength-half-life-30d. A missing base, anchor or pin, or a pin whose digest does not match, leaves it null, never a default; before its anchor it is the base; far past its anchor it has decayed. Reading never writes it back, and writing it fails like any computed member (§18.2). Results that depend on the read's wall-clock instant are pinned only by bounds far from any test date.",
    "setup": [
      {
        "command": "MUTATE {\n  CREATE CONCEPT ?old { TYPE \"Person\" NAME \"Old\" SET FACET \"MnemonicState\" {memory_strength: 0.8, last_metabolized_at: \"2000-01-01T00:00:00.000Z\", strength_policy: :policy} }\n  CREATE CONCEPT ?ahead { TYPE \"Person\" NAME \"Ahead\" SET FACET \"MnemonicState\" {memory_strength: 0.8, last_metabolized_at: \"2999-01-01T00:00:00.000Z\", strength_policy: :policy} }\n  CREATE CONCEPT ?unpinned { TYPE \"Person\" NAME \"Unpinned\" SET FACET \"MnemonicState\" {memory_strength: 0.8, last_metabolized_at: \"2026-01-01T00:00:00.000Z\"} }\n  CREATE CONCEPT ?unanchored { TYPE \"Person\" NAME \"Unanchored\" SET FACET \"MnemonicState\" {memory_strength: 0.8, strength_policy: :policy} }\n  CREATE CONCEPT ?mismatched { TYPE \"Person\" NAME \"Mismatched\" SET FACET \"MnemonicState\" {memory_strength: 0.8, last_metabolized_at: \"2026-01-01T00:00:00.000Z\", strength_policy: :mismatched_policy} }\n}",
        "params": {
          "policy": {
            "artifact_ref": "kip:strength-half-life-30d",
            "content_digest": "sha256:a50a89b83f937c97cabf0f8371cccfd326f4fdd438b6d7b9ead507d77927b227"
          },
          "mismatched_policy": {
            "artifact_ref": "kip:strength-half-life-30d",
            "content_digest": "sha256:0000000000000000000000000000000000000000000000000000000000000001"
          }
        }
      }
    ],
    "cases": [
      {
        "name": "no pinned policy leaves effective strength unknown, never a default",
        "command": "FIND(?c.facets[\"MnemonicState\"].effective_strength) WHERE { ?c CONCEPT {name: \"Unpinned\"} }",
        "expect": {
          "result": [
            null
          ]
        },
        "vectors": [
          "MEM-030"
        ]
      },
      {
        "name": "no anchor leaves it unknown",
        "command": "FIND(?c.facets[\"MnemonicState\"].effective_strength) WHERE { ?c CONCEPT {name: \"Unanchored\"} }",
        "expect": {
          "result": [
            null
          ]
        },
        "vectors": [
          "MEM-030"
        ]
      },
      {
        "name": "a pin whose digest does not match leaves it unknown",
        "command": "FIND(?c.facets[\"MnemonicState\"].effective_strength) WHERE { ?c CONCEPT {name: \"Mismatched\"} }",
        "expect": {
          "result": [
            null
          ]
        },
        "vectors": [
          "MEM-030"
        ]
      },
      {
        "name": "before its anchor the effective strength is the base",
        "command": "FIND(?c.facets[\"MnemonicState\"].effective_strength) WHERE { ?c CONCEPT {name: \"Ahead\"} }",
        "expect": {
          "result": [
            0.8
          ]
        },
        "vectors": [
          "MEM-030"
        ]
      },
      {
        "name": "far past its anchor the half-life has decayed it",
        "command": "FIND(?c.name) WHERE { ?c CONCEPT {name: \"Old\"} FILTER(?c.facets[\"MnemonicState\"].effective_strength < 0.001) }",
        "expect": {
          "result": [
            "Old"
          ]
        },
        "vectors": [
          "MEM-030"
        ]
      },
      {
        "name": "a read never writes strength back",
        "command": "FIND(?c.facets[\"MnemonicState\"].memory_strength, ?c.facets[\"MnemonicState\"].last_metabolized_at) WHERE { ?c CONCEPT {name: \"Old\"} }",
        "expect": {
          "result": [
            [
              0.8,
              "2000-01-01T00:00:00.000Z"
            ]
          ]
        },
        "vectors": [
          "MEM-030"
        ]
      },
      {
        "name": "effective strength is computed, never written",
        "command": "UPDATE ?c SET FACET \"MnemonicState\" { effective_strength: 0.9 } WHERE { ?c CONCEPT {name: \"Old\"} }",
        "expect": {
          "error": "ConstraintViolation"
        },
        "vectors": [
          "MEM-030"
        ]
      }
    ]
  },
  {
    "name": "mutation-selection",
    "description": "A mutation may choose what it acts on. The judgement calls an engine has to make here are what this fixture pins down: UPDATE reaches mutable state and nothing else, a bounded sweep takes a documented order, a selection block reads the transaction's starting state, and a merge consolidates identity without copying or erasing anything.",
    "setup": [
      "MUTATE {\n  CREATE CONCEPT ?alice { TYPE \"Person\" NAME \"Alice\" SET FIELDS {key: \"person:alice\"} }\n  CREATE CONCEPT ?dark { TYPE \"Option\" NAME \"Dark\" }\n  CREATE CONCEPT ?e1 { TYPE \"Experience\" NAME \"First\" SET ATTRIBUTES {goal: \"learn\", outcome_status: \"success\"} SET FACET \"MnemonicState\" {memory_strength: 0.8, salience: 0.5} }\n  CREATE CONCEPT ?e2 { TYPE \"Experience\" NAME \"Second\" SET ATTRIBUTES {goal: \"learn\", outcome_status: \"failure\"} SET FACET \"MnemonicState\" {memory_strength: 0.4, salience: 0.5} }\n  CREATE CONCEPT ?e3 { TYPE \"Experience\" NAME \"Third\" SET ATTRIBUTES {goal: \"rest\", outcome_status: \"success\"} SET FACET \"MnemonicState\" {memory_strength: 0.2, salience: 0.5} }\n  ENSURE PROPOSITION ?p (?alice, \"prefers\", ?dark)\n  CREATE ASSERTION ?a {\n    SET FIELDS { proposition: ?p, asserted_by: ?alice, stance: \"support\", mode: \"stated\", confidence: 0.9 }\n  }\n}"
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
          "error": "IdentityMergeConflict"
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
        "command": "UPSERT CONCEPT ?b { MATCH {type: \"Option\", key: \"shared:label\"} SET FIELDS {name: \"A preference\"} }",
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
    ],
    "packages": [
      {
        "format": "KIP-Schema-Package",
        "manifest": {
          "package_id": "kip://conformance/options",
          "version": "1.0.0"
        },
        "definitions": {
          "concept_types": {
            "Option": {
              "kind": "ConceptType",
              "description": "A preference option used by these fixtures. A real Space types each option by its kind (Profile §5.5, §7), because prefers partitions by the option's Concept Type; one catch-all type is one partition."
            }
          }
        }
      }
    ]
  },
  {
    "name": "query-scope-contracts",
    "description": "KIP 2.0 §§42.4–45: correlated scopes, independent union, complete-solution identity, null logic and empty aggregates, exercised identically by both engines.",
    "setup": [
      "MUTATE { CREATE CONCEPT ?alice {TYPE \"Person\" NAME \"Alice\"} CREATE CONCEPT ?bob {TYPE \"Person\" NAME \"Bob\"} CREATE CONCEPT ?tea {TYPE \"Option\" NAME \"Tea\"} CREATE CONCEPT ?coffee {TYPE \"Option\" NAME \"Coffee\"} ENSURE PROPOSITION ?t (?alice,\"prefers\",?tea) ENSURE PROPOSITION ?c (?alice,\"prefers\",?coffee) }"
    ],
    "cases": [
      {
        "name": "NOT filters each incoming binding",
        "command": "FIND(?p.name) WHERE { ?p {type:\"Person\"} NOT { FILTER(?p.name == \"Alice\") } }",
        "expect": {
          "result": [
            "Bob"
          ]
        }
      },
      {
        "name": "OPTIONAL preserves Bob and every compatible Alice match",
        "command": "FIND(?p.name, ?t.name) WHERE { ?p {type:\"Person\"} OPTIONAL { FILTER(?p.name == \"Alice\") (?p,\"prefers\",?t) } }",
        "expect": {
          "result": [
            [
              "Alice",
              "Tea"
            ],
            [
              "Alice",
              "Coffee"
            ],
            [
              "Bob",
              null
            ]
          ]
        }
      },
      {
        "name": "NOT locals are not optional outputs",
        "command": "FIND(?t) WHERE { ?p {type:\"Person\"} NOT { (?p,\"prefers\",?t) } }",
        "expect": {
          "error": "InvalidSyntax"
        }
      },
      {
        "name": "an unmatched optional variable can be bound by a later pattern",
        "command": "FIND(?p.name,?t.name) WHERE { ?p {name:\"Bob\"} OPTIONAL { (?p,\"prefers\",?t) } ?t {name:\"Tea\"} }",
        "expect": {
          "result": [
            [
              "Bob",
              "Tea"
            ]
          ]
        }
      },
      {
        "name": "UNION branches do not unify same-named bindings",
        "command": "FIND(?p.name) WHERE { ?p {name:\"Alice\"} UNION { ?p {name:\"Bob\"} } }",
        "expect": {
          "result": [
            "Alice",
            "Bob"
          ]
        }
      },
      {
        "name": "UNION executes after an empty left branch",
        "command": "FIND(?p.name) WHERE { ?p {name:\"Nobody\"} UNION { ?p {name:\"Bob\"} } }",
        "expect": {
          "result": [
            "Bob"
          ]
        }
      },
      {
        "name": "branch-only variables project as null",
        "command": "FIND(?p.name,?t.name) WHERE { ?p {name:\"Alice\"} UNION { ?t {name:\"Tea\"} } }",
        "expect": {
          "result": [
            [
              "Alice",
              null
            ],
            [
              null,
              "Tea"
            ]
          ]
        }
      },
      {
        "name": "filters after UNION apply to all accumulated rows",
        "command": "FIND(?p.name) WHERE { ?p {name:\"Alice\"} UNION { ?p {name:\"Bob\"} } FILTER(?p.name == \"Bob\") }",
        "expect": {
          "result": [
            "Bob"
          ]
        }
      },
      {
        "name": "the right branch needs its own expression binding sites",
        "command": "FIND(?p.name) WHERE { ?p {name:\"Alice\"} UNION { FILTER(?p.name == \"Alice\") } }",
        "expect": {
          "error": "InvalidSyntax"
        }
      },
      {
        "name": "nested UNION cannot overwrite an OPTIONAL input",
        "command": "FIND(?p.name,?t.name) WHERE { ?p {name:\"Alice\"} OPTIONAL { ?t {name:\"Nobody\"} UNION { ?p {name:\"Bob\"} ?t {name:\"Tea\"} } } }",
        "expect": {
          "result": [
            [
              "Alice",
              null
            ]
          ]
        }
      },
      {
        "name": "compatible independent nested UNION extends the OPTIONAL input",
        "command": "FIND(?p.name,?t.name) WHERE { ?p {name:\"Alice\"} OPTIONAL { ?t {name:\"Nobody\"} UNION { ?t {name:\"Tea\"} } } }",
        "expect": {
          "result": [
            [
              "Alice",
              "Tea"
            ]
          ]
        }
      },
      {
        "name": "identical complete UNION solutions count once",
        "command": "FIND(COUNT(?p)) WHERE { ?p {name:\"Alice\"} UNION { ?p {name:\"Alice\"} } }",
        "expect": {
          "result": [
            1
          ]
        }
      },
      {
        "name": "nonprojected bindings keep complete solutions distinct",
        "command": "FIND(COUNT(?p),COUNT(DISTINCT ?p)) WHERE { ?p {name:\"Alice\"} ?link (?p,\"prefers\",?t) }",
        "expect": {
          "result": [
            [
              2,
              1
            ]
          ]
        }
      },
      {
        "name": "empty global group counts zero and has no numeric sum",
        "command": "FIND(COUNT(?p),SUM(?p.attributes.display_name),AVG(?p.attributes.display_name)) WHERE { ?p {name:\"Nobody\"} }",
        "expect": {
          "result": [
            [
              0,
              null,
              null
            ]
          ]
        }
      },
      {
        "name": "empty grouped query produces no groups",
        "command": "FIND(?p.name,COUNT(?p)) WHERE { ?p {name:\"Nobody\"} }",
        "expect": {
          "result": []
        }
      },
      {
        "name": "numeric aggregates cannot silently ignore a string",
        "command": "FIND(SUM(?p.name)) WHERE { ?p {name:\"Alice\"} }",
        "expect": {
          "error": "TypeMismatch"
        }
      },
      {
        "name": "negating a null string test stays unknown",
        "command": "FIND(?p.name) WHERE { ?p {name:\"Bob\"} OPTIONAL { (?p,\"prefers\",?t) } FILTER(!CONTAINS(?t.name,\"Tea\")) }",
        "expect": {
          "result": []
        }
      },
      {
        "name": "unknown OR true retains the fallback row",
        "command": "FIND(?p.name) WHERE { ?p {name:\"Bob\"} OPTIONAL { (?p,\"prefers\",?t) } FILTER(?t.name == \"Tea\" || ?p.name == \"Bob\") }",
        "expect": {
          "result": [
            "Bob"
          ]
        }
      },
      {
        "name": "missing parameters fail even when prior matches are empty",
        "command": "FIND(?p.name) WHERE { ?p {name:\"Nobody\"} FILTER(?p.name == :missing) }",
        "expect": {
          "error": "ReferenceError"
        }
      },
      {
        "name": "invalid constant regex is rejected before matching",
        "command": "FIND(?p.name) WHERE { ?p {name:\"Nobody\"} FILTER(REGEX(?p.name,\"[\")) }",
        "expect": {
          "error": "InvalidSyntax"
        }
      },
      {
        "name": "name equality cannot deduplicate separate identities",
        "command": "MUTATE {CREATE CONCEPT ?a {TYPE \"Person\" NAME \"Same\"} CREATE CONCEPT ?b {TYPE \"Person\" NAME \"Same\"}}",
        "expect": {
          "result": null
        }
      },
      {
        "name": "equal projected names remain two rows",
        "command": "FIND(?p.name) WHERE { ?p {name:\"Same\"} }",
        "expect": {
          "result": [
            "Same",
            "Same"
          ]
        }
      }
    ],
    "packages": [
      {
        "format": "KIP-Schema-Package",
        "manifest": {
          "package_id": "kip://conformance/options",
          "version": "1.0.0"
        },
        "definitions": {
          "concept_types": {
            "Option": {
              "kind": "ConceptType",
              "description": "A preference option used by these fixtures. A real Space types each option by its kind (Profile §5.5, §7), because prefers partitions by the option's Concept Type; one catch-all type is one partition."
            }
          }
        }
      }
    ]
  },
  {
    "name": "reads",
    "description": "The read language: joins on shared variables, OPTIONAL pads rather than drops, NOT asks about the record and never about the world, typed comparison, nulls last, deterministic paging.",
    "setup": [
      "MUTATE {\n  CREATE CONCEPT ?alice { TYPE \"Person\" NAME \"Alice\" SET ATTRIBUTES { display_name: \"Alice A\" } }\n  CREATE CONCEPT ?bob { TYPE \"Person\" NAME \"Bob\" }\n  CREATE CONCEPT ?dark { TYPE \"Option\" NAME \"Dark\" }\n  ENSURE PROPOSITION ?p (?alice, \"prefers\", ?dark)\n}"
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
        "name": "ORDER BY rejects an aggregate that is not projected",
        "command": "FIND(?c.name) WHERE { ?c CONCEPT {type: \"Person\"} } ORDER BY COUNT(?c) DESC, ?c.name",
        "ordered": true,
        "expect": {
          "error": "InvalidSyntax"
        }
      },
      {
        "name": "but a sort key that varies inside a group has no value to sort by",
        "command": "FIND(?c.name, COUNT(?c)) WHERE { ?c CONCEPT {type: \"Person\"} } ORDER BY ?c.attributes.display_name",
        "expect": {
          "error": "InvalidSyntax"
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
    ],
    "packages": [
      {
        "format": "KIP-Schema-Package",
        "manifest": {
          "package_id": "kip://conformance/options",
          "version": "1.0.0"
        },
        "definitions": {
          "concept_types": {
            "Option": {
              "kind": "ConceptType",
              "description": "A preference option used by these fixtures. A real Space types each option by its kind (Profile §5.5, §7), because prefers partitions by the option's Concept Type; one catch-all type is one partition."
            }
          }
        }
      }
    ]
  },
  {
    "name": "request-envelope",
    "description": "Two things the envelope decides rather than the command (§71, §26, §33). `ingest` mints Evidence from the payload the transport carried, so the observation never passes through model-generated command text — the fidelity risk §88.12 names, where a model retyping what it saw truncates or paraphrases it and the record then says the source said something it did not. `execution.idempotency_key` makes a lost response recoverable: a timeout is not an abort, so a resend replays the outcome the first attempt produced rather than writing a second time. Both are envelope contracts, so both are pinned through the envelope. `requires` is the third: §67.4 fixes the capability names, so a fail-fast precondition written once must get the same answer from either engine — including for an entry whose value is a detail object rather than a bare `true`, and for a name no registry knows, which fails exactly as an unsupported one does. And an `ingest` block is minted inside the request's transaction, so a request that carries only reads opens no scope to mint into: refused, because minting nothing while answering `succeeded` leaves the caller believing the observation was recorded. And `extensions` is the fourth: a block marked `critical` is a precondition the runtime must honor or refuse, never one it may silently drop.",
    "setup": [
      "MUTATE {\n  CREATE CONCEPT ?alice { TYPE \"Person\" NAME \"Alice\" SET FIELDS {canonical_id: \"urn:x:alice\"} }\n  CREATE CONCEPT ?dark { TYPE \"Option\" NAME \"Dark\" }\n  ENSURE PROPOSITION ?p (?alice, \"prefers\", ?dark)\n}"
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
      },
      {
        "name": "a critical extension this engine does not implement fails the request rather than being ignored",
        "command": "FIND(?c.name) WHERE { ?c CONCEPT {name: \"Alice\"} }",
        "envelope": {
          "extensions": {
            "acme/redaction": {
              "critical": true,
              "mode": "strict"
            }
          }
        },
        "expect": {
          "error": "UnsupportedCapability"
        }
      },
      {
        "name": "and a non-critical one is carried past without effect",
        "command": "FIND(?c.name) WHERE { ?c CONCEPT {name: \"Alice\"} }",
        "envelope": {
          "extensions": {
            "acme/tracing": {
              "critical": false,
              "trace_id": "t-1"
            }
          }
        },
        "expect": {
          "result": [
            "Alice"
          ]
        }
      }
    ],
    "packages": [
      {
        "format": "KIP-Schema-Package",
        "manifest": {
          "package_id": "kip://conformance/options",
          "version": "1.0.0"
        },
        "definitions": {
          "concept_types": {
            "Option": {
              "kind": "ConceptType",
              "description": "A preference option used by these fixtures. A real Space types each option by its kind (Profile §5.5, §7), because prefers partitions by the option's Concept Type; one catch-all type is one partition."
            }
          }
        }
      }
    ]
  },
  {
    "name": "retention",
    "description": "`SET RETENTION` writes storage lifecycle and nothing else (§19). The judgement calls pinned here are the ones an engine gets to make wrong quietly: the block replaces rather than patches, a member outside §19.1's shape is refused rather than stored and lost, and a lapsed `expires_at` changes what a sweep may collect without changing what recall returns — retention says how long the record is kept, never whether the claim still holds.",
    "setup": [
      "MUTATE {\n  CREATE CONCEPT ?alice { TYPE \"Person\" NAME \"Alice\" }\n  CREATE CONCEPT ?bob { TYPE \"Person\" NAME \"Bob\" }\n  CREATE CONCEPT ?dark { TYPE \"Option\" NAME \"Dark\" }\n}"
    ],
    "cases": [
      {
        "name": "a retention block is written onto the element the target names",
        "command": "SET RETENTION ?c {retention_class: \"short\", expires_at: \"2030-01-01T00:00:00.000Z\"} WHERE { ?c CONCEPT {name: \"Alice\"} }",
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
        "command": "SET RETENTION ?c {retention_class: \"standard\", review_at: \"2030-01-01T00:00:00.000Z\"} WHERE { ?c CONCEPT {name: \"Alice\"} }",
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
          "error": "ConstraintViolation"
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
        "command": "SET RETENTION ?c {expires_at: \"2031-01-01T00:00:00.000Z\"} WHERE { ?c CONCEPT {name: \"Alice\"} }",
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
        "command": "SET RETENTION ?c {retention_class: \"short\", expires_at: \"2020-01-01T00:00:00.000Z\"} WHERE { ?c CONCEPT {name: \"Bob\"} }",
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
    ],
    "packages": [
      {
        "format": "KIP-Schema-Package",
        "manifest": {
          "package_id": "kip://conformance/options",
          "version": "1.0.0"
        },
        "definitions": {
          "concept_types": {
            "Option": {
              "kind": "ConceptType",
              "description": "A preference option used by these fixtures. A real Space types each option by its kind (Profile §5.5, §7), because prefers partitions by the option's Concept Type; one catch-all type is one partition."
            }
          }
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
      },
      {
        "format": "KIP-Schema-Package",
        "manifest": {
          "package_id": "kip://conformance/options",
          "version": "1.0.0"
        },
        "definitions": {
          "concept_types": {
            "Option": {
              "kind": "ConceptType",
              "description": "A preference option used by these fixtures. A real Space types each option by its kind (Profile §5.5, §7), because prefers partitions by the option's Concept Type; one catch-all type is one partition."
            }
          }
        }
      }
    ],
    "setup": [
      "MUTATE {\n  CREATE CONCEPT ?alice { TYPE \"Person\" NAME \"Alice\" }\n  CREATE CONCEPT ?dark { TYPE \"Option\" NAME \"Dark\" }\n  CREATE CONCEPT ?note { TYPE \"Note\" NAME \"A note\" }\n  CREATE CONCEPT ?citing {\n    TYPE \"Note\"\n    NAME \"Citing note\"\n    SET STRUCTURAL { (\"evidence\", ?note) }\n  }\n  ENSURE PROPOSITION ?p (?alice, \"prefers\", ?dark)\n  CREATE EVIDENCE ?e {\n    SET FIELDS {\n      evidence_class: \"user_statement\",\n      payload: \"I prefer dark mode.\",\n      content_digest: \"sha3-256:d1ge5t\",\n      media_type: \"text/plain\",\n      observed_at: \"2026-08-16T09:00:00.000Z\"\n    }\n  }\n  CREATE ASSERTION ?a {\n    SET FIELDS { proposition: ?p, asserted_by: ?alice, stance: \"support\", mode: \"stated\", confidence: 0.9 }\n    SET STRUCTURAL { (\"evidence\", ?e) {role: \"support\"} }\n  }\n  CREATE ACTIVITY ?run {\n    SET FIELDS {activity_class: \"semantic_consolidation\", status: \"completed\"}\n    SET STRUCTURAL {\n      (\"inputs\", ?alice)\n      (\"outputs\", ?dark)\n      (\"associated_actors\", ?alice)\n      (\"reviewed\", ?note)\n    }\n  }\n}"
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
    "name": "supersession-scope",
    "description": "Supersession stays inside its actor and its scope (§14.2): the replacement has the same canonical actor, the same Proposition or one of the same subject and Predicate lineage, and the same canonical context set. A correction cannot move a general claim into a context or a scoped claim out of one — that would widen or narrow what the actor said — and fails SupersessionMismatch without writing anything; a same-scope correction commits and supersedes.",
    "packages": [
      {
        "format": "KIP-Schema-Package",
        "manifest": {
          "package_id": "kip://conformance/supersession-scope",
          "version": "1.0.0"
        },
        "definitions": {
          "predicates": {
            "timezone": {
              "kind": "PredicateType",
              "description": "Current timezone as a UTC offset string.",
              "functional": true,
              "object": {
                "literal_types": [
                  "string"
                ]
              }
            }
          }
        }
      }
    ],
    "setup": [
      "MUTATE {\n  CREATE CONCEPT ?alice { TYPE \"Person\" NAME \"Alice\" }\n  CREATE CONCEPT ?work { TYPE \"Person\" NAME \"work\" }\n  ENSURE PROPOSITION ?p_general (?alice, \"timezone\", \"+08:00\")\n  ENSURE PROPOSITION ?p_work (?alice, \"timezone\", \"+09:00\")\n  CREATE ASSERTION ?general { SET FIELDS { proposition: ?p_general, asserted_by: ?alice, stance: \"support\", mode: \"stated\", asserted_at: \"2026-01-01T00:00:00.000Z\" } }\n  CREATE ASSERTION ?scoped { SET FIELDS { proposition: ?p_work, asserted_by: ?alice, stance: \"support\", mode: \"stated\", asserted_at: \"2026-01-01T00:00:00.000Z\", context_refs: [?work] } }\n}",
      {
        "command": "FIND(?alice.id, ?work.id, ?general.id, ?scoped.id) WHERE {\n  ?alice CONCEPT {name: \"Alice\"}\n  ?work CONCEPT {name: \"work\"}\n  ?pg PROPOSITION (?alice, \"timezone\", \"+08:00\")\n  ?pw PROPOSITION (?alice, \"timezone\", \"+09:00\")\n  ?general ASSERTION {proposition: ?pg}\n  ?scoped ASSERTION {proposition: ?pw}\n}",
        "capture": {
          "alice": "/0/0",
          "work": "/0/1",
          "general": "/0/2",
          "scoped": "/0/3"
        }
      }
    ],
    "cases": [
      {
        "name": "a correction cannot move a general claim into a context",
        "command": "MUTATE {\n  ASSERT (:alice, \"timezone\", \"+07:00\") {\n    by: :alice, mode: \"stated\", at: \"2026-09-21T00:00:00.000Z\", context: [:work],\n    valid: {from: {latest: \"2026-01-01T00:00:00.000Z\"}}\n  } SUPERSEDING :general\n}",
        "expect": {
          "error": "SupersessionMismatch"
        },
        "vectors": [
          "KML-036"
        ]
      },
      {
        "name": "nor a scoped claim out of its context",
        "command": "MUTATE {\n  ASSERT (:alice, \"timezone\", \"+07:00\") {\n    by: :alice, mode: \"stated\", at: \"2026-09-21T00:00:00.000Z\",\n    valid: {from: {latest: \"2026-01-01T00:00:00.000Z\"}}\n  } SUPERSEDING :scoped\n}",
        "expect": {
          "error": "SupersessionMismatch"
        },
        "vectors": [
          "KML-036"
        ]
      },
      {
        "name": "a refused supersession writes nothing",
        "command": "FIND(?v, ?a.lifecycle.status) WHERE {\n  ?alice CONCEPT {name: \"Alice\"}\n  ?p PROPOSITION (?alice, \"timezone\", ?v)\n  ?a ASSERTION {proposition: ?p}\n}",
        "expect": {
          "result": [
            [
              "+08:00",
              "active"
            ],
            [
              "+09:00",
              "active"
            ]
          ]
        },
        "vectors": [
          "KML-036"
        ]
      },
      {
        "name": "a correction within the same scope supersedes",
        "command": "MUTATE {\n  ASSERT (:alice, \"timezone\", \"+07:00\") {\n    by: :alice, mode: \"stated\", at: \"2026-09-21T00:00:00.000Z\", context: [:work],\n    valid: {from: {latest: \"2026-01-01T00:00:00.000Z\"}}\n  } SUPERSEDING :scoped\n}",
        "expect": {},
        "vectors": [
          "KML-018",
          "KML-036"
        ]
      },
      {
        "name": "only the scoped claim was superseded",
        "command": "FIND(?v, ?a.lifecycle.status) WHERE {\n  ?alice CONCEPT {name: \"Alice\"}\n  ?p PROPOSITION (?alice, \"timezone\", ?v)\n  ?a ASSERTION {proposition: ?p}\n}",
        "expect": {
          "result": [
            [
              "+07:00",
              "active"
            ],
            [
              "+08:00",
              "active"
            ],
            [
              "+09:00",
              "superseded"
            ]
          ]
        },
        "vectors": [
          "KML-018",
          "KML-036"
        ]
      }
    ]
  },
  {
    "name": "timestamps",
    "description": "KIP dcde1de §6.5: strict UTC millisecond inputs, exact error classes, calendar validation and preserved values.",
    "packages": [
      {
        "format": "KIP-Schema-Package",
        "manifest": {
          "package_id": "kip://test/timestamps",
          "version": "1.0.0"
        },
        "definitions": {
          "concept_types": {
            "TimestampProbe": {
              "kind": "ConceptType",
              "attributes": {
                "open": false,
                "fields": {
                  "at": {
                    "type": "timestamp"
                  },
                  "nullable_at": {
                    "type": [
                      "timestamp",
                      "null"
                    ]
                  },
                  "formatted": {
                    "type": "string",
                    "format": "timestamp"
                  }
                }
              }
            }
          },
          "predicates": {
            "moment": {
              "kind": "PredicateType",
              "object": {
                "literal_types": [
                  "string"
                ],
                "format": "timestamp"
              }
            }
          }
        }
      }
    ],
    "setup": [
      "MUTATE {CREATE CONCEPT ?a {TYPE \"Person\" NAME \"clock\"} ENSURE PROPOSITION ?p (?a, \"moment\", \"2024-02-29T12:34:56.123Z\")}"
    ],
    "cases": [
      {
        "name": "reject noncanonical observed_at '2026-01-01T00:00:00Z'",
        "command": "CREATE EVIDENCE ?e {SET FIELDS {evidence_class:\"timestamp_test\",payload:\"arbitrary text 2026-01-01T00:00:00Z\",observed_at::at}}",
        "expect": {
          "error": "ConstraintViolation"
        },
        "params": {
          "at": "2026-01-01T00:00:00Z"
        }
      },
      {
        "name": "reject noncanonical observed_at '2026-01-01T00:00:00.1Z'",
        "command": "CREATE EVIDENCE ?e {SET FIELDS {evidence_class:\"timestamp_test\",payload:\"arbitrary text 2026-01-01T00:00:00Z\",observed_at::at}}",
        "expect": {
          "error": "ConstraintViolation"
        },
        "params": {
          "at": "2026-01-01T00:00:00.1Z"
        }
      },
      {
        "name": "reject noncanonical observed_at '2026-01-01T00:00:00.12Z'",
        "command": "CREATE EVIDENCE ?e {SET FIELDS {evidence_class:\"timestamp_test\",payload:\"arbitrary text 2026-01-01T00:00:00Z\",observed_at::at}}",
        "expect": {
          "error": "ConstraintViolation"
        },
        "params": {
          "at": "2026-01-01T00:00:00.12Z"
        }
      },
      {
        "name": "reject noncanonical observed_at '2026-01-01T00:00:00.1234Z'",
        "command": "CREATE EVIDENCE ?e {SET FIELDS {evidence_class:\"timestamp_test\",payload:\"arbitrary text 2026-01-01T00:00:00Z\",observed_at::at}}",
        "expect": {
          "error": "ConstraintViolation"
        },
        "params": {
          "at": "2026-01-01T00:00:00.1234Z"
        }
      },
      {
        "name": "reject noncanonical observed_at '2026-01-01T00:00:00.000+00:00'",
        "command": "CREATE EVIDENCE ?e {SET FIELDS {evidence_class:\"timestamp_test\",payload:\"arbitrary text 2026-01-01T00:00:00Z\",observed_at::at}}",
        "expect": {
          "error": "ConstraintViolation"
        },
        "params": {
          "at": "2026-01-01T00:00:00.000+00:00"
        }
      },
      {
        "name": "reject noncanonical observed_at '2026-01-01T08:00:00.000+08:00'",
        "command": "CREATE EVIDENCE ?e {SET FIELDS {evidence_class:\"timestamp_test\",payload:\"arbitrary text 2026-01-01T00:00:00Z\",observed_at::at}}",
        "expect": {
          "error": "ConstraintViolation"
        },
        "params": {
          "at": "2026-01-01T08:00:00.000+08:00"
        }
      },
      {
        "name": "reject noncanonical observed_at '2026-01-01t00:00:00.000z'",
        "command": "CREATE EVIDENCE ?e {SET FIELDS {evidence_class:\"timestamp_test\",payload:\"arbitrary text 2026-01-01T00:00:00Z\",observed_at::at}}",
        "expect": {
          "error": "ConstraintViolation"
        },
        "params": {
          "at": "2026-01-01t00:00:00.000z"
        }
      },
      {
        "name": "reject noncanonical observed_at '2026-01-01 00:00:00.000Z'",
        "command": "CREATE EVIDENCE ?e {SET FIELDS {evidence_class:\"timestamp_test\",payload:\"arbitrary text 2026-01-01T00:00:00Z\",observed_at::at}}",
        "expect": {
          "error": "ConstraintViolation"
        },
        "params": {
          "at": "2026-01-01 00:00:00.000Z"
        }
      },
      {
        "name": "reject noncanonical observed_at '2026-02-29T00:00:00.000Z'",
        "command": "CREATE EVIDENCE ?e {SET FIELDS {evidence_class:\"timestamp_test\",payload:\"arbitrary text 2026-01-01T00:00:00Z\",observed_at::at}}",
        "expect": {
          "error": "ConstraintViolation"
        },
        "params": {
          "at": "2026-02-29T00:00:00.000Z"
        }
      },
      {
        "name": "reject noncanonical observed_at '2024-02-30T00:00:00.000Z'",
        "command": "CREATE EVIDENCE ?e {SET FIELDS {evidence_class:\"timestamp_test\",payload:\"arbitrary text 2026-01-01T00:00:00Z\",observed_at::at}}",
        "expect": {
          "error": "ConstraintViolation"
        },
        "params": {
          "at": "2024-02-30T00:00:00.000Z"
        }
      },
      {
        "name": "reject noncanonical observed_at '2026-01-01T24:00:00.000Z'",
        "command": "CREATE EVIDENCE ?e {SET FIELDS {evidence_class:\"timestamp_test\",payload:\"arbitrary text 2026-01-01T00:00:00Z\",observed_at::at}}",
        "expect": {
          "error": "ConstraintViolation"
        },
        "params": {
          "at": "2026-01-01T24:00:00.000Z"
        }
      },
      {
        "name": "reject noncanonical observed_at '2026-01-01T00:00:60.000Z'",
        "command": "CREATE EVIDENCE ?e {SET FIELDS {evidence_class:\"timestamp_test\",payload:\"arbitrary text 2026-01-01T00:00:00Z\",observed_at::at}}",
        "expect": {
          "error": "ConstraintViolation"
        },
        "params": {
          "at": "2026-01-01T00:00:60.000Z"
        }
      },
      {
        "name": "reject noncanonical observed_at '2026-01-01T00:00:00.000Z\\n'",
        "command": "CREATE EVIDENCE ?e {SET FIELDS {evidence_class:\"timestamp_test\",payload:\"arbitrary text 2026-01-01T00:00:00Z\",observed_at::at}}",
        "expect": {
          "error": "ConstraintViolation"
        },
        "params": {
          "at": "2026-01-01T00:00:00.000Z\n"
        }
      },
      {
        "name": "reject non-string observed_at 0",
        "command": "CREATE EVIDENCE ?e {SET FIELDS {evidence_class:\"timestamp_test\",payload:\"arbitrary text 2026-01-01T00:00:00Z\",observed_at::at}}",
        "expect": {
          "error": "TypeMismatch"
        },
        "params": {
          "at": 0
        }
      },
      {
        "name": "reject non-string observed_at True",
        "command": "CREATE EVIDENCE ?e {SET FIELDS {evidence_class:\"timestamp_test\",payload:\"arbitrary text 2026-01-01T00:00:00Z\",observed_at::at}}",
        "expect": {
          "error": "TypeMismatch"
        },
        "params": {
          "at": true
        }
      },
      {
        "name": "reject non-string observed_at []",
        "command": "CREATE EVIDENCE ?e {SET FIELDS {evidence_class:\"timestamp_test\",payload:\"arbitrary text 2026-01-01T00:00:00Z\",observed_at::at}}",
        "expect": {
          "error": "TypeMismatch"
        },
        "params": {
          "at": []
        }
      },
      {
        "name": "reject non-string observed_at {}",
        "command": "CREATE EVIDENCE ?e {SET FIELDS {evidence_class:\"timestamp_test\",payload:\"arbitrary text 2026-01-01T00:00:00Z\",observed_at::at}}",
        "expect": {
          "error": "TypeMismatch"
        },
        "params": {
          "at": {}
        }
      },
      {
        "name": "accept canonical observed_at 2024-02-29T12:34:56.000Z",
        "command": "CREATE EVIDENCE ?e {SET FIELDS {evidence_class:\"timestamp_test\",payload:\"arbitrary text 2026-01-01T00:00:00Z\",observed_at::at}}",
        "expect": {
          "result": null
        },
        "params": {
          "at": "2024-02-29T12:34:56.000Z"
        }
      },
      {
        "name": "accept canonical observed_at 2024-02-29T12:34:56.123Z",
        "command": "CREATE EVIDENCE ?e {SET FIELDS {evidence_class:\"timestamp_test\",payload:\"arbitrary text 2026-01-01T00:00:00Z\",observed_at::at}}",
        "expect": {
          "result": null
        },
        "params": {
          "at": "2024-02-29T12:34:56.123Z"
        }
      },
      {
        "name": "preserve milliseconds on read",
        "command": "FIND(?e.observed_at) WHERE {?e EVIDENCE {evidence_class:\"timestamp_test\"}}",
        "expect": {
          "result": [
            "2024-02-29T12:34:56.000Z",
            "2024-02-29T12:34:56.123Z"
          ]
        }
      },
      {
        "name": "leave payload timestamps untouched",
        "command": "FIND(?e.payload.inline) WHERE {?e EVIDENCE {evidence_class:\"timestamp_test\"}}",
        "expect": {
          "result": [
            "arbitrary text 2026-01-01T00:00:00Z",
            "arbitrary text 2026-01-01T00:00:00Z"
          ]
        }
      },
      {
        "name": "asserted_at rejects whole seconds",
        "command": "CREATE ASSERTION ?x {SET FIELDS {proposition:\"P-1\",asserted_by:\"C-1\",stance:\"support\",mode:\"stated\", asserted_at::at}}",
        "expect": {
          "error": "ConstraintViolation"
        },
        "params": {
          "at": "2026-01-01T00:00:00Z"
        }
      },
      {
        "name": "asserted_at rejects epoch values",
        "command": "CREATE ASSERTION ?x {SET FIELDS {proposition:\"P-1\",asserted_by:\"C-1\",stance:\"support\",mode:\"stated\", asserted_at::at}}",
        "expect": {
          "error": "TypeMismatch"
        },
        "params": {
          "at": 0
        }
      },
      {
        "name": "valid_time.from rejects whole seconds",
        "command": "CREATE ASSERTION ?x {SET FIELDS {proposition:\"P-1\",asserted_by:\"C-1\",stance:\"support\",mode:\"stated\", valid_time:{from::at}}}",
        "expect": {
          "error": "ConstraintViolation"
        },
        "params": {
          "at": "2026-01-01T00:00:00Z"
        }
      },
      {
        "name": "valid_time.from rejects epoch values",
        "command": "CREATE ASSERTION ?x {SET FIELDS {proposition:\"P-1\",asserted_by:\"C-1\",stance:\"support\",mode:\"stated\", valid_time:{from::at}}}",
        "expect": {
          "error": "TypeMismatch"
        },
        "params": {
          "at": 0
        }
      },
      {
        "name": "valid_time.until rejects whole seconds",
        "command": "CREATE ASSERTION ?x {SET FIELDS {proposition:\"P-1\",asserted_by:\"C-1\",stance:\"support\",mode:\"stated\", valid_time:{until::at}}}",
        "expect": {
          "error": "ConstraintViolation"
        },
        "params": {
          "at": "2026-01-01T00:00:00Z"
        }
      },
      {
        "name": "valid_time.until rejects epoch values",
        "command": "CREATE ASSERTION ?x {SET FIELDS {proposition:\"P-1\",asserted_by:\"C-1\",stance:\"support\",mode:\"stated\", valid_time:{until::at}}}",
        "expect": {
          "error": "TypeMismatch"
        },
        "params": {
          "at": 0
        }
      },
      {
        "name": "Activity.started_at rejects whole seconds",
        "command": "CREATE ACTIVITY ?x {SET FIELDS {activity_class:\"timestamp_test\",started_at::at}}",
        "expect": {
          "error": "ConstraintViolation"
        },
        "params": {
          "at": "2026-01-01T00:00:00Z"
        }
      },
      {
        "name": "Activity.started_at rejects epoch values",
        "command": "CREATE ACTIVITY ?x {SET FIELDS {activity_class:\"timestamp_test\",started_at::at}}",
        "expect": {
          "error": "TypeMismatch"
        },
        "params": {
          "at": 0
        }
      },
      {
        "name": "Activity.ended_at rejects whole seconds",
        "command": "CREATE ACTIVITY ?x {SET FIELDS {activity_class:\"timestamp_test\",ended_at::at}}",
        "expect": {
          "error": "ConstraintViolation"
        },
        "params": {
          "at": "2026-01-01T00:00:00Z"
        }
      },
      {
        "name": "Activity.ended_at rejects epoch values",
        "command": "CREATE ACTIVITY ?x {SET FIELDS {activity_class:\"timestamp_test\",ended_at::at}}",
        "expect": {
          "error": "TypeMismatch"
        },
        "params": {
          "at": 0
        }
      },
      {
        "name": "retention.expires_at rejects whole seconds",
        "command": "SET RETENTION \"C-1\" {expires_at::at}",
        "expect": {
          "error": "ConstraintViolation"
        },
        "params": {
          "at": "2026-01-01T00:00:00Z"
        }
      },
      {
        "name": "retention.expires_at rejects epoch values",
        "command": "SET RETENTION \"C-1\" {expires_at::at}",
        "expect": {
          "error": "TypeMismatch"
        },
        "params": {
          "at": 0
        }
      },
      {
        "name": "FOR TIME rejects whole seconds",
        "command": "FIND(?c.name) WHERE {?c CONCEPT {id:\"C-1\"}} FOR TIME :at",
        "expect": {
          "error": "ConstraintViolation"
        },
        "params": {
          "at": "2026-01-01T00:00:00Z"
        }
      },
      {
        "name": "FOR TIME rejects epoch values",
        "command": "FIND(?c.name) WHERE {?c CONCEPT {id:\"C-1\"}} FOR TIME :at",
        "expect": {
          "error": "TypeMismatch"
        },
        "params": {
          "at": 0
        }
      },
      {
        "name": "DESCRIBE SNAPSHOT AT TIME rejects whole seconds",
        "command": "DESCRIBE SNAPSHOT AT TIME :at",
        "expect": {
          "error": "ConstraintViolation"
        },
        "params": {
          "at": "2026-01-01T00:00:00Z"
        }
      },
      {
        "name": "DESCRIBE SNAPSHOT AT TIME rejects epoch values",
        "command": "DESCRIBE SNAPSHOT AT TIME :at",
        "expect": {
          "error": "TypeMismatch"
        },
        "params": {
          "at": 0
        }
      },
      {
        "name": "profile timestamp rejects whole seconds",
        "command": "CREATE CONCEPT ?c {TYPE \"TimestampProbe\" SET ATTRIBUTES {at::at}}",
        "expect": {
          "error": "ConstraintViolation"
        },
        "params": {
          "at": "2026-01-01T00:00:00Z"
        }
      },
      {
        "name": "profile timestamp rejects epoch values",
        "command": "CREATE CONCEPT ?c {TYPE \"TimestampProbe\" SET ATTRIBUTES {at::at}}",
        "expect": {
          "error": "TypeMismatch"
        },
        "params": {
          "at": 0
        }
      },
      {
        "name": "profile format timestamp rejects whole seconds",
        "command": "CREATE CONCEPT ?c {TYPE \"TimestampProbe\" SET ATTRIBUTES {formatted::at}}",
        "expect": {
          "error": "ConstraintViolation"
        },
        "params": {
          "at": "2026-01-01T00:00:00Z"
        }
      },
      {
        "name": "profile format timestamp rejects epoch values",
        "command": "CREATE CONCEPT ?c {TYPE \"TimestampProbe\" SET ATTRIBUTES {formatted::at}}",
        "expect": {
          "error": "TypeMismatch"
        },
        "params": {
          "at": 0
        }
      },
      {
        "name": "predicate timestamp rejects whole seconds",
        "command": "ENSURE PROPOSITION ?p (:actor, \"moment\", :at)",
        "expect": {
          "error": "ConstraintViolation"
        },
        "params": {
          "at": "2026-01-01T00:00:00Z",
          "actor": {
            "id": "C-1"
          }
        }
      },
      {
        "name": "predicate timestamp rejects epoch values",
        "command": "ENSURE PROPOSITION ?p (:actor, \"moment\", :at)",
        "expect": {
          "error": "TypeMismatch"
        },
        "params": {
          "at": 0,
          "actor": {
            "id": "C-1"
          }
        }
      },
      {
        "name": "nullable profile timestamp permits null",
        "command": "CREATE CONCEPT ?c {TYPE \"TimestampProbe\" SET ATTRIBUTES {nullable_at:null}}",
        "expect": {
          "result": null
        }
      },
      {
        "name": "ingest observed_at rejects '2026-01-01T00:00:00Z'",
        "command": "FIND(?c) WHERE {?c CONCEPT {id:\"C-1\"}}",
        "expect": {
          "error": "ConstraintViolation"
        },
        "envelope": {
          "ingest": {
            "evidence": [
              {
                "key": "entry",
                "evidence_class": "user_statement",
                "payload": "text",
                "observed_at": "2026-01-01T00:00:00Z"
              }
            ]
          }
        }
      },
      {
        "name": "ingest observed_at rejects 0",
        "command": "FIND(?c) WHERE {?c CONCEPT {id:\"C-1\"}}",
        "expect": {
          "error": "TypeMismatch"
        },
        "envelope": {
          "ingest": {
            "evidence": [
              {
                "key": "entry",
                "evidence_class": "user_statement",
                "payload": "text",
                "observed_at": 0
              }
            ]
          }
        }
      },
      {
        "name": "ingest observed_at rejects None",
        "command": "FIND(?c) WHERE {?c CONCEPT {id:\"C-1\"}}",
        "expect": {
          "error": "TypeMismatch"
        },
        "envelope": {
          "ingest": {
            "evidence": [
              {
                "key": "entry",
                "evidence_class": "user_statement",
                "payload": "text",
                "observed_at": null
              }
            ]
          }
        }
      }
    ]
  },
  {
    "name": "transaction-final-validation",
    "description": "Final identity validation agrees between preview and commit; client keys also resolve inside one mutation.",
    "setup": [],
    "cases": [
      {
        "name": "commit refuses the same duplicate logical keys",
        "command": "MUTATE { CREATE CONCEPT ?a { TYPE \"Person\" NAME \"Duplicate\" SET FIELDS {key: \"same-key\"} } CREATE CONCEPT ?b { TYPE \"Person\" NAME \"Duplicate\" SET FIELDS {key: \"same-key\"} } }",
        "expect": {
          "error": "IdentityConflict"
        }
      },
      {
        "name": "rejected creations leave no visible rows",
        "command": "FIND(COUNT(?c)) WHERE { ?c CONCEPT {name:\"Duplicate\"} }",
        "expect": {
          "result": [
            0
          ]
        }
      },
      {
        "name": "one client key resolves to one creation inside a MUTATE",
        "command": "MUTATE { CREATE CONCEPT ?a { TYPE \"Person\" NAME \"Batch Alice\" CLIENT KEY \"batch-identity\" } CREATE CONCEPT ?b { TYPE \"Person\" NAME \"Batch Alice\" CLIENT KEY \"batch-identity\" } }",
        "expect": {}
      },
      {
        "name": "the batch contains one logical creation",
        "command": "FIND(COUNT(?c)) WHERE { ?c CONCEPT {name:\"Batch Alice\"} }",
        "expect": {
          "result": [
            1
          ]
        }
      },
      {
        "name": "the same creation can still retry after commit",
        "command": "CREATE CONCEPT ?c {TYPE \"Person\" NAME \"Batch Alice\" CLIENT KEY \"batch-identity\"}",
        "expect": {}
      },
      {
        "name": "a conflicting use of that client key is rejected",
        "command": "CREATE CONCEPT ?c {TYPE \"Person\" NAME \"Other\" CLIENT KEY \"batch-identity\"}",
        "expect": {
          "error": "ClientKeyConflict"
        }
      }
    ]
  },
  {
    "name": "transactions",
    "description": "A MUTATE block is one transaction, not a script that happens to run in order: everything in it commits or none of it does. A precondition that fails leaves the element exactly as the caller last saw it. EXPECT VERSION is the one guard, and it is always the trailing clause (Spec §52.8); there is no EXPECT STATE — TRANSITION validates the target's current lifecycle state itself and fails InvalidLifecycleTransition from the wrong one (§35.3, §52.5), while a move to the state already held is a no_effect rather than an error.",
    "setup": [
      "MUTATE {\n  CREATE CONCEPT ?alice { TYPE \"Person\" NAME \"Alice\" SET FIELDS {key: \"person:alice\"} }\n  CREATE CONCEPT ?dark { TYPE \"Option\" NAME \"Dark\" }\n  ENSURE PROPOSITION ?p (?alice, \"prefers\", ?dark)\n  CREATE ASSERTION ?a {\n    SET FIELDS { proposition: ?p, asserted_by: ?alice, stance: \"support\", mode: \"stated\", confidence: 0.9 }\n  }\n}"
    ],
    "cases": [
      {
        "name": "an omitted assertion time defaults to the engine transaction time",
        "command": "FIND(COUNT(?a)) WHERE { ?a ASSERTION {} FILTER(?a.asserted_at >= \"1970-01-01T00:00:00.000Z\") }",
        "expect": {
          "result": [
            1
          ]
        }
      },
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
    ],
    "packages": [
      {
        "format": "KIP-Schema-Package",
        "manifest": {
          "package_id": "kip://conformance/options",
          "version": "1.0.0"
        },
        "definitions": {
          "concept_types": {
            "Option": {
              "kind": "ConceptType",
              "description": "A preference option used by these fixtures. A real Space types each option by its kind (Profile §5.5, §7), because prefers partitions by the option's Concept Type; one catch-all type is one partition."
            }
          }
        }
      }
    ]
  },
  {
    "name": "tuple-endpoints",
    "description": "KQL endpoint patterns constrain visible Concepts, nested Propositions and canonical references; KML creation endpoints still require stable identity. An extra field is never silently ignored.",
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
        "name": "an inline Concept pattern does not match a canonical reference",
        "command": "FIND(?s.name) WHERE { ?p PROPOSITION (?s, \"same_as\", {name: \"Alice\"}) }",
        "expect": {
          "result": []
        }
      },
      {
        "name": "a variable canonical_id matches local Concepts, not nonlocal reference objects",
        "command": "FIND(?s.name) WHERE { ?p PROPOSITION (?s, \"same_as\", {canonical_id: ?whatever}) }",
        "expect": {
          "result": []
        }
      },
      {
        "name": "a nested Proposition endpoint matches no canonical reference",
        "command": "FIND(?s.name) WHERE { ?meta PROPOSITION (?s, \"same_as\", (id: :other)) }",
        "params": {
          "other": "P-1"
        },
        "expect": {
          "result": []
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
        "name": "additional endpoint fields are constraints, never ignored",
        "command": "FIND(?s.name) WHERE { ?p PROPOSITION (?s, \"same_as\", {canonical_id: \"urn:x:alice\", name: \"Zed\"}) }",
        "expect": {
          "result": []
        }
      }
    ]
  },
  {
    "name": "world-time",
    "description": "This revision's memory behavior. Temporal succession (§25.4): a world change is one Assertion, the old value answers for its time, nothing is superseded; a claim with no stated start is indeterminate before it was made (§25.2); time bounds are three-valued (§25.5); functional_by partitions preferences by option kind (§20.15); kip:memory-default decides by context specificity, first-person testimony and recency, and discloses the rule (§21.13); two inferences without a written start never succeed one another; the Search Pattern is bounded and never inside NOT (§43.8); a value-only correction keeps its world interval (§14.2). The DEFINE cases moved to draft-vocabulary.json.",
    "packages": [
      {
        "format": "KIP-Schema-Package",
        "manifest": {
          "package_id": "kip://conformance/world-time",
          "version": "1.0.0"
        },
        "definitions": {
          "concept_types": {
            "Place": {
              "kind": "ConceptType",
              "description": "A place."
            },
            "ColorScheme": {
              "kind": "ConceptType",
              "description": "A color scheme: one kind of preference option."
            },
            "Editor": {
              "kind": "ConceptType",
              "description": "An editor: another kind of preference option."
            }
          },
          "predicates": {
            "lives_in": {
              "kind": "PredicateType",
              "description": "Primary residence; one at a time.",
              "functional": true
            },
            "located_in": {
              "kind": "PredicateType",
              "description": "The smaller place lies within the larger.",
              "functional": true
            },
            "timezone": {
              "kind": "PredicateType",
              "description": "Current timezone as a UTC offset string.",
              "functional": true
            }
          }
        }
      }
    ],
    "setup": [
      "MUTATE {\n  CREATE CONCEPT ?alice { TYPE \"Person\" NAME \"Alice\" }\n  CREATE CONCEPT ?bob { TYPE \"Person\" NAME \"Bob\" }\n  CREATE CONCEPT ?carol { TYPE \"Person\" NAME \"Carol\" }\n  CREATE CONCEPT ?dave { TYPE \"Person\" NAME \"Dave\" }\n  CREATE CONCEPT ?sensor { TYPE \"Person\" NAME \"Sensor\" }\n  CREATE CONCEPT ?brain { TYPE \"Person\" NAME \"Brain\" }\n  CREATE CONCEPT ?beijing { TYPE \"Place\" NAME \"Beijing\" }\n  CREATE CONCEPT ?shanghai { TYPE \"Place\" NAME \"Shanghai\" }\n  CREATE CONCEPT ?berlin { TYPE \"Place\" NAME \"Berlin\" }\n  CREATE CONCEPT ?austin { TYPE \"Place\" NAME \"Austin\" }\n  CREATE CONCEPT ?dallas { TYPE \"Place\" NAME \"Dallas\" }\n  CREATE CONCEPT ?campus { TYPE \"Place\" NAME \"Acme campus\" }\n  CREATE CONCEPT ?dark { TYPE \"ColorScheme\" NAME \"Dark\" }\n  CREATE CONCEPT ?light { TYPE \"ColorScheme\" NAME \"Light\" }\n  CREATE CONCEPT ?vim { TYPE \"Editor\" NAME \"Vim\" }\n  ENSURE PROPOSITION ?p_bj (?alice, \"lives_in\", ?beijing)\n  ENSURE PROPOSITION ?p_sh (?alice, \"lives_in\", ?shanghai)\n  ENSURE PROPOSITION ?p_be (?carol, \"lives_in\", ?berlin)\n  ENSURE PROPOSITION ?p_dark (?alice, \"prefers\", ?dark)\n  ENSURE PROPOSITION ?p_light (?alice, \"prefers\", ?light)\n  ENSURE PROPOSITION ?p_vim (?alice, \"prefers\", ?vim)\n  ENSURE PROPOSITION ?p_tz8 (?alice, \"timezone\", \"+08:00\")\n  ENSURE PROPOSITION ?p_tz9 (?alice, \"timezone\", \"+09:00\")\n  ENSURE PROPOSITION ?p_d1 (?dave, \"timezone\", \"+01:00\")\n  ENSURE PROPOSITION ?p_d2 (?dave, \"timezone\", \"+02:00\")\n  ENSURE PROPOSITION ?p_au (?campus, \"located_in\", ?austin)\n  ENSURE PROPOSITION ?p_da (?campus, \"located_in\", ?dallas)\n  CREATE ASSERTION ?a1 { SET FIELDS { proposition: ?p_bj, asserted_by: ?alice, stance: \"support\", mode: \"stated\", asserted_at: \"2026-01-10T00:00:00.000Z\" } }\n  CREATE ASSERTION ?a2 { SET FIELDS { proposition: ?p_sh, asserted_by: ?alice, stance: \"support\", mode: \"stated\", asserted_at: \"2026-09-10T00:00:00.000Z\", valid_time: { from: \"2026-09-01T00:00:00.000Z\" } } }\n  CREATE ASSERTION ?a3 { SET FIELDS { proposition: ?p_be, asserted_by: ?carol, stance: \"support\", mode: \"stated\", asserted_at: \"2026-03-01T00:00:00.000Z\", valid_time: { from: { earliest: \"2026-01-01T00:00:00.000Z\", latest: \"2026-12-31T23:59:59.999Z\" } } } }\n  CREATE ASSERTION ?a4 { SET FIELDS { proposition: ?p_dark, asserted_by: ?alice, stance: \"support\", mode: \"stated\", asserted_at: \"2026-01-01T00:00:00.000Z\", valid_time: { from: \"2026-01-01T00:00:00.000Z\" } } }\n  CREATE ASSERTION ?a5 { SET FIELDS { proposition: ?p_light, asserted_by: ?alice, stance: \"support\", mode: \"stated\", asserted_at: \"2026-09-01T00:00:00.000Z\", valid_time: { from: \"2026-09-01T00:00:00.000Z\" } } }\n  CREATE ASSERTION ?a6 { SET FIELDS { proposition: ?p_vim, asserted_by: ?alice, stance: \"support\", mode: \"stated\", asserted_at: \"2026-02-01T00:00:00.000Z\", valid_time: { from: \"2026-02-01T00:00:00.000Z\" } } }\n  CREATE ASSERTION ?a7 { SET FIELDS { proposition: ?p_tz8, asserted_by: ?alice, stance: \"support\", mode: \"stated\", asserted_at: \"2026-01-01T00:00:00.000Z\" } }\n  CREATE ASSERTION ?a8 { SET FIELDS { proposition: ?p_tz9, asserted_by: ?bob, stance: \"support\", mode: \"stated\", asserted_at: \"2026-09-01T00:00:00.000Z\" } }\n  CREATE ASSERTION ?a9 { SET FIELDS { proposition: ?p_d1, asserted_by: ?dave, stance: \"support\", mode: \"stated\", asserted_at: \"2026-01-01T00:00:00.000Z\" } }\n  CREATE ASSERTION ?a10 { SET FIELDS { proposition: ?p_d2, asserted_by: ?sensor, stance: \"support\", mode: \"observed\", asserted_at: \"2026-09-05T00:00:00.000Z\" } }\n  CREATE EVIDENCE ?source_austin { SET FIELDS { evidence_class: \"document\", payload: \"Acme campus is in Austin\", observed_at: \"2026-01-10T00:00:00.000Z\" } }\n  CREATE EVIDENCE ?source_dallas { SET FIELDS { evidence_class: \"document\", payload: \"Acme campus is in Dallas\", observed_at: \"2026-09-10T00:00:00.000Z\" } }\n}",
      {
        "command": "FIND(?campus.id, ?austin.id, ?dallas.id, ?brain.id, ?e1.id, ?e1._system.version, ?e2.id, ?e2._system.version, ?b.basis)\nWHERE {\n  ?campus CONCEPT {name: \"Acme campus\"}\n  ?austin CONCEPT {name: \"Austin\"}\n  ?dallas CONCEPT {name: \"Dallas\"}\n  ?brain CONCEPT {name: \"Brain\"}\n  ?e1 EVIDENCE {observed_at: \"2026-01-10T00:00:00.000Z\"}\n  ?e2 EVIDENCE {observed_at: \"2026-09-10T00:00:00.000Z\"}\n  ?b BELIEF (?campus, \"located_in\", ?austin)\n}\nFOR TIME \"2026-09-20T00:00:00.000Z\"\nWITH EPISTEMIC {policy: \"kip:memory-default\"}",
        "capture": {
          "campus": "/0/0",
          "austin": "/0/1",
          "dallas": "/0/2",
          "brain": "/0/3",
          "source_austin": "/0/4",
          "source_austin_version": "/0/5",
          "source_dallas": "/0/6",
          "source_dallas_version": "/0/7",
          "inference_basis": "/0/8",
          "inference_seq": "/0/8/snapshot_seq"
        }
      },
      "MUTATE {\n  ASSERT ?inference_austin (:campus, \"located_in\", :austin) {\n    by: :brain, mode: \"inferred\", at: \"2026-01-10T00:00:00.000Z\", evidence: :source_austin\n  }\n  ASSERT ?inference_dallas (:campus, \"located_in\", :dallas) {\n    by: :brain, mode: \"inferred\", at: \"2026-09-10T00:00:00.000Z\", evidence: :source_dallas\n  }\n  CREATE ACTIVITY ?derive_austin {\n    SET FIELDS {activity_class: \"extraction\", status: \"completed\"}\n    SET FACET \"DependencyBasis\" {\n      basis_seq: :inference_seq,\n      groups: [{role: \"all_of\", pins: [{id: :source_austin, version: :source_austin_version}]}],\n      policy_basis: :inference_basis\n    }\n    SET STRUCTURAL { (\"inputs\", :source_austin) (\"outputs\", ?inference_austin) }\n  }\n  CREATE ACTIVITY ?derive_dallas {\n    SET FIELDS {activity_class: \"extraction\", status: \"completed\"}\n    SET FACET \"DependencyBasis\" {\n      basis_seq: :inference_seq,\n      groups: [{role: \"all_of\", pins: [{id: :source_dallas, version: :source_dallas_version}]}],\n      policy_basis: :inference_basis\n    }\n    SET STRUCTURAL { (\"inputs\", :source_dallas) (\"outputs\", ?inference_dallas) }\n  }\n}",
      {
        "command": "FIND(?alice.id, ?a.id) WHERE { ?alice CONCEPT {name: \"Alice\"} ?p PROPOSITION (?alice, \"timezone\", \"+08:00\") ?a ASSERTION {proposition: ?p} }",
        "capture": {
          "alice": "/0/0",
          "old_timezone_assertion": "/0/1"
        }
      }
    ],
    "cases": [
      {
        "name": "a world change is one Assertion: the new value holds from its start",
        "command": "FIND(?b.status) WHERE { ?s CONCEPT {name: \"Alice\"} ?o CONCEPT {name: \"Shanghai\"} ?b BELIEF (?s, \"lives_in\", ?o) } FOR TIME \"2026-09-20T00:00:00.000Z\"",
        "expect": {
          "result": [
            "accepted"
          ]
        },
        "vectors": [
          "EPI-031",
          "MEM-026"
        ]
      },
      {
        "name": "the succeeded value is outside its effective interval after the change",
        "command": "FIND(?b.status) WHERE { ?s CONCEPT {name: \"Alice\"} ?o CONCEPT {name: \"Beijing\"} ?b BELIEF (?s, \"lives_in\", ?o) } FOR TIME \"2026-09-20T00:00:00.000Z\"",
        "expect": {
          "result": [
            "insufficient"
          ]
        },
        "vectors": [
          "EPI-031",
          "MEM-026"
        ]
      },
      {
        "name": "the succeeded value still answers for its time",
        "command": "FIND(?b.status) WHERE { ?s CONCEPT {name: \"Alice\"} ?o CONCEPT {name: \"Beijing\"} ?b BELIEF (?s, \"lives_in\", ?o) } FOR TIME \"2026-06-01T00:00:00.000Z\"",
        "expect": {
          "result": [
            "accepted"
          ]
        },
        "vectors": [
          "EPI-031",
          "MEM-026"
        ]
      },
      {
        "name": "and the successor is not yet valid then",
        "command": "FIND(?b.status) WHERE { ?s CONCEPT {name: \"Alice\"} ?o CONCEPT {name: \"Shanghai\"} ?b BELIEF (?s, \"lives_in\", ?o) } FOR TIME \"2026-06-01T00:00:00.000Z\"",
        "expect": {
          "result": [
            "insufficient"
          ]
        },
        "vectors": [
          "EPI-031"
        ]
      },
      {
        "name": "nothing was superseded or retracted",
        "command": "FIND(?a.lifecycle.status) WHERE { ?s CONCEPT {name: \"Alice\"} ?o CONCEPT {name: \"Beijing\"} ?p PROPOSITION (?s, \"lives_in\", ?o) ?a ASSERTION {proposition: ?p} }",
        "expect": {
          "result": [
            "active"
          ]
        },
        "vectors": [
          "EPI-031",
          "MEM-026"
        ]
      },
      {
        "name": "a claim with no stated start is indeterminate before it was made",
        "command": "FIND(?b.status, ?b.uncertainty.reasons) WHERE { ?s CONCEPT {name: \"Alice\"} ?o CONCEPT {name: \"Beijing\"} ?b BELIEF (?s, \"lives_in\", ?o) } FOR TIME \"2026-01-05T00:00:00.000Z\"",
        "expect": {
          "result": [
            [
              "uncertain",
              [
                "temporal_indeterminate"
              ]
            ]
          ]
        },
        "vectors": [
          "EPI-032",
          "MEM-026"
        ]
      },
      {
        "name": "a coarse start is indeterminate inside its range",
        "command": "FIND(?b.status) WHERE { ?s CONCEPT {name: \"Carol\"} ?o CONCEPT {name: \"Berlin\"} ?b BELIEF (?s, \"lives_in\", ?o) } FOR TIME \"2026-06-01T00:00:00.000Z\"",
        "expect": {
          "result": [
            "uncertain"
          ]
        },
        "vectors": [
          "EPI-032",
          "MEM-027"
        ]
      },
      {
        "name": "and certain after it",
        "command": "FIND(?b.status) WHERE { ?s CONCEPT {name: \"Carol\"} ?o CONCEPT {name: \"Berlin\"} ?b BELIEF (?s, \"lives_in\", ?o) } FOR TIME \"2027-01-01T00:00:00.000Z\"",
        "expect": {
          "result": [
            "accepted"
          ]
        },
        "vectors": [
          "EPI-032",
          "MEM-027"
        ]
      },
      {
        "name": "functional_by: the newer preference of one kind succeeds the older",
        "command": "FIND(?b.status) WHERE { ?s CONCEPT {name: \"Alice\"} ?o CONCEPT {name: \"Dark\"} ?b BELIEF (?s, \"prefers\", ?o) } FOR TIME \"2026-09-20T00:00:00.000Z\"",
        "expect": {
          "result": [
            "insufficient"
          ]
        },
        "vectors": [
          "SCHEMA-021",
          "MEM-028"
        ]
      },
      {
        "name": "the newer preference is accepted",
        "command": "FIND(?b.status) WHERE { ?s CONCEPT {name: \"Alice\"} ?o CONCEPT {name: \"Light\"} ?b BELIEF (?s, \"prefers\", ?o) } FOR TIME \"2026-09-20T00:00:00.000Z\"",
        "expect": {
          "result": [
            "accepted"
          ]
        },
        "vectors": [
          "SCHEMA-021",
          "MEM-028"
        ]
      },
      {
        "name": "a preference of another kind coexists",
        "command": "FIND(?b.status) WHERE { ?s CONCEPT {name: \"Alice\"} ?o CONCEPT {name: \"Vim\"} ?b BELIEF (?s, \"prefers\", ?o) } FOR TIME \"2026-09-20T00:00:00.000Z\"",
        "expect": {
          "result": [
            "accepted"
          ]
        },
        "vectors": [
          "SCHEMA-021",
          "MEM-028"
        ]
      },
      {
        "name": "memory-default: the subject's own statement outranks hearsay",
        "command": "FIND(?b.status) WHERE { ?s CONCEPT {name: \"Alice\"} ?b BELIEF (?s, \"timezone\", \"+08:00\") } FOR TIME \"2026-09-20T00:00:00.000Z\" WITH EPISTEMIC {policy: \"kip:memory-default\"}",
        "expect": {
          "result": [
            "accepted"
          ]
        },
        "vectors": [
          "EPI-033",
          "MEM-029"
        ]
      },
      {
        "name": "the outranked hearsay is uncertain and names the rule",
        "command": "FIND(?b.status, ?b.uncertainty.reasons, ?b.precedence.rule) WHERE { ?s CONCEPT {name: \"Alice\"} ?b BELIEF (?s, \"timezone\", \"+09:00\") } FOR TIME \"2026-09-20T00:00:00.000Z\" WITH EPISTEMIC {policy: \"kip:memory-default\"}",
        "expect": {
          "result": [
            [
              "uncertain",
              [
                "outranked"
              ],
              "first_person_testimony"
            ]
          ]
        },
        "vectors": [
          "EPI-033",
          "MEM-029"
        ]
      },
      {
        "name": "memory-default: a newer observation prevails over older testimony by recency",
        "command": "FIND(?b.status) WHERE { ?s CONCEPT {name: \"Dave\"} ?b BELIEF (?s, \"timezone\", \"+02:00\") } FOR TIME \"2026-09-20T00:00:00.000Z\" WITH EPISTEMIC {policy: \"kip:memory-default\"}",
        "expect": {
          "result": [
            "accepted"
          ]
        },
        "vectors": [
          "EPI-033",
          "MEM-029"
        ]
      },
      {
        "name": "the older testimony is outranked by recency, never rejected",
        "command": "FIND(?b.status, ?b.uncertainty.reasons, ?b.precedence.rule) WHERE { ?s CONCEPT {name: \"Dave\"} ?b BELIEF (?s, \"timezone\", \"+01:00\") } FOR TIME \"2026-09-20T00:00:00.000Z\" WITH EPISTEMIC {policy: \"kip:memory-default\"}",
        "expect": {
          "result": [
            [
              "uncertain",
              [
                "outranked"
              ],
              "recency"
            ]
          ]
        },
        "vectors": [
          "EPI-033",
          "MEM-029"
        ]
      },
      {
        "name": "two inferences never succeed one another: the older is still eligible and outranked, not expired",
        "command": "FIND(?b.status, ?b.uncertainty.reasons, ?b.precedence.rule) WHERE { ?s CONCEPT {name: \"Acme campus\"} ?o CONCEPT {name: \"Austin\"} ?b BELIEF (?s, \"located_in\", ?o) } FOR TIME \"2026-09-20T00:00:00.000Z\" WITH EPISTEMIC {policy: \"kip:memory-default\"}",
        "expect": {
          "result": [
            [
              "uncertain",
              [
                "outranked"
              ],
              "recency"
            ]
          ]
        },
        "vectors": [
          "EPI-031",
          "MEM-026"
        ]
      },
      {
        "name": "the newer inference prevails by recency, not by an invented world change",
        "command": "FIND(?b.status) WHERE { ?s CONCEPT {name: \"Acme campus\"} ?o CONCEPT {name: \"Dallas\"} ?b BELIEF (?s, \"located_in\", ?o) } FOR TIME \"2026-09-20T00:00:00.000Z\" WITH EPISTEMIC {policy: \"kip:memory-default\"}",
        "expect": {
          "result": [
            "accepted"
          ]
        },
        "vectors": [
          "EPI-031",
          "MEM-026"
        ]
      },
      {
        "name": "a Search Pattern binds hits with a bounded candidate set",
        "command": "FIND(?x.name) WHERE { ?x SEARCH CONCEPT \"Alice\" WITH TYPE \"Person\" MODE \"keyword\" LIMIT 5 }",
        "expect": {
          "result": [
            "Alice"
          ]
        },
        "vectors": [
          "KQL-032"
        ]
      },
      {
        "name": "a Search Pattern requires LIMIT",
        "command": "FIND(?x) WHERE { ?x SEARCH CONCEPT \"Alice\" }",
        "expect": {
          "error": "InvalidSyntax"
        },
        "vectors": [
          "KQL-032"
        ]
      },
      {
        "name": "a Search Pattern never proves absence",
        "command": "FIND(?x) WHERE { ?x CONCEPT {name: \"Alice\"} NOT { ?y SEARCH CONCEPT \"Bob\" LIMIT 5 } }",
        "expect": {
          "error": "InvalidSyntax"
        },
        "vectors": [
          "KQL-033"
        ]
      },
      {
        "name": "a value-only correction keeps the original world interval and the correction time",
        "command": "MUTATE {\n  CREATE EVIDENCE ?correction_source {\n    SET FIELDS {evidence_class: \"user_statement\", payload: \"I meant +07:00, not +08:00\", observed_at: \"2026-09-21T00:00:00.000Z\"}\n  }\n  ASSERT (:alice, \"timezone\", \"+07:00\") {\n    by: :alice, mode: \"stated\", at: \"2026-09-21T00:00:00.000Z\", evidence: ?correction_source,\n    valid: {from: {latest: \"2026-01-01T00:00:00.000Z\"}}\n  } SUPERSEDING :old_timezone_assertion\n}",
        "expect": {},
        "vectors": [
          "KML-017"
        ]
      },
      {
        "name": "the corrected value answers before the correction was stated",
        "command": "FIND(?b.status) WHERE { ?alice CONCEPT {name: \"Alice\"} ?b BELIEF (?alice, \"timezone\", \"+07:00\") } FOR TIME \"2026-06-01T00:00:00.000Z\" WITH EPISTEMIC {policy: \"kip:memory-default\"}",
        "expect": {
          "result": [
            "accepted"
          ]
        },
        "vectors": [
          "KML-017"
        ]
      }
    ]
  }
] as unknown as Fixture[]

/** The total number of cases, so a silent shrink is visible. */
export const CASE_COUNT = 423
