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
    "name": "core-truth-neutrality",
    "description": "The distinction the version exists for: a Proposition existing is not the Proposition being true. A tuple carries no confidence, the same tuple resolves to one Proposition, and a raw read reports claims rather than beliefs.",
    "setup": [
      "MUTATE {\n  CREATE CONCEPT ?alice { TYPE \"Person\" NAME \"Alice\" }\n  CREATE CONCEPT ?dark { TYPE \"Preference\" NAME \"Dark\" }\n  ENSURE PROPOSITION ?p (?alice, \"prefers\", ?dark)\n  CREATE ASSERTION ?a {\n    SET FIELDS { proposition: ?p, asserted_by: ?alice, stance: \"support\", mode: \"stated\", confidence: 0.9 }\n  }\n}"
    ],
    "cases": [
      {
        "name": "a Proposition carries no confidence",
        "command": "FIND(?p.confidence) WHERE { ?p PROPOSITION (?s, \"prefers\", ?o) }",
        "expect": {
          "result": [
            null
          ]
        }
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
        }
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
        }
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
        }
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
      "MUTATE {\n  CREATE CONCEPT ?svc { TYPE \"Service\" NAME \"api\" }\n  CREATE CONCEPT ?healthy { TYPE \"Status\" NAME \"healthy\" }\n  CREATE CONCEPT ?degraded { TYPE \"Status\" NAME \"degraded\" }\n  ENSURE PROPOSITION ?ok (?svc, \"status\", ?healthy)\n  ENSURE PROPOSITION ?bad (?svc, \"status\", ?degraded)\n}"
    ],
    "cases": [
      {
        "name": "nothing on record is insufficient, not rejected",
        "command": "FIND(?b.status) WHERE {\n  ?s CONCEPT {name: \"Alice\"}\n  ?o CONCEPT {name: \"Quiet\"}\n  ?p PROPOSITION (?s, \"prefers\", ?o)\n  ?b BELIEF (?p)\n}",
        "expect": {
          "result": [
            "insufficient"
          ]
        }
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
        }
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
        }
      },
      {
        "name": "and neither is it writable at creation",
        "command": "CREATE CONCEPT ?c { TYPE \"Person\" NAME \"Bob\" SET FIELDS {governance: {classification: \"public\"}} }",
        "expect": {
          "error": "InvalidSyntax"
        }
      },
      {
        "name": "content claiming an authority class has an ordinary attribute and nothing more",
        "command": "FIND(?c.attributes.authority, ?c.governance.max_influence_authority) WHERE { ?c CONCEPT {name: \"Administrator\"} }",
        "expect": {
          "result": [
            [
              "executable",
              null
            ]
          ]
        }
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
        }
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
    "description": "Two independent time axes. FOR TIME asks what was true then; AS OF asks what this Brain held then. A coordinate keeps what was later corrected, retracted or archived, because the record of what was once believed is the point.",
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
        "command": "RETRACT ASSERTION ?a WHERE { ?a ASSERTION {} }",
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
        "command": "SNAPSHOT AS OF SEQ 9999",
        "expect": {
          "error": "HistoricalSnapshotUnavailable"
        }
      },
      {
        "name": "an unknown transaction names no coordinate",
        "command": "FIND(COUNT(?c)) WHERE { ?c CONCEPT {} } AS OF TX \"kip:space:default#9999\"",
        "expect": {
          "error": "TransactionUnknown"
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
        }
      },
      {
        "name": "an Assertion's stance cannot be rewritten in place",
        "command": "UPDATE ?a SET FIELDS { stance: \"reject\" } WHERE { ?a ASSERTION {} }",
        "expect": {
          "error": "InvalidSyntax"
        }
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
        }
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
    "name": "mutation-selection",
    "description": "A mutation may choose what it acts on. The judgement calls an engine has to make here are what this fixture pins down: UPDATE reaches mutable state and nothing else, a bounded sweep takes a documented order, a selection block reads the transaction's starting state, and a merge consolidates identity without copying or erasing anything.",
    "setup": [
      "MUTATE {\n  CREATE CONCEPT ?alice { TYPE \"Person\" NAME \"Alice\" SET FIELDS {key: \"person:alice\"} }\n  CREATE CONCEPT ?dark { TYPE \"Preference\" NAME \"Dark\" }\n  CREATE CONCEPT ?e1 { TYPE \"Experience\" NAME \"First\" SET ATTRIBUTES {goal: \"learn\", outcome_status: \"success\"} SET FACET \"MnemonicState\" {memory_strength: 0.8, salience: 0.5} }\n  CREATE CONCEPT ?e2 { TYPE \"Experience\" NAME \"Second\" SET ATTRIBUTES {goal: \"learn\", outcome_status: \"failure\"} SET FACET \"MnemonicState\" {memory_strength: 0.4, salience: 0.5} }\n  CREATE CONCEPT ?e3 { TYPE \"Experience\" NAME \"Third\" SET ATTRIBUTES {goal: \"rest\", outcome_status: \"success\"} SET FACET \"MnemonicState\" {memory_strength: 0.2, salience: 0.5} }\n  ENSURE PROPOSITION ?p (?alice, \"prefers\", ?dark)\n  CREATE ASSERTION ?a {\n    SET FIELDS { proposition: ?p, asserted_by: ?alice, stance: \"support\", mode: \"stated\", confidence: 0.9 }\n  }\n}"
    ],
    "cases": [
      {
        "name": "a sweep decays a Facet member by reading the target's own value",
        "command": "UPDATE ?m SET FACET \"MnemonicState\" { memory_strength: MUL(?m.facets[\"MnemonicState\"].memory_strength, 0.5) } WHERE { ?m CONCEPT {type: \"Experience\"} } LIMIT 2",
        "expect": {}
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
        "command": "MUTATE {\n  CREATE CONCEPT ?fresh { TYPE \"Experience\" NAME \"Fourth\" SET ATTRIBUTES {goal: \"learn\", outcome_status: \"success\"} }\n  ARCHIVE ?m WHERE { ?m CONCEPT {type: \"Experience\"} }\n}",
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
        "command": "UPSERT CONCEPT ?dup { MATCH {key: \"person:alice-duplicate\"} SET FIELDS {name: \"Alice\"} }",
        "expect": {}
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
        }
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
        }
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
      }
    ]
  }
] as unknown as Fixture[]

/** The total number of cases, so a silent shrink is visible. */
export const CASE_COUNT = 62
