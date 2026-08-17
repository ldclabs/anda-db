/**
 * What this engine can do, and what it cannot.
 *
 * `DESCRIBE CAPABILITIES` reports both, as structured data. An Agent that has
 * to discover a gap by triggering an error has already wasted a turn, and one
 * that never discovers it will read an absent *feature* as an absent *fact* —
 * "no results" and "this engine cannot answer that" are different answers and
 * only one of them is true.
 *
 * This is the honest half of the engine, so it is maintained as a contract:
 * when a gap is closed, its entry moves out of `unsupported`; when one is
 * found, an entry goes in — with a reason, not just a name.
 */

import type { Json } from '../json.js'
import { specRevision, parserVersion } from '../kip/parser.js'
import { BASELINE_ID } from '../projection/policy.js'

/** The KIP revision this engine implements. */
export const KIP_VERSION = '2.0'

export function capabilities(): Json {
  return {
    kip: KIP_VERSION,
    languages: ['KQL', 'KML', 'META'],
    supported: {
      kml: [
        'CREATE CONCEPT',
        'UPSERT CONCEPT (matching an existing identity)',
        'ENSURE PROPOSITION',
        'CREATE EVIDENCE',
        'CREATE ASSERTION',
        'CREATE ACTIVITY',
        'ASSERT (desugared)',
        'RETRACT ASSERTION',
        'SUPERSEDE ASSERTION',
        'CORRECT EVIDENCE',
        'TRANSITION ACTIVITY',
        'ARCHIVE',
        'TOMBSTONE',
        'UPDATE',
        'MERGE CONCEPT',
        'PURGE, with all three reference policies',
        'selection blocks: WHERE and LIMIT on UPDATE, ARCHIVE, TOMBSTONE, ' +
          'RETRACT, PURGE and MERGE CONCEPT',
      ],
      transaction: {
        // Not a claim about this engine's care, but about the platform: a
        // Durable Object's `transactionSync` either commits the statement whole
        // or rolls it back, shells included.
        atomicity: 'all-or-none per statement, from SQLite',
        versioning: 'one version increment per element per transaction',
        no_effect: 'a transaction that changes nothing takes no Space sequence',
      },
      kql: [
        'CONCEPT',
        'PROPOSITION',
        'ASSERTION',
        'EVIDENCE',
        'ACTIVITY',
        'STRUCTURAL (Profile fields)',
        'BELIEF',
        'BELIEF SLOT',
        'FILTER',
        'NOT',
        'OPTIONAL',
        'UNION',
        'ORDER BY',
        'LIMIT',
        'CURSOR',
        'WITH EPISTEMIC',
        'global aggregates',
      ],
      read: {
        // §52.7: a bounded read may be assumed repeatable only where the
        // runtime documents an order. This one does.
        limit_order: 'the ORDER BY, then ascending element id',
        cursor: 'a non-negative offset over that order',
        default_state: 'active; a pattern naming {state: …} sees the rest',
        null_order: 'nulls last, whichever direction was asked for',
        comparison: 'ordering is defined within one type; across two it is false',
      },
      meta: [
        'DESCRIBE',
        'LIST',
        'VALIDATE KQL',
        'VALIDATE KML',
        'PREVIEW KML',
        'HISTORY',
        'CHANGES',
        'EXPORT CAPSULE',
        'VERIFY CAPSULE',
      ],
      projection: {
        policies: [BASELINE_ID, 'kip:policy:forecast'],
        statuses: ['accepted', 'rejected', 'contested', 'uncertain', 'insufficient'],
        score_semantics: 'normalized_support_not_probability',
      },
      historical_read: {
        // Every commit appends the row it wrote, so a past coordinate could be
        // reconstructed rather than approximated — the log is here, the read
        // path over it is not.
        retention: 'unbounded: every element version is kept',
        available_through: ['HISTORY ELEMENT', 'HISTORY SPACE', 'CHANGES'],
      },
      governance: {
        // The granularity is named rather than implied. A caller that reads
        // "governance: yes" and assumes its classification-scoped Grant narrows
        // what a query returns has been misled by this document, which is worse
        // than being told the plane is absent.
        enforced: 'command scope, and element scope on reads and writes',
        meaning:
          'every KQL, KML and META command is authorized against the control ' +
          'plane before it runs, and every element a command reaches — read or ' +
          'written — is authorized again individually',
        read_scope: {
          visibility:
            'an element outside the Grant is not in the query universe: not ' +
            'matched, not counted, not ranked, and asking for it by id answers ' +
            'as it would for one that was never written',
          field_mask:
            'applied to the view a query caches, so a masked field is invisible ' +
            'to FILTER and ORDER BY as well as to the projection list',
          raw_origin:
            '`_system.origin` needs `read_raw_origin`, and is withheld rather ' +
            'than removed — removing it would claim no origin was recorded',
          projection:
            'an Assertion the caller may not read does not contribute to a belief',
          history:
            'HISTORY and CHANGES narrow to the elements the caller may read',
          export:
            'a Capsule roots only on readable elements and carries the masked view',
        },
        write_scope: {
          per_element:
            'each element a clause touches is authorized on its own kind, type ' +
            'and classification, so a narrowed Grant narrows what a mutation ' +
            'may change and not only whether it may run',
          sweeps:
            'a selection block that reaches an element the caller may not touch ' +
            'fails; it never quietly does less, which would report success ' +
            'having done half the job and would leak what lies outside the Grant',
          attribution:
            'a new Assertion needs `assert`, plus `record_attributed_assertion` ' +
            'or `assert_as_actor` depending on what an ActorBinding says about ' +
            'the writer — never on what the command claims (§17)',
          retraction:
            'RETRACT and SUPERSEDE need standing: the caller wrote the record, ' +
            'or a binding says it represents the actor. ARCHIVE and TOMBSTONE ' +
            'are the honest alternative for anyone else',
          retention:
            'a `retention` block on a creation needs `manage_retention`; the ' +
            'UPDATE path refuses the field outright',
          protected_fields:
            '`_system` and `governance` are refused by the parser, on the text ' +
            'and pre-parsed paths alike, so no mutation can reach them',
        },
        element_operations: {
          // Host APIs on a Session, not KML clauses: an element's `governance`
          // block is not author-writable, and the parser refuses it in every
          // assignment. These are the authorized ways it does change.
          classify:
            'raising a label needs `update`, lowering one needs `declassify` — ' +
            'it is disclosure that requires authority, not caution',
          elevate_authority:
            'raising is bounded by the element’s recorded lineage, so no chain ' +
            'of summarizing turns a descriptive note into an executable one; ' +
            'lowering needs no approval, because a demotion that waited would ' +
            'arrive late',
          quarantine:
            'a state ordinary recall excludes, distinct from `archived` and ' +
            'claiming nothing about whether the source retracted anything',
        },
        erasure: {
          reference_policy:
            'deny_if_referenced by default, plus tombstone_reference and ' +
            'authorized_cascade; an unrecognized one is refused rather than ' +
            'defaulted into a destructive operation the caller did not ask for',
          legal_hold:
            'checked before anything destructive is decided, and placing one ' +
            'needs `legal_hold` rather than `manage_retention` — content that ' +
            'could set its own hold could make itself undeletable',
          order:
            'the version log is destroyed before the row is scrubbed: the other ' +
            'order leaves a stub whose full contents are still readable, with ' +
            'nothing saying to look',
          stub:
            'identity, kind, Space, origin and a content digest survive; ' +
            'deleting the row would leave references pointing at nothing, which ' +
            'does not say "erased" — it says nothing',
          refusal:
            'a denial names how many elements still reference the target, never ' +
            'which: the referrers may be ones the caller cannot read',
        },
        propagation: {
          classification:
            'a derived element joins its inputs’ labels upward at commit, ' +
            'walking an element’s own citations and, the other way, the inputs ' +
            'of any Activity that lists it as an output',
          authority:
            'recorded as `authority_lineage`, not enforced at derivation: ' +
            'everything is created at the bottom of the ladder, so the rule ' +
            'holds until somebody asks to raise a ceiling',
        },
        records: [
          'Principals and Principal groups',
          'ActorBindings',
          'Grants and Delegations',
          'Governance Policy versions',
          'Approvals',
        ],
        resolution:
          'explicit deny, then the least restrictive matching allow — owner, ' +
          'Grant, Delegation or Policy statement — then default deny',
        revocation: 'resolved per command, so a session does not outlive it',
        reports: ['DESCRIBE ACCESS', 'DESCRIBE EXECUTION CONTEXT'],
        audit: {
          records:
            'every control-plane mutation with its whole new record, plus every ' +
            'decision §172 or a policy obligation asks for — allows and denials ' +
            'alike. An ordinary read is not audited: a log that recorded every ' +
            'read would bury the entries that matter',
          reading:
            '`read_audit`, which is separate from reading the Space: a caller ' +
            'who may read the cognition has not earned the right to read who ' +
            'has been reading it',
          history:
            '`read_governance_history` answers who had access at a past instant, ' +
            'from the records’ own timestamps — which is what "revoke, never ' +
            'delete" was for. It is a separate permission from `read_audit`: one ' +
            'is what the control plane was, the other is what people did',
          receipt:
            'a high-impact statement carries the deciding identity, delegation ' +
            'chain and policy version on its receipt (§178); an ordinary write ' +
            'carries none',
        },
      },
      grammar: { parser: parserVersion(), spec_revision: specRevision() },
    },
    unsupported: [
      {
        capability: 'ungated_permissions',
        detail:
          'derive, moderate_assertion, share, bind_canonical_identity, and the ' +
          'control-plane management names: manage_membership, manage_grants, ' +
          'manage_delegation, delegate, manage_actor_binding, manage_trust, ' +
          'manage_schema, approve_high_risk',
        reason:
          'these are registered names that no gate currently asks for, so a ' +
          'Grant listing one confers nothing — the failure mode the registry ' +
          'exists to prevent, named here rather than discovered during an ' +
          'incident. Two different causes. The control-plane management names ' +
          'are host APIs by design: no KML clause reaches the plane, which is ' +
          'what keeps a prompt injection off it, and the consequence is that ' +
          'managing the plane cannot be delegated *through* KIP. The rest name ' +
          'operations this engine does not distinguish yet — setting ' +
          '`canonical_id` currently needs only `update`, and a moderator uses ' +
          'ARCHIVE or TOMBSTONE rather than `moderate_assertion`. The reference ' +
          'engine has the same gap, so closing it is a change both engines make ' +
          'together or the two disagree about what a command costs',
      },
      {
        capability: 'set_retention',
        detail: 'the SET RETENTION clause',
        reason:
          'storage-lifecycle policy is not implemented; the clause is refused ' +
          'by name rather than accepted and ignored. `retention.expires_at` is ' +
          'stored and indexed, so what is missing is the clause that sets it ' +
          'and the sweep that acts on it — not the column',
      },
      {
        capability: 'historical_read',
        detail: 'FIND ... AS OF SEQ | TX | TIME, FOR TIME, SNAPSHOT',
        reason:
          'the element version log is written and readable through HISTORY, ' +
          'but the query path that reconstructs candidates from it is not built',
      },
      {
        capability: 'search',
        detail: 'SEARCH in every mode',
        reason:
          'no search index is built in this engine yet. A keyword search over ' +
          'unsegmented text would silently disagree with the reference ' +
          'engine’s BM25 on which documents match, which is worse than ' +
          'refusing: the caller cannot tell a narrow index from a narrow world',
      },
      {
        capability: 'hop_quantifiers',
        detail: '(?a, "p"{1,3}, ?b)',
        reason: 'transitive traversal is not implemented',
      },
      {
        capability: 'grouped_aggregation',
        detail: 'FIND(?c.name, COUNT(?x))',
        reason:
          'a plain variable projected beside an aggregate would need grouping; ' +
          'answering it as a global aggregate returns one row where the caller ' +
          'asked for one per group',
      },
      {
        capability: 'structural_core_fields',
        detail:
          'STRUCTURAL over an Assertion’s evidence, an Activity’s ' +
          'inputs/outputs, an Evidence record’s source',
        reason:
          'the pattern walks Profile structural fields only. The reverse index ' +
          'holds the answer to "which Assertions cite this Evidence"; the ' +
          'pattern is what does not ask it. The reference engine has the same gap',
      },
      {
        capability: 'capsule_import',
        detail: 'the merge, isolate and restore import modes',
        reason:
          'export and verification are built; the semantic merge is not. A ' +
          'half-built import hands the destination a graph with broken edges ' +
          'and no way to tell',
      },
      {
        capability: 'capsule_signatures',
        detail: 'signing an exported Capsule and verifying a signed one',
        reason:
          'no signing keys; an exported Capsule is unsigned, and its stated ' +
          'source is a claim a destination cannot check. VERIFY reports ' +
          '`signed` separately from `valid` rather than conflating them',
      },
      {
        capability: 'trust_model',
        detail: 'source trust and evidence-quality evaluation in the projection',
        reason:
          'not implemented; every eligible corroboration group counts equally, ' +
          'and every projection says so in its warnings',
      },
      {
        capability: 'atomic_batch',
        detail: 'execution.mode "atomic" over several operations',
        reason:
          'one transaction across several operations is not implemented; a ' +
          'batch runs operation by operation, each atomic on its own',
      },
    ],
  } as Json
}
