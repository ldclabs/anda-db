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
        'PURGE (deny_if_referenced only)',
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
        enforced: 'command scope, and element scope on the read path',
        meaning:
          'every KQL, KML and META command is authorized against the control ' +
          'plane before it runs, and every element a read reaches is authorized ' +
          'again individually. What a *write* touches is not yet checked per ' +
          'element',
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
        audit:
          'every control-plane mutation, plus every decision §172 or a policy ' +
          'obligation asks to record',
      },
      grammar: { parser: parserVersion(), spec_revision: specRevision() },
    },
    unsupported: [
      {
        capability: 'write_scope_authorization',
        detail:
          'the per-element checks a mutation performs: authorizing each element ' +
          'a clause touches, refining the Assertion permission from ' +
          '`asserted_by`, and the standing a retraction needs',
        reason:
          'a write is authorized at command scope only. A Grant narrowed to a ' +
          'kind, a type or a classification gates the statement and does not ' +
          'narrow what that statement may change, so such a Grant is more ' +
          'permissive on the write path than it reads. The read path does check ' +
          'per element, and DESCRIBE ACCESS reports which is which rather than ' +
          'letting a caller infer one granularity from the other',
      },
      {
        capability: 'classification_writes',
        detail:
          'classify / declassify, influence-authority elevation, quarantine, ' +
          'and the upward join of a derived element’s classification',
        reason:
          'a classification label is *read* for every authorization decision — ' +
          'a Grant’s ceiling and scope both consult it — but nothing writes one ' +
          'yet, so every element carries the Space default. `authority_lineage` ' +
          'is recorded at commit, which is a record and not an enforcement',
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
        capability: 'purge_reference_policies',
        detail: 'REFERENCE POLICY "tombstone_reference" and "authorized_cascade"',
        reason:
          'PURGE implements "deny_if_referenced", the conservative default. ' +
          'The other two rewrite or erase the elements that point at the ' +
          'target, which is not something to fall back into silently — they ' +
          'are refused by name',
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
