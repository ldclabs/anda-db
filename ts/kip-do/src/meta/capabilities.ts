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

/**
 * How deep a `LIST DEPENDENTS` closure this engine will walk (§63.5).
 *
 * The traversal is bounded by construction: each level is a fan-out over the
 * provenance DAG, and a depth nobody bounded is a whole-Space scan wearing a
 * `LIMIT`. A caller that needs further walks the closure a page at a time.
 *
 * Declared here rather than beside the traversal so the number a caller reads
 * from `DESCRIBE CAPABILITIES` is the number the walk actually stops at.
 */
export const MAX_DEPENDENTS_DEPTH = 8

/**
 * The §67.4 capability registry, and what this engine answers for each.
 *
 * A runtime MUST NOT rename these; it MAY add namespaced entries of its own,
 * and this engine's older local names stay beside them below so a `requires`
 * block written against either spelling still gets an answer. `false` is a
 * fact a caller can plan around; a name absent from the registry and from the
 * local list is *unrecognized*, which §67.4 makes a failure rather than a
 * pass.
 */
export const CAPABILITY_REGISTRY: Readonly<Record<string, Json>> = {
  serializable_isolation: true, // §32.2: one Durable Object serializes its callers
  // §34.5: every committed transaction is kept, so a key never expires. The
  // detail rides on the entry rather than in a sibling map, because §67.4
  // shows the value there and a caller that read the two apart could read a
  // ceiling as belonging to the wrong name.
  idempotency_retention: { unbounded: true },
  historical_reads: true, // §48, §100
  historical_search: false, // §66.1: the index keeps no history of itself
  semantic_search: false, // §66.3: no embedding model
  hybrid_search: false, // §66.3
  search_index_freshness: { mode: 'synchronous' }, // §66.5: written by the committing transaction
  belief_slot: true, // §47
  weighted_projection: false, // §21.10: the structural baseline only
  materialized_projection: false, // §21.9: every projection is computed on read
  signed_receipts: false, // §33.3: no signing keys
  ingestion_context: true, // §71.1
  streaming: false, // §84
  artifacts: false, // §85: no artifact store
  change_stream: true, // §36, §68
  filtered_delivery: false, // §36.3: CHANGES is unfiltered
  watch_evaluation: false, // Cognitive Memory Profile §5.11
  list_dependents: true, // §63.5
  payload_purge: true, // §60.6
  capsule_export: true, // §63.4
  capsule_import: false, // §39
  capsule_signatures: false, // §37.8
  derive_permission: false, // §29.6: this engine does not distinguish derived writes
  record_outcome_permission: true, // §29.8
}

/**
 * Ceilings that apply whatever the caller is allowed to ask for (§67).
 *
 * A registry entry's own value is not a limit and does not belong here: it is
 * reported on the entry, where §67.4 puts it.
 */
export const CAPABILITY_LIMITS: Readonly<Record<string, Json>> = {
  list_dependents_max_depth: MAX_DEPENDENTS_DEPTH,
}

/**
 * This engine's own capability names, beside the §67.4 registry (§67).
 *
 * Spelled out rather than derived from the `supported` map, because the map is
 * organized for a reader and this list is a contract: a name here is one a
 * caller may build a fail-fast check on. Kept because `anda-brain` reads them;
 * a name is added, never renamed.
 */
const SUPPORTED_NAMES: readonly string[] = [
  'kql',
  'kml',
  'meta',
  'governance',
  'projection',
  'historical_read',
  'keyword_search',
  'capsule_export',
  'client_key_retry',
  'set_retention',
  'structural_core_fields',
  'ingest',
  'idempotent_replay',
  'preconditions',
  'dry_run',
  'snapshot_token',
  'ordered_structural',
  'structural_edge_binding',
  'exclusive_conflict',
  'space_self_identity',
  'discover_read_separation',
  'retention_expiry',
  'opaque_cursors',
  'payload_purge',
  'list_dependents',
  'transition',
  'version_planes',
  'snapshot_at_time',
  'per_operation_receipts',
  'canonical_matching',
  'symbol_lineage',
]

/**
 * The capability names this engine reports as *not* implemented.
 *
 * Kept beside {@link SUPPORTED_NAMES} so the two cannot drift into claiming and
 * disclaiming the same thing, and checked against the `unsupported` prose list
 * by `test/meta.test.ts`: a gap documented for a reader but missing here would
 * answer a `requires` check as *unrecognized* rather than as absent.
 */
const UNSUPPORTED_NAMES: readonly string[] = [
  'atomic_batch',
  'grouped_aggregation',
  'unregistered_permissions',
  'capsule_digest_profiles',
  'capsule_import',
  'capsule_signatures',
  'historical_search',
  'semantic_search',
  'search_over_assertions_and_activities',
  'hop_quantifiers',
  'nested_proposition_endpoint',
  'trust_model',
  'trust_governance',
  'retention_policy',
  'capsule_restore_mode',
  'deadlines',
  'artifact_store',
]

/**
 * Whether `requires` can answer for a capability name, and how.
 *
 * `undefined` is not "supported by omission" (§67): a fail-fast check that
 * passed because nobody recognized the name is the failure mode the mechanism
 * exists to prevent, because the caller believes it ran.
 */
export function capabilityState(name: string): boolean | undefined {
  // An entry that carries a detail object is supported; only a literal `false`
  // is a refusal. Reading the value's truthiness instead would make a future
  // `{"seconds": 0}` read as unsupported.
  if (Object.hasOwn(CAPABILITY_REGISTRY, name)) {
    return CAPABILITY_REGISTRY[name] !== false
  }
  if (UNSUPPORTED_NAMES.includes(name)) return false
  return SUPPORTED_NAMES.includes(name) ? true : undefined
}

/** The gap names this engine documents, for the drift test. */
export function unsupportedCapabilityNames(): string[] {
  return [
    ...UNSUPPORTED_NAMES,
    ...Object.entries(CAPABILITY_REGISTRY)
      .filter(([, state]) => state === false)
      .map(([name]) => name),
  ]
}

export function capabilities(): Json {
  return {
    kip: KIP_VERSION,
    limits: { ...CAPABILITY_LIMITS },
    // §21.9, §27: what the old DESCRIBE PROJECTION CAPABILITY reported, now a
    // member here (§68).
    projection: {
      policies: [BASELINE_ID, 'kip:policy:forecast'],
      statuses: ['accepted', 'rejected', 'contested', 'uncertain', 'insufficient'],
      leading: ['support', 'opposition', 'none'],
      score_semantics: 'normalized_support_not_probability',
      explanation: true,
      // §21.10: the structural baseline, and nothing weighted on top of it.
      weighted_projection: false,
      missing_stages: [
        {
          stage: 'trust_evaluation',
          reason: 'no trust model; every eligible corroboration group counts equally',
        },
        {
          stage: 'evidence_quality',
          reason:
            'a cited Evidence record is counted for its independence, never ' +
            'for how good it is',
        },
      ],
    },
    // §89 makes declaring the conformance profiles a MUST. A claim, not a
    // wish — each of these is exercised by the shared conformance fixtures both
    // engines run, and the four §89 names that are absent are absent for a
    // reason a caller can check in `unsupported`:
    //
    //   KIP-KQL             §96 requires aggregation, and §44.6 defines it
    //                       with implicit grouping — see `grouped_aggregation`
    //   KIP-Transactions    §94 also requires one transaction across several
    //                       operations — see `atomic_batch`
    //   KIP-Capsule         export and verification are built, import is not
    //   KIP-High-Assurance  this engine signs nothing (§101)
    profiles: [
      'KIP-Core',
      'KIP-Schema',
      'KIP-Epistemic',
      'KIP-Governance',
      'KIP-KML',
      'KIP-META',
      'KIP-Runtime',
      'KIP-Historical',
    ],
    languages: ['KQL', 'KML', 'META'],
    supported: {
      // §67.4: the registry, by the names the Specification fixes, at the
      // address the reference engine reports it from. A registry a client has
      // to look for in a different place on each engine is not a negotiation
      // surface. Reported beside this engine's own names rather than instead
      // of them, because clients read those too.
      registry: { ...CAPABILITY_REGISTRY },
      kml: [
        'CREATE CONCEPT',
        'UPSERT CONCEPT (matching an existing identity)',
        'ENSURE PROPOSITION',
        'CREATE EVIDENCE',
        'CREATE ASSERTION',
        'CREATE ACTIVITY',
        'ASSERT (desugared, SUPERSEDING included)',
        'TRANSITION TO retracted | superseded | corrected | running | ' +
          'completed | failed | cancelled | archived | tombstoned (§52.5)',
        'SET RETENTION',
        'UPDATE',
        'MERGE CONCEPT',
        'PURGE, with all three reference policies',
        'PURGE PAYLOAD',
        // §52.7 names exactly these five. `MERGE CONCEPT` is deliberately not
        // among them: its source and target are already named, and its WHERE
        // only guards them — so it takes no LIMIT, and the grammar has no
        // field for one.
        'selection blocks: WHERE and LIMIT on UPDATE, TRANSITION, SET ' +
          'RETENTION, PURGE and PURGE PAYLOAD; WHERE alone on MERGE CONCEPT',
        'EXPECT VERSION n [OF ATTRIBUTES | STRUCTURAL | RETENTION | FACET ' +
          '"<symbol>"], one guard per plane, trailing (§35.1, §52.8)',
      ],
      // §52.5: the one lifecycle statement, and what each move needs.
      transition: {
        states: {
          retracted: 'Assertion, from active; retract_own and standing (§57.3)',
          superseded:
            'Assertion BY the newer Assertion, from active, same Proposition; ' +
            'supersede_own and standing (§57.4)',
          corrected: 'Evidence BY the new Evidence, from active; create + maintain (§57.2)',
          running: 'Activity, from pending; update',
          'completed | failed | cancelled':
            'Activity, from pending or running; update; SET FIELDS / SET ' +
            'STRUCTURAL finalize started_at, ended_at, parameters_digest and topology',
          archived: 'any element; archive, plus moderate_assertion over another actor\'s Assertion',
          tombstoned: 'any element; tombstone, plus moderate_assertion likewise',
        },
        wrong_move:
          'InvalidLifecycleTransition with details.from / details.to; a move ' +
          'to the state already held is no_effect; from a terminal Activity ' +
          'state, ActivityTerminal',
        parameter_state:
          'a :parameter state is checked for BY and SET at execution time under ' +
          'InvalidSyntax, and each selected element is authorized with the ' +
          'permission the literal form would have paid',
      },
      // §6.3, §35.1: the four planes, and what moves each.
      version_planes: {
        planes: ['attributes', 'structural', 'retention', 'facets.<Symbol>'],
        attributes: 'Core fields and a Concept\'s attributes',
        structural: 'Structural References, Core and Profile alike',
        retention: 'the retention record',
        facets: 'one counter per Facet, keyed by its local symbol name',
        lifecycle: 'advances _system.version and no plane, unless it finalizes one',
        guard:
          'EXPECT VERSION n OF <plane> compares that counter; a mismatch names ' +
          'the plane in VersionConflict.details.plane; EXPECT VERSION 0 OF ' +
          '<plane> means never written, and only the bare EXPECT VERSION 0 is ' +
          'create-only (§35.2)',
        read: '?x._system.plane_versions.attributes, .structural, .retention, .facets["<Symbol>"]',
      },
      // §11: identity consolidation is non-destructive, and the three rules
      // that make it so are stated because a caller who assumed any of them
      // backwards would read a forwarded write as a lost one.
      merge: {
        source: 'stays addressable, in state `merged`, forwarding via merged_into',
        history:
          'a Proposition written before the merge keeps referring to what it ' +
          'referred to (§11.2); raw history is not rewritten',
        new_writes:
          'canonicalized to the surviving identity (§11.3) — tuple endpoints, ' +
          'asserted_by, and structural references on create and on SET STRUCTURAL',
        cycles:
          'a merge whose target already resolves back to the source is refused ' +
          '(§11.1), so following merged_into to its fixpoint always terminates',
      },
      transaction: {
        // Not a claim about this engine's care, but about the platform: a
        // Durable Object's `transactionSync` either commits the statement whole
        // or rolls it back, shells included.
        atomicity: 'all-or-none per statement, from SQLite',
        versioning: 'one version increment per element per transaction',
        no_effect: 'a transaction that changes nothing takes no Space sequence',
        // §26, §33, and this is the block a retry policy reads: a timeout is
        // not an abort, so a resend under a key this Space already committed
        // hands back that transaction's receipt instead of writing again.
        idempotency: {
          mode: 'replayed',
          scope:
            'per Space; an operation’s own key wins over the request’s, so a ' +
            'batch sharing one key does not have its second write replay the ' +
            'first',
          answer:
            'the recorded receipt — same tx_id, space_seq, committed_at and ' +
            'handles — plus a warning saying it is a replay, because the ' +
            'caller resent precisely to find out whether the first attempt ' +
            'landed',
          warnings:
            'the original run’s own warnings are not persisted and are not ' +
            'reconstructed; inventing them would be worse than saying nothing',
          dry_run:
            'never replays and is never replayed: a preview establishes no ' +
            'durable commit (§69.3), and answering one from an earlier real ' +
            'commit would report a write as a preview of itself',
          authorization:
            'the command’s own permissions, checked as they would be for the ' +
            'write; an outstanding approval obligation does not block a ' +
            'replay, because the approval authorized work that already happened',
        },
      },
      kql: [
        'CONCEPT',
        'PROPOSITION',
        'ASSERTION',
        'EVIDENCE',
        'ACTIVITY',
        'STRUCTURAL (Profile fields and Core fields alike)',
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
      epistemic: {
        // Every member `WITH EPISTEMIC` accepts, and what each does — an
        // unlisted one is refused (SchemaFieldNotFound), so the list is the
        // contract rather than a sample.
        settings: [
          'policy',
          'accept',
          'material',
          'modes',
          'include_hypothetical',
          'include_predicted',
          'explanation',
          'purpose',
          'risk',
          'include_historical',
        ],
        settings_note:
          '`purpose` and `risk` are the caller’s own non-authoritative context ' +
          'and do not move a verdict; `include_historical` is accepted and ' +
          'refused, because admitting retracted and superseded Assertions ' +
          'would let a withdrawn claim decide a current belief',
        explanation_levels: ['none', 'summary', 'ledger'],
        // §25.1 and §92: both conflict shapes, not just the strong one.
        conflicts: ['functional', 'exclusive values'],
      },
      paging: {
        // §44.8 and §88.4: a cursor is opaque, carries the coordinate the
        // traversal began at, and belongs to the family that issued it. Each
        // family issues its own, because a family that accepts one and never
        // hands one out cannot be paged at all.
        cursor: 'opaque token, snapshot-pinned, per operation family',
        families: ['kql', 'search', 'list', 'history'],
        // §87.7: one code for every family, with the family and the reason
        // in `details`. A change cursor is a sequence and not a token, so its
        // refusal is `family: changes`.
        refusal:
          'CursorInvalid {family, reason: malformed | access_revoked | ' +
          'schema_changed}; CursorExpired {family, reason: expired}',
      },
      structural: {
        // §8.2 and §17: the pattern reads both planes. A Profile field is
        // addressed by its resolved symbol, a Core one by its plain name, and
        // `?edge.field` says which answered — so a Profile that declares a
        // field named `evidence` adds edges rather than changing what an
        // Assertion cites.
        planes: {
          profile: 'addressed by resolved symbol; ordered where declared',
          core:
            'Assertion.evidence and .context, Evidence.source and ' +
            '.generated_by, Activity.inputs, .outputs and ' +
            '.associated_actors — addressed by plain name, and reporting no ' +
            '`index`, because their order is storage order rather than a ' +
            'declared position',
        },
        // §17.4: an ordered field keeps one dense zero-based order per source
        // element, and exposes each reference's position.
        ordered_fields: true,
        edge_binding:
          '?edge STRUCTURAL (...) binds virtual edge state carrying source, ' +
          'field, target and index',
        single_cardinality: 'SET STRUCTURAL replaces rather than appends',
      },
      envelope: {
        client_key:
          'a CREATE under a client_key already used resolves to that element ' +
          'instead of creating a second (§52.1)',
        // §71.1, and §88.12 is the reason: a model retyping an observation
        // into command text truncates it, normalizes it, or paraphrases it,
        // and the record then says the source said something it did not.
        ingest: {
          mints: 'Evidence, from the payload the transport carried',
          binds: 'each entry as :key, so the command cites rather than retypes',
          scope:
            'inside the statement’s own transaction, so an aborted statement ' +
            'takes its ingested Evidence with it',
          retry:
            'an entry’s client_key resolves to the Evidence the first attempt ' +
            'minted, exactly as CLIENT KEY does on a CREATE (§52.1)',
          source_actor:
            'an element reference, {id} or {type, key} — the key resolved ' +
            'through the Concept Type lineage — and refused rather than stored ' +
            'as a name nothing resolves; a bare string is InvalidRequestEnvelope',
          facets:
            'a map from Facet name to value object, validated exactly as SET ' +
            'FACET on CREATE EVIDENCE; an outcome entry needs record_outcome (§29.8)',
          payload_artifact:
            'refused — see `artifact_store`; a handle would name bytes this ' +
            'engine cannot read',
        },
      },
      // §36.1, §68.1: HISTORY and CHANGES are the same unit — one committed
      // transition — asked for over different ranges, so they answer in one
      // shape. Stated because a consumer that assumed a flat change list would
      // lose the atomicity §36.2 guarantees and the deduplication key §36.3
      // needs.
      change_stream: {
        grain: 'one Change Envelope per committed transition',
        envelope: [
          'space_id',
          'space_seq',
          'tx_id',
          'committed_at',
          'transaction_class',
          'snapshot_seq',
          'status',
          'schema_environment_version',
          'changes',
        ],
        // §36.1, schemas/kip-change-envelope.schema.json: names and versions,
        // never values.
        change: {
          always: ['op', 'kind', 'id', 'new_version'],
          op: ['create', 'update', 'lifecycle', 'retention', 'merge', 'purge', 'payload_purge'],
          old_version: 'when the element existed',
          state: '{from, to} on a lifecycle entry',
          schema_ref: 'on a Concept entry',
          refs: 'proposition on an Assertion; subject and predicate_ref on a Proposition; merged_into on a merge source',
          touched:
            'paths only, sorted: fields.<name>, attributes.<name>, ' +
            'structural.<name>, facets.<name>, retention, state, ' +
            'governance.<member> — Profile symbols by their local name, and ' +
            'empty on a create, whose paths would only repeat the row',
          planes:
            'the element\'s plane counters after the commit, on every entry ' +
            'that moved a plane; a lifecycle move or a merge that only ' +
            're-pointed identity moved none, and reports none',
        },
        deduplicate_by: 'space_id + space_seq + tx_id',
        shared_by: ['HISTORY ELEMENT', 'HISTORY SPACE', 'CHANGES'],
        // The cursor is the coordinate the page consumed, issued whenever it
        // consumed one — not only when the stream was truncated, and never
        // taken from the rows that survived the visibility filter.
        cursor: 'the last space_seq consumed, opaque to nobody',
      },
      // §60.6: the data-minimization instrument. Byte destruction that keeps
      // the evidence event, which is a different promise from element purge and
      // worth stating as one.
      payload_purge: {
        targets: 'Evidence only',
        destroys:
          'inline payload and content_ref bytes, in the current row and in ' +
          'every recorded version',
        keeps:
          'identity, evidence_class, content_digest, media_type, observed_at, ' +
          'source, generated_by, citations',
        reports: 'payload.mode becomes "purged"',
        repeat: 'purging an already-purged payload is a no_effect',
      },
      // §63.5: what this engine actually traverses, stated because the
      // Structural-Field extension is optional and an Agent that assumed it
      // would read a missing route as an absent dependent.
      dependents: {
        traverses: 'Activity inputs -> Activity -> Activity outputs',
        structural_lineage: false,
        default_depth: 1,
        max_depth: MAX_DEPENDENTS_DEPTH,
        row: ['id', 'kind', 'distance', 'via.activity'],
        note:
          'a transformation that recorded no Activity provenance is not ' +
          'discoverable here',
      },
      retention: {
        // §19.2: this is storage lifecycle, never world validity.
        hook: ['retention_class', 'expires_at', 'legal_hold'],
        expiry:
          'enforced by an explicit sweep the host runs, not by a background ' +
          'alarm: forgetting happens when a Principal asks for it and is ' +
          'accountable for it',
        actions: ['archive', 'tombstone'],
        // §19.1 and §60.3. Stated because the replacement semantics and the gate are one
        // contract: a caller who read only the first would expect an omitted
        // `legal_hold` to leave the hold alone.
        set:
          'SET RETENTION replaces the whole block rather than patching it, so ' +
          'an omitted member is cleared',
        legal_hold:
          'gated in both directions — placing a hold needs `manage_legal_hold`, ' +
          'and so does any SET RETENTION over an element that currently holds ' +
          'one, because replacement would otherwise lift it silently (§29.9)',
      },
      capsule: {
        // §37.7, and the same profile rs/anda_cognitive_nexus writes: a
        // Capsule is the one artifact that crosses between engines, so the
        // algorithm is part of the contract rather than an engine choice. The
        // two are pinned to one digest by a literal in each engine's tests.
        digest_profile: 'sha3-256 over RFC 8785 canonical JSON',
        closure: ['closed', 'referential', 'selective'],
      },
      space: {
        // §5.6, reported by DESCRIBE PRIMER as §64.2 requires.
        self_identity: 'protected Space configuration, set through a host API',
      },
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
        'LIST DEPENDENTS',
        'SEARCH CONCEPT | PROPOSITION | EVIDENCE | COGNITION, keyword mode',
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
      search: {
        modes: ['keyword'],
        kinds: ['Concept', 'Proposition', 'Evidence', 'Cognition'],
        ranking: 'SQLite FTS5 BM25 over segmented text',
        // Not the same numbers as the Rust engine's, and saying so is the
        // point: both are BM25, over the same corpus, under different
        // dictionaries. A caller may compare scores *within* one answer and
        // never across engines.
        score_semantics: 'bm25_relevance_not_confidence',
        segmentation:
          'Intl.Segmenter (ICU dictionary) in process, then FTS5 unicode61; ' +
          'the write and read paths run the same function',
        // §66.5 and §79: a derived recall surface has to declare its freshness,
        // and this one can declare the strongest form because the index is
        // written inside the same transaction as the row.
        consistency: 'index is maintained synchronously with commits',
        miss_semantics: 'a miss is not an absence; confirm with FIND',
      },
      historical_read: {
        retention: 'unbounded: every element version is kept',
        // §48.1: AS OF SEQ is the only historical axis. A transaction id
        // resolves through DESCRIBE TRANSACTION and an instant through
        // DESCRIBE SNAPSHOT AT TIME, so a read always names the exact
        // coordinate it was served from.
        available_through: [
          'FIND ... AS OF SEQ',
          'read.snapshot_token',
          'DESCRIBE SNAPSHOT [AS OF SEQ | AT TIME]',
          'DESCRIBE SCHEMA ENVIRONMENT AS OF SEQ',
          'HISTORY ELEMENT',
          'HISTORY SPACE',
          'CHANGES',
        ],
        snapshot:
          'space_id, space_seq, tx_id, committed_at, schema_environment_version ' +
          'and the snapshot_token a later read binds to; AT TIME resolves to ' +
          'the last sequence committed at or before the instant, 0 before the ' +
          'first commit',
        coordinate:
          'one read answers at one coordinate; a request bound by a snapshot ' +
          'token whose command names a different one is refused rather than ' +
          'resolved, because the answer’s own snapshot_seq could not say which ' +
          'it meant',
        out_of_range:
          'a coordinate the Space has not reached is refused, never rounded to ' +
          'the present; a coordinate before anything existed is an empty Space ' +
          'and not an error',
        schema:
          'symbols resolve through the Schema Environment in force at the ' +
          'coordinate (§20.9), never today’s',
        projection:
          'a belief at a coordinate is projected from the Assertions of that ' +
          'coordinate',
        cost:
          'the indexes describe the present, so a historical pattern ' +
          'reconstructs candidates from the version log and re-checks every ' +
          'constraint against the reconstructed row. Charged to the same query ' +
          'budget, so it refuses rather than stalls',
        authorization:
          'a past coordinate is not a way around the present’s authorization: ' +
          'the read is happening now, by this caller, and every reconstructed ' +
          'element goes through the same visibility check',
      },
      valid_time: {
        // A different axis from `AS OF`, and the two never default from each
        // other: what was *true* then is not what this Brain *held* then.
        for_time:
          'FOR TIME narrows to the Assertions whose `valid_time` covers an ' +
          'instant; it reads neither `asserted_at` nor the engine sequence',
        applies_to:
          'Assertions only — a Concept has no validity interval to be outside of',
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
            'TRANSITION TO retracted or superseded needs standing: the caller ' +
            'wrote the record, or a binding says it represents the actor. ' +
            'TRANSITION TO archived or tombstoned is the honest alternative ' +
            'for anyone else',
          outcome:
            'outcome-class Evidence and an outcome_observation Activity need ' +
            'record_outcome on top of create, and never derive (§29.8)',
          legal_hold:
            'setting or lifting retention.legal_hold needs manage_legal_hold, ' +
            'distinct from manage_retention (§29.9)',
          authority_class:
            'governance.authority_class is read as descriptive when unset, ' +
            'written only through the elevate_authority host API, and refused ' +
            'as ProtectedGovernanceField from KML (§31.3)',
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
        reports: [
          'DESCRIBE ACCESS (with elevatable_authority_classes)',
          'DESCRIBE PRIMER execution_context',
          'DESCRIBE SPACE execution_context',
        ],
        audit: {
          records:
            'every control-plane mutation with its whole new record, plus every ' +
            'decision §29 or a policy obligation asks for — allows and denials ' +
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
            'chain and policy version on its receipt (§33.1); an ordinary write ' +
            'carries none',
        },
      },
      // §33.2, §75: every state-changing operation answers with its own
      // Receipt.
      receipts: {
        where: 'results[].receipt in independent and sequence modes; the top-level receipt only in atomic, which this engine has no',
        members: [
          'status',
          'tx_id',
          'space_id',
          'snapshot_seq',
          'space_seq (absent on no_effect)',
          'committed_at (absent on no_effect)',
          'transaction_class',
          'schema_environment_version',
          'receipt_digest',
          'origin {principal_id, actor_binding_id, delegation_digest}',
        ],
        receipt_digest:
          'sha3-256 over RFC 8785 canonical JSON of the Receipt without ' +
          'receipt_digest, proofs and extensions, spelled sha3-256:<hex> — ' +
          'the profile a Capsule digest already carries (§37.7)',
        origin:
          'the authenticated Principal; the ActorBinding the statement spoke ' +
          'through, or null; sha3-256:<hex> over the canonical JSON of the ' +
          'delegation chain, or null when the request ran on none',
        idempotency: 'execution.idempotency_key is echoed on the response',
        on_error: 'stop by default; operations not started are reported skipped',
      },
      // §9.2, §9.4, §9.6: what a Literal is here.
      literals: {
        datatypes: ['string', 'number', 'boolean', 'null'],
        canonical:
          'strings NFC-normalized, never trimmed or case-folded; numbers by ' +
          'mathematical value, -0 as 0',
        language: 'a `language` member is TypeMismatch',
      },
      // §12.3, §43.2: canonical matching through merges.
      canonical_matching:
        'a tuple pattern naming a Concept matches tuples recorded on any ' +
        'Concept merged into the same identity; ?p.subject / ?p.object are the ' +
        'stored endpoints, ?p.canonical_subject / ?p.canonical_object the ' +
        'merge-resolved ones; AS OF SEQ before the merge resolves nothing through it',
      // §20.14: identity and matching compare lineages.
      symbol_lineage:
        'one version per package path is active at a time; key uniqueness, ' +
        'the Proposition tuple key, type: and predicate matching are computed ' +
        'from kip://<path>/<Symbol>, so an upgrade never splits memory; ' +
        'schema_ref and predicate_ref stay exact',
      grammar: { parser: parserVersion(), spec_revision: specRevision() },
    },
    unsupported: [
      {
        capability: 'capsule_digest_profiles',
        detail:
          'verifying a Capsule digested under an algorithm other than sha3-256',
        reason:
          'this engine digests a Capsule as sha3-256 over RFC 8785 canonical ' +
          'JSON, and so does rs/anda_cognitive_nexus — the two interoperate. ' +
          'An artifact from somewhere else under another profile is refused ' +
          'as an unsupported profile rather than reported as a digest ' +
          'mismatch, because the second is an accusation of tampering and the ' +
          'first is the truth',
      },
      {
        capability: 'unregistered_permissions',
        detail: 'derive, share, manage_trust, approve',
        reason:
          '§29.6 requires a runtime that does not distinguish derived writes ' +
          'to refuse `derive` where a Grant names it, and the same reasoning ' +
          'covers the others: a permission that is accepted and gates ' +
          'nothing is authority that looks conferred and is not, discovered ' +
          'during an incident. So these names are not in the registry at all ' +
          'and a Grant listing one is rejected where it is written. `share` ' +
          'and `manage_trust` name operations this engine has no surface for ' +
          '— no controlled cross-Space view to expose, no trust policy to ' +
          'version — and `approve` is what this engine spells ' +
          '`approve_high_risk`. `derive` is the one that is a judgement rather ' +
          'than an absence: §29.6 triggers it on an element recorded as an ' +
          'output of an Activity that has at least one input, and this engine ' +
          'does not make that distinction at the gate yet; `derive_permission` ' +
          'in the §67.4 registry says so. `record_outcome` and ' +
          '`manage_legal_hold` are registered because a gate asks for each. ' +
          'Every other registered name is asked for by a gate, which is the ' +
          'property this entry exists to report the exceptions to',
      },
      {
        capability: 'semantic_search',
        detail: 'SEARCH ... MODE "semantic" | "hybrid"',
        reason:
          'this engine has no embedding model, so there is nothing to compare ' +
          'vectors against; keyword search is built and is the portable ' +
          'baseline §66.3 asks for',
      },
      {
        capability: 'historical_search',
        detail: 'SEARCH ... AS OF SEQ',
        reason:
          'the index is maintained against current state and keeps no history ' +
          'of itself; answering from today’s index under a past coordinate ' +
          'would be searching the present and calling it the past (§66.1)',
      },
      {
        capability: 'search_over_assertions_and_activities',
        detail: 'SEARCH ASSERTION | ACTIVITY',
        reason:
          'an Assertion carries a stance, a mode and a number, and an Activity ' +
          'a class and two timestamps — neither has free text to index. ' +
          'Refusing says so; an empty answer would read as “no such claim ' +
          'exists”. Reach them through the Proposition or Evidence they are about',
      },
      {
        capability: 'hop_quantifiers',
        detail: '(?a, "p"{1,3}, ?b)',
        reason: 'transitive traversal is not implemented',
      },
      {
        capability: 'nested_proposition_endpoint',
        detail: '?meta (?p, "contradicts", (id: :other_proposition_id))',
        reason:
          '§43.2 makes the id form usable wherever a triple is, including as a ' +
          'tuple endpoint — which is how a statement about a statement names ' +
          'an existing Proposition. Not built here. Refused rather than ' +
          'ignored: an endpoint nobody constrained matches every tuple under ' +
          'its predicate, which is a wrong answer wearing the shape of a right ' +
          'one. An object endpoint still resolves through {id: …} or ' +
          '{canonical_id: …}. The reference engine has the same gap',
      },
      {
        capability: 'grouped_aggregation',
        detail: 'FIND(?c.name, COUNT(?x)) and ORDER BY COUNT(?x)',
        reason:
          'a plain variable projected beside an aggregate, or an aggregate used ' +
          'as a sort key, needs grouping. Answering either without it returns ' +
          'one global row where the caller asked for one per group, or sorts by ' +
          'the bare variable instead of the aggregate. The reference engine has ' +
          'the same gap',
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
        capability: 'trust_governance',
        detail: 'DESCRIBE TRUST',
        reason:
          'the trust policy binding is Governance state, but this engine ' +
          'evaluates no source trust, so there is no trust judgement to ' +
          'report — see `trust_model`. Named here as well as there so a ' +
          '`requires` block written against either engine gets an answer ' +
          'rather than an unrecognized name',
      },
      {
        capability: 'retention_policy',
        detail:
          'Space-level retention defaults by kind, type or classification ' +
          '(§19.1)',
        reason:
          'retention is set per element and enforced per element; a Space ' +
          'cannot yet declare that raw Experiences expire in 90 days and ' +
          'audit records in 7 years. `SET RETENTION` and the expiry sweep are ' +
          'both built — what is missing is the default a new element would ' +
          'inherit',
      },
      {
        capability: 'capsule_restore_mode',
        detail: 'the "restore" import mode (§39.4)',
        reason:
          'no import mode is built here at all — see `capsule_import`. Named ' +
          'separately because the reference engine builds the others and not ' +
          'this one, so a `requires` block asking about restore gets the same ' +
          'answer from both',
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
          'batch runs operation by operation, each atomic on its own. Asking ' +
          'for it is refused rather than run as a sequence that looks like one',
      },
      {
        capability: 'artifact_store',
        detail: 'ArtifactRef handles (§85), including `ingest.payload_artifact`',
        reason:
          'there is no artifact store, so a handle would name bytes this ' +
          'engine cannot read. Minting an Evidence record with an empty ' +
          'payload under one would be exactly the fabrication the mechanism ' +
          'exists to prevent',
      },
      {
        capability: 'deadlines',
        detail: 'options.deadline_ms (§80.1)',
        reason:
          'a statement runs to completion inside the Durable Object and is ' +
          'not cancellable mid-commit, and §80.2 is explicit that a client ' +
          'timeout is not an abort. Accepting the deadline would promise a ' +
          'cancellation that never happens',
      },
    ],
  } as Json
}
