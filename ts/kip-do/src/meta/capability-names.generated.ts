/**
 * The shared capability vocabulary — GENERATED FILE, DO NOT EDIT.
 *
 * Source of truth: `rs/anda_kip/capabilities.json`, read by the Rust engine
 * through `anda_kip::capability_engine_names()`. Regenerate with
 * `pnpm run codegen:capabilities`.
 *
 * Membership is a promise to answer, not a claim of support (§67.4): this
 * engine partitions {@link CAPABILITY_ENGINE_NAMES} into what it implements and
 * what it does not, so `requires` gets `true` or `false` for every name
 * rather than the `unrecognized` that §67.4 makes a failure.
 */

/** The §67.4 registry names, in the Specification's order. */
export const CAPABILITY_REGISTRY_NAMES: readonly string[] = [
  'serializable_isolation',
  'atomic_batch',
  'idempotency_retention',
  'historical_reads',
  'historical_search',
  'semantic_search',
  'hybrid_search',
  'search_index_freshness',
  'belief_slot',
  'weighted_projection',
  'materialized_projection',
  'signed_receipts',
  'ingestion_context',
  'streaming',
  'artifacts',
  'change_stream',
  'filtered_delivery',
  'watch_evaluation',
  'list_dependents',
  'payload_purge',
  'capsule_export',
  'capsule_import',
  'capsule_signatures',
  'derive_permission',
  'record_outcome_permission',
  'kip1_migration',
]

/** The engine-local names every engine in this repository answers. */
export const CAPABILITY_ENGINE_NAMES: readonly string[] = [
  'artifact_store',
  'canonical_matching',
  'capsule_digest_profiles',
  'capsule_export',
  'capsule_import',
  'capsule_restore_mode',
  'capsule_signatures',
  'client_key_retry',
  'deadlines',
  'discover_read_separation',
  'dry_run',
  'exclusive_conflict',
  'governance',
  'grouped_aggregation',
  'historical_read',
  'historical_search',
  'hop_quantifiers',
  'idempotent_replay',
  'ingest',
  'keyword_search',
  'kml',
  'kql',
  'list_dependents',
  'meta',
  'nested_proposition_endpoint',
  'opaque_cursors',
  'ordered_structural',
  'payload_purge',
  'per_operation_receipts',
  'preconditions',
  'projection',
  'readonly_endpoint',
  'retention_expiry',
  'retention_policy',
  'search_over_assertions_and_activities',
  'semantic_search',
  'set_retention',
  'snapshot_at_time',
  'snapshot_token',
  'space_self_identity',
  'structural_core_fields',
  'structural_edge_binding',
  'symbol_lineage',
  'transition',
  'trust_governance',
  'trust_model',
  'unregistered_permissions',
  'version_planes',
]
