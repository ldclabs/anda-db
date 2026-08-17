/**
 * The KIP 2.0 Core Error Registry — GENERATED FILE, DO NOT EDIT.
 *
 * Source of truth: `KipErrorCode` in `rs/anda_kip/src/error.rs`, read through
 * `anda_kip_wasm::error_catalog()`.
 * Regenerate with `pnpm run codegen:errors` after changing the Rust registry.
 *
 * Grammar version: 0.12.0
 */

/** The coarse family an error belongs to (Spec §86.2). */
export type KipErrorCategory =
  | "artifact"
  | "data"
  | "epistemic"
  | "governance"
  | "history"
  | "protocol"
  | "resource"
  | "schema"
  | "search"
  | "syntax"
  | "system"
  | "transaction"

/** What kind of retry, if any, can make progress (Spec §86.3). */
export type KipRetryClass =
  | "non_retryable"
  | "outcome_lookup_required"
  | "requires_authority"
  | "requires_different_input"
  | "requires_new_snapshot"
  | "requires_reacquire_artifact"
  | "requires_refresh"
  | "safe_same_request"

/**
 * Every registered error code (Spec §87).
 *
 * KIP 2.0 codes are stable *names*, not the numbers 1.x used: an Agent
 * switching on `EpistemicRevisionRequired` keeps working across protocol
 * revisions in a way a renumbered `KIP_3007` would not.
 */
export type KipErrorCode =
  | "InvalidSyntax"
  | "InvalidIdentifier"
  | "InvalidRequestEnvelope"
  | "UnsupportedProtocolVersion"
  | "UnsupportedCapability"
  | "UnsupportedIsolation"
  | "LanguageMismatch"
  | "ReadonlyViolation"
  | "DuplicateLocalHandle"
  | "DuplicateMutationTarget"
  | "SchemaSymbolNotFound"
  | "SchemaSymbolAmbiguous"
  | "SchemaFieldNotFound"
  | "SchemaPackageUnavailable"
  | "SchemaEnvironmentChanged"
  | "HistoricalSchemaUnavailable"
  | "TypeMismatch"
  | "ConstraintViolation"
  | "NotFoundOrNotVisible"
  | "ReferenceError"
  | "StructuralReferenceInvalid"
  | "IdentitySelectorRequired"
  | "NameIdentityForbidden"
  | "IdentityConflict"
  | "ClientKeyConflict"
  | "IdentityMergeConflict"
  | "ImmutableField"
  | "EpistemicRevisionRequired"
  | "EvidenceCorrectionRequired"
  | "InvalidLifecycleTransition"
  | "RetractionNotAuthorized"
  | "SupersessionMismatch"
  | "EvidenceCorrectionConflict"
  | "ActivityTerminal"
  | "ProjectionTargetUnbound"
  | "ProjectionTargetUnbounded"
  | "ProjectionNotAuthorized"
  | "ProjectionPolicyUnavailable"
  | "Unauthenticated"
  | "NotAuthorized"
  | "RequiresApproval"
  | "RequiresStrongerAuthentication"
  | "ActorBindingRequired"
  | "ProtectedSystemField"
  | "ProtectedGovernanceField"
  | "ProtectedSchemaState"
  | "LegalHoldConflict"
  | "PurgeDenied"
  | "VersionConflict"
  | "PreconditionFailed"
  | "SerializationConflict"
  | "IdempotencyConflict"
  | "TransactionUnknown"
  | "OutcomeUnknown"
  | "TransactionTooLarge"
  | "HistoricalSnapshotUnavailable"
  | "CursorMismatch"
  | "CursorTypeMismatch"
  | "CursorExpired"
  | "CursorInvalidated"
  | "ChangeCursorExpired"
  | "ChangeCursorInvalid"
  | "SearchModeUnsupported"
  | "SearchIndexUnavailable"
  | "HistoricalSearchUnavailable"
  | "ArtifactUnavailable"
  | "ArtifactTooLarge"
  | "ArtifactParseError"
  | "DigestMismatch"
  | "ProofInvalid"
  | "SignerUnknown"
  | "BlobUnavailable"
  | "CapsuleValidationFailed"
  | "ImportPreviewConflict"
  | "ResourceExhausted"
  | "ResultLimitExceeded"
  | "ExecutionTimeout"
  | "RateLimited"
  | "InternalError"

export const KIP_ERROR_CODES: readonly KipErrorCode[] = [
  "InvalidSyntax",
  "InvalidIdentifier",
  "InvalidRequestEnvelope",
  "UnsupportedProtocolVersion",
  "UnsupportedCapability",
  "UnsupportedIsolation",
  "LanguageMismatch",
  "ReadonlyViolation",
  "DuplicateLocalHandle",
  "DuplicateMutationTarget",
  "SchemaSymbolNotFound",
  "SchemaSymbolAmbiguous",
  "SchemaFieldNotFound",
  "SchemaPackageUnavailable",
  "SchemaEnvironmentChanged",
  "HistoricalSchemaUnavailable",
  "TypeMismatch",
  "ConstraintViolation",
  "NotFoundOrNotVisible",
  "ReferenceError",
  "StructuralReferenceInvalid",
  "IdentitySelectorRequired",
  "NameIdentityForbidden",
  "IdentityConflict",
  "ClientKeyConflict",
  "IdentityMergeConflict",
  "ImmutableField",
  "EpistemicRevisionRequired",
  "EvidenceCorrectionRequired",
  "InvalidLifecycleTransition",
  "RetractionNotAuthorized",
  "SupersessionMismatch",
  "EvidenceCorrectionConflict",
  "ActivityTerminal",
  "ProjectionTargetUnbound",
  "ProjectionTargetUnbounded",
  "ProjectionNotAuthorized",
  "ProjectionPolicyUnavailable",
  "Unauthenticated",
  "NotAuthorized",
  "RequiresApproval",
  "RequiresStrongerAuthentication",
  "ActorBindingRequired",
  "ProtectedSystemField",
  "ProtectedGovernanceField",
  "ProtectedSchemaState",
  "LegalHoldConflict",
  "PurgeDenied",
  "VersionConflict",
  "PreconditionFailed",
  "SerializationConflict",
  "IdempotencyConflict",
  "TransactionUnknown",
  "OutcomeUnknown",
  "TransactionTooLarge",
  "HistoricalSnapshotUnavailable",
  "CursorMismatch",
  "CursorTypeMismatch",
  "CursorExpired",
  "CursorInvalidated",
  "ChangeCursorExpired",
  "ChangeCursorInvalid",
  "SearchModeUnsupported",
  "SearchIndexUnavailable",
  "HistoricalSearchUnavailable",
  "ArtifactUnavailable",
  "ArtifactTooLarge",
  "ArtifactParseError",
  "DigestMismatch",
  "ProofInvalid",
  "SignerUnknown",
  "BlobUnavailable",
  "CapsuleValidationFailed",
  "ImportPreviewConflict",
  "ResourceExhausted",
  "ResultLimitExceeded",
  "ExecutionTimeout",
  "RateLimited",
  "InternalError",
]

/** One registry entry. */
export interface KipErrorSpec {
  category: KipErrorCategory
  retry: KipRetryClass
  /**
   * Agent-facing recovery instruction. This is what makes KIP errors
   * self-correcting; it is part of the wire contract, not a developer comment.
   */
  hint: string
}

export const KIP_ERROR_REGISTRY: Readonly<Record<KipErrorCode, KipErrorSpec>> = {
  "InvalidSyntax": {
    category: "syntax",
    retry: "requires_different_input",
    hint: "Check bracket matching, keyword spelling and clause order. Run `VALIDATE KQL`/`VALIDATE KML` on the text before re-sending.",
  },
  "InvalidIdentifier": {
    category: "syntax",
    retry: "requires_different_input",
    hint: "Identifiers must match `[A-Za-z_][A-Za-z0-9_]*`.",
  },
  "InvalidRequestEnvelope": {
    category: "protocol",
    retry: "requires_different_input",
    hint: "Check the envelope: `kip` version, `operations[]` shape, and that `execution.mode` is one of independent, sequence, atomic.",
  },
  "UnsupportedProtocolVersion": {
    category: "protocol",
    retry: "non_retryable",
    hint: "Run `DESCRIBE PROTOCOL` to learn which protocol versions this runtime speaks.",
  },
  "UnsupportedCapability": {
    category: "protocol",
    retry: "non_retryable",
    hint: "Run `DESCRIBE CAPABILITIES` and request only what is both supported and available.",
  },
  "UnsupportedIsolation": {
    category: "protocol",
    retry: "non_retryable",
    hint: "Run `DESCRIBE CAPABILITIES` for the isolation levels this runtime offers.",
  },
  "LanguageMismatch": {
    category: "protocol",
    retry: "requires_different_input",
    hint: "The `language` label must match the command's real semantics; a KML write cannot be labelled KQL.",
  },
  "ReadonlyViolation": {
    category: "protocol",
    retry: "requires_different_input",
    hint: "This endpoint executes KQL and META only. Re-send state-changing KML through the general runtime.",
  },
  "DuplicateLocalHandle": {
    category: "protocol",
    retry: "requires_different_input",
    hint: "Two clauses claim the same `?handle`. Rename one: forward references must resolve to exactly one clause.",
  },
  "DuplicateMutationTarget": {
    category: "protocol",
    retry: "requires_different_input",
    hint: "One transaction may mutate an element once. Merge the two clauses into a single mutation.",
  },
  "SchemaSymbolNotFound": {
    category: "schema",
    retry: "requires_different_input",
    hint: "Run `LIST TYPES` / `LIST PREDICATES` or `DESCRIBE TYPE` to confirm the symbol. Symbols are case-sensitive.",
  },
  "SchemaSymbolAmbiguous": {
    category: "schema",
    retry: "requires_different_input",
    hint: "The local name resolves in more than one package. Qualify it with its package path.",
  },
  "SchemaFieldNotFound": {
    category: "schema",
    retry: "requires_different_input",
    hint: "Run `DESCRIBE TYPE` / `DESCRIBE FACET` to see which fields the element actually declares.",
  },
  "SchemaPackageUnavailable": {
    category: "schema",
    retry: "requires_refresh",
    hint: "Run `LIST SCHEMA PACKAGES` to check what is active in this Schema Environment.",
  },
  "SchemaEnvironmentChanged": {
    category: "schema",
    retry: "requires_refresh",
    hint: "The environment changed under the request. Re-read `DESCRIBE SCHEMA ENVIRONMENT` and retry.",
  },
  "HistoricalSchemaUnavailable": {
    category: "schema",
    retry: "non_retryable",
    hint: "The Schema needed to interpret that history is no longer retained; the historical read cannot be served.",
  },
  "TypeMismatch": {
    category: "schema",
    retry: "requires_different_input",
    hint: "Correct the value's type to match its declaration.",
  },
  "ConstraintViolation": {
    category: "schema",
    retry: "requires_different_input",
    hint: "Supply the missing required fields, or relax the value to satisfy the constraint.",
  },
  "NotFoundOrNotVisible": {
    category: "data",
    retry: "requires_different_input",
    hint: "The target does not exist or is not visible to you. Ground with `SEARCH` and confirm with an exact id before writing.",
  },
  "ReferenceError": {
    category: "data",
    retry: "requires_different_input",
    hint: "Bind the variable in the WHERE block, or create the handle earlier in the same MUTATE plan.",
  },
  "StructuralReferenceInvalid": {
    category: "data",
    retry: "requires_different_input",
    hint: "Run `DESCRIBE STRUCTURAL FIELD` for the field's legal target kinds and cardinality.",
  },
  "IdentitySelectorRequired": {
    category: "data",
    retry: "requires_different_input",
    hint: "Add a stable selector: `{id: ...}` or `{key: ...}`.",
  },
  "NameIdentityForbidden": {
    category: "data",
    retry: "requires_different_input",
    hint: "`name` is mutable grounding state and never identifies an element. Match on `id` or `key`.",
  },
  "IdentityConflict": {
    category: "data",
    retry: "requires_different_input",
    hint: "Two identity claims disagree. Resolve which element you mean before retrying.",
  },
  "ClientKeyConflict": {
    category: "data",
    retry: "requires_different_input",
    hint: "That `client_key` already names a different element. Use a fresh key, or address the existing element by id.",
  },
  "IdentityMergeConflict": {
    category: "data",
    retry: "requires_different_input",
    hint: "The two Concepts cannot be merged. Inspect both with `DESCRIBE`/`FIND` before deciding a canonical target.",
  },
  "ImmutableField": {
    category: "epistemic",
    retry: "requires_different_input",
    hint: "The field is immutable after creation; express the change as new state instead.",
  },
  "EpistemicRevisionRequired": {
    category: "epistemic",
    retry: "requires_different_input",
    hint: "An Assertion's epistemic payload never changes. Record a new Assertion and `SUPERSEDE` the old one.",
  },
  "EvidenceCorrectionRequired": {
    category: "epistemic",
    retry: "requires_different_input",
    hint: "Evidence payload never changes. Use `CORRECT EVIDENCE :old BY :new`.",
  },
  "InvalidLifecycleTransition": {
    category: "epistemic",
    retry: "requires_different_input",
    hint: "Read the element's current lifecycle state first; that transition is not legal from where it is.",
  },
  "RetractionNotAuthorized": {
    category: "epistemic",
    retry: "requires_authority",
    hint: "Only the assertor may retract their own Assertion.",
  },
  "SupersessionMismatch": {
    category: "epistemic",
    retry: "requires_refresh",
    hint: "The superseding Assertion must address the same slot as the one it supersedes.",
  },
  "EvidenceCorrectionConflict": {
    category: "epistemic",
    retry: "requires_refresh",
    hint: "That Evidence already has a conflicting correction. Re-read its lineage.",
  },
  "ActivityTerminal": {
    category: "epistemic",
    retry: "requires_different_input",
    hint: "A terminal Activity is immutable. Finalize outputs in the same `TRANSITION ACTIVITY` that ends it.",
  },
  "ProjectionTargetUnbound": {
    category: "epistemic",
    retry: "requires_different_input",
    hint: "Bind the projection's Proposition in the WHERE block first.",
  },
  "ProjectionTargetUnbounded": {
    category: "epistemic",
    retry: "requires_different_input",
    hint: "BELIEF needs a bounded target: name the Proposition, or ground the subject and predicate.",
  },
  "ProjectionNotAuthorized": {
    category: "epistemic",
    retry: "requires_authority",
    hint: "You may read the raw claims but not project belief here.",
  },
  "ProjectionPolicyUnavailable": {
    category: "epistemic",
    retry: "requires_refresh",
    hint: "Run `LIST EPISTEMIC POLICIES` / `DESCRIBE EPISTEMIC POLICY` to see what can be projected with.",
  },
  "Unauthenticated": {
    category: "governance",
    retry: "requires_authority",
    hint: "Authenticate before issuing this request.",
  },
  "NotAuthorized": {
    category: "governance",
    retry: "requires_authority",
    hint: "Run `DESCRIBE ACCESS` to see which operations you may perform here.",
  },
  "RequiresApproval": {
    category: "governance",
    retry: "requires_authority",
    hint: "The operation is queued behind an out-of-band approval.",
  },
  "RequiresStrongerAuthentication": {
    category: "governance",
    retry: "requires_authority",
    hint: "Re-authenticate with a stronger factor and retry.",
  },
  "ActorBindingRequired": {
    category: "governance",
    retry: "requires_authority",
    hint: "Attribution needs an ActorBinding: you cannot assert on behalf of an actor you are not bound to.",
  },
  "ProtectedSystemField": {
    category: "governance",
    retry: "non_retryable",
    hint: "`_system` is engine truth and is never written by a mutation.",
  },
  "ProtectedGovernanceField": {
    category: "governance",
    retry: "non_retryable",
    hint: "Governance lives in the protected control plane, not in cognitive mutations.",
  },
  "ProtectedSchemaState": {
    category: "governance",
    retry: "non_retryable",
    hint: "Schema state is immutable Package state; publish and activate a Package instead.",
  },
  "LegalHoldConflict": {
    category: "governance",
    retry: "requires_authority",
    hint: "A legal hold covers this element; removal is blocked until it is lifted.",
  },
  "PurgeDenied": {
    category: "governance",
    retry: "requires_authority",
    hint: "Physical purge was denied by policy.",
  },
  "VersionConflict": {
    category: "transaction",
    retry: "requires_refresh",
    hint: "The element changed since you read it. Re-read it, re-apply your change, and retry with the fresh `EXPECT VERSION`.",
  },
  "PreconditionFailed": {
    category: "transaction",
    retry: "requires_refresh",
    hint: "A declared precondition no longer holds. Re-read the current state and retry.",
  },
  "SerializationConflict": {
    category: "transaction",
    retry: "safe_same_request",
    hint: "The transaction lost a race. Re-sending the identical request is safe.",
  },
  "IdempotencyConflict": {
    category: "transaction",
    retry: "requires_different_input",
    hint: "That idempotency key already names a different request. Use a new key, or re-send the original request bytes.",
  },
  "TransactionUnknown": {
    category: "transaction",
    retry: "outcome_lookup_required",
    hint: "Look the transaction up by its idempotency key before assuming anything about it.",
  },
  "OutcomeUnknown": {
    category: "transaction",
    retry: "outcome_lookup_required",
    hint: "Do not create a fresh mutation. Look the transaction up by idempotency key, or retry the exact same logical request with the same key.",
  },
  "TransactionTooLarge": {
    category: "transaction",
    retry: "requires_different_input",
    hint: "Split the mutation into smaller coherent transactions.",
  },
  "HistoricalSnapshotUnavailable": {
    category: "history",
    retry: "requires_new_snapshot",
    hint: "That history is no longer retained. Read at a newer coordinate.",
  },
  "CursorMismatch": {
    category: "history",
    retry: "requires_different_input",
    hint: "The cursor belongs to a different query. Restart pagination.",
  },
  "CursorTypeMismatch": {
    category: "history",
    retry: "requires_different_input",
    hint: "The cursor is for a different result kind. Restart pagination.",
  },
  "CursorExpired": {
    category: "history",
    retry: "requires_new_snapshot",
    hint: "Restart pagination from a fresh first page.",
  },
  "CursorInvalidated": {
    category: "history",
    retry: "requires_new_snapshot",
    hint: "An intervening change invalidated the cursor. Restart pagination.",
  },
  "ChangeCursorExpired": {
    category: "history",
    retry: "requires_new_snapshot",
    hint: "Re-subscribe from a newer change coordinate.",
  },
  "ChangeCursorInvalid": {
    category: "history",
    retry: "requires_different_input",
    hint: "The change cursor is malformed. Re-acquire it from the runtime.",
  },
  "SearchModeUnsupported": {
    category: "search",
    retry: "requires_different_input",
    hint: "Run `DESCRIBE CAPABILITIES` for the SEARCH modes this runtime offers.",
  },
  "SearchIndexUnavailable": {
    category: "search",
    retry: "safe_same_request",
    hint: "The index is temporarily unavailable; the same request may succeed shortly.",
  },
  "HistoricalSearchUnavailable": {
    category: "search",
    retry: "non_retryable",
    hint: "Historical SEARCH is not supported here; read the current index instead.",
  },
  "ArtifactUnavailable": {
    category: "artifact",
    retry: "requires_reacquire_artifact",
    hint: "Re-upload or re-stage the artifact, then retry with the new handle.",
  },
  "ArtifactTooLarge": {
    category: "artifact",
    retry: "requires_different_input",
    hint: "The artifact exceeds this runtime's limit. Split it or reference it externally.",
  },
  "ArtifactParseError": {
    category: "artifact",
    retry: "requires_different_input",
    hint: "The bytes are not a well-formed artifact of the declared kind.",
  },
  "DigestMismatch": {
    category: "artifact",
    retry: "requires_reacquire_artifact",
    hint: "The bytes do not match the declared digest. Re-acquire the artifact.",
  },
  "ProofInvalid": {
    category: "artifact",
    retry: "non_retryable",
    hint: "The proof did not verify. Do not treat the artifact as trusted.",
  },
  "SignerUnknown": {
    category: "artifact",
    retry: "non_retryable",
    hint: "The signer is unknown here. Establish trust explicitly before importing.",
  },
  "BlobUnavailable": {
    category: "artifact",
    retry: "requires_reacquire_artifact",
    hint: "A referenced blob is missing. Re-acquire it, or import with a redaction-tolerant mode.",
  },
  "CapsuleValidationFailed": {
    category: "artifact",
    retry: "requires_different_input",
    hint: "Run `VALIDATE CAPSULE` to see exactly which invariant the Capsule breaks.",
  },
  "ImportPreviewConflict": {
    category: "artifact",
    retry: "requires_refresh",
    hint: "The destination changed since the preview. Re-run `PREVIEW IMPORT CAPSULE` and retry.",
  },
  "ResourceExhausted": {
    category: "resource",
    retry: "requires_different_input",
    hint: "Reduce the request's cost: lower `LIMIT`, narrow the patterns, or paginate.",
  },
  "ResultLimitExceeded": {
    category: "resource",
    retry: "requires_different_input",
    hint: "Use `LIMIT` with `CURSOR` to page through the result set.",
  },
  "ExecutionTimeout": {
    category: "resource",
    retry: "safe_same_request",
    hint: "Simplify the query: fewer UNION branches, a lower LIMIT, fewer path hops.",
  },
  "RateLimited": {
    category: "resource",
    retry: "safe_same_request",
    hint: "Back off and retry the identical request.",
  },
  "InternalError": {
    category: "system",
    retry: "safe_same_request",
    hint: "Retry under the same idempotency key; if it persists, report the `request_id`.",
  },
}
