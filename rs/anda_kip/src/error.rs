//! # The KIP 2.0 error model
//!
//! KIP 2.0 replaces the numeric `KIP_xxxx` codes of 1.x with a registry of
//! stable names (Spec §86–§87). Every error carries four things an Agent can
//! act on without reading prose:
//!
//! - a stable `code`, so a retry policy can switch on it;
//! - a `category`, so unrelated failures are not lumped together;
//! - a `retry.class`, which says what — if anything — makes a retry meaningful;
//! - a `hint`, which says what to change.
//!
//! The retry classification here is this crate's default reading of §86.3. A
//! runtime with more information (whether a write reached the log, say) MAY
//! narrow it on a specific response; it MUST NOT widen it into claiming a
//! failed write never happened.

use nom_language::error::{VerboseError, VerboseErrorKind};
use serde::{Deserialize, Serialize};
use std::{fmt::Display, str::FromStr};
use thiserror::Error;

use crate::ast::Json;

wire_enum! {
    /// The coarse family an error belongs to (Spec §86.2).
    pub enum ErrorCategory {
        /// The command text could not be parsed.
        Syntax = "syntax",
        /// The request envelope or operation shape is wrong.
        Protocol = "protocol",
        /// A Schema symbol, field or package problem.
        Schema = "schema",
        /// An identity or reference problem in the data.
        Data = "data",
        /// An epistemic or mutability rule was violated.
        Epistemic = "epistemic",
        /// Authentication, authorization or protected state.
        Governance = "governance",
        /// A transaction precondition, conflict or unknown outcome.
        Transaction = "transaction",
        /// Historical reads and cursors.
        History = "history",
        /// SEARCH modes and indexes.
        Search = "search",
        /// Artifacts, digests and proofs.
        Artifact = "artifact",
        /// Limits, quotas and timeouts.
        Resource = "resource",
        /// The transport itself failed.
        Transport = "transport",
        /// An unclassified internal failure.
        System = "system",
    }
}

wire_enum! {
    /// What kind of retry, if any, can make progress (Spec §86.3).
    pub enum RetryClass {
        /// Nothing durable happened; re-sending the identical request is safe.
        ///
        /// A serialization loss is an abort, a rate limit refused to run at
        /// all, and an unavailable index only ever failed a read.
        SafeSameRequest = "safe_same_request",
        /// Re-read the current state, then retry with what you learned.
        RequiresRefresh = "requires_refresh",
        /// The request itself must change.
        ///
        /// The default a code carries when nothing more specific applies: the
        /// problem is with what was sent, not with the runtime or the caller's
        /// authority.
        RequiresDifferentInput = "requires_different_input",
        /// The caller lacks authority, not information.
        RequiresAuthority = "requires_authority",
        /// Acquire a fresh coordinate first.
        ///
        /// A cursor that is gone — expired, invalidated by a schema change or
        /// a revocation, or never issued by this engine — is restarted from a
        /// fresh first page, whichever reason `details` names.
        RequiresNewSnapshot = "requires_new_snapshot",
        /// The bytes are gone or wrong; fetch them again, then retry.
        RequiresReacquireArtifact = "requires_reacquire_artifact",
        /// The write's fate is undecided; look the transaction up before
        /// deciding.
        ///
        /// A deadline is not an abort (§80.2): the transaction may still be
        /// running, and may still commit. An internal failure says nothing
        /// about whether the write landed either. Classifying either as
        /// `safe_same_request` would state that nothing durable happened —
        /// which is the one thing neither of them establishes — and a caller
        /// acting on it re-issues a mutation that may already be in the log.
        /// The conservative default is to look the transaction up (§80.3,
        /// §80.4); a runtime that *knows* its read timed out without touching
        /// state may override `retry` on the wire.
        OutcomeLookupRequired = "outcome_lookup_required",
        /// Retrying cannot help: the runtime will never support it, or the
        /// history it needs is gone for good.
        NonRetryable = "non_retryable",
    }
}

/// Declares the Core Error Registry once.
///
/// A code is not one fact but four — its name on the wire, its category, its
/// retry class and the hint an Agent reads. Written as four parallel lists
/// they did agree, but only the compiler's exhaustiveness check held them
/// together, and `ALL` and `from_name` were outside even that: a code added to
/// the enum and to `name()` and forgotten in the list would compile and then
/// be silently unparseable. Declaring the four together removes the failure
/// rather than documenting it.
///
/// ```text
/// Variant: Category [, RetryClass] => "hint";
/// ```
///
/// The retry class is omitted for [`RetryClass::RequiresDifferentInput`],
/// which is what a code means when nothing more specific applies: the request
/// itself has to change.
macro_rules! kip_error_codes {
    (@retry) => { RetryClass::RequiresDifferentInput };
    (@retry $retry:ident) => { RetryClass::$retry };

    ($(
        $(#[$meta:meta])*
        $variant:ident : $category:ident $(, $retry:ident)? => $hint:expr
    );+ $(;)?) => {
        /// The Core Error Registry (Spec §87).
        ///
        /// Codes are stable names, not numbers: an Agent switching on
        /// `EpistemicRevisionRequired` keeps working across protocol revisions in a way
        /// a renumbered `KIP_3007` would not.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
        pub enum KipErrorCode {
            $( $(#[$meta])* $variant, )+
        }

        impl KipErrorCode {
            /// Every registered code, in registry order.
            pub const ALL: &'static [KipErrorCode] = &[ $( KipErrorCode::$variant, )+ ];

            /// The stable wire code, e.g. `"SchemaSymbolAmbiguous"`.
            pub fn name(&self) -> &'static str {
                match self {
                    $( KipErrorCode::$variant => stringify!($variant), )+
                }
            }

            /// Looks a code up by its stable wire name.
            pub fn from_name(name: &str) -> Option<Self> {
                match name {
                    $( stringify!($variant) => Some(KipErrorCode::$variant), )+
                    _ => None,
                }
            }

            /// The registry section this code belongs to (Spec §86.2).
            pub fn category(&self) -> ErrorCategory {
                match self {
                    $( KipErrorCode::$variant => ErrorCategory::$category, )+
                }
            }

            /// This crate's default retry classification (Spec §86.3).
            ///
            /// A runtime that knows more about one occurrence may override
            /// `retry` on the wire; this is what the code alone establishes.
            pub fn retry_class(&self) -> RetryClass {
                match self {
                    $( KipErrorCode::$variant => kip_error_codes!(@retry $($retry)?), )+
                }
            }

            /// A recovery hint aimed at an Agent that must fix its own command.
            pub fn hint(&self) -> &'static str {
                match self {
                    $( KipErrorCode::$variant => $hint, )+
                }
            }
        }
    };
}

kip_error_codes! {
    // ── §87.1 Protocol / syntax ──────────────────────────────────────
    /// The command text could not be parsed.
    InvalidSyntax: Syntax =>
        "Check bracket matching, keyword spelling and clause order. Run `VALIDATE \
         KQL`/`VALIDATE KML` on the text before re-sending.";
    /// An identifier does not match the required shape.
    InvalidIdentifier: Syntax =>
        "Identifiers must match `[A-Za-z_][A-Za-z0-9_]*`.";
    /// The request envelope is malformed or self-contradictory.
    InvalidRequestEnvelope: Protocol =>
        "Check the envelope: `kip` version, `operations[]` shape, and that `execution.mode` is \
         one of independent, sequence, atomic.";
    /// The declared `kip` protocol version is not supported.
    UnsupportedProtocolVersion: Protocol, NonRetryable =>
        "Run `DESCRIBE PROTOCOL` to learn which protocol versions this runtime speaks.";
    /// A requested capability is not supported by this runtime.
    UnsupportedCapability: Protocol, NonRetryable =>
        "Run `DESCRIBE CAPABILITIES` and request only what is both supported and available.";
    /// The requested transaction isolation is not supported.
    UnsupportedIsolation: Protocol, NonRetryable =>
        "Run `DESCRIBE CAPABILITIES` for the isolation levels this runtime offers.";
    /// The declared language does not match the command's actual semantics.
    LanguageMismatch: Protocol =>
        "The `language` label must match the command's real semantics; a KML write cannot be \
         labelled KQL.";
    /// A state-changing command reached a read-only execution path.
    ReadonlyViolation: Protocol =>
        "This endpoint executes KQL and META only. Re-send state-changing KML through the \
         general runtime.";
    /// Two clauses in one mutation plan claim the same local handle.
    DuplicateLocalHandle: Protocol =>
        "Two clauses claim the same `?handle`. Rename one: forward references must resolve to \
         exactly one clause.";
    /// Two clauses in one transaction mutate the same element.
    DuplicateMutationTarget: Protocol =>
        "One transaction may mutate an element once. Merge the two clauses into a single \
         mutation.";

    // ── §87.2 Schema ─────────────────────────────────────────────────
    /// The Schema symbol does not exist in the active environment.
    SchemaSymbolNotFound: Schema =>
        "Run `LIST TYPES` / `LIST PREDICATES` or `DESCRIBE TYPE` to confirm the symbol. Symbols \
         are case-sensitive.";
    /// A local name resolves to more than one package symbol.
    SchemaSymbolAmbiguous: Schema =>
        "The local name resolves in more than one package. Qualify it with its package path.";
    /// The field is not declared on this type or facet.
    SchemaFieldNotFound: Schema =>
        "Run `DESCRIBE TYPE` / `DESCRIBE FACET` to see which fields the element actually \
         declares.";
    /// The Schema Package is not loaded or not available.
    SchemaPackageUnavailable: Schema, RequiresRefresh =>
        "Run `LIST SCHEMA PACKAGES` to check what is active in this Schema Environment.";
    /// The Schema Environment changed under the request.
    SchemaEnvironmentChanged: Schema, RequiresRefresh =>
        "The environment changed under the request. Re-read `DESCRIBE SCHEMA ENVIRONMENT` and \
         retry.";
    /// The historical Schema needed for this read is no longer retained.
    HistoricalSchemaUnavailable: Schema, NonRetryable =>
        "The Schema needed to interpret that history is no longer retained; the historical read \
         cannot be served.";
    /// A value's type does not match its declaration.
    TypeMismatch: Schema =>
        "Correct the value's type to match its declaration.";
    /// A declared Schema constraint was violated.
    ConstraintViolation: Schema =>
        "Supply the missing required fields, or relax the value to satisfy the constraint.";

    // ── §87.3 Identity / reference ───────────────────────────────────
    /// The target does not exist, or is not visible to this Principal.
    ///
    /// Deliberately existence-neutral, so a probe cannot map protected state
    /// by distinguishing "absent" from "forbidden" (Spec §86.4).
    NotFoundOrNotVisible: Data =>
        "The target does not exist or is not visible to you. Ground with `SEARCH` and confirm \
         with an exact id before writing.";
    /// A referenced variable, handle or parameter is not bound.
    ReferenceError: Data =>
        "Bind the variable in the WHERE block, or create the handle earlier in the same MUTATE \
         plan.";
    /// A Structural Reference is not legal for its field.
    StructuralReferenceInvalid: Data =>
        "Run `DESCRIBE STRUCTURAL FIELD` for the field's legal target kinds and cardinality.";
    /// The statement needs a stable identity selector and none was given.
    IdentitySelectorRequired: Data =>
        "Add a stable selector: `{id: ...}` or `{key: ...}`.";
    /// A name was used where only a stable identity is accepted.
    NameIdentityForbidden: Data =>
        "`name` is mutable grounding state and never identifies an element. Match on `id` or \
         `key`.";
    /// Two identity claims for one element disagree.
    IdentityConflict: Data =>
        "Two identity claims disagree. Resolve which element you mean before retrying.";
    /// The `client_key` is already bound to a different element.
    ClientKeyConflict: Data =>
        "That `client_key` already names a different element. Use a fresh key, or address the \
         existing element by id.";
    /// A merge would join two irreconcilable identities.
    IdentityMergeConflict: Data =>
        "The two Concepts cannot be merged. Inspect both with `DESCRIBE`/`FIND` before deciding \
         a canonical target.";

    // ── §87.4 Epistemic / mutability ─────────────────────────────────
    /// The field is immutable after creation.
    ImmutableField: Epistemic =>
        "The field is immutable after creation; express the change as new state instead.";
    /// Changing this requires a new Assertion plus supersession.
    EpistemicRevisionRequired: Epistemic =>
        "An Assertion's epistemic payload never changes. Record a new Assertion with `ASSERT \
         ... SUPERSEDING :old`, or `TRANSITION :old TO \"superseded\" BY :new`.";
    /// Changing this requires `TRANSITION :old TO "corrected" BY :new`.
    EvidenceCorrectionRequired: Epistemic =>
        "Evidence payload never changes. Record the corrected Evidence and `TRANSITION :old TO \
         \"corrected\" BY :new`.";
    /// The requested lifecycle transition is not legal from the current state.
    InvalidLifecycleTransition: Epistemic =>
        "Read the element's current lifecycle state first (`details.from` / `details.to`); that \
         TRANSITION is not legal from where it is, or not for its kind.";
    /// Only the assertor may retract their own Assertion.
    RetractionNotAuthorized: Epistemic, RequiresAuthority =>
        "Only the assertor may retract their own Assertion.";
    /// The superseding Assertion does not address the superseded slot.
    SupersessionMismatch: Epistemic, RequiresRefresh =>
        "The superseding Assertion must address the same slot as the one it supersedes.";
    /// Two corrections of the same Evidence conflict.
    EvidenceCorrectionConflict: Epistemic, RequiresRefresh =>
        "That Evidence already has a conflicting correction. Re-read its lineage.";
    /// The Activity is terminal and its outputs are frozen.
    ActivityTerminal: Epistemic =>
        "A terminal Activity is immutable. Finalize outputs in the same `TRANSITION ... TO \
         \"completed\" SET STRUCTURAL` that ends it.";
    /// A projection target is not bound by the query.
    ProjectionTargetUnbound: Epistemic =>
        "Bind the projection's Proposition in the WHERE block first.";
    /// A projection target is not sufficiently bounded to evaluate.
    ProjectionTargetUnbounded: Epistemic =>
        "BELIEF needs a bounded target: name the Proposition, or ground the subject and \
         predicate.";
    /// The Principal may read but not project belief here.
    ProjectionNotAuthorized: Epistemic, RequiresAuthority =>
        "You may read the raw claims but not project belief here.";
    /// No epistemic policy is available to project with.
    ProjectionPolicyUnavailable: Epistemic, RequiresRefresh =>
        "Run `LIST EPISTEMIC POLICIES` / `DESCRIBE EPISTEMIC POLICY` to see what can be \
         projected with.";

    // ── §87.5 Governance ─────────────────────────────────────────────
    /// No authenticated Principal.
    Unauthenticated: Governance, RequiresAuthority =>
        "Authenticate before issuing this request.";
    /// The Principal is authenticated but lacks the permission.
    NotAuthorized: Governance, RequiresAuthority =>
        "Run `DESCRIBE ACCESS` to see which operations you may perform here.";
    /// The operation needs out-of-band approval first.
    RequiresApproval: Governance, RequiresAuthority =>
        "The operation is queued behind an out-of-band approval.";
    /// The operation needs a stronger authentication factor.
    RequiresStrongerAuthentication: Governance, RequiresAuthority =>
        "Re-authenticate with a stronger factor and retry.";
    /// The write needs an ActorBinding for the claimed semantic actor.
    ActorBindingRequired: Governance, RequiresAuthority =>
        "Attribution needs an ActorBinding: you cannot assert on behalf of an actor you are not \
         bound to.";
    /// `_system` state is engine-owned and never author-writable.
    ProtectedSystemField: Governance, NonRetryable =>
        "`_system` is engine truth and is never written by a mutation.";
    /// Governance state is part of the protected control plane.
    ProtectedGovernanceField: Governance, NonRetryable =>
        "Governance lives in the protected control plane, not in cognitive mutations.";
    /// The Schema state is protected against this mutation.
    ProtectedSchemaState: Governance, NonRetryable =>
        "Schema state is immutable Package state; publish and activate a Package instead.";
    /// A legal hold forbids the removal.
    LegalHoldConflict: Governance, RequiresAuthority =>
        "A legal hold covers this element; removal is blocked until it is lifted.";
    /// Physical purge was denied by policy.
    PurgeDenied: Governance, RequiresAuthority =>
        "Physical purge was denied by policy.";

    // ── §87.6 Transaction ────────────────────────────────────────────
    /// `EXPECT VERSION` did not match.
    VersionConflict: Transaction, RequiresRefresh =>
        "The element changed since you read it. Re-read it, re-apply your change, and retry \
         with the fresh `EXPECT VERSION`.";
    /// A declared precondition did not hold.
    PreconditionFailed: Transaction, RequiresRefresh =>
        "A declared precondition no longer holds. Re-read the current state and retry.";
    /// The transaction lost a serialization race.
    SerializationConflict: Transaction, SafeSameRequest =>
        "The transaction lost a race. Re-sending the identical request is safe.";
    /// The idempotency key was reused with a different request.
    IdempotencyConflict: Transaction =>
        "That idempotency key already names a different request. Use a new key, or re-send the \
         original request bytes.";
    /// The named transaction is unknown to this runtime.
    TransactionUnknown: Transaction, OutcomeLookupRequired =>
        "Look the transaction up by its idempotency key before assuming anything about it.";
    /// The write may or may not have committed.
    OutcomeUnknown: Transaction, OutcomeLookupRequired =>
        "Do not create a fresh mutation. Look the transaction up by idempotency key, or retry \
         the exact same logical request with the same key.";
    /// The transaction exceeds the runtime's size limit.
    TransactionTooLarge: Transaction =>
        "Split the mutation into smaller coherent transactions.";

    // ── §87.7 Historical / cursor ────────────────────────────────────
    /// The requested historical snapshot is no longer retained.
    HistoricalSnapshotUnavailable: History, RequiresNewSnapshot =>
        "That history is no longer retained. Read at a newer coordinate.";
    /// The cursor does not belong to this query.
    CursorMismatch: History =>
        "The cursor belongs to a different query. Restart pagination.";
    /// The cursor is for a different result kind.
    CursorTypeMismatch: History =>
        "The cursor is for a different result kind. Restart pagination.";
    /// The cursor is past its retention window.
    ///
    /// One code for every cursor family (KQL, SEARCH, HISTORY, LIST, CHANGES,
    /// EXPORT): `details.family` names the family and `details.reason` says
    /// why (§87.7). A Change cursor that expired restarts from a sequence the
    /// consumer durably recorded, never from the current head (§69).
    CursorExpired: History, RequiresNewSnapshot =>
        "Restart pagination from a fresh first page; a change cursor restarts from a sequence \
         you recorded, never from the current head. `details.family` names the cursor family.";
    /// The cursor is malformed, was issued for another traversal, or was
    /// invalidated by an intervening change — `details.reason` is one of
    /// `malformed`, `access_revoked`, `schema_changed`; `details.family` names
    /// the cursor family (§87.7).
    CursorInvalid: History, RequiresNewSnapshot =>
        "The cursor is malformed, belongs to another traversal, or was invalidated \
         (`details.reason`: malformed, access_revoked, schema_changed). Restart pagination from \
         a fresh first page.";

    // ── §87.8 Search ─────────────────────────────────────────────────
    /// The requested SEARCH mode is not supported.
    SearchModeUnsupported: Search =>
        "Run `DESCRIBE CAPABILITIES` for the SEARCH modes this runtime offers.";
    /// The SEARCH index is not currently available.
    SearchIndexUnavailable: Search, SafeSameRequest =>
        "The index is temporarily unavailable; the same request may succeed shortly.";
    /// Historical SEARCH is not supported for this basis.
    HistoricalSearchUnavailable: Search, NonRetryable =>
        "Historical SEARCH is not supported here; read the current index instead.";

    // ── §87.9 Artifact / proof ───────────────────────────────────────
    /// The artifact handle no longer resolves.
    ArtifactUnavailable: Artifact, RequiresReacquireArtifact =>
        "Re-upload or re-stage the artifact, then retry with the new handle.";
    /// The artifact exceeds the runtime's size limit.
    ArtifactTooLarge: Artifact =>
        "The artifact exceeds this runtime's limit. Split it or reference it externally.";
    /// The artifact bytes could not be parsed.
    ArtifactParseError: Artifact =>
        "The bytes are not a well-formed artifact of the declared kind.";
    /// The content digest does not match the bytes.
    DigestMismatch: Artifact, RequiresReacquireArtifact =>
        "The bytes do not match the declared digest. Re-acquire the artifact.";
    /// A cryptographic proof did not verify.
    ProofInvalid: Artifact, NonRetryable =>
        "The proof did not verify. Do not treat the artifact as trusted.";
    /// The signer is not known or not trusted.
    SignerUnknown: Artifact, NonRetryable =>
        "The signer is unknown here. Establish trust explicitly before importing.";
    /// A referenced blob is not available.
    BlobUnavailable: Artifact, RequiresReacquireArtifact =>
        "A referenced blob is missing. Re-acquire it, or import with a redaction-tolerant mode.";
    /// The Capsule failed validation.
    CapsuleValidationFailed: Artifact =>
        "Run `VALIDATE CAPSULE` to see exactly which invariant the Capsule breaks.";
    /// The import preview no longer matches the destination state.
    ImportPreviewConflict: Artifact, RequiresRefresh =>
        "The destination changed since the preview. Re-run `PREVIEW IMPORT CAPSULE` and retry.";

    // ── §87.10 Resource / runtime ────────────────────────────────────
    /// A resource limit was hit.
    ResourceExhausted: Resource =>
        "Reduce the request's cost: lower `LIMIT`, narrow the patterns, or paginate.";
    /// The result set exceeds the allowed size.
    ResultLimitExceeded: Resource =>
        "Use `LIMIT` with `CURSOR` to page through the result set.";
    /// Execution exceeded its deadline and was aborted.
    ExecutionTimeout: Resource, OutcomeLookupRequired =>
        "A deadline is not an abort: look the transaction up by idempotency key before \
         deciding. For a read, simplify it — fewer UNION branches, a lower LIMIT, fewer path \
         hops.";
    /// The caller is being rate limited.
    RateLimited: Resource, SafeSameRequest =>
        "Back off and retry the identical request.";
    /// An unclassified internal failure.
    InternalError: System, OutcomeLookupRequired =>
        "Retry under the same idempotency key; if it persists, report the `request_id`.";
}

impl Display for KipErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl FromStr for KipErrorCode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        KipErrorCode::from_name(s).ok_or_else(|| format!("unknown KIP error code {s:?}"))
    }
}

/// A KIP error, carrying everything §86.1 puts on the wire.
#[derive(Error, Debug, Clone, PartialEq)]
#[error("{code}: {message}")]
pub struct KipError {
    /// The registered code.
    pub code: KipErrorCode,
    /// A human-readable description of what went wrong.
    pub message: String,
    /// A recovery hint; falls back to [`KipErrorCode::hint`] when unset.
    pub hint: Option<String>,
    /// Structured detail an Agent or operator can act on.
    pub details: Option<Json>,
}

impl KipError {
    /// Creates an error with the registry's default hint.
    pub fn new(code: KipErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            hint: None,
            details: None,
        }
    }

    /// Overrides the registry hint with one specific to this occurrence.
    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    /// Attaches structured detail.
    pub fn with_details(mut self, details: Json) -> Self {
        self.details = Some(details);
        self
    }

    /// The stable wire code.
    pub fn name(&self) -> &'static str {
        self.code.name()
    }

    /// The error's category.
    pub fn category(&self) -> ErrorCategory {
        self.code.category()
    }

    /// The retry classification for this error.
    pub fn retry_class(&self) -> RetryClass {
        self.code.retry_class()
    }

    /// The effective hint: the per-occurrence one, else the registry default.
    pub fn effective_hint(&self) -> &str {
        self.hint.as_deref().unwrap_or_else(|| self.code.hint())
    }

    /// A [`KipErrorCode::CursorExpired`] carrying the family it belongs to.
    ///
    /// §87.7: one code covers every cursor family, and `details.family` is
    /// how a consumer tells a KQL page from a change stream — the recovery
    /// differs, a fresh first page against a durably recorded sequence.
    pub fn cursor_expired(family: &str, message: impl Display) -> Self {
        Self::new(KipErrorCode::CursorExpired, message.to_string())
            .with_details(serde_json::json!({ "family": family, "reason": "expired" }))
    }

    /// A [`KipErrorCode::CursorInvalid`] carrying the family and the reason —
    /// `malformed`, `access_revoked` or `schema_changed` (§87.7).
    pub fn cursor_invalid(family: &str, reason: &str, message: impl Display) -> Self {
        Self::new(KipErrorCode::CursorInvalid, message.to_string())
            .with_details(serde_json::json!({ "family": family, "reason": reason }))
    }

    /// A [`KipErrorCode::InvalidLifecycleTransition`] naming the move that
    /// was refused, as `details.from` / `details.to` (§52.5).
    pub fn invalid_lifecycle_transition_from(from: &str, to: &str, message: impl Display) -> Self {
        Self::new(
            KipErrorCode::InvalidLifecycleTransition,
            message.to_string(),
        )
        .with_details(serde_json::json!({ "from": from, "to": to }))
    }

    /// A [`KipErrorCode::VersionConflict`] on one version plane, named in
    /// `details.plane` (§35.1).
    pub fn version_conflict_on_plane(plane: &str, message: impl Display) -> Self {
        Self::new(KipErrorCode::VersionConflict, message.to_string())
            .with_details(serde_json::json!({ "plane": plane }))
    }
}

macro_rules! kip_error_constructors {
    ($($fn_name:ident => $code:ident),* $(,)?) => {
        impl KipError {
            $(
                #[doc = concat!("Creates a [`KipErrorCode::", stringify!($code), "`] error.")]
                pub fn $fn_name(err: impl Display) -> Self {
                    Self::new(KipErrorCode::$code, format!("{err}"))
                }
            )*
        }
    };
}

kip_error_constructors! {
    invalid_syntax => InvalidSyntax,
    invalid_identifier => InvalidIdentifier,
    invalid_request_envelope => InvalidRequestEnvelope,
    unsupported_protocol_version => UnsupportedProtocolVersion,
    unsupported_capability => UnsupportedCapability,
    language_mismatch => LanguageMismatch,
    readonly_violation => ReadonlyViolation,
    duplicate_local_handle => DuplicateLocalHandle,
    duplicate_mutation_target => DuplicateMutationTarget,
    schema_symbol_not_found => SchemaSymbolNotFound,
    schema_field_not_found => SchemaFieldNotFound,
    type_mismatch => TypeMismatch,
    constraint_violation => ConstraintViolation,
    not_found_or_not_visible => NotFoundOrNotVisible,
    reference_error => ReferenceError,
    structural_reference_invalid => StructuralReferenceInvalid,
    identity_selector_required => IdentitySelectorRequired,
    name_identity_forbidden => NameIdentityForbidden,
    client_key_conflict => ClientKeyConflict,
    immutable_field => ImmutableField,
    epistemic_revision_required => EpistemicRevisionRequired,
    evidence_correction_required => EvidenceCorrectionRequired,
    invalid_lifecycle_transition => InvalidLifecycleTransition,
    activity_terminal => ActivityTerminal,
    projection_target_unbound => ProjectionTargetUnbound,
    projection_target_unbounded => ProjectionTargetUnbounded,
    projection_not_authorized => ProjectionNotAuthorized,
    retraction_not_authorized => RetractionNotAuthorized,
    unauthenticated => Unauthenticated,
    not_authorized => NotAuthorized,
    requires_approval => RequiresApproval,
    requires_stronger_authentication => RequiresStrongerAuthentication,
    actor_binding_required => ActorBindingRequired,
    protected_system_field => ProtectedSystemField,
    protected_governance_field => ProtectedGovernanceField,
    protected_schema_state => ProtectedSchemaState,
    legal_hold_conflict => LegalHoldConflict,
    purge_denied => PurgeDenied,
    version_conflict => VersionConflict,
    precondition_failed => PreconditionFailed,
    outcome_unknown => OutcomeUnknown,
    capsule_validation_failed => CapsuleValidationFailed,
    resource_exhausted => ResourceExhausted,
    result_limit_exceeded => ResultLimitExceeded,
    execution_timeout => ExecutionTimeout,
    internal_error => InternalError,
}

/// The wire shape of an error (Spec §86.1).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct ErrorObject {
    /// The stable registered code.
    pub code: String,
    /// The error's category.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub category: Option<ErrorCategory>,
    /// A human-readable description.
    pub message: String,
    /// A recovery hint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
    /// What kind of retry can make progress.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry: Option<RetryInfo>,
    /// Structured detail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Json>,
}

/// The `retry` member of an [`ErrorObject`].
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct RetryInfo {
    /// The retry classification.
    pub class: RetryClass,
    /// How long to wait first, when the runtime knows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_ms: Option<u64>,
}

impl RetryInfo {
    /// A retry classification with no suggested delay.
    pub fn new(class: RetryClass) -> Self {
        Self {
            class,
            after_ms: None,
        }
    }

    /// Suggests how long the caller should wait before retrying.
    pub fn after_ms(mut self, ms: u64) -> Self {
        self.after_ms = Some(ms);
        self
    }
}

impl ErrorObject {
    /// Creates an error object from a registered code and message.
    pub fn new(code: KipErrorCode, message: impl Into<String>) -> Self {
        KipError::new(code, message).into()
    }

    /// The parsed code, when it is one this build knows.
    pub fn parsed_code(&self) -> Option<KipErrorCode> {
        KipErrorCode::from_name(&self.code)
    }
}

impl From<KipError> for ErrorObject {
    fn from(err: KipError) -> Self {
        ErrorObject {
            code: err.code.name().to_string(),
            category: Some(err.code.category()),
            message: err.message,
            hint: Some(err.hint.unwrap_or_else(|| err.code.hint().to_string())),
            retry: Some(RetryInfo::new(err.code.retry_class())),
            details: err.details,
        }
    }
}

impl From<serde_json::Error> for ErrorObject {
    fn from(err: serde_json::Error) -> Self {
        ErrorObject::new(
            KipErrorCode::InvalidRequestEnvelope,
            format!("malformed JSON: {err}"),
        )
    }
}

impl From<serde_json::Error> for KipError {
    fn from(err: serde_json::Error) -> Self {
        KipError::invalid_request_envelope(format!("malformed JSON: {err}"))
    }
}

impl Display for ErrorObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

/// Formats a nom parsing error into a [`KipError`] with source context.
pub fn format_nom_error(input: &str, err: nom::Err<VerboseError<&str>>) -> KipError {
    let message = match err {
        nom::Err::Incomplete(needed) => {
            format!("parse incomplete, need more input: {needed:?}")
        }
        nom::Err::Error(ve) | nom::Err::Failure(ve) => format_verbose_error(input, ve),
    };
    KipError::invalid_syntax(message)
}

fn format_verbose_error(input: &str, ve: VerboseError<&str>) -> String {
    let mut msg = String::new();
    for (i, (substring, kind)) in ve.errors.iter().enumerate() {
        let offset = input.len() - substring.len();
        let (line, column) = line_column(input, offset);
        let snippet = snippet_at(substring);

        if i > 0 {
            msg.push_str("\n  ");
        }
        match kind {
            VerboseErrorKind::Context(ctx) => {
                msg.push_str(&format!("at line {line}, column {column}: expected {ctx}"));
            }
            VerboseErrorKind::Char(c) => {
                msg.push_str(&format!("at line {line}, column {column}: expected {c:?}"));
            }
            VerboseErrorKind::Nom(e) => {
                msg.push_str(&format!("at line {line}, column {column}: {e:?}"));
            }
        }
        if !snippet.is_empty() {
            msg.push_str(&format!(", found {snippet:?}"));
        }
    }
    if msg.is_empty() {
        "the input is not a valid KIP command".to_string()
    } else {
        msg
    }
}

/// 1-based line and column of a byte offset in `input`.
fn line_column(input: &str, offset: usize) -> (usize, usize) {
    let offset = offset.min(input.len());
    let consumed = &input[..offset];
    let line = consumed.matches('\n').count() + 1;
    let column = match consumed.rfind('\n') {
        Some(idx) => consumed[idx + 1..].chars().count() + 1,
        None => consumed.chars().count() + 1,
    };
    (line, column)
}

/// A short, char-boundary-safe excerpt of the unparsed remainder.
fn snippet_at(remaining: &str) -> String {
    const MAX: usize = 24;
    let line = remaining.lines().next().unwrap_or("").trim_end();
    if line.chars().count() <= MAX {
        line.to_string()
    } else {
        let truncated: String = line.chars().take(MAX).collect();
        format!("{truncated}…")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_registered_code_round_trips_by_name() {
        for code in KipErrorCode::ALL {
            assert_eq!(KipErrorCode::from_name(code.name()), Some(*code));
            // The serde encoding is the same stable name.
            assert_eq!(
                serde_json::to_string(code).unwrap(),
                format!("\"{}\"", code.name())
            );
        }
    }

    #[test]
    fn registry_covers_the_whole_spec_listing() {
        // §87 lists 77 codes across ten sections (the four cursor codes of
        // earlier drafts collapsed into `CursorExpired` / `CursorInvalid`,
        // §87.7); a miss here means a section was dropped when the registry
        // was transcribed.
        assert_eq!(KipErrorCode::ALL.len(), 77);
        let mut names: Vec<&str> = KipErrorCode::ALL.iter().map(|c| c.name()).collect();
        names.sort_unstable();
        let unique = names.len();
        names.dedup();
        assert_eq!(names.len(), unique, "duplicate code name in the registry");
    }

    #[test]
    fn every_code_has_a_hint() {
        for code in KipErrorCode::ALL {
            assert!(!code.hint().is_empty(), "{code} has no hint");
        }
    }

    #[test]
    fn existence_neutral_error_stays_neutral() {
        // Distinguishing "absent" from "forbidden" is exactly the leak §86.4
        // closes, so the code lives in `data`, never in `governance`.
        assert_eq!(
            KipErrorCode::NotFoundOrNotVisible.category(),
            ErrorCategory::Data
        );
    }

    #[test]
    fn error_object_carries_category_hint_and_retry() {
        let obj: ErrorObject = KipError::version_conflict("element changed").into();
        assert_eq!(obj.code, "VersionConflict");
        assert_eq!(obj.category, Some(ErrorCategory::Transaction));
        assert_eq!(obj.retry, Some(RetryInfo::new(RetryClass::RequiresRefresh)));
        assert!(obj.hint.unwrap().contains("EXPECT VERSION"));

        let json = serde_json::to_value(ErrorObject::new(
            KipErrorCode::SchemaSymbolAmbiguous,
            "two packages define `Drug`",
        ))
        .unwrap();
        assert_eq!(json["code"], "SchemaSymbolAmbiguous");
        assert_eq!(json["category"], "schema");
        assert_eq!(json["retry"]["class"], "requires_different_input");
    }

    #[test]
    fn custom_hint_and_details_survive_conversion() {
        let err = KipError::not_authorized("no `derive` permission")
            .with_hint("ask the Space owner for `derive`")
            .with_details(serde_json::json!({"permission": "derive"}));
        assert_eq!(err.effective_hint(), "ask the Space owner for `derive`");
        let obj: ErrorObject = err.into();
        assert_eq!(
            obj.hint.as_deref(),
            Some("ask the Space owner for `derive`")
        );
        assert_eq!(obj.details.unwrap()["permission"], "derive");
    }

    #[test]
    fn lost_write_recovery_is_not_a_fresh_mutation() {
        // §80.4: the response being lost must never turn into a second write.
        for code in [
            KipErrorCode::OutcomeUnknown,
            KipErrorCode::TransactionUnknown,
            // §80.2: a client deadline is not proof the transaction aborted.
            KipErrorCode::ExecutionTimeout,
            // And an internal failure proves nothing about it either.
            KipErrorCode::InternalError,
        ] {
            assert_eq!(
                code.retry_class(),
                RetryClass::OutcomeLookupRequired,
                "{code} must not tell a caller the write definitely did not land"
            );
        }
    }

    #[test]
    fn safe_same_request_is_reserved_for_outcomes_that_are_actually_known() {
        // The class means "nothing durable happened", so only codes that
        // establish that may carry it: an abort, a refusal to run, and a read
        // path that never touches state.
        for code in KipErrorCode::ALL {
            if code.retry_class() != RetryClass::SafeSameRequest {
                continue;
            }
            assert!(
                matches!(
                    code,
                    KipErrorCode::SerializationConflict
                        | KipErrorCode::SearchIndexUnavailable
                        | KipErrorCode::RateLimited
                ),
                "{code} claims nothing durable happened; does it establish that?"
            );
        }
    }

    #[test]
    fn line_column_counts_from_one() {
        let input = "FIND(?x)\nWHERE {\n  bad\n}";
        assert_eq!(line_column(input, 0), (1, 1));
        assert_eq!(line_column(input, 9), (2, 1));
        assert_eq!(line_column(input, 19), (3, 3));
        // An offset past the end clamps instead of panicking.
        assert_eq!(line_column(input, 9_999).0, 4);
    }

    #[test]
    fn snippet_truncates_on_char_boundaries() {
        let long = "查询查询查询查询查询查询查询查询查询查询查询查询查询";
        let snippet = snippet_at(long);
        assert!(snippet.ends_with('…'));
        assert_eq!(snippet.chars().count(), 25);
    }
}
