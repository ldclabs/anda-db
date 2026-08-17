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

/// The coarse family an error belongs to (Spec §86.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCategory {
    /// The command text could not be parsed.
    Syntax,
    /// The request envelope or operation shape is wrong.
    Protocol,
    /// A Schema symbol, field or package problem.
    Schema,
    /// An identity or reference problem in the data.
    Data,
    /// An epistemic or mutability rule was violated.
    Epistemic,
    /// Authentication, authorization or protected state.
    Governance,
    /// A transaction precondition, conflict or unknown outcome.
    Transaction,
    /// Historical reads and cursors.
    History,
    /// SEARCH modes and indexes.
    Search,
    /// Artifacts, digests and proofs.
    Artifact,
    /// Limits, quotas and timeouts.
    Resource,
    /// The transport itself failed.
    Transport,
    /// An unclassified internal failure.
    System,
}

impl ErrorCategory {
    /// The wire spelling of this category.
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorCategory::Syntax => "syntax",
            ErrorCategory::Protocol => "protocol",
            ErrorCategory::Schema => "schema",
            ErrorCategory::Data => "data",
            ErrorCategory::Epistemic => "epistemic",
            ErrorCategory::Governance => "governance",
            ErrorCategory::Transaction => "transaction",
            ErrorCategory::History => "history",
            ErrorCategory::Search => "search",
            ErrorCategory::Artifact => "artifact",
            ErrorCategory::Resource => "resource",
            ErrorCategory::Transport => "transport",
            ErrorCategory::System => "system",
        }
    }
}

impl Display for ErrorCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// What kind of retry, if any, can make progress (Spec §86.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryClass {
    /// Nothing durable happened; re-sending the identical request is safe.
    SafeSameRequest,
    /// Re-read the current state, then retry with what you learned.
    RequiresRefresh,
    /// The request itself must change.
    RequiresDifferentInput,
    /// The caller needs authority it does not currently hold.
    RequiresAuthority,
    /// Acquire a new snapshot or cursor first.
    RequiresNewSnapshot,
    /// Re-upload or re-fetch the artifact, then retry.
    RequiresReacquireArtifact,
    /// The outcome is unknown; look the transaction up before deciding.
    OutcomeLookupRequired,
    /// Retrying cannot help.
    NonRetryable,
}

impl RetryClass {
    /// The wire spelling of this retry class.
    pub fn as_str(&self) -> &'static str {
        match self {
            RetryClass::SafeSameRequest => "safe_same_request",
            RetryClass::RequiresRefresh => "requires_refresh",
            RetryClass::RequiresDifferentInput => "requires_different_input",
            RetryClass::RequiresAuthority => "requires_authority",
            RetryClass::RequiresNewSnapshot => "requires_new_snapshot",
            RetryClass::RequiresReacquireArtifact => "requires_reacquire_artifact",
            RetryClass::OutcomeLookupRequired => "outcome_lookup_required",
            RetryClass::NonRetryable => "non_retryable",
        }
    }
}

impl Display for RetryClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Declares the Core Error Registry once.
///
/// The registry is one list, and three things have to agree with it: the enum,
/// the `ALL` slice a caller enumerates, and the wire name a code parses back
/// from. Written out by hand they did agree — but only `ALL` was unchecked by
/// the compiler, so a code added to the enum and to `name()` and forgotten
/// here would compile and then be silently unparseable. Generating all three
/// from the one list removes the failure rather than documenting it.
macro_rules! kip_error_codes {
    ($( $(#[$meta:meta])* $variant:ident ),+ $(,)?) => {
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
        }
    };
}

kip_error_codes! {
    // ── §87.1 Protocol / syntax ──────────────────────────────────────
    /// The command text could not be parsed.
    InvalidSyntax,
    /// An identifier does not match the required shape.
    InvalidIdentifier,
    /// The request envelope is malformed or self-contradictory.
    InvalidRequestEnvelope,
    /// The declared `kip` protocol version is not supported.
    UnsupportedProtocolVersion,
    /// A requested capability is not supported by this runtime.
    UnsupportedCapability,
    /// The requested transaction isolation is not supported.
    UnsupportedIsolation,
    /// The declared language does not match the command's actual semantics.
    LanguageMismatch,
    /// A state-changing command reached a read-only execution path.
    ReadonlyViolation,
    /// Two clauses in one mutation plan claim the same local handle.
    DuplicateLocalHandle,
    /// Two clauses in one transaction mutate the same element.
    DuplicateMutationTarget,

    // ── §87.2 Schema ─────────────────────────────────────────────────
    /// The Schema symbol does not exist in the active environment.
    SchemaSymbolNotFound,
    /// A local name resolves to more than one package symbol.
    SchemaSymbolAmbiguous,
    /// The field is not declared on this type or facet.
    SchemaFieldNotFound,
    /// The Schema Package is not loaded or not available.
    SchemaPackageUnavailable,
    /// The Schema Environment changed under the request.
    SchemaEnvironmentChanged,
    /// The historical Schema needed for this read is no longer retained.
    HistoricalSchemaUnavailable,
    /// A value's type does not match its declaration.
    TypeMismatch,
    /// A declared Schema constraint was violated.
    ConstraintViolation,

    // ── §87.3 Identity / reference ───────────────────────────────────
    /// The target does not exist, or is not visible to this Principal.
    ///
    /// Deliberately existence-neutral, so a probe cannot map protected state
    /// by distinguishing "absent" from "forbidden" (Spec §86.4).
    NotFoundOrNotVisible,
    /// A referenced variable, handle or parameter is not bound.
    ReferenceError,
    /// A Structural Reference is not legal for its field.
    StructuralReferenceInvalid,
    /// The statement needs a stable identity selector and none was given.
    IdentitySelectorRequired,
    /// A name was used where only a stable identity is accepted.
    NameIdentityForbidden,
    /// Two identity claims for one element disagree.
    IdentityConflict,
    /// The `client_key` is already bound to a different element.
    ClientKeyConflict,
    /// A merge would join two irreconcilable identities.
    IdentityMergeConflict,

    // ── §87.4 Epistemic / mutability ─────────────────────────────────
    /// The field is immutable after creation.
    ImmutableField,
    /// Changing this requires a new Assertion plus supersession.
    EpistemicRevisionRequired,
    /// Changing this requires `CORRECT EVIDENCE`.
    EvidenceCorrectionRequired,
    /// The requested lifecycle transition is not legal from the current state.
    InvalidLifecycleTransition,
    /// Only the assertor may retract their own Assertion.
    RetractionNotAuthorized,
    /// The superseding Assertion does not address the superseded slot.
    SupersessionMismatch,
    /// Two corrections of the same Evidence conflict.
    EvidenceCorrectionConflict,
    /// The Activity is terminal and its outputs are frozen.
    ActivityTerminal,
    /// A projection target is not bound by the query.
    ProjectionTargetUnbound,
    /// A projection target is not sufficiently bounded to evaluate.
    ProjectionTargetUnbounded,
    /// The Principal may read but not project belief here.
    ProjectionNotAuthorized,
    /// No epistemic policy is available to project with.
    ProjectionPolicyUnavailable,

    // ── §87.5 Governance ─────────────────────────────────────────────
    /// No authenticated Principal.
    Unauthenticated,
    /// The Principal is authenticated but lacks the permission.
    NotAuthorized,
    /// The operation needs out-of-band approval first.
    RequiresApproval,
    /// The operation needs a stronger authentication factor.
    RequiresStrongerAuthentication,
    /// The write needs an ActorBinding for the claimed semantic actor.
    ActorBindingRequired,
    /// `_system` state is engine-owned and never author-writable.
    ProtectedSystemField,
    /// Governance state is part of the protected control plane.
    ProtectedGovernanceField,
    /// The Schema state is protected against this mutation.
    ProtectedSchemaState,
    /// A legal hold forbids the removal.
    LegalHoldConflict,
    /// Physical purge was denied by policy.
    PurgeDenied,

    // ── §87.6 Transaction ────────────────────────────────────────────
    /// `EXPECT VERSION` did not match.
    VersionConflict,
    /// A declared precondition did not hold.
    PreconditionFailed,
    /// The transaction lost a serialization race.
    SerializationConflict,
    /// The idempotency key was reused with a different request.
    IdempotencyConflict,
    /// The named transaction is unknown to this runtime.
    TransactionUnknown,
    /// The write may or may not have committed.
    OutcomeUnknown,
    /// The transaction exceeds the runtime's size limit.
    TransactionTooLarge,

    // ── §87.7 Historical / cursor ────────────────────────────────────
    /// The requested historical snapshot is no longer retained.
    HistoricalSnapshotUnavailable,
    /// The cursor does not belong to this query.
    CursorMismatch,
    /// The cursor is for a different result kind.
    CursorTypeMismatch,
    /// The cursor is past its retention window.
    CursorExpired,
    /// The cursor was invalidated by an intervening change.
    CursorInvalidated,
    /// The change cursor is past its retention window.
    ChangeCursorExpired,
    /// The change cursor is malformed or forged.
    ChangeCursorInvalid,

    // ── §87.8 Search ─────────────────────────────────────────────────
    /// The requested SEARCH mode is not supported.
    SearchModeUnsupported,
    /// The SEARCH index is not currently available.
    SearchIndexUnavailable,
    /// Historical SEARCH is not supported for this basis.
    HistoricalSearchUnavailable,

    // ── §87.9 Artifact / proof ───────────────────────────────────────
    /// The artifact handle no longer resolves.
    ArtifactUnavailable,
    /// The artifact exceeds the runtime's size limit.
    ArtifactTooLarge,
    /// The artifact bytes could not be parsed.
    ArtifactParseError,
    /// The content digest does not match the bytes.
    DigestMismatch,
    /// A cryptographic proof did not verify.
    ProofInvalid,
    /// The signer is not known or not trusted.
    SignerUnknown,
    /// A referenced blob is not available.
    BlobUnavailable,
    /// The Capsule failed validation.
    CapsuleValidationFailed,
    /// The import preview no longer matches the destination state.
    ImportPreviewConflict,

    // ── §87.10 Resource / runtime ────────────────────────────────────
    /// A resource limit was hit.
    ResourceExhausted,
    /// The result set exceeds the allowed size.
    ResultLimitExceeded,
    /// Execution exceeded its deadline and was aborted.
    ExecutionTimeout,
    /// The caller is being rate limited.
    RateLimited,
    /// An unclassified internal failure.
    InternalError,
}


impl KipErrorCode {
    /// The registry section this code belongs to (Spec §86.2).
    pub fn category(&self) -> ErrorCategory {
        use KipErrorCode::*;
        match self {
            InvalidSyntax | InvalidIdentifier => ErrorCategory::Syntax,
            InvalidRequestEnvelope
            | UnsupportedProtocolVersion
            | UnsupportedCapability
            | UnsupportedIsolation
            | LanguageMismatch
            | ReadonlyViolation
            | DuplicateLocalHandle
            | DuplicateMutationTarget => ErrorCategory::Protocol,
            SchemaSymbolNotFound
            | SchemaSymbolAmbiguous
            | SchemaFieldNotFound
            | SchemaPackageUnavailable
            | SchemaEnvironmentChanged
            | HistoricalSchemaUnavailable
            | TypeMismatch
            | ConstraintViolation => ErrorCategory::Schema,
            NotFoundOrNotVisible
            | ReferenceError
            | StructuralReferenceInvalid
            | IdentitySelectorRequired
            | NameIdentityForbidden
            | IdentityConflict
            | ClientKeyConflict
            | IdentityMergeConflict => ErrorCategory::Data,
            ImmutableField
            | EpistemicRevisionRequired
            | EvidenceCorrectionRequired
            | InvalidLifecycleTransition
            | RetractionNotAuthorized
            | SupersessionMismatch
            | EvidenceCorrectionConflict
            | ActivityTerminal
            | ProjectionTargetUnbound
            | ProjectionTargetUnbounded
            | ProjectionNotAuthorized
            | ProjectionPolicyUnavailable => ErrorCategory::Epistemic,
            Unauthenticated
            | NotAuthorized
            | RequiresApproval
            | RequiresStrongerAuthentication
            | ActorBindingRequired
            | ProtectedSystemField
            | ProtectedGovernanceField
            | ProtectedSchemaState
            | LegalHoldConflict
            | PurgeDenied => ErrorCategory::Governance,
            VersionConflict
            | PreconditionFailed
            | SerializationConflict
            | IdempotencyConflict
            | TransactionUnknown
            | OutcomeUnknown
            | TransactionTooLarge => ErrorCategory::Transaction,
            HistoricalSnapshotUnavailable
            | CursorMismatch
            | CursorTypeMismatch
            | CursorExpired
            | CursorInvalidated
            | ChangeCursorExpired
            | ChangeCursorInvalid => ErrorCategory::History,
            SearchModeUnsupported | SearchIndexUnavailable | HistoricalSearchUnavailable => {
                ErrorCategory::Search
            }
            ArtifactUnavailable
            | ArtifactTooLarge
            | ArtifactParseError
            | DigestMismatch
            | ProofInvalid
            | SignerUnknown
            | BlobUnavailable
            | CapsuleValidationFailed
            | ImportPreviewConflict => ErrorCategory::Artifact,
            ResourceExhausted | ResultLimitExceeded | ExecutionTimeout | RateLimited => {
                ErrorCategory::Resource
            }
            InternalError => ErrorCategory::System,
        }
    }

    /// This crate's default retry classification (Spec §86.3).
    pub fn retry_class(&self) -> RetryClass {
        use KipErrorCode::*;
        match self {
            // Nothing durable happened and the same bytes may work next time.
            SerializationConflict | SearchIndexUnavailable | ExecutionTimeout | RateLimited => {
                RetryClass::SafeSameRequest
            }
            // An internal failure says nothing about whether the write landed;
            // resending under the same idempotency key is the safe recovery.
            InternalError => RetryClass::SafeSameRequest,
            // Re-read, then retry with what you learned.
            SchemaPackageUnavailable
            | SchemaEnvironmentChanged
            | SupersessionMismatch
            | EvidenceCorrectionConflict
            | ProjectionPolicyUnavailable
            | VersionConflict
            | PreconditionFailed
            | ImportPreviewConflict => RetryClass::RequiresRefresh,
            // The caller lacks authority, not information.
            RetractionNotAuthorized
            | ProjectionNotAuthorized
            | Unauthenticated
            | NotAuthorized
            | RequiresApproval
            | RequiresStrongerAuthentication
            | ActorBindingRequired
            | LegalHoldConflict
            | PurgeDenied => RetryClass::RequiresAuthority,
            // Acquire a fresh coordinate first.
            HistoricalSnapshotUnavailable
            | CursorExpired
            | CursorInvalidated
            | ChangeCursorExpired => RetryClass::RequiresNewSnapshot,
            // The bytes are gone or wrong; fetch them again.
            ArtifactUnavailable | DigestMismatch | BlobUnavailable => {
                RetryClass::RequiresReacquireArtifact
            }
            // The write's fate is undecided.
            TransactionUnknown | OutcomeUnknown => RetryClass::OutcomeLookupRequired,
            // Retrying cannot help: the runtime will never support it, or the
            // history it needs is gone for good.
            UnsupportedProtocolVersion
            | UnsupportedCapability
            | UnsupportedIsolation
            | HistoricalSchemaUnavailable
            | HistoricalSearchUnavailable
            | ProtectedSystemField
            | ProtectedGovernanceField
            | ProtectedSchemaState
            | ProofInvalid
            | SignerUnknown => RetryClass::NonRetryable,
            // Everything else is a problem with the request itself.
            _ => RetryClass::RequiresDifferentInput,
        }
    }

    /// A recovery hint aimed at an Agent that must fix its own command.
    pub fn hint(&self) -> &'static str {
        use KipErrorCode::*;
        match self {
            InvalidSyntax => {
                "Check bracket matching, keyword spelling and clause order. Run `VALIDATE KQL`/`VALIDATE KML` on the text before re-sending."
            }
            InvalidIdentifier => "Identifiers must match `[A-Za-z_][A-Za-z0-9_]*`.",
            InvalidRequestEnvelope => {
                "Check the envelope: `kip` version, `operations[]` shape, and that `execution.mode` is one of independent, sequence, atomic."
            }
            UnsupportedProtocolVersion => {
                "Run `DESCRIBE PROTOCOL` to learn which protocol versions this runtime speaks."
            }
            UnsupportedCapability => {
                "Run `DESCRIBE CAPABILITIES` and request only what is both supported and available."
            }
            UnsupportedIsolation => {
                "Run `DESCRIBE CAPABILITIES` for the isolation levels this runtime offers."
            }
            LanguageMismatch => {
                "The `language` label must match the command's real semantics; a KML write cannot be labelled KQL."
            }
            ReadonlyViolation => {
                "This endpoint executes KQL and META only. Re-send state-changing KML through the general runtime."
            }
            DuplicateLocalHandle => {
                "Two clauses claim the same `?handle`. Rename one: forward references must resolve to exactly one clause."
            }
            DuplicateMutationTarget => {
                "One transaction may mutate an element once. Merge the two clauses into a single mutation."
            }
            SchemaSymbolNotFound => {
                "Run `LIST TYPES` / `LIST PREDICATES` or `DESCRIBE TYPE` to confirm the symbol. Symbols are case-sensitive."
            }
            SchemaSymbolAmbiguous => {
                "The local name resolves in more than one package. Qualify it with its package path."
            }
            SchemaFieldNotFound => {
                "Run `DESCRIBE TYPE` / `DESCRIBE FACET` to see which fields the element actually declares."
            }
            SchemaPackageUnavailable => {
                "Run `LIST SCHEMA PACKAGES` to check what is active in this Schema Environment."
            }
            SchemaEnvironmentChanged => {
                "The environment changed under the request. Re-read `DESCRIBE SCHEMA ENVIRONMENT` and retry."
            }
            HistoricalSchemaUnavailable => {
                "The Schema needed to interpret that history is no longer retained; the historical read cannot be served."
            }
            TypeMismatch => "Correct the value's type to match its declaration.",
            ConstraintViolation => {
                "Supply the missing required fields, or relax the value to satisfy the constraint."
            }
            NotFoundOrNotVisible => {
                "The target does not exist or is not visible to you. Ground with `SEARCH` and confirm with an exact id before writing."
            }
            ReferenceError => {
                "Bind the variable in the WHERE block, or create the handle earlier in the same MUTATE plan."
            }
            StructuralReferenceInvalid => {
                "Run `DESCRIBE STRUCTURAL FIELD` for the field's legal target kinds and cardinality."
            }
            IdentitySelectorRequired => "Add a stable selector: `{id: ...}` or `{key: ...}`.",
            NameIdentityForbidden => {
                "`name` is mutable grounding state and never identifies an element. Match on `id` or `key`."
            }
            IdentityConflict => {
                "Two identity claims disagree. Resolve which element you mean before retrying."
            }
            ClientKeyConflict => {
                "That `client_key` already names a different element. Use a fresh key, or address the existing element by id."
            }
            IdentityMergeConflict => {
                "The two Concepts cannot be merged. Inspect both with `DESCRIBE`/`FIND` before deciding a canonical target."
            }
            ImmutableField => {
                "The field is immutable after creation; express the change as new state instead."
            }
            EpistemicRevisionRequired => {
                "An Assertion's epistemic payload never changes. Record a new Assertion and `SUPERSEDE` the old one."
            }
            EvidenceCorrectionRequired => {
                "Evidence payload never changes. Use `CORRECT EVIDENCE :old BY :new`."
            }
            InvalidLifecycleTransition => {
                "Read the element's current lifecycle state first; that transition is not legal from where it is."
            }
            RetractionNotAuthorized => "Only the assertor may retract their own Assertion.",
            SupersessionMismatch => {
                "The superseding Assertion must address the same slot as the one it supersedes."
            }
            EvidenceCorrectionConflict => {
                "That Evidence already has a conflicting correction. Re-read its lineage."
            }
            ActivityTerminal => {
                "A terminal Activity is immutable. Finalize outputs in the same `TRANSITION ACTIVITY` that ends it."
            }
            ProjectionTargetUnbound => {
                "Bind the projection's Proposition in the WHERE block first."
            }
            ProjectionTargetUnbounded => {
                "BELIEF needs a bounded target: name the Proposition, or ground the subject and predicate."
            }
            ProjectionNotAuthorized => "You may read the raw claims but not project belief here.",
            ProjectionPolicyUnavailable => {
                "Run `LIST EPISTEMIC POLICIES` / `DESCRIBE EPISTEMIC POLICY` to see what can be projected with."
            }
            Unauthenticated => "Authenticate before issuing this request.",
            NotAuthorized => "Run `DESCRIBE ACCESS` to see which operations you may perform here.",
            RequiresApproval => "The operation is queued behind an out-of-band approval.",
            RequiresStrongerAuthentication => "Re-authenticate with a stronger factor and retry.",
            ActorBindingRequired => {
                "Attribution needs an ActorBinding: you cannot assert on behalf of an actor you are not bound to."
            }
            ProtectedSystemField => "`_system` is engine truth and is never written by a mutation.",
            ProtectedGovernanceField => {
                "Governance lives in the protected control plane, not in cognitive mutations."
            }
            ProtectedSchemaState => {
                "Schema state is immutable Package state; publish and activate a Package instead."
            }
            LegalHoldConflict => {
                "A legal hold covers this element; removal is blocked until it is lifted."
            }
            PurgeDenied => "Physical purge was denied by policy.",
            VersionConflict => {
                "The element changed since you read it. Re-read it, re-apply your change, and retry with the fresh `EXPECT VERSION`."
            }
            PreconditionFailed => {
                "A declared precondition no longer holds. Re-read the current state and retry."
            }
            SerializationConflict => {
                "The transaction lost a race. Re-sending the identical request is safe."
            }
            IdempotencyConflict => {
                "That idempotency key already names a different request. Use a new key, or re-send the original request bytes."
            }
            TransactionUnknown => {
                "Look the transaction up by its idempotency key before assuming anything about it."
            }
            OutcomeUnknown => {
                "Do not create a fresh mutation. Look the transaction up by idempotency key, or retry the exact same logical request with the same key."
            }
            TransactionTooLarge => "Split the mutation into smaller coherent transactions.",
            HistoricalSnapshotUnavailable => {
                "That history is no longer retained. Read at a newer coordinate."
            }
            CursorMismatch => "The cursor belongs to a different query. Restart pagination.",
            CursorTypeMismatch => "The cursor is for a different result kind. Restart pagination.",
            CursorExpired => "Restart pagination from a fresh first page.",
            CursorInvalidated => {
                "An intervening change invalidated the cursor. Restart pagination."
            }
            ChangeCursorExpired => "Re-subscribe from a newer change coordinate.",
            ChangeCursorInvalid => {
                "The change cursor is malformed. Re-acquire it from the runtime."
            }
            SearchModeUnsupported => {
                "Run `DESCRIBE CAPABILITIES` for the SEARCH modes this runtime offers."
            }
            SearchIndexUnavailable => {
                "The index is temporarily unavailable; the same request may succeed shortly."
            }
            HistoricalSearchUnavailable => {
                "Historical SEARCH is not supported here; read the current index instead."
            }
            ArtifactUnavailable => {
                "Re-upload or re-stage the artifact, then retry with the new handle."
            }
            ArtifactTooLarge => {
                "The artifact exceeds this runtime's limit. Split it or reference it externally."
            }
            ArtifactParseError => "The bytes are not a well-formed artifact of the declared kind.",
            DigestMismatch => {
                "The bytes do not match the declared digest. Re-acquire the artifact."
            }
            ProofInvalid => "The proof did not verify. Do not treat the artifact as trusted.",
            SignerUnknown => {
                "The signer is unknown here. Establish trust explicitly before importing."
            }
            BlobUnavailable => {
                "A referenced blob is missing. Re-acquire it, or import with a redaction-tolerant mode."
            }
            CapsuleValidationFailed => {
                "Run `VALIDATE CAPSULE` to see exactly which invariant the Capsule breaks."
            }
            ImportPreviewConflict => {
                "The destination changed since the preview. Re-run `PREVIEW IMPORT CAPSULE` and retry."
            }
            ResourceExhausted => {
                "Reduce the request's cost: lower `LIMIT`, narrow the patterns, or paginate."
            }
            ResultLimitExceeded => "Use `LIMIT` with `CURSOR` to page through the result set.",
            ExecutionTimeout => {
                "Simplify the query: fewer UNION branches, a lower LIMIT, fewer path hops."
            }
            RateLimited => "Back off and retry the identical request.",
            InternalError => {
                "Retry under the same idempotency key; if it persists, report the `request_id`."
            }
        }
    }

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
        // §87 lists 79 codes across ten sections; a miss here means a section
        // was dropped when the registry was transcribed.
        assert_eq!(KipErrorCode::ALL.len(), 79);
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
        assert_eq!(
            KipErrorCode::OutcomeUnknown.retry_class(),
            RetryClass::OutcomeLookupRequired
        );
        assert_eq!(
            KipErrorCode::TransactionUnknown.retry_class(),
            RetryClass::OutcomeLookupRequired
        );
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
