# KIP v2 synchronization: 22b72b5 → d6e3a45

The protocol corpus follows `ldclabs/KIP` at **d6e3a45** (toolkit release
2.3.1). The wire protocol remains KIP 2.0; the standard Cognitive Memory
vocabulary is now `kip://profiles/cognitive-memory@2.1.0`. These are distinct
version planes. The specification, companions, grammar, schemas, and Agent/Brain
instruction cards are vendored under `rs/anda_kip`.

## Implemented changes

- Strict portable JSON: safe binary64 integral values, source-token checks for
  overflow and nonzero underflow, duplicate decoded-key rejection, scalar Unicode,
  and invalid UTF-8/BOM rejection at the HTTP boundaries. Rust callers accepting
  raw JSON should use `Request::from_json` or `parse_canonical_json` before binding.
  Bound SDK values are also checked before execution.
- JCS number formatting and UTF-16 property ordering. Semantic Literal NFC
  normalization remains separate from artifact serialization.
- Conflict-complete BELIEF: candidate-local diagnosis, final status, slot status,
  discoverable competing references, and constraint reasons. Multiple supported
  values of a non-functional predicate remain valid. LIMIT does not truncate the
  opposition used to decide a belief.
- Exact context set inclusion and canonical Concept resolution. Context-free
  recall excludes scoped Assertions. Finite valid-time intervals require
  `from < until` and use `[from, until)`; the TypeScript projection now uses the
  query's FOR TIME coordinate during aggregation.
- ProjectionBasis includes Space/snapshot, Schema and identity versions, policy,
  trust and authorization identities, context, purpose/risk, valid time, and the
  next known Assertion boundary. The structural projection policy is version 2.
- Terminal Activities retain engine-captured input and committed output versions.
  A supplied DependencyBasis is checked against retained source versions or new
  inputs in the same transaction. Pins support whole-element versions and the
  existing `attributes`, `structural`, `retention`, `facets.<local-name>` planes.
  A terminal audit cannot acquire a retrospective read contract.
- Read-time dependency checking handles required groups, alternative support,
  context-only groups, unavailable sources, corrected/retracted prerequisites,
  temporal eligibility, cycles and bounded traversal. Raw artifacts remain intact;
  computed validity and final inferred belief change immediately. A retrospective
  Activity naming an existing output does not become its producing computation.
- Identity resolution records the supplied and resolved references in protected
  operation audit data. Merge advances the protected identity coordinate. Schema
  activation and merge envelopes expose `control_changes` separately from Elements.
- The 2.1.0 package and its validation-schema pins are checked on activation.
  Missing transitive resources, changed digests and unsupported value contracts
  fail activation. Rust uses a closed resource retriever; TypeScript ships generated
  static validators, including native ESM helper compatibility, with no runtime eval.
  Terminal record attachment and immutability checks cover UPDATE and UNSET too.

## Native Capsules

Native artifacts now use `format_version: "2.0-draft"`, flat `records`,
`schema_dependencies` with exact package refs/digests, `source.space_id`, named
roots/closure, object-valued blobs and handling, and `integrity.digest_profile:
"kip-jcs-safe-v1"`. SHA-256 covers every top-level field except integrity.
Receipt hashing remains independently identified as SHA3-256.

Computed canonical-endpoint views are excluded from the immutable record payload.
Engine-specific source control annotations travel as untrusted
`handling["anda/source_control"]`; they confer no destination authority.
Missing canonical fields and permission-redacted records become explicit
unavailable/redacted ExternalRefs in partial exports. Closed exports reject them.
An import rejects duplicate capsule-local IDs or unavailable selected roots.
Callers must supply genuine observation/assertion timestamps and Evidence digests;
export never fabricates missing historical information to satisfy a schema.

The previous draft Capsule format and numeric contract are rejected. The old
2.0.0 Profile bytes remain under `rs/anda_cognitive_nexus/profiles/legacy` for
inspection and explicit migration. Existing stored symbol lineages are preserved;
this synchronization does not rewrite a user's database or auto-upgrade its lock.
Legacy Rust frame annotations marked `serde(skip)` are not native wire members;
use namespaced handling metadata for additional annotations.

## Capability boundaries

Installing a vocabulary is not a full `KIP-CognitiveMemory` claim. Both engines
recognize the new capability names and keep unavailable requirements fail-fast:

| Capability | Current boundary |
| --- | --- |
| `memory_interface`, `memory_basic`, `memory_experience`, `memory_learning`, `memory_durable`, `memory_exchange` | No connected Agent-to-Brain binding. Schemas, role cards, bundle definitions and dependency-declaration helpers are available; the capabilities remain false. |
| `identity_repair` | Merge audit is retained; protected resolution withdrawal and affected-write repair are not implemented. |
| `dependency_validity` | Read-time checks and pin validation are implemented. The full capability remains false because explicit `dependency_validation` revalidation is not implemented. Create a new derivation with a new producing Activity. |
| `durable_brain_runtime` | No durable Brain outbox, fenced worker leases or dispatch reconciliation. |
| Historical belief | Historical control state is not retained. Default historical BELIEF fails `HistoricalSnapshotUnavailable`; an explicitly selected policy can reinterpret retained cognition and is disclosed in its basis. Raw historical reads remain available. |
| Control stream coverage | Schema/identity notifications are emitted. Other Governance changes retain their existing audit path; no complete filtered control-stream watermark or silence guarantee is claimed. |

Learning records and cache shapes are validated, but a class name or mutable tally
cannot confer local standing. Unsupported Trial/Attempt/Evaluation workflows and
non-proposed 2.1.0 Skill standing are refused. A published schema or artifact does
not implement a Brain ranking algorithm, authorize an external action, or prove
empirical learning improvement.

## Verification

The shared conformance fixture `cognitive-consistency.json` covers conflict/context,
exact boundaries and numeric-source rules. Both engines also execute tests for real
read pins, all-or-nothing pin failures, immediate invalidation, surviving any-of
support, terminal audit immutability, native Capsule shape and schema-lock closure.
The Rust parser's WASM oracle is rebuilt with the new numeric/identity rules.

Generate all committed TypeScript artifacts with `pnpm run codegen` in `ts/kip-do`.
`codegen:contracts` owns the static validators and memory bundle manifest. The
standard package is copied verbatim; changing its bytes requires a new upstream
package version and seal, not editing the generated TypeScript copy.
