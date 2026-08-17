# KIP 2.0 System Instructions

What a KIP 2.0 runtime owes its callers, from the execution and governance side.
Written for whoever implements or operates a Cognitive Nexus. The normative
rules are in [SPECIFICATION.md](./SPECIFICATION.md); where this document
disagrees with it, the Specification wins.

## 1. What the runtime decides, and what it must not

The runtime — not the caller, and never the content — decides:

```text
what a command actually is        parse and classify, ignore the label
who the Principal is              transport authentication only
what the Principal may do         Governance policy
which Space the request runs in   explicit selection, never inferred from chat
what committed                    the engine's own transaction record
```

A caller-supplied `language: "KQL"` on a command that mutates is a
`LanguageMismatch`, not a hint. A read-only endpoint rejects state-changing
semantics by what the parsed command *is*.

Request-body identity claims never replace the transport-authenticated
Principal. `context.purpose`, `context.risk`, `context.locale` and
`context.client` are advisory: they grant no identity, access, representation or
authority.

## 2. Separate the planes, and keep them separate

KIP 2.0 has no universal author-writable metadata bag, on purpose:

```text
semantic payload       → typed fields / attributes
epistemic state        → Assertion
observations           → Evidence
provenance             → Activity / _system.origin
governance             → Governance state
storage lifecycle      → retention
mnemonic/profile state → Facets
engine truth           → _system
```

`_system`, `governance`, `space_id` and `space_seq` are engine-owned. A mutation
that names them is rejected before execution: external cognition cannot
self-escalate authority.

Engine origin and claimed provenance are different records. `_system.origin.principal_id`
is who wrote it; `Assertion.asserted_by` is whose stance it is. Never derive one
from the other.

## 3. Immutability is the audit trail

```text
Proposition tuple      immutable after creation
Assertion payload      immutable; revise with a new Assertion + supersession
Evidence payload       immutable; correct via CORRECT EVIDENCE lineage
terminal Activity      immutable; finalize outputs in the transition that ends it
```

An `UPDATE` reaching any of those is `EpistemicRevisionRequired`,
`EvidenceCorrectionRequired`, `ImmutableField` or `ActivityTerminal` — never a
silent rewrite. Retraction means the assertor actually withdrew: administrative
moderation must not be recorded as a retraction that never happened.

`MERGE CONCEPT` is non-destructive identity consolidation. The source stays
addressable as merged history; raw historical references keep resolving.

## 4. Truth is projected, never stored

Belief is computed by an Epistemic Projection under a named policy, and the
result must say which policy produced it. Projection is read-only: it can never
be a mutation target, which is why `BELIEF` is absent from the KML and EXPORT
grammars.

Open-world by default:

```text
insufficient   no adequate epistemic basis — the unknown state
rejected       eligible opposition is sufficient
```

`rejected` MUST NOT be produced merely because support is absent. Contradiction
is a representable state (`contested`), not a storage error to resolve by
dropping one side.

Confidence, trust, memory strength and salience are four different quantities.
Do not decay confidence as a forgetting mechanism: forgetting is mnemonic, and
degrading epistemic commitment to model it corrupts the audit trail.

Evidence independence matters: N copies of one message are one basis, not N.
Corroboration requires independent provenance roots.

## 5. Transactions, batches and outcomes

```text
operations[]  ≠  transaction        unless execution.mode = atomic
progress      ≠  commit
timeout       ≠  abort
```

Under `sequence`, each state-changing operation commits separately and earlier
commits are **not** rolled back; report that as `partial`, never as `failed`, or
callers will re-issue writes that already landed.

`atomic` means one transaction, one start snapshot, read-your-writes, all-or-none
commit, one `tx_id`, one state-changing `space_seq`. Offer it only if you provide
it. An unsupported stronger isolation must fail explicitly rather than silently
downgrade.

When a write may have committed but the response path cannot establish the
outcome, answer `outcome_unknown`. Never let a lost response become a second
write: recovery is idempotency-key lookup or an identical retry.

A lagging semantic or vector SEARCH index MUST NOT be presented as
transaction-snapshot-consistent when it is not. Report `index_seq` against
`current_space_seq` and let the caller decide.

## 6. Governance

Deny overrides. Protocol invariants override policy: no grant makes `_system`
writable or lets memory content confer permission.

Use existence-neutral errors (`NotFoundOrNotVisible`) where distinguishing
"absent" from "forbidden" would map protected state. Watch the same leak in
aggregates: a `COUNT` over invisible elements is still disclosure.

Cursors are runtime-issued and must be unforgeable. A snapshot token binds a
readable coordinate; it is not an authority token, and current Governance always
applies to it.

## 7. Imports and artifacts

```text
Capsule bytes  ≠  destination mutation authority
```

A valid signature attests that a signer stood behind a digest. It proves nothing
about truth, safety, utility, trust, authority, or applicability here. Import
runs `VERIFY → VALIDATE → PREVIEW → Governance analysis → Import Plan → atomic
Import Transaction`, and the destination applies its own trust, classification,
authority, Schema and Governance.

- A source element id never becomes the destination's local primary id.
- Same name is not same identity.
- Source `$self` maps to the destination `$self` only under a verified restore.
- Embedded Schema Packages are validation-only and never auto-activate.
- Imported Skills arrive inactive.
- Artifact handles are opaque; no arbitrary URL is fetched without separate
  network authority.

## 8. Errors are an interface

Every error carries a stable `code`, a `category`, a `retry.class` and a `hint`.
The retry class is a contract with the caller's recovery logic:

```text
safe_same_request            nothing durable happened
requires_refresh             re-read, then retry
requires_different_input     the request must change
requires_authority           the caller lacks authority, not information
requires_new_snapshot        acquire a fresh coordinate
requires_reacquire_artifact  re-stage the bytes
outcome_lookup_required      the write's fate is undecided
non_retryable                retrying cannot help
```

Do not widen a classification into claiming a failed write never happened. If
you cannot tell, `outcome_lookup_required` is the honest answer.

## 9. Ingestion

Offer an ingestion context (or artifact handles) so observed payloads reach
Evidence from the transport envelope rather than through model-generated command
text. Minting is transactional: if the transaction aborts, no Evidence is
durably created. `client_key` gives it retry-safe logical identity.

## 10. Migration from KIP 1.x

Migration is semantic decomposition, not a field rename, and it must not
fabricate structure the old system never recorded:

```text
legacy fact Proposition  → Proposition + migrated positive Assertion
metadata.confidence      → classify first; it may be truth, accessibility or staleness
author string            → not an authenticated Principal
$self / $system          → not Principal, not admin authority
Domain                   → not a MemorySpace
expires_at               → retention, not valid time
legacy destructive merge → not native merge
legacy EXPORT            → not a native Capsule
schema graph nodes       → not authoritative Schema
```

Preserve ambiguous legacy values explicitly as legacy annotations, surface the
uncertainty as warnings, and stop any inherited confidence decay rather than
carrying it into native epistemic state.
