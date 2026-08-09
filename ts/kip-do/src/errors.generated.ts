/**
 * KIP error taxonomy — GENERATED FILE, DO NOT EDIT.
 *
 * Source of truth: `rs/anda_kip/src/error.rs`.
 * Regenerate with `pnpm run codegen:errors` after changing the Rust enum.
 *
 * Grammar version: 0.11.0
 */

/** Every KIP error code, in the order the Rust enum declares them. */
export type KipErrorCode =
  | "KIP_1001"
  | "KIP_1002"
  | "KIP_2001"
  | "KIP_2002"
  | "KIP_2003"
  | "KIP_3001"
  | "KIP_3002"
  | "KIP_3003"
  | "KIP_3004"
  | "KIP_3005"
  | "KIP_4001"
  | "KIP_4002"
  | "KIP_4003"

export const KIP_ERROR_CODES: readonly KipErrorCode[] = [
  "KIP_1001",
  "KIP_1002",
  "KIP_2001",
  "KIP_2002",
  "KIP_2003",
  "KIP_3001",
  "KIP_3002",
  "KIP_3003",
  "KIP_3004",
  "KIP_3005",
  "KIP_4001",
  "KIP_4002",
  "KIP_4003",
]

/** Stable error name, e.g. `"InvalidSyntax"` for `KIP_1001`. */
export const KIP_ERROR_NAMES: Readonly<Record<KipErrorCode, string>> = {
  KIP_1001: "InvalidSyntax",
  KIP_1002: "InvalidIdentifier",
  KIP_2001: "TypeMismatch",
  KIP_2002: "ConstraintViolation",
  KIP_2003: "InvalidValueType",
  KIP_3001: "ReferenceError",
  KIP_3002: "NotFound",
  KIP_3003: "DuplicateExists",
  KIP_3004: "ImmutableTarget",
  KIP_3005: "VersionConflict",
  KIP_4001: "ExecutionTimeout",
  KIP_4002: "ResourceExhausted",
  KIP_4003: "InternalError",
}

/**
 * Agent-facing recovery hint. This is what makes KIP errors self-correcting;
 * it is part of the wire contract, not a developer comment.
 */
export const KIP_ERROR_HINTS: Readonly<Record<KipErrorCode, string>> = {
  KIP_1001: "Check parenthesis matching, keyword spelling, and statement structure. Ensure JSON data format is valid.",
  KIP_1002: "Identifiers must match regex `[a-zA-Z_][a-zA-Z0-9_]*`.",
  KIP_2001: "Execute `DESCRIBE` to confirm type names. Remember types are case-sensitive (`Drug` vs `drug`).",
  KIP_2002: "Supply the missing required attributes.",
  KIP_2003: "Correct the JSON value type.",
  KIP_3001: "Ensure the variable is defined and bound in the WHERE clause (for KQL) or the CONCEPT block is placed before referencing clauses (for KML).",
  KIP_3002: "Target may have been deleted or never created. Try `SEARCH` or `FIND` to confirm existence first.",
  KIP_3003: "If intent is update, check if `UPSERT` should be used instead of creation logic.",
  KIP_3004: "**Operation Prohibited.** Do not attempt to modify system meta-definitions or core identity nodes.",
  KIP_3005: "The element changed since you read it. Re-read it (obtaining the fresh `_version`), re-apply your merge in memory, and retry with the new `EXPECT VERSION`.",
  KIP_4001: "Optimize query. Reduce `UNION` usage, lower `LIMIT`, or reduce regex/hops.",
  KIP_4002: "Must use `LIMIT` and `CURSOR` for pagination.",
  KIP_4003: "Contact system administrator or retry later.",
}
