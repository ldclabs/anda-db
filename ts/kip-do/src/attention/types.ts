import type { Json, JsonMap } from '../json.js'
import type { ArtifactPin } from '../cognitive.js'

export interface RuntimeScope {
  space_id: string
  space_instance: string
}
export interface RuntimePin {
  id: string
  digest: string
}
export interface RuntimePins {
  policy: RuntimePin
  evaluator: RuntimePin | null
  binding: RuntimePin | null
}
export interface AttentionConfig {
  scope: RuntimeScope
  pins: RuntimePins
}
export type WatchTrigger =
  | { kind: 'delta'; matched_seq: number }
  | { kind: 'silence'; due_at: string; due_seq: number }
export interface WatchFire {
  watch_ref: string
  arm_generation: number
  trigger: WatchTrigger
}
export interface WakeLease {
  owner: string
  expires_at_ms: number
}
export type WakeResume =
  | { kind: 'at'; not_before_ms: number }
  | { kind: 'on_change'; condition_digest: string }
export interface WakeRetry {
  reason: string
  resume: WakeResume
}
export type WakeState =
  | { stage: 'pending'; not_before_ms: number }
  | { stage: 'running'; lease: WakeLease }
  | { stage: 'blocked'; retry: WakeRetry }
  | { stage: 'completed' | 'cancelled'; receipt_ref: string }
export interface WakeRecord {
  format: string
  scope: RuntimeScope
  wake_ref: string
  fire: WatchFire
  fire_activity_ref: string
  pins: RuntimePins
  version: number
  fence: number
  state: WakeState
  parent_ref?: string
  continuation_key?: string
}
export interface WakeContinuation {
  key: string
  not_before_ms: number
}
export interface WakePage {
  items: WakeRecord[]
  snapshot_seq: number
  scanned: number
  next_cursor: string | null
  complete: boolean
}
export interface WakeResumeInput {
  wake: WakeRecord
  condition: Json
}
export type WakeResumeVerifier = (
  input: WakeResumeInput,
) => boolean | Promise<boolean>
export interface WatchCandidate {
  id: string
  envelope_seq: number
  change: JsonMap
  before: Json | null
  after: Json
}
export interface PreparedWatchPage {
  ticket_ref: string
  watch_ref: string
  arm_generation: number
  expected_version: number
  source_snapshot_seq: number
  through_seq: number
  deadline_covered: boolean
  page_digest: string
  evaluator: RuntimePin | null
  condition: Json
  candidates: WatchCandidate[]
}
export type WatchMatch = 'match' | 'no_match' | 'unknown'
export interface WatchJudgment {
  candidate_id: string
  result: WatchMatch
  rationale: string
}
export interface WatchEvaluation {
  evaluation_key: string
  evaluator: RuntimePin | null
  judgments: WatchJudgment[]
}
export interface DispatchLookupObserver {
  binding: RuntimePin
  principal_id: string
  configuration_digest: string
}
export type DispatchLookupStatus =
  | 'not_started'
  | 'running'
  | 'finished'
  | 'unknown'
export interface DispatchLookup {
  observation_key: string
  observed_at: string
  configuration_digest: string
  status: DispatchLookupStatus
}

// Private persisted host records, separate from cognitive Profile facets.
export interface WatchCheckpoint {
  format: string
  watch_ref: string
  arm_generation: number
  watch_class: string
  due_at: string | null
  condition_digest: string
  basis: JsonMap
  config: AttentionConfig
  due_seq: number | null
  matched_seq: number | null
}
export interface Ticket {
  format: string
  principal: string
  watch_ref: string
  expected: number
  generation: number
  limit: number
  material: ArtifactPin
}
export interface Payload {
  prepared: PreparedWatchPage
  source_at: string
  target: number
  due_seq: number | null
  deadline: boolean
  checkpoint_digest: string | null
  basis: JsonMap
}
export type Mode =
  | { kind: 'immediate' }
  | { kind: 'prepare'; key: string }
  | {
      kind: 'evaluate'
      ticketRef: string
      ticket: Ticket
      payload: Payload
      evaluation: WatchEvaluation
    }
