/**
 * The recording state of an extraction (Spec §57.8): the keys and pure reads
 * the view, projection and dependency checks share, kept apart from the repair
 * operation itself so they import nothing heavier than a type.
 */

import type { JsonMap } from './json.js'
import type { Element } from './store/rows.js'

/** The `governance` member naming the repair that invalidated an extraction. */
export const REPAIR_KEY = 'recording_repair'
/** The Activity class of a repair (Profile §6.5). */
export const REPAIR_CLASS = 'recording_repair'
/** The Facet a repair Activity carries (Profile §6.5). */
export const REPAIR_FACET = 'RecordingRepair'

/** `_system.recording_validity` of an Assertion. */
export interface RecordingValidity {
  status: 'valid' | 'invalidated'
  repair_ref: string | null
}

/** The repair that invalidated an element, if one did. */
export function repairRef(governance: JsonMap | undefined): string | null {
  const value = governance?.[REPAIR_KEY]
  return typeof value === 'string' && value !== '' ? value : null
}

export function isInvalidated(element: Element): boolean {
  return repairRef(element.row.governance) !== null
}

export function recordingValidity(governance: JsonMap | undefined): RecordingValidity {
  const ref = repairRef(governance)
  return ref === null ? { status: 'valid', repair_ref: null } : { status: 'invalidated', repair_ref: ref }
}

