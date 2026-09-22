import { CognitiveNexus, SYSTEM_PRINCIPAL } from '../../src/nexus.js'
import {
  spaceResource,
  subjectDigest,
  type Permission,
} from '../../src/governance/index.js'

/** Install a one-use approval requirement without changing other permissions. */
export function requireHostApproval(n: CognitiveNexus, permission: Permission) {
  const gov = n.store.governance,
    policyId = `kip:policy:approve-${permission}`
  gov.publishPolicy(
    {
      policy_id: policyId,
      space_id: n.space,
      statements: [
        {
          effect: 'allow',
          actions: [permission],
          obligations: {
            audit: true,
            approvals_required: 1,
            redaction_profile: '',
          },
        },
      ],
    },
    SYSTEM_PRINCIPAL,
  )
  const space = n.spaceRow()
  space.default_policy_id = policyId
  n.store.putSpace(space)
  return {
    approve() {
      const row = gov.requestApproval(
        {
          space_id: n.space,
          operation: permission,
          resource: 'the Space',
          subject_digest: subjectDigest(n.space, permission, spaceResource()),
          required: 1,
        },
        SYSTEM_PRINCIPAL,
      )
      gov.approve(row.id, 'kip:principal:independent-reviewer')
      return row.id
    },
    deny() {
      gov.publishPolicy(
        {
          policy_id: policyId,
          space_id: n.space,
          statements: [{ effect: 'deny', actions: [permission] }],
        },
        SYSTEM_PRINCIPAL,
      )
    },
  }
}
