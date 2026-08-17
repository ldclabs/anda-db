import { env, runInDurableObject } from 'cloudflare:test'
import { describe, expect, it } from 'vitest'
import {
  ALL_PERMISSIONS,
  authStrength,
  authority,
  ceilingOf,
  classification,
  conditionsContain,
  constraintsContain,
  emptyConditions,
  emptyConstraints,
  emptyScope,
  familyOf,
  govStatus,
  grantId,
  inForceAt,
  isAlwaysAudited,
  mergeObligations,
  parsePermission,
  principalAuth,
  rowIdOf,
  scopeContains,
  spaceResource,
  subjectDigest,
  tightenConstraints,
} from '../src/governance/index.js'
import { parseElementId } from '../src/id.js'
import type { Json } from '../src/json.js'
import { CognitiveNexus, SYSTEM_PRINCIPAL } from '../src/nexus.js'
import { nowTime } from '../src/time.js'
import { COGNITIVE_MEMORY } from '../src/schema/index.js'
import { Store, classificationOf } from '../src/store/index.js'

/**
 * The Governance plane's storage and its lattices.
 *
 * The lattice tests look small and are the load-bearing ones: every rule they
 * pin resolves toward *refusing* when something unrecognized turns up, and each
 * of them is a place where the obvious implementation is wrong in the direction
 * that widens authority.
 */

async function withStore<T>(name: string, body: (store: Store) => T): Promise<T> {
  const stub = env.KIP_DB.getByName(name)
  return await runInDurableObject(stub, (_instance, state) =>
    body(new Store(state.storage.sql)),
  )
}

/**
 * Waits until the object's clock has moved past an instant.
 *
 * A Workers isolate's clock does not advance during synchronous execution — it
 * moves at I/O boundaries — so two Governance mutations made in one request
 * share a timestamp and the historical view cannot separate them. That is a real
 * property of the engine, documented in `store/governance.ts`, rather than
 * something to work around: a test that means to observe a *past* state has to
 * put the two writes in different observable instants.
 *
 * It waits for the clock rather than sleeping a fixed amount, because how far a
 * sleep moves the object's clock is the platform's business and not something a
 * test should be betting on. Sleeping "long enough" made this pass about two
 * runs in three.
 */
async function tickPast(name: string, at: string): Promise<void> {
  for (let attempt = 0; attempt < 40; attempt++) {
    await new Promise((resolve) => setTimeout(resolve, 5))
    const now = await withStore(name, () => nowTime())
    if (now > at) return
  }
  throw new Error(`the object clock never moved past ${at}`)
}

describe('the classification lattice', () => {
  it('does not read an absent classification as public', () => {
    // §95: the one classification rule a deployment cannot vary.
    expect(classification.DEFAULT).not.toBe(classification.PUBLIC)
    expect(classification.rank('')).toBeGreaterThan(
      classification.rank(classification.PUBLIC),
    )
  })

  it('ranks an unknown label above every known one', () => {
    expect(classification.rank('ultra')).toBeGreaterThan(
      classification.rank(classification.SECRET),
    )
    expect(classification.join('ultra', classification.SECRET)).toBe('ultra')
  })

  it('gives derived content the more restrictive label', () => {
    // §242: a summary of secret Evidence does not become public by being a
    // summary.
    expect(classification.join(classification.SECRET, classification.PUBLIC)).toBe(
      classification.SECRET,
    )
  })
})

describe('review regressions', () => {
  async function withRegression<T>(
    name: string,
    body: (nexus: CognitiveNexus) => T,
  ): Promise<T> {
    const stub = env.KIP_DB.getByName(`review-${name}`)
    return await runInDurableObject(stub, (_instance, state) => {
      const nexus = CognitiveNexus.connect(state.storage)
      nexus.activatePackages([COGNITIVE_MEMORY])
      return body(nexus)
    })
  }

  it('requires delegation permission and attenuates every constraint', async () => {
    await withRegression('delegation-attenuation', (nexus) => {
      nexus.execute('CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }')
      const gov = nexus.store.governance
      for (const id of ['kip:principal:lead', 'kip:principal:sub']) {
        gov.ensurePrincipal({ principal_id: id })
      }
      gov.createGrant(
        {
          space_id: nexus.space,
          grantee_principal: 'kip:principal:lead',
          actions: ['read'],
          scope: { kinds: ['concept'] },
          constraints: { fields: ['name'], max_results: 1 },
          delegation_allowed: false,
        },
        SYSTEM_PRINCIPAL,
      )
      const forbidden = gov.createDelegation(
        {
          space_id: nexus.space,
          delegator_principal: 'kip:principal:lead',
          delegate_principal: 'kip:principal:sub',
          actions: ['read'],
          scope: { kinds: ['concept'] },
          constraints: { fields: ['name'], max_results: 1 },
        },
        'kip:principal:lead',
      )
      const sub = nexus.session(principalAuth('kip:principal:sub'))
      expect(() => sub.query('FIND(?c.name) WHERE { ?c CONCEPT {} }')).toThrowError(
        /requires the read permission/,
      )
      gov.revokeDelegation(forbidden.id, 'kip:principal:lead')

      gov.createGrant(
        {
          space_id: nexus.space,
          grantee_principal: 'kip:principal:lead',
          actions: ['read'],
          scope: { kinds: ['concept'] },
          constraints: { fields: ['name'], max_results: 1 },
          delegation_allowed: true,
        },
        SYSTEM_PRINCIPAL,
      )
      gov.createDelegation(
        {
          space_id: nexus.space,
          delegator_principal: 'kip:principal:lead',
          delegate_principal: 'kip:principal:sub',
          actions: ['read'],
          scope: { kinds: ['concept'] },
        },
        'kip:principal:lead',
      )
      expect(() => sub.query('FIND(?c.name) WHERE { ?c CONCEPT {} }')).toThrowError(
        /requires the read permission/,
      )

      gov.createDelegation(
        {
          space_id: nexus.space,
          delegator_principal: 'kip:principal:lead',
          delegate_principal: 'kip:principal:sub',
          actions: ['read'],
          scope: { kinds: ['concept'] },
          constraints: { fields: ['name'], max_results: 1 },
        },
        'kip:principal:lead',
      )
      expect(sub.query('FIND(?c.name) WHERE { ?c CONCEPT {} }')).toEqual(['Alice'])
    })
  })

  it('caps query rows and refuses an unavailable redaction obligation', async () => {
    await withRegression('read-constraints', (nexus) => {
      nexus.execute(`MUTATE {
        CREATE CONCEPT ?a { TYPE "Person" NAME "Alice" }
        CREATE CONCEPT ?b { TYPE "Person" NAME "Bob" }
      }`)
      const gov = nexus.store.governance
      gov.ensurePrincipal({ principal_id: 'kip:principal:reader' })
      gov.createGrant(
        {
          space_id: nexus.space,
          grantee_principal: 'kip:principal:reader',
          actions: ['read'],
          constraints: { max_results: 1 },
        },
        SYSTEM_PRINCIPAL,
      )
      const reader = nexus.session(principalAuth('kip:principal:reader'))
      expect(reader.query('FIND(?c.name) WHERE { ?c CONCEPT {} }')).toHaveLength(1)

      gov.publishPolicy(
        {
          policy_id: 'kip:policy:redact',
          space_id: nexus.space,
          statements: [{
            effect: 'allow',
            actions: ['read'],
            obligations: {
              audit: false,
              approvals_required: 0,
              redaction_profile: 'safe-summary',
            },
          }],
        },
        SYSTEM_PRINCIPAL,
      )
      const space = nexus.spaceRow()
      space.default_policy_id = 'kip:policy:redact'
      nexus.store.putSpace(space)
      expect(() => reader.query('FIND(?c.name) WHERE { ?c CONCEPT {} }')).toThrowError(
        /redaction profile/,
      )
    })
  })

  it('enforces the influence-authority ceiling carried by a Grant', async () => {
    await withRegression('authority-ceiling', (nexus) => {
      nexus.execute('CREATE EVIDENCE ?e { SET FIELDS {evidence_class: "Document", payload: "x"} }')
      const gov = nexus.store.governance
      gov.ensurePrincipal({ principal_id: 'kip:principal:steward' })
      gov.createGrant(
        {
          space_id: nexus.space,
          grantee_principal: 'kip:principal:steward',
          actions: ['read', 'elevate_authority'],
          constraints: { max_influence_authority: 'descriptive' },
        },
        SYSTEM_PRINCIPAL,
      )
      const steward = nexus.session(principalAuth('kip:principal:steward'))
      expect(() => steward.elevateAuthority(parseElementId('E-1'), 'behavioral')).toThrowError(
        /ceiling/,
      )
      expect(ceilingOf(nexus.store.load(parseElementId('E-1'))!)).toBe('descriptive')
    })
  })

  it('counts approvers globally and spends them only after success', async () => {
    await withRegression('approval-lifecycle', (nexus) => {
      const gov = nexus.store.governance
      gov.publishPolicy(
        {
          policy_id: 'kip:policy:approval',
          space_id: nexus.space,
          statements: [{
            effect: 'allow',
            actions: ['export'],
            obligations: {
              audit: false,
              approvals_required: 2,
              redaction_profile: '',
            },
          }],
        },
        SYSTEM_PRINCIPAL,
      )
      const space = nexus.spaceRow()
      space.default_policy_id = 'kip:policy:approval'
      nexus.store.putSpace(space)
      const digest = subjectDigest(nexus.space, 'export', spaceResource())
      const approvals = [1, 2].map(() =>
        gov.requestApproval(
          {
            space_id: nexus.space,
            operation: 'export',
            resource: 'the Space',
            subject_digest: digest,
            required: 1,
          },
          'kip:principal:requester',
        ),
      )
      for (const approval of approvals) {
        gov.approve(approval.id, 'kip:principal:one-reviewer')
      }
      expect(() =>
        nexus.describe('EXPORT CAPSULE :out WHERE { ?c CONCEPT {} }', { out: 'x' }),
      ).toThrowError(/independent approval/)

      gov.publishPolicy(
        {
          policy_id: 'kip:policy:approval',
          space_id: nexus.space,
          statements: [{
            effect: 'allow',
            actions: ['update'],
            obligations: {
              audit: false,
              approvals_required: 2,
              redaction_profile: '',
            },
          }],
        },
        SYSTEM_PRINCIPAL,
      )
      const updateDigest = subjectDigest(nexus.space, 'update', spaceResource())
      const second = gov.requestApproval(
        {
          space_id: nexus.space,
          operation: 'update',
          resource: 'the Space',
          subject_digest: updateDigest,
          required: 2,
        },
        'kip:principal:requester',
      )
      gov.approve(second.id, 'kip:principal:reviewer-a')
      gov.approve(second.id, 'kip:principal:reviewer-b')
      expect(() =>
        nexus.execute('UPDATE "C-99" SET FIELDS { name: "nobody" }'),
      ).toThrowError(/C-99|not found|visible/i)
      expect(gov.findApproval(second.id)?.status).toBe('granted')
    })
  })

  it('journals element Governance changes and reconstructs past Principal state', async () => {
    const suspended = await withRegression('history-control', (nexus) => {
      nexus.execute('CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }')
      nexus.systemSession().classify(parseElementId('C-1'), 'secret')
      const changes = nexus.describe('CHANGES AFTER SEQ 1') as {
        changes: { id: string; op: string }[]
      }
      expect(changes.changes).toContainEqual(
        expect.objectContaining({ id: 'C-1', op: 'classify' }),
      )
      return nexus.store.governance.setPrincipalStatus(
        SYSTEM_PRINCIPAL,
        govStatus.SUSPENDED,
        SYSTEM_PRINCIPAL,
      )
    })
    await tickPast('review-history-control', suspended.updated_at)
    await withRegression('history-control', (nexus) => {
      nexus.store.governance.setPrincipalStatus(
        SYSTEM_PRINCIPAL,
        govStatus.ACTIVE,
        SYSTEM_PRINCIPAL,
      )
      const then = nexus.systemSession().accessAsOf(suspended.updated_at) as {
        permissions: string[]
      }
      expect(then.permissions).toEqual([])
    })
  })

  it('compares constraint sets as part of attenuation', () => {
    expect(
      constraintsContain(
        { ...emptyConstraints(), fields: ['name'], max_results: 1 },
        emptyConstraints(),
      ),
    ).toBe(false)
  })
})

describe('the influence-authority ladder', () => {
  it('never raises a derived ceiling', () => {
    // §243: reformatting a descriptive Skill does not make it executable.
    expect(authority.meet(authority.DESCRIPTIVE, authority.EXECUTABLE)).toBe(
      authority.DESCRIPTIVE,
    )
    expect(authority.DEFAULT).toBe(authority.DESCRIPTIVE)
  })

  it('does not let an unknown class outrank executable', () => {
    expect(authority.rank('supreme')).toBeLessThan(authority.rank(authority.EXECUTABLE))
  })
})

describe('the authentication strength ladder', () => {
  it('puts an invented strength on the weakest rung', () => {
    // The opposite of the classification rule, and for the same reason: both
    // resolve toward refusing. An unknown strength must satisfy no bar; an
    // unknown sensitivity must fall below none.
    expect(authStrength.rank('quantum-grade')).toBe(0)
    expect(authStrength.rank('quantum-grade')).toBeLessThan(
      authStrength.rank(authStrength.STANDARD),
    )
  })
})

describe('the permission registry', () => {
  it('round-trips every name', () => {
    for (const permission of ALL_PERMISSIONS) {
      expect(parsePermission(permission)).toBe(permission)
    }
  })

  it('refuses an unimplemented permission rather than ignoring it', () => {
    // A Grant that silently confers nothing is worse than one that fails: the
    // holder finds out during an incident instead of at write time.
    expect(() => parsePermission('read_everything')).toThrowError(/not a permission/)
  })

  it('keeps the distinctions the spec requires', () => {
    // §271: the core governance equations. If a refactor ever merges one of
    // these, this test is what says so.
    for (const [a, b] of [
      ['read', 'export'],
      ['read', 'discover'],
      ['update', 'declassify'],
      ['assert', 'assert_as_actor'],
      ['record_attributed_assertion', 'assert_as_actor'],
      ['maintain', 'manage_policy'],
      ['tombstone', 'purge'],
      ['retract_own', 'moderate_assertion'],
    ] as const) {
      expect(a).not.toBe(b)
      expect(ALL_PERMISSIONS).toContain(a)
      expect(ALL_PERMISSIONS).toContain(b)
    }
  })

  it('always audits changes to the control plane', () => {
    expect(isAlwaysAudited('manage_policy')).toBe(true)
    expect(isAlwaysAudited('elevate_authority')).toBe(true)
    expect(isAlwaysAudited('purge')).toBe(true)
    expect(isAlwaysAudited('export')).toBe(true)
    // An ordinary read is audited only where a policy asks for it (§173).
    expect(isAlwaysAudited('read')).toBe(false)
  })

  it('resolves a family without resolving authorization on one', () => {
    expect(familyOf('read')).toBe('discovery')
    expect(familyOf('manage_grants')).toBe('governance')
  })
})

describe('attenuation', () => {
  it('lets anything narrow an unrestricted parent', () => {
    expect(scopeContains(emptyScope(), { ...emptyScope(), kinds: ['concept'] })).toBe(true)
    expect(scopeContains(emptyScope(), emptyScope())).toBe(true)
  })

  it('refuses a child that widens a restricted parent', () => {
    const parent = { ...emptyScope(), kinds: ['concept'] }
    expect(scopeContains(parent, { ...emptyScope(), kinds: ['concept'] })).toBe(true)
    expect(scopeContains(parent, { ...emptyScope(), kinds: ['concept', 'evidence'] })).toBe(
      false,
    )
    // "Every kind" is the widening §31 exists to refuse, so an empty child list
    // against a bounded parent must not pass.
    expect(scopeContains(parent, emptyScope())).toBe(false)
  })

  it('refuses a child delegation that outlives its parent', () => {
    const parent = { ...emptyConditions(), valid_until: '2026-09-01T00:00:00.000Z' }
    expect(
      conditionsContain(parent, {
        ...emptyConditions(),
        valid_until: '2026-08-20T00:00:00.000Z',
      }),
    ).toBe(true)
    expect(
      conditionsContain(parent, {
        ...emptyConditions(),
        valid_until: '2027-01-01T00:00:00.000Z',
      }),
    ).toBe(false)
    // §238: "read + export, valid 1 year" under a one-day parent.
    expect(conditionsContain(parent, emptyConditions())).toBe(false)
  })

  it('refuses a child that lowers the authentication bar', () => {
    const parent = { ...emptyConditions(), min_auth_strength: authStrength.STRONG }
    expect(
      conditionsContain(parent, {
        ...emptyConditions(),
        min_auth_strength: authStrength.STRONG,
      }),
    ).toBe(true)
    expect(
      conditionsContain(parent, {
        ...emptyConditions(),
        min_auth_strength: authStrength.STANDARD,
      }),
    ).toBe(false)
  })

  it('only ever tightens constraints', () => {
    const broad = { ...emptyConstraints(), max_results: 1000, export: true }
    const narrow = {
      ...emptyConstraints(),
      fields: ['summary'],
      max_results: 10,
      export: false,
    }
    const effective = tightenConstraints(broad, narrow)
    expect(effective.max_results).toBe(10)
    expect(effective.fields).toEqual(['summary'])
    expect(effective.export).toBe(false)
  })

  it('only ever accumulates obligations', () => {
    const merged = mergeObligations(
      { audit: true, approvals_required: 0, redaction_profile: '' },
      { audit: false, approvals_required: 2, redaction_profile: 'safe-summary' },
    )
    expect(merged.audit).toBe(true)
    expect(merged.approvals_required).toBe(2)
    expect(merged.redaction_profile).toBe('safe-summary')
  })
})

describe('being in force at an instant', () => {
  it('reads the timestamps rather than the status', () => {
    // A record created after the coordinate did not exist then; one revoked at
    // or before it was already gone.
    expect(inForceAt('2026-01-01T00:00:00.000Z', '', '2025-12-31T23:59:59.999Z')).toBe(false)
    expect(inForceAt('2026-01-01T00:00:00.000Z', '', '2026-01-01T00:00:00.000Z')).toBe(true)
    expect(
      inForceAt('2026-01-01T00:00:00.000Z', '2026-02-01T00:00:00.000Z', '2026-01-15T00:00:00.000Z'),
    ).toBe(true)
    expect(
      inForceAt('2026-01-01T00:00:00.000Z', '2026-02-01T00:00:00.000Z', '2026-03-01T00:00:00.000Z'),
    ).toBe(false)
  })

  it('resolves a same-instant create-and-revoke toward not in force', () => {
    // The Workers clock does not advance during synchronous execution, so this
    // is the ordinary case for two mutations in one request rather than a rare
    // race. Under-reporting authority is the safe direction.
    const at = '2026-01-01T00:00:00.000Z'
    expect(inForceAt(at, at, at)).toBe(false)
  })
})

describe('minted Governance ids', () => {
  it('round-trips a row number', () => {
    expect(rowIdOf(grantId(42))).toBe(42)
    expect(rowIdOf('kip:grant:not-a-number')).toBeNull()
    expect(rowIdOf('no-colons')).toBeNull()
  })
})

describe('the Governance store', () => {
  it('creates a Principal once, however often bootstrap runs', async () => {
    await withStore('gov-principal', (store) => {
      const first = store.governance.ensurePrincipal({
        principal_id: 'kip:principal:alice',
        display_name: 'Alice',
      })
      const again = store.governance.ensurePrincipal({
        principal_id: 'kip:principal:alice',
        display_name: 'somebody else entirely',
      })
      expect(again.id).toBe(first.id)
      expect(again.display_name).toBe('Alice')
    })
  })

  it('revokes rather than deletes, and keeps the audit', async () => {
    await withStore('gov-revoke', (store) => {
      const gov = store.governance
      gov.ensurePrincipal({ principal_id: 'kip:principal:agent' })
      const grant = gov.createGrant(
        {
          space_id: 'kip:space:default',
          grantee_principal: 'kip:principal:agent',
          actions: ['read'],
        },
        'kip:principal:system',
      )
      expect(gov.grantsFor('kip:space:default', 'kip:principal:agent', [])).toHaveLength(1)

      gov.revokeGrant(grant.id, 'kip:principal:system')
      // Gone for future operations…
      expect(gov.grantsFor('kip:space:default', 'kip:principal:agent', [])).toHaveLength(0)
      // …and still there, so an audit of a past operation still resolves.
      expect(gov.grant(grant.id)?.status).toBe(govStatus.REVOKED)
      expect(gov.grant(grant.id)?.revoked_at).not.toBe('')
    })
  })

  it('answers what was in force then, not what is in force now', async () => {
    const grant = await withStore('gov-historical', (store) =>
      store.governance.createGrant(
        {
          space_id: 'kip:space:default',
          grantee_principal: 'kip:principal:agent',
          actions: ['read'],
        },
        'kip:principal:system',
      ),
    )
    await tickPast('gov-historical', grant.created_at)
    await withStore('gov-historical', (store) => {
      store.governance.revokeGrant(grant.id, 'kip:principal:system')
    })

    await withStore('gov-historical', (store) => {
      const gov = store.governance
      // §177/§179: an auditor asking about the past gets the past's answer, and
      // that answer is not a claim about today.
      expect(
        gov.grantsAt('kip:space:default', 'kip:principal:agent', [], grant.created_at),
      ).toHaveLength(1)
      expect(
        gov.grantsAt('kip:space:default', 'kip:principal:agent', [], '2999-01-01T00:00:00.000Z'),
      ).toHaveLength(0)
      // …and the live lookup is unaffected by the historical one.
      expect(gov.grantsFor('kip:space:default', 'kip:principal:agent', [])).toHaveLength(0)
    })
  })

  it('finds a Principal through its groups', async () => {
    await withStore('gov-groups', (store) => {
      const gov = store.governance
      gov.putGroup(
        { group_id: 'kip:group:maintainers', members: ['kip:principal:agent'] },
        'kip:principal:system',
      )
      gov.putGroup({ group_id: 'kip:group:others', members: [] }, 'kip:principal:system')
      expect(gov.groupsOf('kip:principal:agent')).toEqual(['kip:group:maintainers'])

      const grant = gov.createGrant(
        {
          space_id: 'kip:space:default',
          grantee_group: 'kip:group:maintainers',
          actions: ['maintain'],
        },
        'kip:principal:system',
      )
      const found = gov.grantsFor('kip:space:default', 'kip:principal:agent', [
        'kip:group:maintainers',
      ])
      expect(found.map((row) => row.id)).toEqual([grant.id])
    })
  })

  it('replays past membership from the audit, not from the current list', async () => {
    const whileMember = await withStore('gov-groups-history', (store) =>
      store.governance.putGroup(
        { group_id: 'kip:group:maintainers', members: ['kip:principal:agent'] },
        'kip:principal:system',
      ),
    ).then((group) => group.updated_at)
    await tickPast('gov-groups-history', whileMember)
    await withStore('gov-groups-history', (store) => {
      store.governance.putGroup(
        { group_id: 'kip:group:maintainers', members: [] },
        'kip:principal:system',
      )
    })

    await withStore('gov-groups-history', (store) => {
      const gov = store.governance
      expect(gov.groupsOf('kip:principal:agent')).toEqual([])
      // The group row now says the Principal is not a member. Only the audit
      // says it was — which is the whole reason §177 is answerable.
      expect(gov.groupsOfAt('kip:principal:agent', whileMember)).toEqual([
        'kip:group:maintainers',
      ])
    })
  })

  it('appends a policy version instead of editing one', async () => {
    const first = await withStore('gov-policy', (store) =>
      store.governance.publishPolicy(
        { policy_id: 'kip:policy:default', statements: [] },
        'kip:principal:system',
      ),
    )
    await tickPast('gov-policy', first.created_at)
    const second = await withStore('gov-policy', (store) =>
      store.governance.publishPolicy(
        {
          policy_id: 'kip:policy:default',
          statements: [{ effect: 'deny', actions: ['export'] }],
        },
        'kip:principal:system',
      ),
    )

    await withStore('gov-policy', (store) => {
      const gov = store.governance
      expect(first.version).toBe(1)
      expect(second.version).toBe(2)
      expect(gov.activePolicy('kip:policy:default')?.version).toBe(2)
      // An audit record naming version 1 still resolves to what version 1 said,
      // which is the whole reason a policy update appends (§46).
      expect(gov.policyAt('kip:policy:default', first.created_at)?.version).toBe(1)
      expect(gov.policyVersions('kip:policy:default')).toHaveLength(2)
      // The first version's statements were not rewritten by the second.
      expect(gov.policyVersions('kip:policy:default')[0]?.statements).toEqual([])
    })
  })

  it('counts one Principal once, however many times it approves', async () => {
    await withStore('gov-approval', (store) => {
      const gov = store.governance
      const approval = gov.requestApproval(
        {
          space_id: 'kip:space:default',
          operation: 'purge',
          resource: 'E-1',
          subject_digest: 'sha256:abc',
          required: 2,
        },
        'kip:principal:requester',
      )
      gov.approve(approval.id, 'kip:principal:reviewer-a')
      expect(() => gov.approve(approval.id, 'kip:principal:reviewer-a')).toThrowError(
        /counts once/,
      )
      // §246: one approval where two are required is not partial activation.
      expect(gov.grantedApprovals('kip:space:default', 'sha256:abc')).toHaveLength(0)

      gov.approve(approval.id, 'kip:principal:reviewer-b')
      expect(gov.grantedApprovals('kip:space:default', 'sha256:abc')).toHaveLength(1)
    })
  })

  it('refuses the requester its own approval', async () => {
    await withStore('gov-self-approval', (store) => {
      const gov = store.governance
      const approval = gov.requestApproval(
        {
          space_id: 'kip:space:default',
          operation: 'declassify',
          resource: 'C-1',
          subject_digest: 'sha256:def',
        },
        'kip:principal:requester',
      )
      expect(() => gov.approve(approval.id, 'kip:principal:requester')).toThrowError(
        /separation of duties/,
      )
    })
  })

  it('spends an approval rather than leaving it standing', async () => {
    await withStore('gov-consume', (store) => {
      const gov = store.governance
      const approval = gov.requestApproval(
        {
          space_id: 'kip:space:default',
          operation: 'purge',
          resource: 'E-2',
          subject_digest: 'sha256:ghi',
        },
        'kip:principal:requester',
      )
      gov.approve(approval.id, 'kip:principal:reviewer')
      expect(gov.grantedApprovals('kip:space:default', 'sha256:ghi')).toHaveLength(1)
      gov.consumeApproval(approval.id)
      // The same signature must not authorize the operation twice.
      expect(gov.grantedApprovals('kip:space:default', 'sha256:ghi')).toHaveLength(0)
      expect(gov.findApproval(approval.id)?.status).toBe('consumed')
    })
  })

  it('normalizes an actor reference into the key it is compared against', async () => {
    await withStore('gov-binding', (store) => {
      const gov = store.governance
      const local = gov.createBinding(
        { principal_id: 'kip:principal:agent', actor_ref: 'C-7' },
        'kip:principal:system',
      )
      const canonical = gov.createBinding(
        { principal_id: 'kip:principal:agent', actor_ref: 'did:example:alice' },
        'kip:principal:system',
      )
      // `assertions.asserted_by_key` is an endpoint key; a binding stored in
      // any other spelling would silently never match.
      expect(local.actor_key).toBe('idC-7')
      expect(canonical.actor_key).toBe('ciddid:example:alice')

      expect(gov.bindingsOf('kip:principal:agent', 'kip:space:default')).toHaveLength(2)
      gov.revokeBinding(local.id, 'kip:principal:system')
      expect(gov.bindingsOf('kip:principal:agent', 'kip:space:default')).toHaveLength(1)
    })
  })

  it('keeps a Space-scoped binding out of another Space', async () => {
    await withStore('gov-binding-scope', (store) => {
      const gov = store.governance
      gov.createBinding(
        {
          principal_id: 'kip:principal:agent',
          actor_ref: 'C-1',
          scope: 'kip:space:project',
        },
        'kip:principal:system',
      )
      expect(gov.bindingsOf('kip:principal:agent', 'kip:space:project')).toHaveLength(1)
      // Representation is not global: speaking for an organization inside its
      // project Space is not speaking for it in someone's personal Brain.
      expect(gov.bindingsOf('kip:principal:agent', 'kip:space:personal')).toHaveLength(0)
    })
  })

  it('mirrors every mutation into the audit with the whole record', async () => {
    await withStore('gov-audit', (store) => {
      const gov = store.governance
      gov.createGrant(
        {
          space_id: 'kip:space:default',
          grantee_principal: 'kip:principal:agent',
          actions: ['read', 'export'],
        },
        'kip:principal:system',
      )
      const entries = gov
        .readAudit('kip:space:default', 10)
        .filter((entry) => entry.operation === 'create_grant')
      expect(entries).toHaveLength(1)
      expect(entries[0]?.entry_class).toBe('mutation')
      // A whole record, not a diff: a chain with one missing link answers a
      // historical question wrongly instead of refusing (§175).
      expect((entries[0]?.record as { actions?: string[] })?.actions).toEqual([
        'read',
        'export',
      ])
    })
  })
})

describe('the command gate', () => {
  async function withNexus<T>(
    name: string,
    body: (nexus: CognitiveNexus) => T,
  ): Promise<T> {
    const stub = env.KIP_DB.getByName(`gate-${name}`)
    return await runInDurableObject(stub, (_instance, state) => {
      const nexus = CognitiveNexus.connect(state.storage)
      nexus.activatePackages([COGNITIVE_MEMORY])
      return body(nexus)
    })
  }

  const CREATE = 'CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }'
  const READ = 'FIND(?c) WHERE { ?c CONCEPT {type: "Person"} }'

  it('runs the embedded host through the authorization path, not around it', async () => {
    await withNexus('system-owner', (nexus) => {
      // §212: the system Principal owns the default Space, so an in-process
      // host is not locked out by default deny — and is not exempt from it
      // either.
      const authority = nexus.systemSession().effectiveAuthority()
      expect(authority.isOwner).toBe(true)
      expect(nexus.execute(CREATE).status).toBe('committed')
      expect(nexus.query(READ)).toHaveLength(1)
    })
  })

  it('refuses a Principal the control plane has never heard of', async () => {
    await withNexus('unknown-principal', (nexus) => {
      // A host naming an unregistered identity has a configuration bug.
      // Resolving it to "some caller with no Grants" would hide that bug behind
      // a denial that looks like policy.
      const session = nexus.session(principalAuth('kip:principal:ghost'))
      expect(() => session.query(READ)).toThrowError(/is registered in this Nexus/)
    })
  })

  it('denies by default: a registered Principal with no Grants may do nothing', async () => {
    await withNexus('default-deny', (nexus) => {
      nexus.store.governance.ensurePrincipal({ principal_id: 'kip:principal:agent' })
      const session = nexus.session(principalAuth('kip:principal:agent'))
      // §41: a missing policy must never become public access.
      expect(() => session.query(READ)).toThrowError(/requires the read permission/)
      expect(() => session.execute(CREATE)).toThrowError(/requires the create permission/)
    })
  })

  it('lets a Grant confer exactly what it names, and nothing beside it', async () => {
    await withNexus('narrow-grant', (nexus) => {
      const gov = nexus.store.governance
      gov.ensurePrincipal({ principal_id: 'kip:principal:reader' })
      gov.createGrant(
        {
          space_id: nexus.space,
          grantee_principal: 'kip:principal:reader',
          actions: ['read'],
        },
        SYSTEM_PRINCIPAL,
      )
      nexus.execute(CREATE)

      const session = nexus.session(principalAuth('kip:principal:reader'))
      expect(session.query(READ)).toHaveLength(1)
      // §271: read ≠ export. A caller who may read every element still may not
      // package them and take them away.
      expect(() =>
        session.describe('EXPORT CAPSULE :out WHERE { ?c CONCEPT {type: "Person"} }', {
          out: 'x',
        }),
      ).toThrowError(/requires the export permission/)
      expect(() => session.execute(CREATE)).toThrowError(/requires the create permission/)
    })
  })

  it('takes effect the moment a Grant is revoked, mid-session', async () => {
    await withNexus('revocation', (nexus) => {
      const gov = nexus.store.governance
      gov.ensurePrincipal({ principal_id: 'kip:principal:reader' })
      const grant = gov.createGrant(
        {
          space_id: nexus.space,
          grantee_principal: 'kip:principal:reader',
          actions: ['read'],
        },
        SYSTEM_PRINCIPAL,
      )
      const session = nexus.session(principalAuth('kip:principal:reader'))
      expect(session.query(READ)).toEqual([])

      gov.revokeGrant(grant.id, SYSTEM_PRINCIPAL)
      // §188, §245: the session held identity, never authority. It does not
      // still hold what its first request resolved.
      expect(() => session.query(READ)).toThrowError(/requires the read permission/)
    })
  })

  it('lets an explicit deny outrank every allow, including the owner’s', async () => {
    await withNexus('explicit-deny', (nexus) => {
      const gov = nexus.store.governance
      gov.publishPolicy(
        {
          policy_id: 'kip:policy:no-export',
          space_id: nexus.space,
          statements: [{ effect: 'deny', actions: ['export'] }],
        },
        SYSTEM_PRINCIPAL,
      )
      const space = nexus.spaceRow()
      space.default_policy_id = 'kip:policy:no-export'
      nexus.store.putSpace(space)

      // §42: nothing arriving through a request can talk past a deny. The owner
      // is not locked out — a host holds the control plane directly and can
      // publish a new version — but it cannot out-argue one from here.
      expect(() =>
        nexus.describe('EXPORT CAPSULE :out WHERE { ?c CONCEPT {type: "Person"} }', {
          out: 'x',
        }),
      ).toThrowError(/requires the export permission/)
      expect(nexus.query(READ)).toEqual([])
    })
  })

  it('blocks rather than softly allowing when a policy requires approval', async () => {
    await withNexus('require-approval', (nexus) => {
      const gov = nexus.store.governance
      gov.publishPolicy(
        {
          policy_id: 'kip:policy:two-eyes',
          space_id: nexus.space,
          statements: [
            {
              effect: 'allow',
              actions: ['export'],
              obligations: { audit: true, approvals_required: 2, redaction_profile: '' },
            },
          ],
        },
        SYSTEM_PRINCIPAL,
      )
      const space = nexus.spaceRow()
      space.default_policy_id = 'kip:policy:two-eyes'
      nexus.store.putSpace(space)

      const EXPORT = 'EXPORT CAPSULE :out WHERE { ?c CONCEPT {type: "Person"} }'
      // §40: `require_approval` is not a soft yes. The operation does not run
      // while it is outstanding, and the owner is no exception.
      expect(() => nexus.describe(EXPORT, { out: 'x' })).toThrowError(
        /independent approval/,
      )

      const digest = subjectDigest(nexus.space, 'export', spaceResource())
      const approval = gov.requestApproval(
        {
          space_id: nexus.space,
          operation: 'export',
          resource: 'the Space',
          subject_digest: digest,
          required: 2,
        },
        'kip:principal:requester',
      )
      gov.approve(approval.id, 'kip:principal:reviewer-a')
      // §246: one approval where two are required is not partial activation.
      expect(() => nexus.describe(EXPORT, { out: 'x' })).toThrowError(
        /independent approval/,
      )

      gov.approve(approval.id, 'kip:principal:reviewer-b')
      expect(() => nexus.describe(EXPORT, { out: 'x' })).not.toThrow()
      // Consumed by use: the same two signatures do not authorize it twice.
      expect(gov.findApproval(approval.id)?.status).toBe('consumed')
      expect(() => nexus.describe(EXPORT, { out: 'x' })).toThrowError(
        /independent approval/,
      )
    })
  })

  it('describes the engine without asking for authority', async () => {
    await withNexus('open-describe', (nexus) => {
      nexus.store.governance.ensurePrincipal({ principal_id: 'kip:principal:agent' })
      const session = nexus.session(principalAuth('kip:principal:agent'))
      // Otherwise an unauthorized caller could not learn how to become one.
      expect(() => session.describe('DESCRIBE PROTOCOL')).not.toThrow()
      expect(() => session.describe('DESCRIBE CAPABILITIES')).not.toThrow()
      // §266: and it must be able to learn what it may do without first being
      // permitted to do it.
      const access = session.describe('DESCRIBE ACCESS') as {
        permissions: string[]
        principal_id: string
      }
      expect(access.principal_id).toBe('kip:principal:agent')
      expect(access.permissions).toEqual([])
      // Describing the *Space* is a different question and does need discovery.
      expect(() => session.describe('DESCRIBE PRIMER')).toThrowError(
        /requires the discover permission/,
      )
    })
  })

  it('reports what a Grant actually confers, grouped by family', async () => {
    await withNexus('access-report', (nexus) => {
      const gov = nexus.store.governance
      gov.ensurePrincipal({ principal_id: 'kip:principal:agent' })
      gov.createGrant(
        {
          space_id: nexus.space,
          grantee_principal: 'kip:principal:agent',
          actions: ['read', 'discover'],
        },
        SYSTEM_PRINCIPAL,
      )
      const session = nexus.session(principalAuth('kip:principal:agent'))
      const access = session.describe('DESCRIBE ACCESS') as {
        permissions: string[]
        families: Record<string, unknown[]>
      }
      expect(access.permissions.sort()).toEqual(['discover', 'read'])
      expect(access.families.discovery).toHaveLength(2)
    })
  })

  it('stamps the acting Principal on what it writes', async () => {
    await withNexus('origin', (nexus) => {
      const gov = nexus.store.governance
      gov.ensurePrincipal({ principal_id: 'kip:principal:writer' })
      gov.createGrant(
        {
          space_id: nexus.space,
          grantee_principal: 'kip:principal:writer',
          actions: ['create', 'read'],
        },
        SYSTEM_PRINCIPAL,
      )
      const session = nexus.session(principalAuth('kip:principal:writer'))
      const receipt = session.execute(CREATE)
      const id = receipt.changes[0]?.id ?? ''
      const element = nexus.store.load(parseElementId(id))
      // §26: origin is what the runtime observed, never what the content
      // claimed — so it names the session's Principal and not the engine's.
      expect(element?.row.origin.principal_id).toBe('kip:principal:writer')
    })
  })

  it('does not let a declared purpose widen what a session may do', async () => {
    await withNexus('purpose', (nexus) => {
      const gov = nexus.store.governance
      gov.ensurePrincipal({ principal_id: 'kip:principal:agent' })
      gov.createGrant(
        {
          space_id: nexus.space,
          grantee_principal: 'kip:principal:agent',
          actions: ['read'],
          conditions: {
            purpose: ['incident_response'],
            min_purpose_assurance: 'session_bound',
          },
        },
        SYSTEM_PRINCIPAL,
      )
      // §12: writing purpose: "incident_response" in a request gets a caller
      // nothing. Only the host can vouch for a purpose.
      const declared = nexus.session(
        principalAuth('kip:principal:agent', {
          purpose: 'incident_response',
          purpose_assurance: 'declared',
        }),
      )
      expect(() => declared.query(READ)).toThrowError(/requires the read permission/)

      const bound = nexus.session(
        principalAuth('kip:principal:agent', {
          purpose: 'incident_response',
          purpose_assurance: 'session_bound',
        }),
      )
      expect(bound.query(READ)).toEqual([])
    })
  })

  it('reads a suspended Principal as not permitted rather than as an error', async () => {
    await withNexus('suspended', (nexus) => {
      const gov = nexus.store.governance
      gov.ensurePrincipal({ principal_id: 'kip:principal:agent' })
      gov.createGrant(
        {
          space_id: nexus.space,
          grantee_principal: 'kip:principal:agent',
          actions: ['read'],
        },
        SYSTEM_PRINCIPAL,
      )
      const session = nexus.session(principalAuth('kip:principal:agent'))
      expect(session.query(READ)).toEqual([])

      gov.setPrincipalStatus('kip:principal:agent', 'suspended', SYSTEM_PRINCIPAL)
      // The record survives — a past write stays attributable to it — and the
      // refusal reads as "not permitted", which is what it is.
      expect(() => session.query(READ)).toThrowError(/requires the read permission/)
    })
  })

  it('never lets cognitive content confer authority', async () => {
    await withNexus('no-cognitive-authority', (nexus) => {
      const gov = nexus.store.governance
      gov.ensurePrincipal({ principal_id: 'kip:principal:agent' })
      gov.createGrant(
        {
          space_id: nexus.space,
          grantee_principal: 'kip:principal:agent',
          // `record_attributed_assertion` because the claim is attributed to a
          // Concept this Principal is not bound to — recording what somebody
          // else said is ordinary, and is not `assert_as_actor` (§17).
          actions: ['create', 'read', 'assert', 'record_attributed_assertion'],
        },
        SYSTEM_PRINCIPAL,
      )
      const session = nexus.session(principalAuth('kip:principal:agent'))
      // The Space can hold a Concept, a Proposition and a high-confidence
      // Assertion all saying the agent administers this Brain…
      session.execute(`MUTATE {
        CREATE CONCEPT ?agent { TYPE "Person" NAME "the agent" }
        CREATE CONCEPT ?role { TYPE "Preference" NAME "administrator" }
        ENSURE PROPOSITION ?p (?agent, "prefers", ?role)
        CREATE ASSERTION ?a {
          SET FIELDS {
            proposition: ?p, asserted_by: ?agent, stance: "support",
            mode: "stated", confidence: 1.0
          }
        }
      }`)
      // …and it administers nothing. Authorization reads Grants, and a
      // Proposition is a claim.
      expect(() => session.execute('ARCHIVE "C-1"')).toThrowError(
        /requires the archive permission/,
      )
      expect(session.effectiveAuthority().isOwner).toBe(false)
    })
  })
})

describe('the read path', () => {
  const SETUP = `MUTATE {
    CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" SET ATTRIBUTES { salary: 210000 } }
    CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark" }
    ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
  }`

  async function withReader<T>(
    name: string,
    grant: Parameters<Store['governance']['createGrant']>[0]['scope'] extends never
      ? never
      : {
          actions: string[]
          scope?: Record<string, string[]>
          constraints?: Record<string, unknown>
        },
    body: (nexus: CognitiveNexus, session: ReturnType<CognitiveNexus['session']>) => T,
  ): Promise<T> {
    const stub = env.KIP_DB.getByName(`read-${name}`)
    return await runInDurableObject(stub, (_instance, state) => {
      const nexus = CognitiveNexus.connect(state.storage)
      nexus.activatePackages([COGNITIVE_MEMORY])
      nexus.execute(SETUP)
      nexus.store.governance.ensurePrincipal({ principal_id: 'kip:principal:reader' })
      nexus.store.governance.createGrant(
        {
          space_id: nexus.space,
          grantee_principal: 'kip:principal:reader',
          actions: grant.actions,
          scope: grant.scope,
          constraints: grant.constraints,
        },
        SYSTEM_PRINCIPAL,
      )
      return body(nexus, nexus.session(principalAuth('kip:principal:reader')))
    })
  }

  it('leaves an element outside the Grant out of the query universe', async () => {
    await withReader(
      'kind-scope',
      { actions: ['read'], scope: { kinds: ['concept'] } },
      (nexus, session) => {
        // The owner sees both kinds…
        expect(nexus.query('FIND(?c) WHERE { ?c CONCEPT {type: "Person"} }')).toHaveLength(1)
        expect(
          nexus.query('FIND(?p) WHERE { ?p PROPOSITION (?s, "prefers", ?o) }'),
        ).toHaveLength(1)

        // …and a Grant scoped to Concepts sees only those. Not an error: the
        // Proposition is simply not in this caller's universe (§104).
        expect(session.query('FIND(?c) WHERE { ?c CONCEPT {type: "Person"} }')).toHaveLength(1)
        expect(
          session.query('FIND(?p) WHERE { ?p PROPOSITION (?s, "prefers", ?o) }'),
        ).toEqual([])
      },
    )
  })

  it('answers a hidden element by id exactly as one that was never written', async () => {
    await withReader(
      'existence',
      { actions: ['read'], scope: { elements: ['C-1'] } },
      (_nexus, session) => {
        // §103: a distinguishable "exists but hidden" is the existence leak.
        const hidden = session.query('FIND(?c) WHERE { ?c CONCEPT {id: "C-2"} }')
        const absent = session.query('FIND(?c) WHERE { ?c CONCEPT {id: "C-99"} }')
        expect(hidden).toEqual(absent)
        expect(hidden).toEqual([])
      },
    )
  })

  it('hides a masked field from FILTER, not only from the projection', async () => {
    await withReader(
      'field-mask',
      { actions: ['read'], constraints: { fields: ['name', 'schema_ref'] } },
      (nexus, session) => {
        // The value is really there, and the owner can filter on it.
        expect(
          nexus.query(
            'FIND(?c.name) WHERE { ?c CONCEPT {type: "Person"} FILTER(?c.attributes.salary > 200000) }',
          ),
        ).toEqual(['Alice'])

        // §109: answering this with an empty projection but a matching row
        // would disclose the value through row membership. The mask is applied
        // to the cached view, so the filter reads what the projection would.
        expect(
          session.query(
            'FIND(?c.name) WHERE { ?c CONCEPT {type: "Person"} FILTER(?c.attributes.salary > 200000) }',
          ),
        ).toEqual([])
        // The element itself is still readable, under the name the mask allows.
        expect(
          session.query('FIND(?c.name) WHERE { ?c CONCEPT {type: "Person"} }'),
        ).toEqual(['Alice'])
      },
    )
  })

  it('withholds engine origin rather than erasing it', async () => {
    await withReader('origin', { actions: ['read'] }, (nexus, session) => {
      expect(
        nexus.query(
          'FIND(?c._system.origin) WHERE { ?c CONCEPT {type: "Person"} }',
        ),
      ).toEqual([{ principal_id: SYSTEM_PRINCIPAL, channel: 'engine' }])

      // §110: removing it would say "no origin was recorded", which is false
      // for every element here. What is withheld is *whose*.
      expect(
        session.query(
          'FIND(?c._system.origin) WHERE { ?c CONCEPT {type: "Person"} }',
        ),
      ).toEqual([{ redacted: 'read_raw_origin' }])
    })
  })

  it('honours a classification ceiling on the way in', async () => {
    await withReader(
      'classification',
      {
        actions: ['read'],
        constraints: { max_classification: 'internal' },
      },
      (nexus, session) => {
        // Writing the label is the classification stage's job; reading it for a
        // decision is this one's. So the label is set through storage here, the
        // way `classify` will.
        const element = nexus.store.load(parseElementId('C-1'))
        if (element === null) throw new Error('C-1 should exist')
        element.row.governance = { classification: 'secret' }
        nexus.store.put(element, 'update', 'test')

        expect(nexus.query('FIND(?c.name) WHERE { ?c CONCEPT {} }').sort()).toEqual([
          'Alice',
          'Dark',
        ])
        // The secret Concept is above the ceiling and drops out; the other one,
        // which states no label, is judged at the Space default and stays.
        expect(session.query('FIND(?c.name) WHERE { ?c CONCEPT {} }')).toEqual(['Dark'])
      },
    )
  })

  it('does not let a history read resolve what a read cannot', async () => {
    await withReader(
      'history',
      { actions: ['read', 'read_history'], scope: { elements: ['C-1'] } },
      (_nexus, session) => {
        expect(() => session.describe('HISTORY ELEMENT "C-1"')).not.toThrow()
        // §103 again: the version log must not become an existence oracle.
        expect(() => session.describe('HISTORY ELEMENT "C-2"')).toThrowError(
          /no element C-2/,
        )
      },
    )
  })

  it('narrows a change stream to the elements the caller may read', async () => {
    await withReader(
      'changes',
      { actions: ['read', 'read_history'], scope: { elements: ['C-1'] } },
      (nexus, session) => {
        const all = nexus.describe('CHANGES SINCE 0') as { changes: { id: string }[] }
        expect(all.changes.map((change) => change.id).sort()).toEqual([
          'C-1',
          'C-2',
          'P-1',
        ])

        const mine = session.describe('CHANGES SINCE 0') as {
          changes: { id: string }[]
        }
        expect(mine.changes.map((change) => change.id)).toEqual(['C-1'])
      },
    )
  })

  it('advances the change cursor past a window it may not see', async () => {
    await withReader(
      'changes-cursor',
      { actions: ['read', 'read_history'], scope: { elements: ['C-1'] } },
      (nexus, session) => {
        // Every transaction after the setup touches an element this reader may
        // not see, so its whole page is filtered away.
        nexus.execute('CREATE CONCEPT ?c { TYPE "Person" NAME "Bob" }')
        nexus.execute('CREATE CONCEPT ?c { TYPE "Person" NAME "Carol" }')

        const first = session.describe('CHANGES SINCE 0') as {
          changes: { id: string }[]
          cursor: number
        }
        expect(first.changes.map((change) => change.id)).toEqual(['C-1'])
        // The cursor names the coordinate the page consumed, not the last one
        // it could show. Taking it from the visible rows would leave a reader
        // whose page was entirely hidden exactly where it started, re-reading
        // the same window forever.
        expect(first.cursor).toBe(nexus.store.currentSeq(nexus.space))

        const next = session.describe(`CHANGES SINCE ${first.cursor}`) as {
          changes: { id: string }[]
          cursor: number
        }
        expect(next.changes).toEqual([])
        expect(next.cursor).toBe(first.cursor)
      },
    )
  })

  it('keeps a belief from being projected out of what the caller may not read', async () => {
    const stub = env.KIP_DB.getByName('read-projection')
    await runInDurableObject(stub, (_instance, state) => {
      const nexus = CognitiveNexus.connect(state.storage)
      nexus.activatePackages([COGNITIVE_MEMORY])
      nexus.execute(`MUTATE {
        CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
        CREATE CONCEPT ?bob { TYPE "Person" NAME "Bob" }
        CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark" }
        ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
        CREATE ASSERTION ?yes {
          SET FIELDS { proposition: ?p, asserted_by: ?alice, stance: "support", mode: "stated", confidence: 0.9 }
        }
        CREATE ASSERTION ?no {
          SET FIELDS { proposition: ?p, asserted_by: ?bob, stance: "reject", mode: "stated", confidence: 0.9 }
        }
      }`)

      const BELIEF =
        'FIND(?b.opposition.assertion_ids) WHERE { ?p PROPOSITION (?s, "prefers", ?o) ?b BELIEF (?p) }'
      expect(nexus.query(BELIEF)).toEqual([['A-2']])

      nexus.store.governance.ensurePrincipal({ principal_id: 'kip:principal:reader' })
      nexus.store.governance.createGrant(
        {
          space_id: nexus.space,
          grantee_principal: 'kip:principal:reader',
          actions: ['read', 'project'],
          // Everything except the opposing Assertion, A-2.
          scope: { elements: ['C-1', 'C-2', 'C-3', 'P-1', 'A-1'] },
        },
        SYSTEM_PRINCIPAL,
      )
      const session = nexus.session(principalAuth('kip:principal:reader'))

      // The dissent is outside this caller's query universe, so the belief is
      // projected without it. Silence and exclusion look the same here, and
      // that is the point: reporting the opposition without its Assertion —
      // or naming an id the caller may not resolve — would disclose exactly
      // what the visibility rule refused.
      expect(session.query(BELIEF)).toEqual([[]])
    })
  })

  it('cannot root a Capsule export on an element the caller may not read', async () => {
    await withReader(
      'export',
      { actions: ['read', 'export'], scope: { elements: ['C-1'] } },
      (nexus, session) => {
        const exported = (capsule: Json): number =>
          Object.values(
            (capsule as { payload: { records: Record<string, unknown[]> } }).payload
              .records,
          ).reduce((total, bucket) => total + bucket.length, 0)

        const all = nexus.describe('EXPORT CAPSULE :out WHERE { ?c CONCEPT {} }', {
          out: 'x',
        })
        expect(exported(all)).toBe(2)

        // §78: export is a further permission over what a read already reached,
        // never a way around it.
        const mine = session.describe('EXPORT CAPSULE :out WHERE { ?c CONCEPT {} }', {
          out: 'x',
        })
        expect(exported(mine)).toBe(1)
      },
    )
  })
})

describe('the write path', () => {
  async function withWriter<T>(
    name: string,
    actions: string[],
    body: (
      nexus: CognitiveNexus,
      session: ReturnType<CognitiveNexus['session']>,
    ) => T,
    scope?: Record<string, string[]>,
  ): Promise<T> {
    const stub = env.KIP_DB.getByName(`write-${name}`)
    return await runInDurableObject(stub, (_instance, state) => {
      const nexus = CognitiveNexus.connect(state.storage)
      nexus.activatePackages([COGNITIVE_MEMORY])
      nexus.store.governance.ensurePrincipal({ principal_id: 'kip:principal:agent' })
      nexus.store.governance.createGrant(
        {
          space_id: nexus.space,
          grantee_principal: 'kip:principal:agent',
          actions,
          scope,
        },
        SYSTEM_PRINCIPAL,
      )
      return body(nexus, nexus.session(principalAuth('kip:principal:agent')))
    })
  }

  it('judges a creation on the element it will write, not on the command', async () => {
    await withWriter(
      'kind-scope',
      ['create', 'read'],
      (_nexus, session) => {
        // The command gate said `create` is allowed here; this says it is not
        // allowed *for that*. A Grant narrowed to Concepts must not be a way to
        // create Evidence.
        expect(() =>
          session.execute('CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }'),
        ).not.toThrow()
        expect(() =>
          session.execute(
            'CREATE EVIDENCE ?e { SET FIELDS { evidence_class: "Observation" } }',
          ),
        ).toThrowError(/requires the create permission/)
      },
      { kinds: ['concept'] },
    )
  })

  it('fails a sweep that reaches something it may not touch', async () => {
    await withWriter('sweep', ['create', 'read'], (nexus, session) => {
      nexus.execute(`MUTATE {
        CREATE CONCEPT ?a { TYPE "Person" NAME "Alice" }
        CREATE CONCEPT ?b { TYPE "Person" NAME "Bob" }
      }`)
      // Reading and writing are separate authorities, and the sweep rule bites
      // exactly where they differ: this caller may *read* both Concepts, so the
      // selection block sees both, and may archive only one.
      nexus.store.governance.createGrant(
        {
          space_id: nexus.space,
          grantee_principal: 'kip:principal:agent',
          actions: ['archive'],
          scope: { elements: ['C-1'] },
        },
        SYSTEM_PRINCIPAL,
      )

      // Archiving only C-1 would report success having done half the job — the
      // defect shape this project keeps finding — and would let the caller
      // learn what it may not touch by counting what changed.
      expect(() =>
        session.execute('ARCHIVE ?c WHERE { ?c CONCEPT {type: "Person"} }'),
      ).toThrowError(/requires the archive permission/)
      // Nothing was archived: the statement unwound whole.
      expect(
        nexus.query('FIND(COUNT(?c)) WHERE { ?c CONCEPT {type: "Person"} }'),
      ).toEqual([2])

      // Naming the one it may touch works, which is what makes the refusal a
      // narrowing rather than a lockout.
      expect(session.execute('ARCHIVE "C-1"').status).toBe('committed')
    })
  })

  it('keeps a selection block inside what the caller may read', async () => {
    await withWriter(
      'sweep-visibility',
      ['create', 'read', 'archive'],
      (nexus, session) => {
        nexus.execute(`MUTATE {
          CREATE CONCEPT ?a { TYPE "Person" NAME "Alice" }
          CREATE CONCEPT ?b { TYPE "Person" NAME "Bob" }
        }`)
        // C-2 is outside the Grant entirely, so it is not in this caller's
        // query universe — the block cannot select what a read cannot see, and
        // the sweep succeeds over exactly what it could.
        expect(
          session.execute('ARCHIVE ?c WHERE { ?c CONCEPT {type: "Person"} }').status,
        ).toBe('committed')
        expect(
          nexus.query(
            'FIND(COUNT(?c)) WHERE { ?c CONCEPT {type: "Person", state: "archived"} }',
          ),
        ).toEqual([1])
      },
      { elements: ['C-1'] },
    )
  })

  it('keeps recording what somebody said apart from speaking as them', async () => {
    await withWriter('attribution', ['create', 'read', 'assert'], (nexus, session) => {
      nexus.execute(`MUTATE {
        CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
        CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark" }
        ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
      }`)
      const ASSERT = `CREATE ASSERTION ?a {
        SET FIELDS {
          proposition: {id: "P-1"}, asserted_by: {id: "C-1"},
          stance: "support", mode: "stated", confidence: 0.9
        }
      }`

      // §17: attributing a claim to an actor this Principal is not bound to is
      // *recording what somebody said*. It needs its own permission and is not
      // covered by `assert` — but it is also not impersonation, so it must stay
      // ordinary rather than needing representation authority.
      expect(() => session.execute(ASSERT)).toThrowError(
        /requires the record_attributed_assertion permission/,
      )

      nexus.store.governance.createGrant(
        {
          space_id: nexus.space,
          grantee_principal: 'kip:principal:agent',
          actions: ['record_attributed_assertion'],
        },
        SYSTEM_PRINCIPAL,
      )
      expect(session.execute(ASSERT).status).toBe('committed')
    })
  })

  it('asks for representation authority only where the caller is bound', async () => {
    await withWriter(
      'as-actor',
      ['create', 'read', 'assert', 'record_attributed_assertion'],
      (nexus, session) => {
        nexus.execute(`MUTATE {
          CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
          CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark" }
          ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
        }`)
        // A binding that says this Principal *represents* Alice moves the
        // Assertion out of "recording a claim" and into "exercising Alice's
        // authority", which is a permission it does not hold.
        nexus.store.governance.createBinding(
          {
            principal_id: 'kip:principal:agent',
            actor_ref: 'C-1',
            binding_class: 'represents',
          },
          SYSTEM_PRINCIPAL,
        )
        expect(() =>
          session.execute(`CREATE ASSERTION ?a {
            SET FIELDS {
              proposition: {id: "P-1"}, asserted_by: {id: "C-1"},
              stance: "support", mode: "stated", confidence: 0.9
            }
          }`),
        ).toThrowError(/requires the assert_as_actor permission/)
      },
    )
  })

  it('does not let a moderator record a retraction that did not happen', async () => {
    await withWriter(
      'retraction',
      [
        'create',
        'read',
        'assert',
        'record_attributed_assertion',
        'retract_own',
        'archive',
      ],
      (nexus, session) => {
        // Written by the system Principal, attributed to Alice. The agent
        // neither wrote it nor represents her.
        nexus.execute(`MUTATE {
          CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
          CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark" }
          ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
          CREATE ASSERTION ?a {
            SET FIELDS { proposition: ?p, asserted_by: ?alice, stance: "support", mode: "stated", confidence: 0.9 }
          }
        }`)

        // §68: RETRACT states that the *source* took its claim back. Saying so
        // on somebody else's behalf would be the engine asserting something
        // that never happened.
        expect(() => session.execute('RETRACT ASSERTION "A-1"')).toThrowError(
          /neither wrote it nor is bound to the actor/,
        )
        // The honest alternative is available and needs no impersonation.
        expect(session.execute('ARCHIVE "A-1"').status).toBe('committed')
      },
    )
  })

  it('lets a Principal withdraw what it wrote itself', async () => {
    await withWriter(
      'own-retraction',
      ['create', 'read', 'assert', 'record_attributed_assertion', 'retract_own'],
      (_nexus, session) => {
        session.execute(`MUTATE {
          CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
          CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark" }
          ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
          CREATE ASSERTION ?a {
            SET FIELDS { proposition: ?p, asserted_by: ?alice, stance: "support", mode: "stated", confidence: 0.9 }
          }
        }`)
        // Withdrawing one's own record is not impersonation, so it needs no
        // ActorBinding — which is what keeps the ordinary case ordinary.
        expect(session.execute('RETRACT ASSERTION "A-1"').status).toBe('committed')
      },
    )
  })

  it('needs a lifecycle permission to set retention, not just create', async () => {
    await withWriter('retention', ['create', 'read'], (_nexus, session) => {
      // §80: how long an element is kept is a lifecycle decision, not a side
      // effect of writing content.
      expect(() =>
        session.execute(`CREATE CONCEPT ?c {
          TYPE "Person" NAME "Alice"
          SET FIELDS { retention: {expires_at: "2030-01-01T00:00:00.000Z"} }
        }`),
      ).toThrowError(/requires the manage_retention permission/)
    })
  })

  it('refuses a governance block written by cognitive content', async () => {
    await withWriter('protected', ['create', 'read'], (_nexus, session) => {
      // The parser refuses it, which is the right layer: an engine-side check
      // would be one branch away from an ungoverned path. §264 is the reason —
      // content that could set this would be granting itself authority.
      expect(() =>
        session.execute(`CREATE CONCEPT ?c {
          TYPE "Person" NAME "Alice"
          SET FIELDS { governance: {classification: "public"} }
        }`),
      ).toThrowError(/governance/i)
    })
  })
})

describe('classification and influence authority', () => {
  async function withNexus<T>(
    name: string,
    body: (nexus: CognitiveNexus) => T,
  ): Promise<T> {
    const stub = env.KIP_DB.getByName(`class-${name}`)
    return await runInDurableObject(stub, (_instance, state) => {
      const nexus = CognitiveNexus.connect(state.storage)
      nexus.activatePackages([COGNITIVE_MEMORY])
      return body(nexus)
    })
  }

  const labelOf = (nexus: CognitiveNexus, id: string): string => {
    const element = nexus.store.load(parseElementId(id))
    if (element === null) throw new Error(`${id} should exist`)
    return classificationOf(element)
  }

  it('carries a secret input’s label onto what was derived from it', async () => {
    await withNexus('derivation', (nexus) => {
      const session = nexus.systemSession()
      session.execute(`CREATE EVIDENCE ?e {
        SET FIELDS { evidence_class: "Observation", content_digest: "sha256:x" }
      }`)
      session.classify(parseElementId('E-1'), 'secret')

      // A summary that cited the secret Evidence. §98/§242: *read secret
      // Evidence, summarize, write public summary* is an exfiltration path if
      // the summary lands public even briefly, so the label joins upward at
      // commit rather than being applied afterwards.
      session.execute(`CREATE EVIDENCE ?s {
        SET FIELDS { evidence_class: "Summary", content_digest: "sha256:y" }
        SET STRUCTURAL { ("source", {id: "E-1"}) }
      }`)
      expect(labelOf(nexus, 'E-2')).toBe('secret')
    })
  })

  it('joins upward through an Activity’s outputs as well as its inputs', async () => {
    await withNexus('activity', (nexus) => {
      const session = nexus.systemSession()
      session.execute(`CREATE EVIDENCE ?e {
        SET FIELDS { evidence_class: "Observation", content_digest: "sha256:x" }
      }`)
      session.classify(parseElementId('E-1'), 'sensitive')

      // The output never mentions the input; the Activity says it came from
      // there. A one-directional walk would miss exactly the link a summarizer
      // leaves behind.
      session.execute(`MUTATE {
        CREATE EVIDENCE ?out {
          SET FIELDS { evidence_class: "Summary", content_digest: "sha256:z" }
        }
        CREATE ACTIVITY ?a {
          SET FIELDS { activity_class: "Consolidation" }
          SET STRUCTURAL { ("inputs", {id: "E-1"}) ("outputs", ?out) }
        }
      }`)
      expect(labelOf(nexus, 'E-2')).toBe('sensitive')
    })
  })

  it('needs declassify to lower a label and only update to raise one', async () => {
    await withNexus('direction', (nexus) => {
      const gov = nexus.store.governance
      gov.ensurePrincipal({ principal_id: 'kip:principal:agent' })
      gov.createGrant(
        {
          space_id: nexus.space,
          grantee_principal: 'kip:principal:agent',
          actions: ['create', 'read', 'update'],
        },
        SYSTEM_PRINCIPAL,
      )
      const session = nexus.session(principalAuth('kip:principal:agent'))
      session.execute('CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }')

      // Raising is ordinary: an agent that notices it wrote something sensitive
      // should be able to say so without a Governance ticket.
      expect(session.classify(parseElementId('C-1'), 'secret')).toBe('')
      expect(labelOf(nexus, 'C-1')).toBe('secret')

      // Lowering is the direction that discloses, so it is the privileged one.
      expect(() => session.classify(parseElementId('C-1'), 'public')).toThrowError(
        /requires the declassify permission/,
      )
    })
  })

  it('refuses to raise a derived artifact past what it was derived from', async () => {
    await withNexus('non-amplification', (nexus) => {
      const session = nexus.systemSession()
      session.execute(`CREATE EVIDENCE ?e {
        SET FIELDS { evidence_class: "Skill", content_digest: "sha256:x" }
      }`)
      session.execute(`CREATE EVIDENCE ?s {
        SET FIELDS { evidence_class: "Summary", content_digest: "sha256:y" }
        SET STRUCTURAL { ("source", {id: "E-1"}) }
      }`)

      // The input is `descriptive`, the bottom of the ladder, so the summary
      // cannot be raised above it however locally it was written (§127, §243).
      expect(() =>
        session.elevateAuthority(parseElementId('E-2'), 'behavioral'),
      ).toThrowError(/Transformation does not raise authority/)

      // Elevating the input first is the honest route, and then the derived
      // artifact may follow.
      session.elevateAuthority(parseElementId('E-1'), 'behavioral')
      expect(session.elevateAuthority(parseElementId('E-2'), 'behavioral')).toBe(
        'descriptive',
      )
    })
  })

  it('lets an incident response demote immediately, with no approval', async () => {
    await withNexus('downgrade', (nexus) => {
      const session = nexus.systemSession()
      session.execute(`CREATE EVIDENCE ?e {
        SET FIELDS { evidence_class: "Skill", content_digest: "sha256:x" }
      }`)
      session.elevateAuthority(parseElementId('E-1'), 'executable')
      // §132: a demotion that had to wait for a Governance ticket would arrive
      // late, which is the opposite of what the ceiling is for.
      expect(session.elevateAuthority(parseElementId('E-1'), 'descriptive')).toBe(
        'executable',
      )
    })
  })

  it('keeps quarantine apart from archival, and from retraction', async () => {
    await withNexus('quarantine', (nexus) => {
      const session = nexus.systemSession()
      session.execute('CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }')
      session.quarantine(parseElementId('C-1'), 'under review')

      // Ordinary recall excludes it by construction: a pattern naming no state
      // matches `active`.
      expect(nexus.query('FIND(?c) WHERE { ?c CONCEPT {type: "Person"} }')).toEqual([])
      // A reviewer that asks for it by state still sees it — §134: quarantine
      // says *this Brain does not currently allow ordinary use*, not that the
      // element was retired or that anybody took anything back.
      expect(
        nexus.query(
          'FIND(?c.governance.quarantine_reason) WHERE { ?c CONCEPT {type: "Person", state: "quarantined"} }',
        ),
      ).toEqual(['under review'])

      session.releaseQuarantine(parseElementId('C-1'))
      expect(nexus.query('FIND(?c) WHERE { ?c CONCEPT {type: "Person"} }')).toHaveLength(1)
    })
  })

  it('does not let release revive something archived for another reason', async () => {
    await withNexus('release-guard', (nexus) => {
      const session = nexus.systemSession()
      session.execute('CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }')
      session.execute('ARCHIVE "C-1"')
      expect(() => session.releaseQuarantine(parseElementId('C-1'))).toThrowError(
        /not quarantined/,
      )
    })
  })

  it('records a classification change in the version log and the audit', async () => {
    await withNexus('records', (nexus) => {
      const session = nexus.systemSession()
      session.execute('CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }')
      session.classify(parseElementId('C-1'), 'secret')

      // The two logs answer different questions: the version log says what the
      // element looked like, the audit says who decided that and why (§177).
      const versions = nexus.describe('HISTORY ELEMENT "C-1"') as { op: string }[]
      expect(versions.map((v) => v.op)).toEqual(['create', 'classify'])

      const entries = session.readAudit()
      const classified = entries.find((entry) => entry.operation === 'classify')
      expect(classified?.resource).toBe('C-1')
      expect((classified?.record as { to?: string })?.to).toBe('secret')
    })
  })
})

describe('erasure', () => {
  const SETUP = `MUTATE {
    CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
    CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark" }
    ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
  }`

  async function withNexus<T>(
    name: string,
    body: (nexus: CognitiveNexus) => T,
  ): Promise<T> {
    const stub = env.KIP_DB.getByName(`purge-${name}`)
    return await runInDurableObject(stub, (_instance, state) => {
      const nexus = CognitiveNexus.connect(state.storage)
      nexus.activatePackages([COGNITIVE_MEMORY])
      nexus.execute(SETUP)
      return body(nexus)
    })
  }

  it('refuses by default, and names how many rather than which', async () => {
    await withNexus('default', (nexus) => {
      // §103: the referring elements may be ones this caller cannot read, so a
      // refusal must not become a way to enumerate them.
      expect(() => nexus.execute('PURGE "C-1" CONFIRM "PURGE"')).toThrowError(
        /1 element\(s\) still reference C-1/,
      )
      expect(() => nexus.execute('PURGE "C-1" CONFIRM "PURGE"')).not.toThrowError(
        /P-1/,
      )
    })
  })

  it('refuses a reference policy it does not implement, rather than defaulting', async () => {
    await withNexus('unknown-policy', (nexus) => {
      // Defaulting would silently run a destructive operation under a policy
      // the caller did not ask for.
      expect(() =>
        nexus.execute(
          'PURGE "C-2" REFERENCE POLICY "delete_everything" CONFIRM "PURGE"',
        ),
      ).toThrowError(/is not a reference policy/)
    })
  })

  it('writes the Governance audit one receipt per erased element', async () => {
    await withNexus('purge-audit', (nexus) => {
      nexus.execute(
        'PURGE "C-1" REFERENCE POLICY "authorized_cascade" CONFIRM "PURGE"',
      )
      const purges = nexus
        .systemSession()
        .readAudit(100)
        .filter((entry) => (entry as { operation?: string }).operation === 'purge')
      // One per erased element, cascade included: an erasure that left no trace
      // of having happened would defeat the reason §164 permits a receipt at
      // all — the auditor still has to be able to say what was destroyed.
      expect(purges.length).toBeGreaterThanOrEqual(1)
      const record = (purges[0] as { record?: Record<string, unknown> }).record
      expect(record?.element).toMatch(/^[CPAEX]-\d+$/)
      // The digest of what was there, and none of what was there.
      expect(record?.content_digest).toMatch(/^[0-9a-f]{64}$/)
      expect(record?.reference_policy).toBe('authorized_cascade')
    })
  })

  it('leaves an identity stub rather than a hole', async () => {
    await withNexus('stub', (nexus) => {
      nexus.execute(
        'PURGE "C-1" REFERENCE POLICY "tombstone_reference" CONFIRM "PURGE"',
      )
      const stub = nexus.store.load(parseElementId('C-1'))
      // Deleting the row outright would break every reference to it — and a
      // dangling reference does not say "this was erased", it says nothing.
      expect(stub?.row.state).toBe('purged')
      expect(stub?.row.governance.purged).toBe(true)
      expect(stub?.row.governance.content_digest).toMatch(/^[0-9a-f]{64}$/)
      expect((stub?.row as { name?: string }).name).toBe('')
      // The reference still resolves — to a stub that says what happened.
      expect(nexus.store.referrers(nexus.space, parseElementId('C-1'))).toHaveLength(1)
    })
  })

  it('erases every content column, not the ones somebody remembered to list', async () => {
    await withNexus('every-column', (nexus) => {
      nexus.execute(`MUTATE {
        CREATE ASSERTION ?a {
          SET FIELDS { proposition: {id: "P-1"}, asserted_by: {id: "C-1"},
                       stance: "support", mode: "stated", confidence: 0.9 }
        }
      }`)
      nexus.execute(
        'PURGE "P-1" REFERENCE POLICY "tombstone_reference" CONFIRM "PURGE"',
      )
      nexus.execute(
        'PURGE "A-1" REFERENCE POLICY "tombstone_reference" CONFIRM "PURGE"',
      )

      // A Proposition *is* its tuple, and an Assertion *is* its epistemic
      // payload. An erasure that scrubbed a Concept's `name` and left these
      // behind would be an erasure of the one kind whose content somebody
      // happened to enumerate.
      const proposition = nexus.store.load(parseElementId('P-1'))
      const tuple = proposition?.row as unknown as Record<string, unknown>
      expect(tuple.subject).toEqual({})
      expect(tuple.object).toEqual({})
      expect(tuple.predicate_ref).toBe('')
      expect(tuple.subject_key).toBe('')
      expect(tuple.tuple_key).toBe('')

      const assertion = nexus.store.load(parseElementId('A-1'))
      const claim = assertion?.row as unknown as Record<string, unknown>
      expect(claim.stance).toBe('')
      expect(claim.mode).toBe('')
      expect(claim.confidence).toBe(0)
      expect(claim.asserted_by).toEqual({})
      expect(claim.proposition_id).toBe('')

      // Both still resolve as stubs, so nothing pointing at them dangles.
      expect(proposition?.row.state).toBe('purged')
      expect(assertion?.row.state).toBe('purged')
    })
  })

  it('destroys the history, not only the current row', async () => {
    await withNexus('history', (nexus) => {
      expect(nexus.describe('HISTORY ELEMENT "C-2"')).toHaveLength(1)
      nexus.execute('PURGE "C-2" REFERENCE POLICY "tombstone_reference" CONFIRM "PURGE"')
      // An element scrubbed only in its current row stays fully readable
      // through the version log, which would make the purge a purge in name
      // only (§19.3).
      const versions = nexus.describe('HISTORY ELEMENT "C-2"') as { op: string }[]
      expect(versions.map((v) => v.op)).toEqual(['purge'])
    })
  })

  it('does not walk past a legal hold', async () => {
    await withNexus('legal-hold', (nexus) => {
      const element = nexus.store.load(parseElementId('C-2'))
      if (element === null) throw new Error('C-2 should exist')
      element.row.retention = { legal_hold: true }
      nexus.store.put(element, 'update', 'test')

      // §163: the hold is checked before anything destructive is decided, and
      // lifting it is a separate decision under its own permission.
      expect(() =>
        nexus.execute('PURGE "C-2" REFERENCE POLICY "tombstone_reference" CONFIRM "PURGE"'),
      ).toThrowError(/under a legal hold/)
    })
  })

  it('needs its own permission to place a legal hold', async () => {
    await withNexus('hold-permission', (nexus) => {
      const gov = nexus.store.governance
      gov.ensurePrincipal({ principal_id: 'kip:principal:agent' })
      gov.createGrant(
        {
          space_id: nexus.space,
          grantee_principal: 'kip:principal:agent',
          actions: ['create', 'read', 'manage_retention'],
        },
        SYSTEM_PRINCIPAL,
      )
      const session = nexus.session(principalAuth('kip:principal:agent'))

      // An ordinary expiry is retention management…
      expect(() =>
        session.execute(`CREATE CONCEPT ?c {
          TYPE "Person" NAME "Bob"
          SET FIELDS { retention: {expires_at: "2030-01-01T00:00:00.000Z"} }
        }`),
      ).not.toThrow()

      // …but a hold blocks erasure, so a writer that could set one could make
      // its own content undeletable.
      expect(() =>
        session.execute(`CREATE CONCEPT ?c {
          TYPE "Person" NAME "Carol"
          SET FIELDS { retention: {legal_hold: true} }
        }`),
      ).toThrowError(/requires the legal_hold permission/)
    })
  })

  it('authorizes every dependent before erasing any of them', async () => {
    await withNexus('cascade', (nexus) => {
      const gov = nexus.store.governance
      gov.ensurePrincipal({ principal_id: 'kip:principal:agent' })
      gov.createGrant(
        {
          space_id: nexus.space,
          grantee_principal: 'kip:principal:agent',
          actions: ['read'],
        },
        SYSTEM_PRINCIPAL,
      )
      gov.createGrant(
        {
          space_id: nexus.space,
          grantee_principal: 'kip:principal:agent',
          actions: ['purge'],
          // The target, but not the Proposition that points at it.
          scope: { elements: ['C-1'] },
        },
        SYSTEM_PRINCIPAL,
      )
      const session = nexus.session(principalAuth('kip:principal:agent'))

      expect(() =>
        session.execute(
          'PURGE "C-1" REFERENCE POLICY "authorized_cascade" CONFIRM "PURGE"',
        ),
      ).toThrowError(/requires the purge permission/)
      // A cascade that erased half and then refused would leave a graph nothing
      // can describe, so nothing was erased.
      expect(nexus.store.load(parseElementId('C-1'))?.row.state).toBe('active')
      expect(nexus.store.load(parseElementId('P-1'))?.row.state).toBe('active')
    })
  })

  it('erases the dependents when the cascade is authorized', async () => {
    await withNexus('cascade-ok', (nexus) => {
      nexus.execute(
        'PURGE "C-1" REFERENCE POLICY "authorized_cascade" CONFIRM "PURGE"',
      )
      expect(nexus.store.load(parseElementId('C-1'))?.row.state).toBe('purged')
      expect(nexus.store.load(parseElementId('P-1'))?.row.state).toBe('purged')
    })
  })

  it('leaves a stub that still names the Principal that wrote it', async () => {
    // §19.3: the stub exists so an auditor can say something was here and who
    // wrote it. The version log that would otherwise answer the second half has
    // just been destroyed, so stamping the purging Principal over `origin`
    // would leave the erasure unattributable rather than merely opaque.
    await withNexus('purge-origin', (nexus) => {
      const gov = nexus.store.governance
      gov.ensurePrincipal({ principal_id: 'kip:principal:eraser' })
      gov.createGrant(
        {
          space_id: nexus.space,
          grantee_principal: 'kip:principal:eraser',
          actions: ['read', 'purge'],
        },
        SYSTEM_PRINCIPAL,
      )
      nexus
        .session(principalAuth('kip:principal:eraser'))
        .execute(
          'PURGE "C-2" REFERENCE POLICY "tombstone_reference" CONFIRM "PURGE"',
        )
      expect(
        nexus.store.load(parseElementId('C-2'))?.row.origin.principal_id,
      ).toBe(SYSTEM_PRINCIPAL)
    })
  })
})

describe('the audit and the past', () => {
  async function withNexus<T>(
    name: string,
    body: (nexus: CognitiveNexus) => T,
  ): Promise<T> {
    const stub = env.KIP_DB.getByName(`audit-${name}`)
    return await runInDurableObject(stub, (_instance, state) => {
      const nexus = CognitiveNexus.connect(state.storage)
      nexus.activatePackages([COGNITIVE_MEMORY])
      return body(nexus)
    })
  }

  it('needs its own permission to read what everyone else did', async () => {
    await withNexus('read-audit', (nexus) => {
      const gov = nexus.store.governance
      gov.ensurePrincipal({ principal_id: 'kip:principal:agent' })
      gov.createGrant(
        {
          space_id: nexus.space,
          grantee_principal: 'kip:principal:agent',
          actions: ['read', 'create'],
        },
        SYSTEM_PRINCIPAL,
      )
      const session = nexus.session(principalAuth('kip:principal:agent'))
      // A caller who may read a Space's cognition has not thereby earned the
      // right to read who has been reading it.
      expect(() => session.readAudit()).toThrowError(
        /requires the read_audit permission/,
      )
      expect(() => nexus.systemSession().readAudit()).not.toThrow()
    })
  })

  it('records a denial as well as an allow', async () => {
    await withNexus('denials', (nexus) => {
      const gov = nexus.store.governance
      gov.ensurePrincipal({ principal_id: 'kip:principal:agent' })
      const session = nexus.session(principalAuth('kip:principal:agent'))
      expect(() =>
        session.describe('EXPORT CAPSULE :out WHERE { ?c CONCEPT {} }', { out: 'x' }),
      ).toThrow()

      // §172 lists the operations whose absence from a log is itself the
      // incident, and export is one of them — refused or not.
      const denied = nexus
        .systemSession()
        .readAudit()
        .find((entry) => entry.operation === 'export')
      expect(denied?.entry_class).toBe('decision')
      expect(denied?.decision).toBe('deny')
      expect(denied?.principal_id).toBe('kip:principal:agent')
    })
  })

  it('does not audit an ordinary read', async () => {
    await withNexus('quiet', (nexus) => {
      nexus.query('FIND(?c) WHERE { ?c CONCEPT {type: "Person"} }')
      // §173: a log that recorded every read would bury the entries that
      // matter under the ones that do not.
      const reads = nexus
        .systemSession()
        .readAudit()
        .filter((entry) => entry.operation === 'read')
      expect(reads).toEqual([])
    })
  })

  it('carries the deciding identity on a high-impact receipt, and only there', async () => {
    await withNexus('provenance', (nexus) => {
      const session = nexus.systemSession()
      // An ordinary write carries none: attaching it everywhere would bury the
      // cases the record exists for.
      const ordinary = session.execute('CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }')
      expect(ordinary.governance).toBeUndefined()

      // §178: an erasure has to be explainable later in terms of the identity
      // and policy that authorized it.
      const erasure = session.execute(
        'PURGE "C-1" REFERENCE POLICY "tombstone_reference" CONFIRM "PURGE"',
      )
      expect(erasure.governance?.principal_id).toBe(SYSTEM_PRINCIPAL)
      expect(erasure.governance?.operations).toEqual(['purge'])
    })
  })

  it('answers who had access then, without claiming anything about now', async () => {
    const grant = await withNexus('as-of', (nexus) => {
      const gov = nexus.store.governance
      gov.ensurePrincipal({ principal_id: 'kip:principal:agent' })
      return gov.createGrant(
        {
          space_id: nexus.space,
          grantee_principal: 'kip:principal:agent',
          actions: ['read', 'export'],
        },
        SYSTEM_PRINCIPAL,
      )
    })
    await tickPast('audit-as-of', grant.created_at)
    await withNexus('as-of', (nexus) => {
      nexus.store.governance.revokeGrant(grant.id, SYSTEM_PRINCIPAL)
    })

    await withNexus('as-of', (nexus) => {
      const auditor = nexus.systemSession()
      const then = auditor.accessAsOf(grant.created_at) as {
        permissions: string[]
        caveats: string[]
      }
      // The Grant is revoked now and was in force then. Revocation being a
      // status change rather than a delete is exactly what makes this
      // answerable (§177).
      expect(then.permissions).toContain('export')
      // §179: and the report says out loud that it is not a claim about today.
      expect(then.caveats.join(' ')).toMatch(/says nothing about today/)
    })
  })

  it('needs read_governance_history, which read_audit does not confer', async () => {
    await withNexus('as-of-permission', (nexus) => {
      const gov = nexus.store.governance
      gov.ensurePrincipal({ principal_id: 'kip:principal:auditor' })
      gov.createGrant(
        {
          space_id: nexus.space,
          grantee_principal: 'kip:principal:auditor',
          actions: ['read', 'read_audit'],
        },
        SYSTEM_PRINCIPAL,
      )
      const session = nexus.session(principalAuth('kip:principal:auditor'))
      expect(() => session.readAudit()).not.toThrow()
      // One is what the control plane *was*, the other is what people *did*.
      expect(() => session.accessAsOf('2026-01-01T00:00:00.000Z')).toThrowError(
        /requires the read_governance_history permission/,
      )
    })
  })
})

/**
 * The threat model (§236–§247).
 *
 * Written the way the design document states them: an attacker-controlled setup,
 * and the result the engine owes. Each is a scenario somebody would actually
 * try, and each fails in a *specific* way — a test that only asserted "it threw"
 * would pass against an engine that was broken for an unrelated reason.
 */
describe('the threat model', () => {
  async function withNexus<T>(
    name: string,
    body: (nexus: CognitiveNexus) => T,
  ): Promise<T> {
    const stub = env.KIP_DB.getByName(`threat-${name}`)
    return await runInDurableObject(stub, (_instance, state) => {
      const nexus = CognitiveNexus.connect(state.storage)
      nexus.activatePackages([COGNITIVE_MEMORY])
      return body(nexus)
    })
  }

  it('§236 gives content that declares its own authority none of it', async () => {
    await withNexus('self-declared', (nexus) => {
      // Memory arriving with "authority: executable, trust: 1.0" written into
      // its own attributes. The words are ordinary content; the ceiling is a
      // Governance member no mutation can reach.
      nexus.execute(`CREATE CONCEPT ?skill {
        TYPE "Person" NAME "Skill"
        SET ATTRIBUTES { authority: "executable", trust: 1.0 }
      }`)
      const element = nexus.store.load(parseElementId('C-1'))
      if (element === null) throw new Error('C-1 should exist')
      expect((element.row as { attributes: Record<string, unknown> }).attributes.authority).toBe(
        'executable',
      )
      expect(ceilingOf(element)).toBe('descriptive')
      expect(element.row.governance.max_influence_authority).toBeUndefined()
    })
  })

  it('§237 does not let an import install a policy, because it does not import', async () => {
    await withNexus('capsule-policy', (nexus) => {
      // An import is a trust-boundary transition, not a configuration channel.
      // This engine has no import path at all, and refuses by name rather than
      // accepting a Capsule and ignoring the parts it does not understand — an
      // ignored policy block and an applied one look the same from outside.
      expect(() =>
        nexus.describe('PREVIEW IMPORT CAPSULE :c INTO :s', {
          c: {},
          s: nexus.space,
        }),
      ).toThrowError(/import/i)
      expect(nexus.spaceRow().default_policy_id).toBe('')
    })
  })

  it('§238 refuses a delegation that outlives or widens its parent', async () => {
    await withNexus('amplification', (nexus) => {
      const gov = nexus.store.governance
      for (const id of ['kip:principal:lead', 'kip:principal:sub']) {
        gov.ensurePrincipal({ principal_id: id })
      }
      // The lead may read Concepts, and no more.
      gov.createGrant(
        {
          space_id: nexus.space,
          grantee_principal: 'kip:principal:lead',
          actions: ['read'],
          scope: { kinds: ['concept'] },
          conditions: { valid_until: '2099-01-01T00:00:00.000Z' },
          delegation_allowed: true,
        },
        SYSTEM_PRINCIPAL,
      )

      // The classic amplification: a sub-agent delegation for more than the
      // delegator holds, unbounded in time and over every kind.
      gov.createDelegation(
        {
          space_id: nexus.space,
          delegator_principal: 'kip:principal:lead',
          delegate_principal: 'kip:principal:sub',
          actions: ['read', 'export'],
        },
        'kip:principal:lead',
      )
      const sub = nexus.session(principalAuth('kip:principal:sub'))
      // Neither action survives attenuation: `export` because the delegator
      // never held it, `read` because an unbounded child of a bounded parent is
      // a widening and not a narrowing.
      expect(() =>
        sub.query('FIND(?c) WHERE { ?c CONCEPT {type: "Person"} }'),
      ).toThrowError(/requires the read permission/)

      // A properly attenuated one works, which is what makes the refusal a rule
      // rather than a lockout.
      gov.createDelegation(
        {
          space_id: nexus.space,
          delegator_principal: 'kip:principal:lead',
          delegate_principal: 'kip:principal:sub',
          actions: ['read'],
          scope: { kinds: ['concept'] },
          conditions: { valid_until: '2098-01-01T00:00:00.000Z' },
        },
        'kip:principal:lead',
      )
      expect(sub.query('FIND(?c) WHERE { ?c CONCEPT {type: "Person"} }')).toEqual([])
    })
  })

  it('§238 stops a delegation the moment its delegator loses the authority', async () => {
    await withNexus('parent-revoked', (nexus) => {
      const gov = nexus.store.governance
      for (const id of ['kip:principal:lead', 'kip:principal:sub']) {
        gov.ensurePrincipal({ principal_id: id })
      }
      const parent = gov.createGrant(
        {
          space_id: nexus.space,
          grantee_principal: 'kip:principal:lead',
          actions: ['read'],
          delegation_allowed: true,
        },
        SYSTEM_PRINCIPAL,
      )
      gov.createDelegation(
        {
          space_id: nexus.space,
          delegator_principal: 'kip:principal:lead',
          delegate_principal: 'kip:principal:sub',
          actions: ['read'],
        },
        'kip:principal:lead',
      )
      const sub = nexus.session(principalAuth('kip:principal:sub'))
      expect(sub.query('FIND(?c) WHERE { ?c CONCEPT {type: "Person"} }')).toEqual([])

      // §35: the Delegation's own row still says `active` and its own expiry is
      // still in the future. A Delegation is only ever as good as its
      // delegator's authority right now, which is why it is not stored as a
      // kind of Grant.
      gov.revokeGrant(parent.id, SYSTEM_PRINCIPAL)
      expect(() =>
        sub.query('FIND(?c) WHERE { ?c CONCEPT {type: "Person"} }'),
      ).toThrowError(/requires the read permission/)
    })
  })

  it('§238 refuses a named chain whose links do not link', async () => {
    await withNexus('broken-chain', (nexus) => {
      const gov = nexus.store.governance
      for (const id of ['kip:principal:lead', 'kip:principal:sub']) {
        gov.ensurePrincipal({ principal_id: id })
      }
      gov.createGrant(
        {
          space_id: nexus.space,
          grantee_principal: 'kip:principal:lead',
          actions: ['read'],
          delegation_allowed: true,
        },
        SYSTEM_PRINCIPAL,
      )
      const first = gov.createDelegation(
        {
          space_id: nexus.space,
          delegator_principal: 'kip:principal:lead',
          delegate_principal: 'kip:principal:sub',
          actions: ['read'],
        },
        'kip:principal:lead',
      )
      const second = gov.createDelegation(
        {
          space_id: nexus.space,
          delegator_principal: 'kip:principal:lead',
          delegate_principal: 'kip:principal:sub',
          actions: ['read'],
        },
        'kip:principal:lead',
      )
      // Two unrelated Delegations presented as one chain. The second does not
      // name the first as its parent, so this is not a narrower authority — it
      // is a fiction about how the authority was conferred.
      const session = nexus.session(
        principalAuth('kip:principal:sub', {
          delegation_chain: [
            `kip:delegation:${first.id}`,
            `kip:delegation:${second.id}`,
          ],
        }),
      )
      expect(() =>
        session.query('FIND(?c) WHERE { ?c CONCEPT {type: "Person"} }'),
      ).toThrowError(/does not descend from the one before it/)
    })
  })

  it('§244 does not let a source vouch for itself into anything', async () => {
    await withNexus('self-vouching', (nexus) => {
      // `(Source, prefers, Everything)` with maximum confidence is an ordinary
      // meta-epistemic claim. It is recorded, and it changes no decision — there
      // is no trust resolver for it to reach, and the projection says so in
      // every answer rather than letting the absence read as calibration.
      nexus.execute(`MUTATE {
        CREATE CONCEPT ?src { TYPE "Person" NAME "Source" }
        CREATE CONCEPT ?all { TYPE "Preference" NAME "Everything" }
        ENSURE PROPOSITION ?p (?src, "prefers", ?all)
        CREATE ASSERTION ?a {
          SET FIELDS { proposition: ?p, asserted_by: ?src, stance: "support", mode: "stated", confidence: 1.0 }
        }
      }`)
      const warnings = nexus.query(
        'FIND(?b.explanation.warnings) WHERE { ?p PROPOSITION (?s, "prefers", ?o) ?b BELIEF (?p) }',
      ) as string[][]
      expect(warnings[0]?.join(' ')).toMatch(/trust/i)
      // And the control plane is untouched by any of it.
      expect(nexus.systemSession().effectiveAuthority().bindings).toEqual([])
    })
  })

  it('§247 still names the policy version that authorized a past operation', async () => {
    const first = await withNexus('policy-version', (nexus) => {
      const gov = nexus.store.governance
      const published = gov.publishPolicy(
        {
          policy_id: 'kip:policy:space',
          space_id: nexus.space,
          statements: [{ effect: 'allow', actions: ['export'] }],
        },
        SYSTEM_PRINCIPAL,
      )
      const space = nexus.spaceRow()
      space.default_policy_id = 'kip:policy:space'
      nexus.store.putSpace(space)
      nexus.describe('EXPORT CAPSULE :out WHERE { ?c CONCEPT {} }', { out: 'x' })
      return published
    })
    await tickPast('threat-policy-version', first.created_at)

    await withNexus('policy-version', (nexus) => {
      // The policy moves on. The record of what authorized the past operation
      // does not: an audit that could be edited would answer with today's answer
      // rather than the one that was true.
      nexus.store.governance.publishPolicy(
        {
          policy_id: 'kip:policy:space',
          space_id: nexus.space,
          statements: [{ effect: 'deny', actions: ['export'] }],
        },
        SYSTEM_PRINCIPAL,
      )
      expect(() =>
        nexus.describe('EXPORT CAPSULE :out WHERE { ?c CONCEPT {} }', { out: 'x' }),
      ).toThrowError(/requires the export permission/)

      const exports = nexus
        .systemSession()
        .readAudit(100)
        .filter((entry) => entry.operation === 'export')
      const allowed = exports.find((entry) => entry.decision !== 'deny')
      expect(allowed?.policy_id).toBe('kip:policy:space')
      expect(allowed?.policy_version).toBe(first.version)
      expect(nexus.store.governance.activePolicy('kip:policy:space')?.version).toBe(2)
    })
  })
})
