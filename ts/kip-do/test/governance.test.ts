import { env, runInDurableObject } from 'cloudflare:test'
import { describe, expect, it } from 'vitest'
import {
  ALL_PERMISSIONS,
  authStrength,
  authority,
  classification,
  conditionsContain,
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
  rowIdOf,
  scopeContains,
  tightenConstraints,
} from '../src/governance/index.js'
import { Store } from '../src/store/index.js'

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
 * Crosses a millisecond boundary between two steps.
 *
 * A Workers isolate's clock does not advance during synchronous execution, so
 * two Governance mutations made in one request share a timestamp and the
 * historical view cannot separate them. That is a real property of the engine —
 * documented in `store/governance.ts` — rather than something to work around,
 * so a test that means to observe a *past* state says so by waiting.
 */
const tick = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 5))

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
    await tick()
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
    await tick()
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
    await tick()
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
      const entries = gov.readAudit('kip:space:default', 10)
      expect(entries).toHaveLength(1)
      expect(entries[0]?.entry_class).toBe('mutation')
      expect(entries[0]?.operation).toBe('create_grant')
      // A whole record, not a diff: a chain with one missing link answers a
      // historical question wrongly instead of refusing (§175).
      expect((entries[0]?.record as { actions?: string[] })?.actions).toEqual([
        'read',
        'export',
      ])
    })
  })
})
