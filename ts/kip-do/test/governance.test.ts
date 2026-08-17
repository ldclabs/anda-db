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
  principalAuth,
  rowIdOf,
  scopeContains,
  spaceResource,
  subjectDigest,
  tightenConstraints,
} from '../src/governance/index.js'
import { parseElementId } from '../src/id.js'
import { CognitiveNexus, SYSTEM_PRINCIPAL } from '../src/nexus.js'
import { COGNITIVE_MEMORY } from '../src/schema/index.js'
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
          actions: ['create', 'read', 'assert'],
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
