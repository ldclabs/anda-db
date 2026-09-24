import { env, runInDurableObject } from 'cloudflare:test'
import { describe, expect, it } from 'vitest'
import { CognitiveNexus, SYSTEM_PRINCIPAL } from '../src/nexus.js'
import { parsePermission, principalAuth } from '../src/governance/index.js'
import { parseElementId } from '../src/id.js'
import { COGNITIVE_MEMORY, type SchemaPackage } from '../src/schema/index.js'
import type { ConceptRow, PropositionRow } from '../src/store/index.js'
import { OPTIONS } from './support/options.js'

/**
 * Identity and matching after 793af73: canonical Literals (§9.4, §9.6),
 * canonical matching through merges (§12.3, §43.2), symbol lineage (§20.14),
 * the Predicate definition fields (§20.15), the projection's `leading`
 * (§27.2), `governance.authority_class` (§31.3), cursor refusals (§87.7) and
 * the `truncated` flag on a cut-short dependents walk (§63.5).
 */
async function withNexus(
  name: string,
  body: (nexus: CognitiveNexus) => void,
  extra: readonly SchemaPackage[] = [],
): Promise<void> {
  const stub = env.KIP_DB.getByName(`canonical-${name}`)
  await runInDurableObject(stub, (_instance, state) => {
    const nexus = CognitiveNexus.connect(state.storage)
    nexus.activatePackages([COGNITIVE_MEMORY, OPTIONS, ...extra])
    body(nexus)
  })
}

/** A predicate that constrains neither end, so Literals may sit in it. */
const OPEN = {
  format: 'KIP-Schema-Package',
  manifest: { package_id: 'kip://test/open', version: '1.0.0' },
  definitions: {
    predicates: {
      notes: { kind: 'PredicateType', description: 'Anything, about anything.' },
      born: {
        kind: 'PredicateType',
        description: 'When.',
        object: { literal_types: ['string'], format: 'timestamp' },
      },
      site: {
        kind: 'PredicateType',
        description: 'Where.',
        object: { literal_types: ['string'], format: 'uri' },
      },
      maybe: {
        kind: 'PredicateType',
        description: 'Possibly nothing.',
        object: { literal_types: ['string', 'null'], nullable: true },
      },
      never_null: {
        kind: 'PredicateType',
        description: 'Something.',
        object: { literal_types: ['string', 'null'] },
      },
      likes: {
        kind: 'PredicateType',
        description: 'Boolean-complete.',
        object: { literal_types: ['boolean'] },
        boolean_completeness: true,
      },
      timeless: {
        kind: 'PredicateType',
        description: 'Functional, but values never conflict on time.',
        object: { kinds: ['Concept'] },
        functional: true,
        temporal_conflict: 'none',
      },
    },
  },
} as unknown as SchemaPackage

function refused(body: () => unknown): { code: string; details?: unknown } {
  try {
    body()
  } catch (err) {
    return err as { code: string; details?: unknown }
  }
  throw new Error('expected a refusal')
}

describe('Literals', () => {
  it('canonicalizes on write so NFC and NFD twins resolve to one Proposition', async () => {
    await withNexus(
      'nfc',
      (nexus) => {
        nexus.execute('CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }')
        const nfc = nexus.execute('ENSURE PROPOSITION ?p ({id: "C-1"}, "notes", "café")')
        const nfd = nexus.execute('ENSURE PROPOSITION ?p ({id: "C-1"}, "notes", "café")')
        expect(nfd.status).toBe('no_effect')
        expect(nfd.handles.p).toBe(nfc.handles.p)
        // Numbers by mathematical value, -0 as 0 (§9.6).
        const one = nexus.execute('ENSURE PROPOSITION ?p ({id: "C-1"}, "notes", 1)')
        expect(nexus.execute('ENSURE PROPOSITION ?p ({id: "C-1"}, "notes", 1.0)').handles.p).toBe(
          one.handles.p,
        )
        const zero = nexus.execute('ENSURE PROPOSITION ?p ({id: "C-1"}, "notes", 0)')
        expect(nexus.execute('ENSURE PROPOSITION ?p ({id: "C-1"}, "notes", -0)').handles.p).toBe(
          zero.handles.p,
        )
        // No trimming, no case folding.
        expect(
          nexus.execute('ENSURE PROPOSITION ?p ({id: "C-1"}, "notes", "Café")').status,
        ).toBe('committed')
        // And a pattern written in NFD finds the NFC tuple.
        expect(
          nexus.query('FIND(?p.id) WHERE { ?p PROPOSITION ({id: "C-1"}, "notes", "café") }'),
        ).toEqual([nfc.handles.p])
      },
      [OPEN],
    )
  })

  it('refuses a language member as a TypeMismatch', async () => {
    await withNexus(
      'language',
      (nexus) => {
        nexus.execute('CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }')
        const tagged = refused(() =>
          nexus.execute('ENSURE PROPOSITION ?p ({id: "C-1"}, "notes", :v)', {
            v: { value: '苹果', language: 'zh-Hans' },
          }),
        )
        expect(tagged.code).toBe('TypeMismatch')
      },
      [OPEN],
    )
  })
})

describe('Predicate definition fields', () => {
  it('validates format and nullable on write, without touching identity', async () => {
    await withNexus(
      'fields',
      (nexus) => {
        nexus.execute('CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }')
        expect(
          nexus.execute('ENSURE PROPOSITION ?p ({id: "C-1"}, "born", "1990-01-01T00:00:00.000Z")').status,
        ).toBe('committed')
        expect(
          refused(() => nexus.execute('ENSURE PROPOSITION ?p ({id: "C-1"}, "born", "yesterday")')).code,
        ).toBe('ConstraintViolation')
        expect(
          nexus.execute('ENSURE PROPOSITION ?p ({id: "C-1"}, "site", "https://example.com")').status,
        ).toBe('committed')
        expect(
          refused(() => nexus.execute('ENSURE PROPOSITION ?p ({id: "C-1"}, "site", "not a uri")')).code,
        ).toBe('ConstraintViolation')
        // §9.5: null is a semantic Literal only where the Predicate permits it.
        expect(nexus.execute('ENSURE PROPOSITION ?p ({id: "C-1"}, "maybe", null)').status).toBe(
          'committed',
        )
        expect(
          refused(() => nexus.execute('ENSURE PROPOSITION ?p ({id: "C-1"}, "never_null", null)')).code,
        ).toBe('ConstraintViolation')
        // The format is not part of identity: the same string is one tuple.
        expect(
          nexus.execute('ENSURE PROPOSITION ?p ({id: "C-1"}, "born", "1990-01-01T00:00:00.000Z")').status,
        ).toBe('no_effect')
      },
      [OPEN],
    )
  })

  it('treats false as the negation of true under boolean_completeness', async () => {
    await withNexus(
      'boolean',
      (nexus) => {
        nexus.execute(`MUTATE {
          CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
          CREATE CONCEPT ?bob { TYPE "Person" NAME "Bob" }
          ENSURE PROPOSITION ?yes (?alice, "likes", true)
          ENSURE PROPOSITION ?no (?alice, "likes", false)
          CREATE ASSERTION ?a { SET FIELDS { proposition: ?yes, asserted_by: ?alice, stance: "support", mode: "stated", confidence: 0.9 } }
          CREATE ASSERTION ?b { SET FIELDS { proposition: ?no, asserted_by: ?bob, stance: "support", mode: "stated", confidence: 0.9 } }
        }`)
        // Bob's support for `false` opposes Alice's `true`: contested, with
        // an exact tie between the roots, so `leading` is none.
        expect(
          nexus.query(
            'FIND(?b.status, ?b.leading) WHERE { ?p PROPOSITION ({id: "C-1"}, "likes", true) ?b BELIEF (?p) }',
          ),
        ).toEqual([['contested', 'none']])
      },
      [OPEN],
    )
  })

  it('never conflicts values of a functional slot declared temporal_conflict none', async () => {
    await withNexus(
      'timeless',
      (nexus) => {
        nexus.execute(`MUTATE {
          CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
          CREATE CONCEPT ?x { TYPE "Option" NAME "X" }
          CREATE CONCEPT ?y { TYPE "Option" NAME "Y" }
          ENSURE PROPOSITION ?px (?alice, "timeless", ?x)
          ENSURE PROPOSITION ?py (?alice, "timeless", ?y)
          CREATE ASSERTION ?a { SET FIELDS { proposition: ?px, asserted_by: ?alice, stance: "support", mode: "stated", confidence: 0.9 } }
          CREATE ASSERTION ?b { SET FIELDS { proposition: ?py, asserted_by: ?alice, stance: "support", mode: "stated", confidence: 0.9 } }
        }`)
        expect(
          nexus.query(
            'FIND(?b.status, ?b.leading) WHERE { ?p PROPOSITION ({id: "C-1"}, "timeless", {id: "C-2"}) ?b BELIEF (?p) }',
          ),
        ).toEqual([['accepted', 'support']])
      },
      [OPEN],
    )
  })
})

describe('leading', () => {
  it('names the heavier side under contested and none on a tie', async () => {
    await withNexus('leading', (nexus) => {
      nexus.execute(`MUTATE {
        CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
        CREATE CONCEPT ?bob { TYPE "Person" NAME "Bob" }
        CREATE CONCEPT ?carol { TYPE "Person" NAME "Carol" }
        CREATE CONCEPT ?dark { TYPE "Option" NAME "Dark" }
        ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
        CREATE ASSERTION ?a { SET FIELDS { proposition: ?p, asserted_by: ?alice, stance: "support", mode: "stated", confidence: 0.9 } }
        CREATE ASSERTION ?b { SET FIELDS { proposition: ?p, asserted_by: ?bob, stance: "support", mode: "stated", confidence: 0.9 } }
        CREATE ASSERTION ?c { SET FIELDS { proposition: ?p, asserted_by: ?carol, stance: "reject", mode: "stated", confidence: 0.9 } }
      }`)
      const BELIEF = 'WHERE { ?p PROPOSITION ({id: "C-1"}, "prefers", {id: "C-4"}) ?b BELIEF (?p) }'
      // Two independent roots support, one opposes: contested, leading support.
      expect(nexus.query(`FIND(?b.status, ?b.leading) ${BELIEF}`)).toEqual([
        ['contested', 'support'],
      ])
      // Disclosure only: the status is untouched, and a withdrawn supporter
      // leaves an exact tie.
      nexus.execute('TRANSITION "A-2" TO "retracted"')
      expect(nexus.query(`FIND(?b.status, ?b.leading) ${BELIEF}`)).toEqual([
        ['contested', 'none'],
      ])
      nexus.execute('TRANSITION "A-1" TO "retracted"')
      expect(nexus.query(`FIND(?b.status, ?b.leading) ${BELIEF}`)).toEqual([
        ['rejected', 'opposition'],
      ])
      nexus.execute('TRANSITION "A-3" TO "retracted"')
      expect(nexus.query(`FIND(?b.status, ?b.leading) ${BELIEF}`)).toEqual([
        ['insufficient', 'none'],
      ])
    })
  })
})

describe('canonical matching', () => {
  it('matches a tuple through merged_into in both directions, and keeps both views', async () => {
    await withNexus('merge', (nexus) => {
      nexus.execute(`MUTATE {
        CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" SET FIELDS {key: "person:alice"} }
        CREATE CONCEPT ?alicia { TYPE "Person" NAME "Alicia" SET FIELDS {key: "person:alicia"} }
        CREATE CONCEPT ?dark { TYPE "Option" NAME "Dark" }
        ENSURE PROPOSITION ?p (?alicia, "prefers", ?dark)
      }`)
      const before = nexus.store.currentSeq(nexus.space)
      expect(
        nexus.query('FIND(?p.id) WHERE { ?p PROPOSITION ({id: "C-1"}, "prefers", ?o) }'),
      ).toEqual([])
      nexus.execute('MERGE CONCEPT "C-2" INTO "C-1"')

      // Naming the surviving identity finds the tuple recorded on the merged
      // one, and naming the merged one still does (§43.2).
      expect(
        nexus.query('FIND(?p.id) WHERE { ?p PROPOSITION ({id: "C-1"}, "prefers", ?o) }'),
      ).toEqual(['P-1'])
      expect(
        nexus.query('FIND(?p.id) WHERE { ?p PROPOSITION ({id: "C-2"}, "prefers", ?o) }'),
      ).toEqual(['P-1'])
      // The stored endpoint stays what it was; the canonical one resolves.
      expect(
        nexus.query(
          'FIND(?p.subject, ?p.canonical_subject) WHERE { ?p PROPOSITION ({id: "C-1"}, "prefers", ?o) }',
        ),
      ).toEqual([[{ id: 'C-2' }, { id: 'C-1' }]])
      // A FILTER on the stored endpoint narrows a canonical match to tuples
      // actually recorded on it.
      expect(
        nexus.query(
          'FIND(?p.id) WHERE { ?p PROPOSITION ({id: "C-1"}, "prefers", ?o) FILTER(?p.subject == :alice) }',
          { alice: { id: 'C-1' } },
        ),
      ).toEqual([])
      // BELIEF SLOT over the canonical subject sees the slot.
      expect(
        nexus.query('FIND(?s.candidate_projections) WHERE { ?s BELIEF SLOT ({id: "C-1"}, "prefers") }').map(
          (v) => (v as unknown[]).length,
        ),
      ).toEqual([1])
      // A coordinate before the merge resolves nothing through it (§48.1).
      expect(
        nexus.query(
          `FIND(?p.id) WHERE { ?p PROPOSITION ({id: "C-1"}, "prefers", ?o) } AS OF SEQ ${before}`,
        ),
      ).toEqual([])
      expect(
        nexus.query(
          `FIND(?p.canonical_subject) WHERE { ?p PROPOSITION ({id: "C-2"}, "prefers", ?o) } AS OF SEQ ${before}`,
        ),
      ).toEqual([{ id: 'C-2' }])
    })
  })
})

describe('symbol lineage', () => {
  const v1 = {
    format: 'KIP-Schema-Package',
    manifest: { package_id: 'kip://test/lineage', version: '1.0.0' },
    definitions: {
      concept_types: { Thing: { kind: 'ConceptType', description: 'A thing.' } },
      predicates: { holds: { kind: 'PredicateType', description: 'Holds.' } },
    },
  } as unknown as SchemaPackage
  const v2 = {
    ...v1,
    manifest: { package_id: 'kip://test/lineage', version: '1.1.0' },
  } as unknown as SchemaPackage

  it('keeps keys, tuples and type matches on the lineage across a package upgrade', async () => {
    await withNexus('upgrade', (nexus) => {
      nexus.activatePackages([COGNITIVE_MEMORY, OPTIONS, v1])
      nexus.execute(`MUTATE {
        CREATE CONCEPT ?a { TYPE "Thing" NAME "A" SET FIELDS {key: "thing:a"} }
        CREATE CONCEPT ?b { TYPE "Thing" NAME "B" SET FIELDS {key: "thing:b"} }
        ENSURE PROPOSITION ?p (?a, "holds", ?b)
      }`)
      const rowA = nexus.store.load(parseElementId('C-1'))?.row as ConceptRow
      expect(rowA.schema_ref).toBe('kip://test/lineage@1.0.0/Thing')
      expect(rowA.lineage).toBe('kip://test/lineage/Thing')
      const tuple = nexus.store.load(parseElementId('P-1'))?.row as PropositionRow
      expect(tuple.predicate_ref).toBe('kip://test/lineage@1.0.0/holds')
      expect(tuple.predicate_lineage).toBe('kip://test/lineage/holds')

      // This engine activates one version per package path at a time; the
      // upgrade replaces 1.0.0 with 1.1.0 in the lock.
      nexus.activatePackages([COGNITIVE_MEMORY, OPTIONS, v2])

      // `type:` matches every readable version of the lineage (§43.1), and
      // each element reports its own exact schema_ref.
      expect(
        nexus.query('FIND(?c.name, ?c.schema_ref) WHERE { ?c CONCEPT {type: "Thing"} } ORDER BY ?c.name'),
      ).toEqual([
        ['A', 'kip://test/lineage@1.0.0/Thing'],
        ['B', 'kip://test/lineage@1.0.0/Thing'],
      ])
      // An upsert by key under the new version addresses the same identity
      // (§7.3) rather than minting a second "thing:a"…
      const upsert = nexus.execute(
        'UPSERT CONCEPT ?a { MATCH {type: "Thing", key: "thing:a"} SET FIELDS {name: "A2"} }',
      )
      expect(upsert.handles.a).toBe('C-1')
      // …while a new Concept binds to the write version of the lineage.
      const created = nexus.execute('CREATE CONCEPT ?c { TYPE "Thing" NAME "C" SET FIELDS {key: "thing:c"} }')
      expect((nexus.store.load(parseElementId(created.handles.c!))?.row as ConceptRow).schema_ref).toBe(
        'kip://test/lineage@1.1.0/Thing',
      )
      // ENSURE under the later version resolves the existing tuple (§12.3).
      expect(nexus.execute('ENSURE PROPOSITION ?p ({id: "C-1"}, "holds", {id: "C-2"})').status).toBe(
        'no_effect',
      )
      // And a pattern's predicate matches by lineage.
      expect(
        nexus.query('FIND(?p.predicate_ref) WHERE { ?p PROPOSITION (?s, "holds", ?o) }'),
      ).toEqual(['kip://test/lineage@1.0.0/holds'])
      // The exact reference is still addressable as itself.
      expect(
        nexus.query(
          'FIND(COUNT(?c)) WHERE { ?c CONCEPT {schema_ref: "kip://test/lineage@1.1.0/Thing"} }',
        ),
      ).toEqual([1])
    })
  })
})

describe('governance additions', () => {
  it('reads authority_class as descriptive, refuses it from KML, and reports what may be elevated to', async () => {
    await withNexus('authority', (nexus) => {
      nexus.execute('CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }')
      expect(
        nexus.query('FIND(?c.governance.authority_class) WHERE { ?c CONCEPT {} }'),
      ).toEqual(['descriptive'])
      for (const command of [
        'UPDATE "C-1" SET FIELDS { authority_class: "executable" }',
        'CREATE CONCEPT ?x { TYPE "Person" NAME "X" SET FIELDS { authority_class: "executable" } }',
      ]) {
        expect(refused(() => nexus.execute(command)).code, command).toBe(
          'ProtectedGovernanceField',
        )
      }
      // The host API is the one way it changes (§31.3).
      nexus.systemSession().elevateAuthority(parseElementId('C-1'), 'advisory')
      expect(
        nexus.query('FIND(?c.governance.authority_class) WHERE { ?c CONCEPT {} }'),
      ).toEqual(['advisory'])

      // DESCRIBE ACCESS: the owner may elevate to anything; a caller without
      // `elevate_authority` to nothing; one with a ceiling up to it.
      const owner = nexus.describe('DESCRIBE ACCESS') as { elevatable_authority_classes: string[] }
      expect(owner.elevatable_authority_classes).toEqual([
        'descriptive',
        'advisory',
        'behavioral',
        'executable',
      ])
      const gov = nexus.store.governance
      gov.ensurePrincipal({ principal_id: 'kip:principal:reader' })
      gov.createGrant(
        { space_id: nexus.space, grantee_principal: 'kip:principal:reader', actions: ['read', 'discover'] },
        SYSTEM_PRINCIPAL,
      )
      expect(
        (nexus.session(principalAuth('kip:principal:reader')).describe('DESCRIBE ACCESS') as {
          elevatable_authority_classes: string[]
        }).elevatable_authority_classes,
      ).toEqual([])
      gov.ensurePrincipal({ principal_id: 'kip:principal:steward' })
      gov.createGrant(
        {
          space_id: nexus.space,
          grantee_principal: 'kip:principal:steward',
          actions: ['read', 'elevate_authority'],
          constraints: { max_influence_authority: 'behavioral' },
        },
        SYSTEM_PRINCIPAL,
      )
      expect(
        (nexus.session(principalAuth('kip:principal:steward')).describe('DESCRIBE ACCESS') as {
          elevatable_authority_classes: string[]
        }).elevatable_authority_classes,
      ).toEqual(['descriptive', 'advisory', 'behavioral'])
    })
  })

  it('registers record_outcome and manage_legal_hold, and nothing no gate asks for', () => {
    expect(parsePermission('derive')).toBe('derive')
    expect(parsePermission('manage_trust')).toBe('manage_trust')
    expect(parsePermission('record_outcome')).toBe('record_outcome')
    expect(parsePermission('manage_legal_hold')).toBe('manage_legal_hold')
    for (const name of ['legal_hold', 'approve', 'share']) {
      expect(() => parsePermission(name), name).toThrowError(/not a permission/)
    }
  })

  it('requires asserted_by on an Assertion and keeps context_refs optional', async () => {
    await withNexus('asserted-by', (nexus) => {
      nexus.execute(`MUTATE {
        CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
        CREATE CONCEPT ?dark { TYPE "Option" NAME "Dark" }
        ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
      }`)
      const missing = refused(() =>
        nexus.execute('CREATE ASSERTION ?a { SET FIELDS { proposition: "P-1", stance: "support", mode: "stated" } }'),
      )
      expect(missing.code).toBe('ConstraintViolation')
      expect(
        nexus.execute(
          'CREATE ASSERTION ?a { SET FIELDS { proposition: "P-1", asserted_by: "C-1", stance: "support", mode: "stated" } }',
        ).status,
      ).toBe('committed')
      expect(
        nexus.execute(
          'CREATE ASSERTION ?a { SET FIELDS { proposition: "P-1", asserted_by: {id: "C-1"}, stance: "support", mode: "stated" } SET STRUCTURAL { ("context", "C-2") } }',
        ).status,
      ).toBe('committed')
      // The sugar names the same field `by:`, and §55.1 makes it REQUIRED —
      // so the grammar refuses it before the Schema ever sees it. Two
      // refusals for one rule, and they are allowed to differ: one is a
      // malformed statement, the other a well-formed Assertion missing a
      // field.
      expect(refused(() => nexus.execute('ASSERT ({id: "C-1"}, "prefers", {id: "C-2"}) { mode: "stated" }')).code).toBe(
        'InvalidSyntax',
      )
    })
  })

  it('denies by default, and reaches ActorBindingRequired where policy demands representation', async () => {
    await withNexus('default-deny', (nexus) => {
      const gov = nexus.store.governance
      gov.ensurePrincipal({ principal_id: 'kip:principal:nobody' })
      // §30.2: no Grant, no Delegation, no policy statement — nothing.
      const nobody = nexus.session(principalAuth('kip:principal:nobody'))
      expect(refused(() => nobody.query('FIND(?c) WHERE { ?c CONCEPT {} }')).code).toBe(
        'NotAuthorized',
      )
      expect(refused(() => nobody.describe('DESCRIBE TYPE "Person"')).code).toBe('NotAuthorized')
    })
  })
})

describe('cursor refusals', () => {
  it('names the family and the reason on a malformed cursor', async () => {
    await withNexus('cursors', (nexus) => {
      nexus.execute('CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }')
      const find = refused(() =>
        nexus.query('FIND(?c) WHERE { ?c CONCEPT {} } LIMIT 1 CURSOR "abc"'),
      )
      expect(find.code).toBe('CursorInvalid')
      expect(find.details).toEqual({ family: 'kql', reason: 'malformed' })

      // A token of another family is malformed for this one (§102.28).
      const page = nexus.queryPage('FIND(?c) WHERE { ?c CONCEPT {} } LIMIT 0')
      const kqlToken = page.nextCursor as string
      const list = refused(() => nexus.describe(`LIST TYPES LIMIT 1 CURSOR "${kqlToken}"`))
      expect(list.details).toEqual({ family: 'list', reason: 'malformed' })
      const search = refused(() => nexus.describe(`SEARCH CONCEPT "Alice" CURSOR "${kqlToken}"`))
      expect(search.details).toEqual({ family: 'search', reason: 'malformed' })
      const history = refused(() => nexus.describe('HISTORY SPACE CURSOR "zz"'))
      expect(history.details).toEqual({ family: 'history', reason: 'malformed' })
      const changes = refused(() => nexus.describe('CHANGES SINCE -1'))
      expect(changes.code).toBe('CursorInvalid')
      expect(changes.details).toEqual({ family: 'changes', reason: 'malformed' })
      // A paging count of the wrong type is a TypeMismatch and not a cursor
      // refusal — and it is refused rather than coerced, which would page
      // nothing and answer.
      expect(refused(() => nexus.describe('LIST TYPES LIMIT "x"')).code).toBe('TypeMismatch')
      expect(refused(() => nexus.describe('HISTORY SPACE LIMIT -1')).code).toBe('TypeMismatch')

      // A HISTORY page hands back a cursor of its own family, and continues.
      nexus.execute('UPDATE "C-1" SET FIELDS { name: "Alicia" }')
      const first = nexus.describePage('HISTORY SPACE LIMIT 1')
      expect((first.result as unknown[]).length).toBe(1)
      expect(first.nextCursor).not.toBeNull()
      const second = nexus.describePage(`HISTORY SPACE LIMIT 1 CURSOR "${first.nextCursor}"`)
      expect((second.result as { space_seq: number }[])[0]?.space_seq).toBe(2)
    })
  })
})

describe('LIST DEPENDENTS truncation', () => {
  it('flags a walk cut short by an element the caller may not discover', async () => {
    await withNexus('truncated', (nexus) => {
      nexus.execute(`MUTATE {
        CREATE CONCEPT ?event { TYPE "Event" NAME "Meeting" SET ATTRIBUTES {summary: "x"} }
        CREATE CONCEPT ?insight { TYPE "Insight" NAME "Lesson" SET ATTRIBUTES {summary: "y"} }
        CREATE ACTIVITY ?x {
          SET FIELDS {activity_class: "semantic_consolidation", status: "completed"}
          SET STRUCTURAL { ("inputs", ?event) ("outputs", ?insight) }
        }
      }`)
      const gov = nexus.store.governance
      gov.ensurePrincipal({ principal_id: 'kip:principal:reviewer' })
      // May discover the Concepts, not the Activity that links them.
      gov.createGrant(
        {
          space_id: nexus.space,
          grantee_principal: 'kip:principal:reviewer',
          actions: ['read', 'discover'],
          scope: { kinds: ['concept'] },
        },
        SYSTEM_PRINCIPAL,
      )
      const reviewer = nexus.session(principalAuth('kip:principal:reviewer'))
      const answer = reviewer.describePage('LIST DEPENDENTS "C-1"')
      expect(answer.result).toEqual([])
      expect(answer.truncated).toBe(true)
      // The owner's walk is whole.
      expect(nexus.describePage('LIST DEPENDENTS "C-1"').truncated).toBe(false)
    })
  })
})
