import type { SchemaPackage } from '../../src/schema/index.js'

/**
 * A preference option type for these tests. The Profile has no Preference type
 * (Profile §5.5): a real Space types each option by its kind, because
 * `prefers` partitions by the option's Concept Type. Open attributes, so tests
 * that annotate an option keep working. The Rust tests carry the same package.
 */
export const OPTIONS: SchemaPackage = {
  format: 'KIP-Schema-Package',
  manifest: { package_id: 'kip://test/options', version: '1.0.0' },
  definitions: {
    concept_types: {
      Option: {
        kind: 'ConceptType',
        description: 'A preference option used by these tests.',
        attributes: { open: true },
      },
    },
  },
} as unknown as SchemaPackage
