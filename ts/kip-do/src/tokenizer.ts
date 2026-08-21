/**
 * Segmentation for full-text search.
 *
 * Durable Object SQLite ships FTS5 with only the built-in tokenizers (`ascii`,
 * `unicode61`, `porter`, `trigram`) and a Worker cannot load a C extension.
 * `unicode61` finds word boundaries from Unicode categories, which works for
 * scripts that write spaces and fails completely for the ones that do not: a
 * whole Han run collapses into one token, so `深色模式` indexes as a single
 * term and no realistic query ever matches it.
 *
 * So the boundaries are inserted before the text reaches SQLite. That is this
 * module's whole job — **not** to tokenize, but to put spaces where
 * `unicode61` cannot see a break. FTS5 still does the final tokenization on
 * both paths, which is what keeps the index and the query in step through the
 * cases this module does not touch (case folding, apostrophes, hyphens).
 *
 * KIP 1.x delegated this to `cf-tokenizer`, an external jieba-rs service that
 * was the sole segmentation authority for both the write and the read path.
 * That cannot survive into 2.0: the engine commits inside
 * `ctx.storage.transactionSync`, and an HTTP call is not something a
 * synchronous transaction can make. The alternatives were to make every write
 * async — losing the all-or-none commit the platform hands us — or to index out
 * of band and then report a freshness the index does not have, which §66.5 and
 * §79 both forbid. `Intl.Segmenter` dissolves the problem: ICU's dictionary
 * breaking, in process, synchronous.
 *
 * The cost is that ICU's dictionary is not jieba's, so this engine and the Rust
 * one segment the same Chinese sentence slightly differently and rank the same
 * corpus slightly differently. That is a *recall* difference between two
 * engines, not an asymmetry inside either one — and an asymmetry inside one is
 * the failure that actually loses data, because a document indexed under
 * boundaries the query path does not reproduce is unreachable forever.
 *
 * ICU's dictionary can change when the runtime upgrades. {@link segmenterMark}
 * makes that *detectable*: it is stored beside the index and a mismatch
 * triggers a rebuild, because tokens from two vocabularies are not comparable.
 */

/**
 * The locale the segmenter is pinned to.
 *
 * ICU selects dictionary breaking by *script*, not by locale — `zh`, `ja`, `en`
 * and the host default all segment `我喜欢深色模式` identically today, verified
 * in workerd. Pinning one anyway costs nothing and means a future ICU that does
 * read the locale cannot make the index depend on where the object happens to
 * run.
 */
const LOCALE = 'zh'

/** The most tokens taken from one document. Bounds pathological input. */
const MAX_TOKENS = 4096

/** The most tokens taken from one query term. */
export const MAX_QUERY_TOKENS = 64

const SEGMENTER = new Intl.Segmenter(LOCALE, { granularity: 'word' })

/**
 * Splits text into the terms an FTS5 column should see, in order.
 *
 * Order and repetition are preserved: BM25 scores on term frequency, and a
 * deduplicating segmenter would tell the ranker that a name mentioned nine
 * times was mentioned once.
 */
export function segment(text: string, maxTokens = MAX_TOKENS): string[] {
  if (text === '') return []
  const out: string[] = []
  // NFKC first: a full-width `Ａ` and an `A` are the same term to a reader, and
  // normalizing on both paths is what makes them the same term here.
  for (const part of SEGMENTER.segment(text.normalize('NFKC'))) {
    if (!part.isWordLike) continue
    out.push(part.segment.toLowerCase())
    if (out.length >= maxTokens) break
  }
  return out
}

/**
 * The segmented form of one document, ready to store in an FTS5 column.
 *
 * Joined with spaces because a space is the one separator every built-in FTS5
 * tokenizer agrees on.
 */
export function segmentToText(text: string): string {
  return segment(text).join(' ')
}

/**
 * A fingerprint of what this runtime's ICU does to a fixed probe.
 *
 * Stored beside the index. When it changes, the vocabulary that produced every
 * indexed row is gone and the rows are stale — not wrong in a way anything
 * would notice, which is exactly why it has to be checked rather than assumed.
 * The probe deliberately spans Han, Kana and Latin, the three cases whose
 * boundaries this module exists to fix or to leave alone.
 */
export function segmenterMark(): string {
  return `${LOCALE}:${segment('深色模式東京都に住むAlice-Aurora').join('|')}`
}

/**
 * Collects the searchable text of a JSON value.
 *
 * Mirrors `extract_json_text` in `anda_db`: the corpus is built from every
 * string *and* every object key reachable inside `attributes`, not just
 * top-level values. Reproducing this shape matters — an index built over a
 * different corpus ranks differently even with identical tokenization.
 *
 * The caps mirror the Rust ones: they bound the work a single pathological
 * document can cause on a request path that also holds the Durable Object's
 * single thread.
 */
export function extractJsonText(
  value: unknown,
  out: string[] = [],
  depth = 0,
): string[] {
  const MAX_DEPTH = 8
  const MAX_FRAGMENTS = 512

  if (depth > MAX_DEPTH || out.length >= MAX_FRAGMENTS) return out

  if (typeof value === 'string') {
    out.push(value)
  } else if (typeof value === 'number' || typeof value === 'boolean') {
    out.push(String(value))
  } else if (Array.isArray(value)) {
    for (const item of value) {
      if (out.length >= MAX_FRAGMENTS) break
      extractJsonText(item, out, depth + 1)
    }
  } else if (value && typeof value === 'object') {
    for (const [key, item] of Object.entries(value)) {
      if (out.length >= MAX_FRAGMENTS) break
      // Reserved `_`-prefixed metadata is engine bookkeeping (`_version`,
      // `_updated_at`); indexing it would let a query match on timestamps.
      if (key.startsWith('_')) continue
      out.push(key)
      extractJsonText(item, out, depth + 1)
    }
  }
  return out
}
