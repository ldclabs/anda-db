/**
 * The engine-local content digest.
 *
 * Three things need a stable digest and need it *synchronously*: Proposition
 * tuple identity, Schema Package content identity, and the stub a purged
 * element leaves behind. All three are computed inside `transactionSync`,
 * which cannot await, so `crypto.subtle.digest` is unavailable — it is async
 * by design.
 *
 * SHA-256 is therefore implemented here rather than imported. `node:crypto`'s
 * `createHash` would do the job on Workers, but only behind the
 * `nodejs_compat` compatibility flag, and requiring a deployment flag of every
 * consumer to hash a few hundred bytes is a poor trade. This is the standard
 * FIPS 180-4 algorithm with no tuning; `test/digest.test.ts` pins it to the
 * published vectors.
 *
 * This is deliberately **not** presented as the KIP canonicalization profile:
 * that profile is still a draft, and the Rust engine's
 * `store::schema::content_digest` makes the same reservation. Two engines are
 * not expected to produce equal digests for equal content today.
 */

/** SHA-256 round constants: the first 32 bits of the cube roots of the first 64 primes. */
// prettier-ignore
const K = new Uint32Array([
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
  0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
  0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
  0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
  0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
  0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
])

const HEX = '0123456789abcdef'

const rotr = (x: number, n: number): number => (x >>> n) | (x << (32 - n))

/** SHA-256 over raw bytes, as a lowercase hex string. */
export function sha256Hex(bytes: Uint8Array): string {
  const h = new Uint32Array([
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c,
    0x1f83d9ab, 0x5be0cd19,
  ])

  // Padding: the message, a 0x80 byte, zeroes, then the bit length as a
  // 64-bit big-endian integer, to the next 64-byte boundary.
  const length = bytes.length
  const padded = new Uint8Array(((length + 9 + 63) >> 6) << 6)
  padded.set(bytes)
  padded[length] = 0x80
  const bits = length * 8
  // A JS number holds bit lengths exactly up to 2^53, which is far past the
  // 2 MB ceiling on anything this engine hashes; the high word covers the
  // bits above 2^32 rather than being written as a constant zero.
  const view = new DataView(padded.buffer)
  view.setUint32(padded.length - 8, Math.floor(bits / 0x100000000))
  view.setUint32(padded.length - 4, bits >>> 0)

  const w = new Uint32Array(64)
  for (let offset = 0; offset < padded.length; offset += 64) {
    for (let i = 0; i < 16; i++) w[i] = view.getUint32(offset + i * 4)
    for (let i = 16; i < 64; i++) {
      const a = w[i - 15] as number
      const b = w[i - 2] as number
      const s0 = rotr(a, 7) ^ rotr(a, 18) ^ (a >>> 3)
      const s1 = rotr(b, 17) ^ rotr(b, 19) ^ (b >>> 10)
      w[i] =
        ((w[i - 16] as number) + s0 + (w[i - 7] as number) + s1) >>> 0
    }

    let a = h[0] as number
    let b = h[1] as number
    let c = h[2] as number
    let d = h[3] as number
    let e = h[4] as number
    let f = h[5] as number
    let g = h[6] as number
    let hh = h[7] as number
    for (let i = 0; i < 64; i++) {
      const s1 = rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25)
      const ch = (e & f) ^ (~e & g)
      const t1 = (hh + s1 + ch + (K[i] as number) + (w[i] as number)) >>> 0
      const s0 = rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22)
      const maj = (a & b) ^ (a & c) ^ (b & c)
      const t2 = (s0 + maj) >>> 0
      hh = g
      g = f
      f = e
      e = (d + t1) >>> 0
      d = c
      c = b
      b = a
      a = (t1 + t2) >>> 0
    }
    h[0] = ((h[0] as number) + a) >>> 0
    h[1] = ((h[1] as number) + b) >>> 0
    h[2] = ((h[2] as number) + c) >>> 0
    h[3] = ((h[3] as number) + d) >>> 0
    h[4] = ((h[4] as number) + e) >>> 0
    h[5] = ((h[5] as number) + f) >>> 0
    h[6] = ((h[6] as number) + g) >>> 0
    h[7] = ((h[7] as number) + hh) >>> 0
  }

  let out = ''
  for (const word of h) {
    for (let shift = 28; shift >= 0; shift -= 4) {
      out += HEX[(word >>> shift) & 0xf]
    }
  }
  return out
}

const encoder = new TextEncoder()

/** SHA-256 over UTF-8 text. */
export function sha256Text(text: string): string {
  return sha256Hex(encoder.encode(text))
}

/**
 * SHA-256 over a sequence of length-prefixed parts.
 *
 * Length-prefixing is what keeps `("ab", "c")` from digesting like
 * `("a", "bc")`. Concatenating the parts instead would let two different
 * tuples share a `tuple_key`, which is to say: silently become one
 * Proposition.
 */
export function digestParts(parts: readonly string[]): string {
  const encoded = parts.map((part) => encoder.encode(part))
  const total = encoded.reduce((sum, part) => sum + part.length + 8, 0)
  const buffer = new Uint8Array(total)
  const view = new DataView(buffer.buffer)
  let offset = 0
  for (const part of encoded) {
    view.setUint32(offset, Math.floor(part.length / 0x100000000))
    view.setUint32(offset + 4, part.length >>> 0)
    buffer.set(part, offset + 8)
    offset += part.length + 8
  }
  return sha256Hex(buffer)
}

// ---------------------------------------------------------------------------
// SHA3-256, for the one digest that crosses between engines
// ---------------------------------------------------------------------------

/**
 * SHA3-256 (FIPS 202), synchronous, for the Capsule content digest.
 *
 * A Capsule is the one artifact that leaves this engine and is checked by
 * another. §37.7 makes the canonical serialization the design target, and
 * canonical bytes are only half of it: two implementations that hash the same
 * bytes with different algorithms produce different digests for the same
 * cognition, and every `VERIFY CAPSULE` across that boundary then fails for a
 * reason neither side can see. `rs/anda_cognitive_nexus` uses SHA3-256, so
 * this does too — which is what makes a Capsule written here verifiable there.
 *
 * Written out rather than imported for the same reason SHA-256 above is: the
 * digest is taken inside `transactionSync`, `crypto.subtle` is async, and
 * `node:crypto` needs a deployment flag from every consumer.
 *
 * The engine-local digests — Proposition tuple identity, Schema Package
 * content, purge stubs — stay on SHA-256. They never cross an engine boundary,
 * and changing them would rewrite every stored `tuple_key`.
 */

/** Lane rotation offsets, by lane index `x + 5y`. */
// prettier-ignore
const KECCAK_ROTATION = [
  0, 1, 62, 28, 27, 36, 44, 6, 55, 20, 3, 10, 43, 25, 39,
  41, 45, 15, 21, 8, 18, 2, 61, 56, 14,
]

/**
 * The ρ/π lane permutation: `B[y][2x+3y] = rot(A[x][y])`.
 *
 * Derived rather than transcribed. A 25-entry table copied by hand is a table
 * with a typo in it, and a wrong Keccak permutation produces a plausible
 * digest that no other implementation agrees with.
 */
const KECCAK_PI = (() => {
  const out = new Int32Array(25)
  for (let y = 0; y < 5; y += 1) {
    for (let x = 0; x < 5; x += 1) {
      out[x + 5 * y] = y + 5 * ((2 * x + 3 * y) % 5)
    }
  }
  return out
})()

/** The 24 round constants, split into 32-bit halves. */
// prettier-ignore
const KECCAK_RC_LO = new Int32Array([
  0x00000001, 0x00008082, 0x0000808a, 0x80008000, 0x0000808b, 0x80000001,
  0x80008081, 0x00008009, 0x0000008a, 0x00000088, 0x80008009, 0x8000000a,
  0x8000808b, 0x0000008b, 0x00008089, 0x00008003, 0x00008002, 0x00000080,
  0x0000800a, 0x8000000a, 0x80008081, 0x00008080, 0x80000001, 0x80008008,
])
// prettier-ignore
const KECCAK_RC_HI = new Int32Array([
  0x00000000, 0x00000000, 0x80000000, 0x80000000, 0x00000000, 0x00000000,
  0x80000000, 0x80000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000,
  0x00000000, 0x80000000, 0x80000000, 0x80000000, 0x80000000, 0x80000000,
  0x00000000, 0x80000000, 0x80000000, 0x80000000, 0x00000000, 0x80000000,
])

/** SHA3-256's rate: 1600 bits of state minus 512 bits of capacity. */
const KECCAK_RATE = 136

/**
 * A 64-bit lane lives in two 32-bit halves, so a rotation is written twice.
 *
 * `x << 32` is `x << 0` in JavaScript — the shift count is taken modulo 32 —
 * so the 0 and 32 cases are branches rather than arithmetic. Getting that
 * wrong yields a hash that is self-consistent and wrong.
 */
function rotLo(lo: number, hi: number, n: number): number {
  if (n === 0) return lo
  if (n === 32) return hi
  if (n < 32) return ((lo << n) | (hi >>> (32 - n))) | 0
  const m = n - 32
  return ((hi << m) | (lo >>> (32 - m))) | 0
}

function rotHi(lo: number, hi: number, n: number): number {
  if (n === 0) return hi
  if (n === 32) return lo
  if (n < 32) return ((hi << n) | (lo >>> (32 - n))) | 0
  const m = n - 32
  return ((lo << m) | (hi >>> (32 - m))) | 0
}

/** The Keccak-f[1600] permutation, in place. */
function keccakF(s: Int32Array): void {
  const c = new Int32Array(10)
  const b = new Int32Array(50)

  for (let round = 0; round < 24; round += 1) {
    // θ: fold each column, then mix the two neighbouring columns back in.
    for (let x = 0; x < 5; x += 1) {
      let lo = 0
      let hi = 0
      for (let y = 0; y < 5; y += 1) {
        const i = 2 * (x + 5 * y)
        lo ^= s[i] as number
        hi ^= s[i + 1] as number
      }
      c[2 * x] = lo
      c[2 * x + 1] = hi
    }
    for (let x = 0; x < 5; x += 1) {
      const p = 2 * ((x + 4) % 5)
      const n = 2 * ((x + 1) % 5)
      const dLo = (c[p] as number) ^ rotLo(c[n] as number, c[n + 1] as number, 1)
      const dHi = (c[p + 1] as number) ^ rotHi(c[n] as number, c[n + 1] as number, 1)
      for (let y = 0; y < 5; y += 1) {
        const i = 2 * (x + 5 * y)
        s[i] = (s[i] as number) ^ dLo
        s[i + 1] = (s[i + 1] as number) ^ dHi
      }
    }

    // ρ and π: rotate each lane, then move it to its permuted position.
    for (let i = 0; i < 25; i += 1) {
      const j = KECCAK_PI[i] as number
      const n = KECCAK_ROTATION[i] as number
      b[2 * j] = rotLo(s[2 * i] as number, s[2 * i + 1] as number, n)
      b[2 * j + 1] = rotHi(s[2 * i] as number, s[2 * i + 1] as number, n)
    }

    // χ: the only non-linear step.
    for (let y = 0; y < 5; y += 1) {
      for (let x = 0; x < 5; x += 1) {
        const i = 2 * (x + 5 * y)
        const i1 = 2 * (((x + 1) % 5) + 5 * y)
        const i2 = 2 * (((x + 2) % 5) + 5 * y)
        s[i] = (b[i] as number) ^ (~(b[i1] as number) & (b[i2] as number))
        s[i + 1] = (b[i + 1] as number) ^ (~(b[i1 + 1] as number) & (b[i2 + 1] as number))
      }
    }

    // ι: break the round symmetry.
    s[0] = (s[0] as number) ^ (KECCAK_RC_LO[round] as number)
    s[1] = (s[1] as number) ^ (KECCAK_RC_HI[round] as number)
  }
}

/** XORs one rate-sized block into the state, little-endian per lane. */
function absorb(s: Int32Array, data: Uint8Array, offset: number): void {
  for (let i = 0; i < KECCAK_RATE; i += 1) {
    const half = 2 * (i >> 3) + (((i & 7) < 4) ? 0 : 1)
    s[half] = (s[half] as number) ^ ((data[offset + i] as number) << (8 * (i & 3)))
  }
}

/** SHA3-256 over raw bytes, as a lowercase hex string. */
export function sha3_256Hex(bytes: Uint8Array): string {
  const s = new Int32Array(50)
  let offset = 0
  while (offset + KECCAK_RATE <= bytes.length) {
    absorb(s, bytes, offset)
    keccakF(s)
    offset += KECCAK_RATE
  }

  // The final block carries the SHA-3 domain separator and the pad. When the
  // remainder is exactly one byte short of the rate, both land on the same
  // byte — which is why the second is an OR rather than an assignment.
  const tail = new Uint8Array(KECCAK_RATE)
  tail.set(bytes.subarray(offset), 0)
  tail[bytes.length - offset] = 0x06
  tail[KECCAK_RATE - 1] = (tail[KECCAK_RATE - 1] as number) | 0x80
  absorb(s, tail, 0)
  keccakF(s)

  let out = ''
  for (let i = 0; i < 32; i += 1) {
    const half = 2 * (i >> 3) + (((i & 7) < 4) ? 0 : 1)
    const byte = ((s[half] as number) >>> (8 * (i & 3))) & 0xff
    out += HEX[byte >>> 4]
    out += HEX[byte & 0xf]
  }
  return out
}

/** SHA3-256 over UTF-8 text. */
export function sha3_256Text(text: string): string {
  return sha3_256Hex(encoder.encode(text))
}
