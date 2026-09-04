/**
 * The journal key a client's idempotency key is stored and looked up under.
 *
 * §34.2 scopes a key to the Space, the authenticated Principal and the
 * operation class, so that unrelated callers cannot collide: two Principals
 * reusing the same string are two pieces of work, and answering the second
 * with the first's Receipt would hand one caller the other's commit. The Space
 * is the table's own column; the class and the Principal are folded into the
 * stored key, the same way the reference engine does it.
 */
export function scopedIdempotencyKey(principalId: string, key: string): string {
  return key === '' ? '' : `kml\u001f${principalId}\u001f${key}`
}
