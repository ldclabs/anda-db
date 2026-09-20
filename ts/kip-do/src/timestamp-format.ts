/** Calendar round-trip also rejects Date.parse's rollover of invalid dates. */
export function isTimestamp(value: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(value) || value.length !== 24) return false
  const at = Date.parse(value)
  return Number.isFinite(at) && new Date(at).toISOString() === value
}
