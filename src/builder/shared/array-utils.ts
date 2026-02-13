export function deduplicatePreserveOrder<T>(items: readonly T[]): readonly T[] {
  if (items.length <= 1) return Object.freeze([...items])

  const seen = new Set<T>()
  const out: T[] = []

  for (const item of items) {
    if (!seen.has(item)) {
      seen.add(item)
      out.push(item)
    }
  }

  return out
}
