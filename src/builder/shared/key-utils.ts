export function buildCompositeKey(row: any, fields: string[]): string | null {
  if (fields.length === 0) return null

  if (fields.length === 1) {
    const val = row[fields[0]]
    if (val == null) return null

    const t = typeof val
    if (t === 'string') return `s:${val}`
    if (t === 'number') return Number.isFinite(val) ? `n:${val}` : null
    if (t === 'boolean') return val ? 'b:1' : 'b:0'
    if (t === 'bigint') return `i:${val}`
    return `o:${val}`
  }

  const parts = new Array<string>(fields.length)

  for (let i = 0; i < fields.length; i++) {
    const val = row[fields[i]]
    if (val == null) return null

    const t = typeof val
    if (t === 'string') {
      parts[i] = `s:${val}`
    } else if (t === 'number') {
      if (!Number.isFinite(val)) return null
      parts[i] = `n:${val}`
    } else if (t === 'boolean') {
      parts[i] = val ? 'b:1' : 'b:0'
    } else if (t === 'bigint') {
      parts[i] = `i:${val}`
    } else {
      parts[i] = `o:${val}`
    }
  }

  return parts.join('\u001f')
}
