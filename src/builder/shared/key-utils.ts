export function buildKey(row: any, fields: string[]): unknown {
  if (fields.length === 0) return null

  if (fields.length === 1) {
    const val = row[fields[0]]
    return val == null ? null : val
  }

  let key = ''
  for (let i = 0; i < fields.length; i++) {
    const val = row[fields[i]]
    if (val == null) return null
    if (i > 0) key += '\u001f'
    key += typeof val === 'string' ? val : String(val)
  }
  return key
}
