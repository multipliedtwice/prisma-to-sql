const MAX_PATH_SEGMENTS = 100

function looksTooComplexForFilter(sql: string): boolean {
  const s = sql.toLowerCase()
  const complexKeywords = [
    ' group by ',
    ' having ',
    ' union ',
    ' intersect ',
    ' except ',
    ' window ',
    ' distinct ',
  ]
  return complexKeywords.some((keyword) => s.includes(keyword))
}

function skipWhitespace(s: string, i: number): number {
  while (i < s.length && /\s/.test(s[i])) {
    i++
  }
  return i
}

function matchKeyword(s: string, i: number, keyword: string): number {
  const lower = s.slice(i).toLowerCase()
  if (!lower.startsWith(keyword)) return -1
  const endPos = i + keyword.length
  if (endPos < s.length && /[a-z0-9_]/i.test(s[endPos])) return -1
  return endPos
}

function parseQuotedIdentifier(
  s: string,
  i: number,
): { value: string; endPos: number } | null {
  let j = i + 1
  while (j < s.length) {
    if (s[j] === '"') {
      if (j + 1 < s.length && s[j + 1] === '"') {
        j += 2
        continue
      }
      return { value: s.slice(i, j + 1), endPos: j + 1 }
    }
    j++
  }
  return null
}

function parseUnquotedIdentifier(
  s: string,
  i: number,
): { value: string; endPos: number } | null {
  if (!/[a-z_]/i.test(s[i])) return null

  let j = i
  while (j < s.length && /[a-z0-9_.]/i.test(s[j])) {
    j++
  }
  return { value: s.slice(i, j), endPos: j }
}

function parseIdentifier(
  s: string,
  i: number,
): { value: string; endPos: number } | null {
  if (i >= s.length) return null

  if (s[i] === '"') {
    return parseQuotedIdentifier(s, i)
  }

  return parseUnquotedIdentifier(s, i)
}

function findFromClauseEnd(s: string, fromStart: number): number {
  const lower = s.slice(fromStart).toLowerCase()
  const whereIdx = lower.indexOf(' where ')
  return whereIdx === -1 ? s.length : fromStart + whereIdx
}

export interface ParsedCountSql {
  fromSql: string
  whereSql: string | null
}

export function parseSimpleCountSql(sql: string): ParsedCountSql | null {
  const trimmed = sql.trim().replace(/;$/, '').trim()
  const lower = trimmed.toLowerCase()

  if (!lower.startsWith('select')) return null
  if (!lower.includes('count(*)')) return null
  if (looksTooComplexForFilter(trimmed)) return null

  let pos = matchKeyword(trimmed, 0, 'select')
  if (pos === -1) return null

  pos = skipWhitespace(trimmed, pos)

  const countMatch = trimmed.slice(pos).match(/^count\(\*\)/i)
  if (!countMatch) return null
  pos += countMatch[0].length

  pos = skipWhitespace(trimmed, pos)

  const castMatch = trimmed.slice(pos).match(/^::\s*\w+/)
  if (castMatch) {
    pos += castMatch[0].length
    pos = skipWhitespace(trimmed, pos)
  }

  const asPos = matchKeyword(trimmed, pos, 'as')
  if (asPos === -1) return null
  pos = skipWhitespace(trimmed, asPos)

  const ident = parseIdentifier(trimmed, pos)
  if (!ident) return null
  pos = skipWhitespace(trimmed, ident.endPos)

  const fromPos = matchKeyword(trimmed, pos, 'from')
  if (fromPos === -1) return null
  pos = skipWhitespace(trimmed, fromPos)

  const fromEnd = findFromClauseEnd(trimmed, pos)
  const fromSql = trimmed.slice(pos, fromEnd).trim()
  if (!fromSql) return null

  let whereSql: string | null = null
  if (fromEnd < trimmed.length) {
    const adjustedPos = skipWhitespace(trimmed, fromEnd)
    const wherePos = matchKeyword(trimmed, adjustedPos, 'where')
    if (wherePos !== -1) {
      whereSql = trimmed.slice(skipWhitespace(trimmed, wherePos)).trim()
    }
  }

  return { fromSql, whereSql }
}
