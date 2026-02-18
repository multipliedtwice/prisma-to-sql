export const DIGIT_0 = 48
export const DIGIT_9 = 57
export const UPPER_A = 65
export const UPPER_Z = 90
export const LOWER_A = 97
export const LOWER_Z = 122
export const UNDERSCORE = 95
export const SINGLE_QUOTE = 39
export const DOUBLE_QUOTE = 34
export const DOLLAR = 36
export const DASH = 45
export const SLASH = 47
export const ASTERISK = 42
export const NEWLINE = 10

export function isDigit(charCode: number): boolean {
  return charCode >= DIGIT_0 && charCode <= DIGIT_9
}

export function isAlphaNumericOrUnderscore(charCode: number): boolean {
  return (
    isDigit(charCode) ||
    (charCode >= UPPER_A && charCode <= UPPER_Z) ||
    (charCode >= LOWER_A && charCode <= LOWER_Z) ||
    charCode === UNDERSCORE
  )
}

export function extractParameterNumber(
  sql: string,
  startPos: number,
): { num: number; nextPos: number } | null {
  const n = sql.length
  let j = startPos + 1
  if (j >= n) return null

  const c1 = sql.charCodeAt(j)
  if (!isDigit(c1)) return null

  let num = c1 - DIGIT_0
  j++

  while (j < n && isDigit(sql.charCodeAt(j))) {
    num = num * 10 + (sql.charCodeAt(j) - DIGIT_0)
    j++
  }

  return { num, nextPos: j }
}

export function readDollarTag(
  s: string,
  pos: number,
  n: number,
): string | null {
  if (s.charCodeAt(pos) !== DOLLAR) return null

  let j = pos + 1
  while (j < n && isAlphaNumericOrUnderscore(s.charCodeAt(j))) {
    j++
  }

  if (j < n && s.charCodeAt(j) === DOLLAR && j > pos) {
    return s.slice(pos, j + 1)
  }

  if (pos + 1 < n && s.charCodeAt(pos + 1) === DOLLAR) {
    return '$$'
  }

  return null
}

export function scanSingleQuote(sql: string, startPos: number): number {
  const n = sql.length
  let i = startPos + 1

  while (i < n) {
    if (sql.charCodeAt(i) === SINGLE_QUOTE) {
      if (i + 1 < n && sql.charCodeAt(i + 1) === SINGLE_QUOTE) {
        i += 2
        continue
      }
      return i + 1
    }
    i++
  }

  return n
}

export function scanDoubleQuote(sql: string, startPos: number): number {
  const n = sql.length
  let i = startPos + 1

  while (i < n) {
    if (sql.charCodeAt(i) === DOUBLE_QUOTE) {
      if (i + 1 < n && sql.charCodeAt(i + 1) === DOUBLE_QUOTE) {
        i += 2
        continue
      }
      return i + 1
    }
    i++
  }

  return n
}

export function scanDollarQuoted(
  sql: string,
  startPos: number,
  dollarTag: string,
): number {
  const n = sql.length
  const tagLen = dollarTag.length
  let i = startPos + tagLen

  while (i < n) {
    if (sql.slice(i, i + tagLen) === dollarTag) {
      return i + tagLen
    }
    i++
  }

  return n
}
