import {
  extractParameterNumber,
  readDollarTag,
  DOLLAR,
  DOUBLE_QUOTE,
  SINGLE_QUOTE,
  DASH,
  SLASH,
  ASTERISK,
  NEWLINE,
} from './sql-placeholder'

type ScanMode =
  | 'normal'
  | 'single'
  | 'double'
  | 'lineComment'
  | 'blockComment'
  | 'dollar'

interface ScanState {
  mode: ScanMode
  dollarTag: string | null
}

export type PlaceholderHandler = (oldIndex: number) => string

export interface ScanOptions {
  pgAware: boolean
  strictPlaceholders?: boolean
}

const NORMAL_STATE: ScanState = Object.freeze({
  mode: 'normal' as const,
  dollarTag: null,
})

function processQuotedContent(
  sql: string,
  pos: number,
  len: number,
  quoteChar: number,
): { endPos: number; text: string } {
  let i = pos
  let text = ''
  while (i < len) {
    const ch = sql.charCodeAt(i)
    if (ch === quoteChar) {
      if (i + 1 < len && sql.charCodeAt(i + 1) === quoteChar) {
        text += sql[i] + sql[i + 1]
        i += 2
        continue
      }
      text += sql[i]
      i++
      return { endPos: i, text }
    }
    text += sql[i]
    i++
  }
  return { endPos: i, text }
}

function findLineCommentEnd(sql: string, pos: number, len: number): number {
  let i = pos
  while (i < len) {
    if (sql.charCodeAt(i) === NEWLINE) return i + 1
    i++
  }
  return len
}

function findBlockCommentEnd(sql: string, pos: number, len: number): number {
  let i = pos
  while (i < len) {
    if (
      sql.charCodeAt(i) === ASTERISK &&
      i + 1 < len &&
      sql.charCodeAt(i + 1) === SLASH
    ) {
      return i + 2
    }
    i++
  }
  return len
}

function processPlaceholder(
  sql: string,
  pos: number,
  handler: PlaceholderHandler,
  strict: boolean,
): { consumed: number; output: string } {
  const result = extractParameterNumber(sql, pos)

  if (!result) {
    return { consumed: 1, output: sql[pos] }
  }

  if (result.num < 1) {
    if (strict) {
      throw new Error(`Invalid param placeholder: $${result.num}`)
    }
    return { consumed: 1, output: sql[pos] }
  }

  return {
    consumed: result.nextPos - pos,
    output: handler(result.num),
  }
}

export function scanSqlPlaceholders(
  sql: string,
  handler: PlaceholderHandler,
  options: ScanOptions,
): string {
  const len = sql.length
  let i = 0
  let out = ''
  let state: ScanState = NORMAL_STATE
  const strict = options.strictPlaceholders !== false

  while (i < len) {
    const ch = sql.charCodeAt(i)

    if (state.mode === 'single' || state.mode === 'double') {
      const quoteChar = state.mode === 'single' ? SINGLE_QUOTE : DOUBLE_QUOTE
      const result = processQuotedContent(sql, i, len, quoteChar)
      out += result.text
      i = result.endPos
      state = NORMAL_STATE
      continue
    }

    if (options.pgAware && state.mode === 'lineComment') {
      const endPos = findLineCommentEnd(sql, i, len)
      out += sql.slice(i, endPos)
      i = endPos
      state = NORMAL_STATE
      continue
    }

    if (options.pgAware && state.mode === 'blockComment') {
      const endPos = findBlockCommentEnd(sql, i, len)
      out += sql.slice(i, endPos)
      i = endPos
      state = NORMAL_STATE
      continue
    }

    if (options.pgAware && state.mode === 'dollar' && state.dollarTag) {
      const tag = state.dollarTag
      if (sql.slice(i, i + tag.length) === tag) {
        out += tag
        i += tag.length
        state = NORMAL_STATE
      } else {
        out += sql[i]
        i++
      }
      continue
    }

    if (ch === SINGLE_QUOTE) {
      out += sql[i]
      i++
      state = { mode: 'single', dollarTag: null }
      continue
    }

    if (ch === DOUBLE_QUOTE) {
      out += sql[i]
      i++
      state = { mode: 'double', dollarTag: null }
      continue
    }

    if (options.pgAware) {
      if (ch === DASH && i + 1 < len && sql.charCodeAt(i + 1) === DASH) {
        out += sql[i] + sql[i + 1]
        i += 2
        state = { mode: 'lineComment', dollarTag: null }
        continue
      }

      if (ch === SLASH && i + 1 < len && sql.charCodeAt(i + 1) === ASTERISK) {
        out += sql[i] + sql[i + 1]
        i += 2
        state = { mode: 'blockComment', dollarTag: null }
        continue
      }
    }

    if (ch === DOLLAR) {
      if (options.pgAware) {
        const tag = readDollarTag(sql, i, len)
        if (tag) {
          out += tag
          i += tag.length
          state = { mode: 'dollar', dollarTag: tag }
          continue
        }
      }

      const placeholder = processPlaceholder(sql, i, handler, strict)
      out += placeholder.output
      i += placeholder.consumed
      continue
    }

    out += sql[i]
    i++
  }

  return out
}

export function reindexPlaceholders(
  sql: string,
  params: readonly unknown[],
  offset: number,
  pgAware: boolean = true,
): { sql: string; params: unknown[] } {
  if (!Number.isInteger(offset) || offset < 0) {
    throw new Error(`Invalid param offset: ${offset}`)
  }

  const newParams: unknown[] = []
  const paramMap = new Map<number, number>()

  const reindexed = scanSqlPlaceholders(
    sql,
    (oldIndex) => {
      const existing = paramMap.get(oldIndex)
      if (existing !== undefined) return `$${existing}`

      const pos = oldIndex - 1
      if (pos >= params.length) {
        throw new Error(
          `Param placeholder $${oldIndex} exceeds params length (${params.length})`,
        )
      }

      const newIndex = offset + newParams.length + 1
      paramMap.set(oldIndex, newIndex)
      newParams.push(params[pos])
      return `$${newIndex}`
    },
    { pgAware, strictPlaceholders: true },
  )

  return { sql: reindexed, params: newParams }
}

export function pgToSqlitePlaceholders(
  sql: string,
  params: readonly unknown[],
): { sql: string; params: unknown[] } {
  const reordered: unknown[] = []

  const converted = scanSqlPlaceholders(
    sql,
    (oldIndex) => {
      reordered.push(params[oldIndex - 1])
      return '?'
    },
    { pgAware: false, strictPlaceholders: false },
  )

  return { sql: converted, params: reordered }
}

export function containsPlaceholder(sql: string): boolean {
  let found = false
  scanSqlPlaceholders(
    sql,
    (oldIndex) => {
      found = true
      return `$${oldIndex}`
    },
    { pgAware: true, strictPlaceholders: true },
  )
  return found
}
