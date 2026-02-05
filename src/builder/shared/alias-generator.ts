import { AliasGenerator } from './types'
import { ALIAS_FORBIDDEN_KEYWORDS } from './constants'

function toSafeSqlIdentifier(input: string): string {
  const raw = String(input)
  const n = raw.length

  let out = ''
  for (let i = 0; i < n; i++) {
    const c = raw.charCodeAt(i)
    const isAZ = (c >= 65 && c <= 90) || (c >= 97 && c <= 122)
    const is09 = c >= 48 && c <= 57
    const isUnderscore = c === 95

    if (isAZ || is09 || isUnderscore) {
      out += raw[i]
    } else {
      out += '_'
    }
  }

  if (out.length === 0) out = '_t'

  const c0 = out.charCodeAt(0)
  const startsOk =
    (c0 >= 65 && c0 <= 90) || (c0 >= 97 && c0 <= 122) || c0 === 95
  if (!startsOk) out = `_${out}`

  const lowered = out.toLowerCase()
  return ALIAS_FORBIDDEN_KEYWORDS.has(lowered) ? `_${lowered}` : lowered
}

export function createAliasGenerator(
  maxAliases: number = 10000,
): AliasGenerator {
  let counter = 0
  const usedAliases = new Set<string>()

  return {
    next(baseName: string): string {
      if (usedAliases.size >= maxAliases) {
        throw new Error(
          `Alias generator exceeded maximum of ${maxAliases} aliases. ` +
            `This indicates a query complexity issue or potential infinite loop.`,
        )
      }

      const base = toSafeSqlIdentifier(baseName)

      const suffix = `_${counter}`
      const maxLen = 63
      const baseMax = Math.max(1, maxLen - suffix.length)
      const trimmedBase = base.length > baseMax ? base.slice(0, baseMax) : base

      const alias = `${trimmedBase}${suffix}`
      counter += 1

      if (usedAliases.has(alias)) {
        throw new Error(
          `CRITICAL: Duplicate alias '${alias}' at counter=${counter}. ` +
            `This indicates a bug in alias generation logic.`,
        )
      }
      usedAliases.add(alias)

      return alias
    },
  }
}
