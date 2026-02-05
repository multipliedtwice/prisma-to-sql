import { AliasGenerator } from './types'
import { ALIAS_FORBIDDEN_KEYWORDS } from './constants'

function toSafeSqlIdentifier(input: string): string {
  const raw = String(input)
  const cleaned = raw.replace(/\W/g, '_')
  const startsOk = /^[a-zA-Z_]/.test(cleaned)
  const base = startsOk ? cleaned : `_${cleaned}`
  const fallback = base.length > 0 ? base : '_t'
  const lowered = fallback.toLowerCase()
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
