import { ParamStore } from './param-store'
import { SqlDialect } from '../../sql-builder-dialect'

export type ComparisonBuilder = (
  expr: string,
  op: string,
  val: unknown,
  params: ParamStore,
  dialect: SqlDialect,
) => string

export function buildComparisons(
  expr: string,
  filter: Record<string, unknown>,
  params: ParamStore,
  dialect: SqlDialect,
  builder: ComparisonBuilder,
  excludeKeys: Set<string> = new Set(['mode']),
): string[] {
  const out: string[] = []

  for (const [op, val] of Object.entries(filter)) {
    if (excludeKeys.has(op) || val === undefined) continue

    const built = builder(expr, op, val, params, dialect)
    if (built && built.trim().length > 0) {
      out.push(built)
    }
  }

  return out
}

export function joinComparisons(clauses: string[]): string {
  if (clauses.length === 0) return ''
  if (clauses.length === 1) return clauses[0]
  return clauses.join(' AND ')
}
