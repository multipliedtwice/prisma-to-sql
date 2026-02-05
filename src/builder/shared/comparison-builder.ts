import { ParamStore } from './param-store'
import { SqlDialect } from '../../sql-builder-dialect'

export type ComparisonBuilder = (
  expr: string,
  op: string,
  val: unknown,
  params: ParamStore,
  dialect: SqlDialect,
) => string

const DEFAULT_EXCLUDE_KEYS: ReadonlySet<string> = new Set(['mode'])

export function buildComparisons(
  expr: string,
  filter: Record<string, unknown>,
  params: ParamStore,
  dialect: SqlDialect,
  builder: ComparisonBuilder,
  excludeKeys: ReadonlySet<string> = DEFAULT_EXCLUDE_KEYS,
): string[] {
  const out: string[] = []

  for (const op in filter) {
    if (!Object.prototype.hasOwnProperty.call(filter, op)) continue
    if (excludeKeys.has(op)) continue

    const val = filter[op]
    if (val === undefined) continue

    const built = builder(expr, op, val, params, dialect)
    if (built && built.trim().length > 0) out.push(built)
  }

  return out
}

export function joinComparisons(clauses: string[]): string {
  if (clauses.length === 0) return ''
  if (clauses.length === 1) return clauses[0]
  return clauses.join(' AND ')
}
