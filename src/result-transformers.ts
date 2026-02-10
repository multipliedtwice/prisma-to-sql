import { PrismaMethod } from './types'

export const RESULT_TRANSFORMERS: Partial<
  Record<PrismaMethod, (results: unknown[]) => unknown>
> = {
  findFirst: (results) => results[0] || null,
  findUnique: (results) => results[0] || null,
}

export function transformQueryResults(
  method: PrismaMethod,
  results: unknown[],
): unknown {
  const transformer = RESULT_TRANSFORMERS[method]
  return transformer ? transformer(results) : results
}
