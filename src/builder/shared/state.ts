import { DEFAULT_WHERE_CLAUSE } from './constants'
import { WhereClauseResult } from './types'
import { ParamStore } from './param-store'

export function toPublicResult(
  clause: string,
  joins: readonly string[],
  params: ParamStore,
): WhereClauseResult {
  const snapshot = params.snapshot()

  return Object.freeze({
    clause: clause || DEFAULT_WHERE_CLAUSE,
    joins: Object.freeze([...joins]),
    params: snapshot.params,
    paramMappings: snapshot.mappings,
    nextParamIndex: snapshot.index,
  })
}
