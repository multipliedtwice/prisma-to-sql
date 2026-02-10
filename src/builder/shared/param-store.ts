// src/builder/shared/param-store.ts
import {
  extractDynamicName,
  isDynamicParameter,
  ParamMap,
} from '@dee-wan/schema-parser'
import { normalizeValue } from '../../utils/normalize-value'
import { SqlDialect } from '../../sql-builder-dialect'

export interface ParamStore {
  add(value: unknown, dynamicName?: string): string
  addAuto(value: unknown): string
  snapshot(): ParamSnapshot
  readonly index: number
  readonly dialect: SqlDialect
}

interface ParamSnapshot {
  readonly index: number
  readonly params: readonly unknown[]
  readonly mappings: readonly ParamMap[]
}

const MAX_PARAM_INDEX = Number.MAX_SAFE_INTEGER - 1000

function assertSameLength(
  params: readonly unknown[],
  mappings: readonly ParamMap[],
): void {
  if (params.length !== mappings.length) {
    throw new Error(
      `CRITICAL: State corruption - params=${params.length}, mappings=${mappings.length}`,
    )
  }
}

function assertValidNextIndex(index: number): void {
  if (!Number.isInteger(index) || index < 1) {
    throw new Error(`CRITICAL: Index must be integer >= 1, got ${index}`)
  }
}

function assertNextIndexMatches(
  mappingsLength: number,
  nextIndex: number,
): void {
  const expected = mappingsLength + 1
  if (nextIndex !== expected) {
    throw new Error(
      `CRITICAL: Next index mismatch - expected ${expected}, got ${nextIndex}`,
    )
  }
}

function assertSequentialIndex(actual: number, expected: number): void {
  if (actual !== expected) {
    throw new Error(
      `CRITICAL: Indices must be sequential from 1..N. Expected ${expected}, got ${actual}`,
    )
  }
}

function assertExactlyOneOfDynamicOrValue(m: ParamMap): void {
  const hasDynamic = typeof m.dynamicName === 'string'
  const hasStatic = m.value !== undefined

  if (hasDynamic === hasStatic) {
    throw new Error(
      `CRITICAL: ParamMap ${m.index} must have exactly one of dynamicName or value`,
    )
  }
}

function normalizeDynamicNameOrThrow(
  dynamicName: string,
  index: number,
): string {
  const dn = dynamicName.trim()
  if (dn.length === 0) {
    throw new Error(`CRITICAL: dynamicName cannot be empty (index=${index})`)
  }
  return dn
}

function validateMappings(mappings: readonly ParamMap[]): void {
  const seenDynamic = new Set<string>()
  for (let i = 0; i < mappings.length; i++) {
    const m = mappings[i]
    assertSequentialIndex(m.index, i + 1)
    assertExactlyOneOfDynamicOrValue(m)

    if (typeof m.dynamicName === 'string') {
      const dn = normalizeDynamicNameOrThrow(m.dynamicName, m.index)
      if (seenDynamic.has(dn)) {
        throw new Error(`CRITICAL: Duplicate dynamic param name: ${dn}`)
      }
      seenDynamic.add(dn)
    }
  }
}

function validateState(
  params: readonly unknown[],
  mappings: readonly ParamMap[],
  index: number,
): void {
  assertSameLength(params, mappings)
  assertValidNextIndex(index)
  if (mappings.length === 0) return
  validateMappings(mappings)
  assertNextIndexMatches(mappings.length, index)
}

function buildDynamicNameIndex(
  mappings: readonly ParamMap[],
): Map<string, number> {
  const dynamicNameToIndex = new Map<string, number>()
  for (const m of mappings) {
    if (typeof m.dynamicName === 'string') {
      dynamicNameToIndex.set(m.dynamicName.trim(), m.index)
    }
  }
  return dynamicNameToIndex
}

function assertCanAddParam(currentIndex: number): void {
  if (currentIndex > MAX_PARAM_INDEX) {
    throw new Error(
      `CRITICAL: Cannot add param - would overflow MAX_SAFE_INTEGER. Current index: ${currentIndex}`,
    )
  }
}

const POSTGRES_POSITION_CACHE = new Array(100)
for (let i = 0; i < 100; i++) {
  POSTGRES_POSITION_CACHE[i] = `$${i + 1}`
}

function formatPositionPostgres(position: number): string {
  if (position <= 100) return POSTGRES_POSITION_CACHE[position - 1]
  return `$${position}`
}

function formatPositionSqlite(_position: number): string {
  return '?'
}

function validateDynamicName(dynamicName: string): string {
  const dn = dynamicName.trim()
  if (dn.length === 0) {
    throw new Error('CRITICAL: dynamicName cannot be empty')
  }
  return dn
}

function createStoreInternal(
  startIndex: number,
  dialect: SqlDialect,
  initialParams: unknown[] = [],
  initialMappings: ParamMap[] = [],
): ParamStore {
  let index = startIndex
  const params: unknown[] =
    initialParams.length > 0 ? initialParams.slice() : []
  const mappings: ParamMap[] =
    initialMappings.length > 0 ? initialMappings.slice() : []

  const dynamicNameToIndex = buildDynamicNameIndex(mappings)

  let dirty = true
  let cachedSnapshot: ParamSnapshot | null = null

  const formatPosition =
    dialect === 'sqlite' ? formatPositionSqlite : formatPositionPostgres

  function addDynamic(dynamicName: string): string {
    const dn = validateDynamicName(dynamicName)
    const existing = dynamicNameToIndex.get(dn)
    if (existing !== undefined) return formatPosition(existing)

    const position = index
    dynamicNameToIndex.set(dn, position)
    params.push(undefined)
    mappings.push({ index: position, dynamicName: dn })
    index++
    dirty = true
    return formatPosition(position)
  }

  function addStatic(value: unknown): string {
    const position = index
    const normalizedValue = normalizeValue(value)
    params.push(normalizedValue)
    mappings.push({ index: position, value: normalizedValue })
    index++
    dirty = true
    return formatPosition(position)
  }

  function add(value: unknown, dynamicName?: string): string {
    assertCanAddParam(index)
    return dynamicName === undefined
      ? addStatic(value)
      : addDynamic(dynamicName)
  }

  function addAuto(value: unknown): string {
    if (isDynamicParameter(value)) {
      const dynamicName = extractDynamicName(value as string)
      return add(undefined, dynamicName)
    }
    return add(value)
  }

  function snapshot(): ParamSnapshot {
    if (!dirty && cachedSnapshot) return cachedSnapshot

    const snap: ParamSnapshot = {
      index,
      params: params.slice(),
      mappings: mappings.slice(),
    }

    cachedSnapshot = snap
    dirty = false
    return snap
  }

  return {
    add,
    addAuto,
    snapshot,
    get index() {
      return index
    },
    get dialect() {
      return dialect
    },
  }
}

export function createParamStore(
  startIndex = 1,
  dialect: SqlDialect = 'postgres',
): ParamStore {
  if (!Number.isInteger(startIndex) || startIndex < 1) {
    throw new Error(`Start index must be integer >= 1, got ${startIndex}`)
  }

  if (startIndex > MAX_PARAM_INDEX) {
    throw new Error(
      `Start index too high (${startIndex}), risk of overflow at MAX_SAFE_INTEGER`,
    )
  }

  return createStoreInternal(startIndex, dialect)
}

export function createParamStoreFrom(
  existingParams: readonly unknown[],
  existingMappings: readonly ParamMap[],
  nextIndex: number,
  dialect: SqlDialect = 'postgres',
): ParamStore {
  validateState(existingParams, existingMappings, nextIndex)
  return createStoreInternal(
    nextIndex,
    dialect,
    existingParams.slice(),
    existingMappings.slice(),
  )
}
