import {
  extractDynamicName,
  isDynamicParameter,
  ParamMap,
} from '@dee-wan/schema-parser'
import { normalizeValue } from '../../utils/normalize-value'

export interface ParamStore {
  add(value: unknown, dynamicName?: string): string
  addAuto(value: unknown): string
  snapshot(): ParamSnapshot
  readonly index: number
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

function assertUniqueDynamicName(dn: string, seen: Set<string>): void {
  if (seen.has(dn)) {
    throw new Error(`CRITICAL: Duplicate dynamic param name in mappings: ${dn}`)
  }
  seen.add(dn)
}

function validateMappingEntry(
  m: ParamMap,
  expectedIndex: number,
  seenDynamic: Set<string>,
): void {
  assertSequentialIndex(m.index, expectedIndex)
  assertExactlyOneOfDynamicOrValue(m)

  if (typeof m.dynamicName === 'string') {
    const dn = normalizeDynamicNameOrThrow(m.dynamicName, m.index)
    assertUniqueDynamicName(dn, seenDynamic)
  }
}

function validateMappings(mappings: readonly ParamMap[]): void {
  const seenDynamic = new Set<string>()
  for (let i = 0; i < mappings.length; i++) {
    validateMappingEntry(mappings[i], i + 1, seenDynamic)
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

function createStoreInternal(
  startIndex: number,
  initialParams: unknown[] = [],
  initialMappings: ParamMap[] = [],
): ParamStore {
  let index = startIndex
  const params: unknown[] = initialParams.length > 0 ? [...initialParams] : []
  const mappings: ParamMap[] =
    initialMappings.length > 0 ? [...initialMappings] : []

  const dynamicNameToIndex = new Map<string, number>()
  for (const m of mappings) {
    if (typeof m.dynamicName === 'string') {
      dynamicNameToIndex.set(m.dynamicName.trim(), m.index)
    }
  }

  let dirty = true
  let cachedSnapshot: ParamSnapshot | null = null

  function assertCanAdd(): void {
    if (index > MAX_PARAM_INDEX) {
      throw new Error(
        `CRITICAL: Cannot add param - would overflow MAX_SAFE_INTEGER. Current index: ${index}`,
      )
    }
  }

  function format(position: number): string {
    return `$${position}`
  }

  function normalizeDynamicName(dynamicName: string): string {
    const dn = dynamicName.trim()
    if (dn.length === 0) {
      throw new Error('CRITICAL: dynamicName cannot be empty')
    }
    return dn
  }

  function addDynamic(dynamicName: string): string {
    const dn = normalizeDynamicName(dynamicName)
    const existing = dynamicNameToIndex.get(dn)
    if (existing !== undefined) return format(existing)

    const position = index
    dynamicNameToIndex.set(dn, position)
    params.push(undefined)
    mappings.push({ index: position, dynamicName: dn })
    index++
    dirty = true
    return format(position)
  }

  function addStatic(value: unknown): string {
    const position = index
    const normalizedValue = normalizeValue(value)
    params.push(normalizedValue)
    mappings.push({ index: position, value: normalizedValue })
    index++
    dirty = true
    return format(position)
  }

  function add(value: unknown, dynamicName?: string): string {
    assertCanAdd()
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

    const snap: ParamSnapshot = Object.freeze({
      index,
      params: Object.freeze(params.slice()),
      mappings: Object.freeze(mappings.slice()),
    })

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
  }
}

export function createParamStore(startIndex = 1): ParamStore {
  if (!Number.isInteger(startIndex) || startIndex < 1) {
    throw new Error(`Start index must be integer >= 1, got ${startIndex}`)
  }

  if (startIndex > MAX_PARAM_INDEX) {
    throw new Error(
      `Start index too high (${startIndex}), risk of overflow at MAX_SAFE_INTEGER`,
    )
  }

  return createStoreInternal(startIndex)
}

export function createParamStoreFrom(
  existingParams: readonly unknown[],
  existingMappings: readonly ParamMap[],
  nextIndex: number,
): ParamStore {
  validateState([...existingParams], [...existingMappings], nextIndex)
  return createStoreInternal(
    nextIndex,
    [...existingParams],
    [...existingMappings],
  )
}
